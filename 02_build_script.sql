    --------------------------------------------------------------------------------
    -- WAREHOUSE OPTIMIZER SERVICE - Build Script
    -- 
    -- Deploys the complete Daily Warehouse Right-Sizing Report service.
    -- Run as ACCOUNTADMIN. Idempotent — safe to re-run.
    --
    -- Prerequisites:
    --   - ACCOUNTADMIN role (or equivalent privileges)
    --   - EMAIL_INTEGRATION notification integration (OUTBOUND, type EMAIL)
    --
    -- Objects created:
    --   Database:   WAREHOUSE_OPTIMIZER_SERVICE
    --   Schema:     WAREHOUSE_OPTIMIZER_SERVICE.SERVICE
    --   Warehouse:  WAREHOUSE_OPTIMIZER_WH (XSmall, auto_suspend=60)
    --   Tables:     SERVICE.RECOMMENDATION_LOG, SERVICE.ALERT_CONFIG
    --   Procedures: SERVICE.GENERATE_RIGHTSIZING_REPORT()
    --               SERVICE.SNOOZE_WAREHOUSE(VARCHAR, NUMBER)
    --               SERVICE.ACKNOWLEDGE_WAREHOUSE(VARCHAR)
    --               SERVICE.MARK_APPLIED(VARCHAR)
    --   Task:       SERVICE.DAILY_RIGHTSIZING_TASK (daily 9am UTC)
    --------------------------------------------------------------------------------

    -- =============================================================================
    -- 1. INFRASTRUCTURE
    -- =============================================================================

    CREATE DATABASE IF NOT EXISTS WAREHOUSE_OPTIMIZER_SERVICE;
    CREATE SCHEMA IF NOT EXISTS WAREHOUSE_OPTIMIZER_SERVICE.SERVICE;

    CREATE WAREHOUSE IF NOT EXISTS WAREHOUSE_OPTIMIZER_WH
        WITH WAREHOUSE_SIZE = 'XSMALL'
        AUTO_SUSPEND = 60
        AUTO_RESUME = TRUE
        INITIALLY_SUSPENDED = TRUE;

    -- =============================================================================
    -- 2. TABLES
    -- =============================================================================

    CREATE TABLE IF NOT EXISTS WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG (
        REC_ID          NUMBER AUTOINCREMENT PRIMARY KEY,
        RUN_DATE        DATE NOT NULL DEFAULT CURRENT_DATE(),
        WAREHOUSE_NAME  VARCHAR NOT NULL,
        CURRENT_SIZE    VARCHAR,
        CLASSIFICATION  VARCHAR NOT NULL,
        RECOMMENDED_ACTION VARCHAR,
        JUSTIFICATION   VARIANT,
        ESTIMATED_CREDIT_IMPACT FLOAT,
        GENERATED_SQL   VARCHAR,
        STATUS          VARCHAR DEFAULT 'NEW',
        SNOOZED_UNTIL   DATE,
        WEEK_CREDITS    FLOAT,
        PREV_4WK_AVG_CREDITS FLOAT
    );

    CREATE TABLE IF NOT EXISTS WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.ALERT_CONFIG (
        EMAIL_RECIPIENTS        VARCHAR NOT NULL,
        MIN_WEEKLY_CREDITS      FLOAT DEFAULT 5.0,
        OVERSIZED_PCT_THRESHOLD FLOAT DEFAULT 75,
        UNDERSIZED_PCT_THRESHOLD FLOAT DEFAULT 5,
        LOOKBACK_DAYS           NUMBER DEFAULT 7,
        TREND_WEEKS             NUMBER DEFAULT 4,
        MIN_BYTES_SCANNED_FILTER NUMBER DEFAULT 1048576
    );

    INSERT INTO WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.ALERT_CONFIG (EMAIL_RECIPIENTS)
        SELECT 'adam.nathan@snowflake.com'
        WHERE NOT EXISTS (SELECT 1 FROM WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.ALERT_CONFIG);

    -- =============================================================================
    -- 3. CORE PROCEDURE
    -- =============================================================================

    CREATE OR REPLACE PROCEDURE WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.GENERATE_RIGHTSIZING_REPORT()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    $$
    DECLARE
        v_email VARCHAR;
        v_min_credits FLOAT;
        v_oversized_thresh FLOAT;
        v_undersized_thresh FLOAT;
        v_lookback NUMBER;
        v_trend_weeks NUMBER;
        v_min_bytes NUMBER;
        v_flagged_count NUMBER DEFAULT 0;
        v_total_savings FLOAT DEFAULT 0;
        v_html VARCHAR DEFAULT '';
        v_subject VARCHAR;
        v_run_date DATE DEFAULT CURRENT_DATE();
    BEGIN

        -- Step 1: Read config
        SELECT EMAIL_RECIPIENTS, MIN_WEEKLY_CREDITS, OVERSIZED_PCT_THRESHOLD,
               UNDERSIZED_PCT_THRESHOLD, LOOKBACK_DAYS, TREND_WEEKS, MIN_BYTES_SCANNED_FILTER
        INTO :v_email, :v_min_credits, :v_oversized_thresh, :v_undersized_thresh,
             :v_lookback, :v_trend_weeks, :v_min_bytes
        FROM WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.ALERT_CONFIG
        LIMIT 1;

        -- Step 2: Build analysis in a temp table
        CREATE OR REPLACE TEMPORARY TABLE WAREHOUSE_OPTIMIZER_SERVICE.SERVICE._WH_ANALYSIS AS
        WITH query_metrics AS (
            SELECT
                WAREHOUSE_NAME,
                UPPER(WAREHOUSE_SIZE) AS query_size,
                COUNT(*) AS total_queries,
                ROUND(MEDIAN(EXECUTION_TIME) / 1000.0, 2) AS p50_exec_sec,
                ROUND(APPROX_PERCENTILE(EXECUTION_TIME, 0.95) / 1000.0, 2) AS p95_exec_sec,
                ROUND(SUM(CASE WHEN EXECUTION_TIME < 1000 AND BYTES_SCANNED > 1048576
                                AND UPPER(WAREHOUSE_SIZE) != 'X-SMALL' THEN 1 ELSE 0 END)
                      * 100.0 / NULLIF(COUNT(*), 0), 1) AS oversized_pct,
                ROUND(SUM(CASE WHEN BYTES_SPILLED_TO_REMOTE_STORAGE > 0
                                OR QUEUED_OVERLOAD_TIME > 5000 THEN 1 ELSE 0 END)
                      * 100.0 / NULLIF(COUNT(*), 0), 1) AS undersized_pct,
                SUM(BYTES_SPILLED_TO_REMOTE_STORAGE) AS total_remote_spill,
                SUM(BYTES_SPILLED_TO_LOCAL_STORAGE) AS total_local_spill,
                SUM(CASE WHEN QUEUED_OVERLOAD_TIME > 5000 THEN 1 ELSE 0 END) AS queued_count,
                ROUND(APPROX_PERCENTILE(QUEUED_OVERLOAD_TIME, 0.95) / 1000.0, 2) AS p95_queue_sec
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
              AND WAREHOUSE_NAME IS NOT NULL
              AND WAREHOUSE_SIZE IS NOT NULL
              AND EXECUTION_STATUS = 'SUCCESS'
            GROUP BY WAREHOUSE_NAME, UPPER(WAREHOUSE_SIZE)
        ),
        current_week_credits AS (
            SELECT WAREHOUSE_NAME,
                   ROUND(SUM(CREDITS_USED), 2) AS week_credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
        ),
        prior_weeks_credits AS (
            SELECT WAREHOUSE_NAME,
                   ROUND(SUM(CREDITS_USED) / 4.0, 2) AS avg_weekly_credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('DAY', -35, CURRENT_TIMESTAMP())
              AND START_TIME < DATEADD('DAY', -7, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
        ),
        workload_segments AS (
            SELECT
                WAREHOUSE_NAME,
                COUNT(DISTINCT ROLE_NAME) AS distinct_roles,
                MAX(med_exec) / NULLIF(MIN(med_exec), 0) AS exec_time_spread
            FROM (
                SELECT WAREHOUSE_NAME, ROLE_NAME,
                       MEDIAN(EXECUTION_TIME) AS med_exec
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE START_TIME >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
                  AND WAREHOUSE_NAME IS NOT NULL
                  AND EXECUTION_STATUS = 'SUCCESS'
                GROUP BY WAREHOUSE_NAME, ROLE_NAME
                HAVING COUNT(*) >= 10
            )
            GROUP BY WAREHOUSE_NAME
        ),
        snoozed AS (
            SELECT WAREHOUSE_NAME
            FROM WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
            WHERE STATUS = 'SNOOZED' AND SNOOZED_UNTIL > CURRENT_DATE()
            GROUP BY WAREHOUSE_NAME
        )
        SELECT
            qm.WAREHOUSE_NAME,
            COALESCE(qm.query_size, 'UNKNOWN') AS current_size,
            qm.total_queries,
            qm.p50_exec_sec,
            qm.p95_exec_sec,
            qm.oversized_pct,
            qm.undersized_pct,
            qm.total_remote_spill,
            qm.total_local_spill,
            qm.queued_count,
            qm.p95_queue_sec,
            COALESCE(cwc.week_credits, 0) AS week_credits,
            COALESCE(pwc.avg_weekly_credits, 0) AS prev_4wk_avg_credits,
            CASE WHEN pwc.avg_weekly_credits > 0
                 THEN ROUND((COALESCE(cwc.week_credits,0) - pwc.avg_weekly_credits) / pwc.avg_weekly_credits * 100, 1)
                 ELSE NULL END AS trend_pct,
            COALESCE(ws.distinct_roles, 0) AS distinct_roles,
            COALESCE(ws.exec_time_spread, 0) AS exec_time_spread,
            CASE WHEN sn.WAREHOUSE_NAME IS NOT NULL THEN TRUE ELSE FALSE END AS is_snoozed,
            CASE
                WHEN sn.WAREHOUSE_NAME IS NOT NULL THEN 'SNOOZED'
                WHEN COALESCE(cwc.week_credits, 0) < 5.0 THEN 'SKIP_LOW_CREDITS'
                WHEN ws.exec_time_spread >= 10 AND ws.distinct_roles >= 2 THEN 'SPLIT_CANDIDATE'
                WHEN qm.oversized_pct > 75 AND qm.query_size != 'X-SMALL' THEN 'OVERSIZED'
                WHEN qm.undersized_pct > 5 THEN 'UNDERSIZED'
                ELSE 'RIGHT_SIZED'
            END AS classification
        FROM query_metrics qm
        LEFT JOIN current_week_credits cwc ON qm.WAREHOUSE_NAME = cwc.WAREHOUSE_NAME
        LEFT JOIN prior_weeks_credits pwc ON qm.WAREHOUSE_NAME = pwc.WAREHOUSE_NAME
        LEFT JOIN workload_segments ws ON qm.WAREHOUSE_NAME = ws.WAREHOUSE_NAME
        LEFT JOIN snoozed sn ON qm.WAREHOUSE_NAME = sn.WAREHOUSE_NAME;

        -- Step 3: Clear any prior NEW rows for today (idempotent on re-run)
        DELETE FROM WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
        WHERE RUN_DATE = CURRENT_DATE() AND STATUS = 'NEW';

        -- Step 3b: Insert flagged warehouses into recommendation log
        INSERT INTO WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
            (RUN_DATE, WAREHOUSE_NAME, CURRENT_SIZE, CLASSIFICATION, RECOMMENDED_ACTION,
             JUSTIFICATION, ESTIMATED_CREDIT_IMPACT, GENERATED_SQL, STATUS, WEEK_CREDITS, PREV_4WK_AVG_CREDITS)
        SELECT
            CURRENT_DATE(),
            a.WAREHOUSE_NAME,
            a.current_size,
            a.classification,
            CASE
                WHEN a.classification = 'OVERSIZED' AND a.current_size = 'SMALL' THEN 'SIZE DOWN TO X-SMALL'
                WHEN a.classification = 'OVERSIZED' AND a.current_size = 'MEDIUM' THEN 'SIZE DOWN TO SMALL'
                WHEN a.classification = 'OVERSIZED' AND a.current_size = 'LARGE' THEN 'SIZE DOWN TO MEDIUM'
                WHEN a.classification = 'OVERSIZED' AND a.current_size = 'X-LARGE' THEN 'SIZE DOWN TO LARGE'
                WHEN a.classification = 'OVERSIZED' AND a.current_size = '2X-LARGE' THEN 'SIZE DOWN TO X-LARGE'
                WHEN a.classification = 'OVERSIZED' AND a.current_size = '3X-LARGE' THEN 'SIZE DOWN TO 2X-LARGE'
                WHEN a.classification = 'OVERSIZED' AND a.current_size = '4X-LARGE' THEN 'SIZE DOWN TO 3X-LARGE'
                WHEN a.classification = 'UNDERSIZED' AND a.current_size = 'X-SMALL' THEN 'SIZE UP TO SMALL'
                WHEN a.classification = 'UNDERSIZED' AND a.current_size = 'SMALL' THEN 'SIZE UP TO MEDIUM'
                WHEN a.classification = 'UNDERSIZED' AND a.current_size = 'MEDIUM' THEN 'SIZE UP TO LARGE'
                WHEN a.classification = 'UNDERSIZED' AND a.current_size = 'LARGE' THEN 'SIZE UP TO X-LARGE'
                WHEN a.classification = 'UNDERSIZED' AND a.current_size = 'X-LARGE' THEN 'SIZE UP TO 2X-LARGE'
                WHEN a.classification = 'SPLIT_CANDIDATE' THEN 'SPLIT INTO DEDICATED WAREHOUSES'
                ELSE 'REVIEW'
            END,
            OBJECT_CONSTRUCT(
                'total_queries', a.total_queries,
                'p50_exec_sec', a.p50_exec_sec,
                'p95_exec_sec', a.p95_exec_sec,
                'oversized_pct', a.oversized_pct,
                'undersized_pct', a.undersized_pct,
                'remote_spill_bytes', a.total_remote_spill,
                'local_spill_bytes', a.total_local_spill,
                'queued_overload_count', a.queued_count,
                'p95_queue_sec', a.p95_queue_sec,
                'distinct_roles', a.distinct_roles,
                'exec_time_spread', a.exec_time_spread,
                'trend_vs_4wk_avg_pct', a.trend_pct
            ),
            CASE
                WHEN a.classification = 'OVERSIZED' THEN ROUND(-a.week_credits * 0.5, 2)
                WHEN a.classification = 'UNDERSIZED' THEN ROUND(a.week_credits * 1.0, 2)
                WHEN a.classification = 'SPLIT_CANDIDATE' THEN 0
                ELSE 0
            END,
            CASE
                WHEN a.classification = 'OVERSIZED' AND a.current_size = 'SMALL'
                    THEN 'ALTER WAREHOUSE ' || a.WAREHOUSE_NAME || ' SET WAREHOUSE_SIZE = ''XSMALL'';'
                WHEN a.classification = 'OVERSIZED' AND a.current_size = 'MEDIUM'
                    THEN 'ALTER WAREHOUSE ' || a.WAREHOUSE_NAME || ' SET WAREHOUSE_SIZE = ''SMALL'';'
                WHEN a.classification = 'OVERSIZED' AND a.current_size = 'LARGE'
                    THEN 'ALTER WAREHOUSE ' || a.WAREHOUSE_NAME || ' SET WAREHOUSE_SIZE = ''MEDIUM'';'
                WHEN a.classification = 'OVERSIZED' AND a.current_size = 'X-LARGE'
                    THEN 'ALTER WAREHOUSE ' || a.WAREHOUSE_NAME || ' SET WAREHOUSE_SIZE = ''LARGE'';'
                WHEN a.classification = 'OVERSIZED' AND a.current_size = '2X-LARGE'
                    THEN 'ALTER WAREHOUSE ' || a.WAREHOUSE_NAME || ' SET WAREHOUSE_SIZE = ''XLARGE'';'
                WHEN a.classification = 'UNDERSIZED' AND a.current_size = 'X-SMALL'
                    THEN 'ALTER WAREHOUSE ' || a.WAREHOUSE_NAME || ' SET WAREHOUSE_SIZE = ''SMALL'';'
                WHEN a.classification = 'UNDERSIZED' AND a.current_size = 'SMALL'
                    THEN 'ALTER WAREHOUSE ' || a.WAREHOUSE_NAME || ' SET WAREHOUSE_SIZE = ''MEDIUM'';'
                WHEN a.classification = 'UNDERSIZED' AND a.current_size = 'MEDIUM'
                    THEN 'ALTER WAREHOUSE ' || a.WAREHOUSE_NAME || ' SET WAREHOUSE_SIZE = ''LARGE'';'
                WHEN a.classification = 'UNDERSIZED' AND a.current_size = 'LARGE'
                    THEN 'ALTER WAREHOUSE ' || a.WAREHOUSE_NAME || ' SET WAREHOUSE_SIZE = ''XLARGE'';'
                WHEN a.classification = 'SPLIT_CANDIDATE'
                    THEN '-- Review workload segmentation for ' || a.WAREHOUSE_NAME || ' and create dedicated warehouses per role/workload.'
                ELSE NULL
            END,
            'NEW',
            a.week_credits,
            a.prev_4wk_avg_credits
        FROM WAREHOUSE_OPTIMIZER_SERVICE.SERVICE._WH_ANALYSIS a
        WHERE a.classification IN ('OVERSIZED', 'UNDERSIZED', 'SPLIT_CANDIDATE');

        -- Step 4: Count flagged; bail if zero
        SELECT COUNT(*), COALESCE(SUM(ESTIMATED_CREDIT_IMPACT), 0)
        INTO :v_flagged_count, :v_total_savings
        FROM WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
        WHERE RUN_DATE = CURRENT_DATE();

        IF (v_flagged_count = 0) THEN
            DROP TABLE IF EXISTS WAREHOUSE_OPTIMIZER_SERVICE.SERVICE._WH_ANALYSIS;
            RETURN '0 warehouses flagged -- no email sent.';
        END IF;

        -- Step 5: Build HTML email
        v_html := '<!DOCTYPE html><html><head><style>'
            || 'body{font-family:Arial,sans-serif;margin:20px;color:#333;}'
            || 'h2{color:#1a73e8;} h3{margin-top:24px;}'
            || 'table{border-collapse:collapse;width:100%;margin:12px 0;}'
            || 'th{background:#2c3e50;color:white;padding:10px 12px;text-align:left;font-size:13px;}'
            || 'td{padding:8px 12px;border-bottom:1px solid #e0e0e0;font-size:13px;}'
            || 'tr:nth-child(even){background:#f8f9fa;}'
            || '.oversized{color:#27ae60;font-weight:bold;}'
            || '.undersized{color:#e74c3c;font-weight:bold;}'
            || '.split{color:#e67e22;font-weight:bold;}'
            || '.savings{color:#27ae60;} .cost{color:#e74c3c;}'
            || 'code{background:#f4f4f4;padding:2px 6px;border-radius:3px;font-size:12px;}'
            || '.summary{background:#eaf2ff;padding:16px;border-radius:8px;margin-bottom:20px;}'
            || '.trend-up{color:#e74c3c;} .trend-down{color:#27ae60;}'
            || '</style></head><body>';

        v_html := v_html || '<h2>Weekly Warehouse Right-Sizing Report</h2>'
            || '<div class="summary">'
            || '<strong>' || :v_flagged_count::VARCHAR || '</strong> warehouse(s) flagged &#160;|&#160; '
            || 'Estimated net weekly impact: <strong>'
            || CASE WHEN :v_total_savings <= 0 THEN '<span class="savings">' ELSE '<span class="cost">' END
            || ROUND(:v_total_savings, 1)::VARCHAR || ' credits</span></strong>'
            || ' &#160;|&#160; Run date: ' || CURRENT_DATE()::VARCHAR
            || '</div>';

        v_html := v_html || '<table><tr>'
            || '<th>Warehouse</th><th>Size</th><th>Classification</th><th>Action</th>'
            || '<th>Credits (wk)</th><th>vs 4-wk Avg</th>'
            || '<th>p50 / p95 (s)</th><th>Oversized %</th><th>Undersized %</th>'
            || '<th>Remote Spill</th><th>Queue Count</th>'
            || '<th>Impact</th><th>SQL</th></tr>';

        -- Step 6: Loop through flagged recommendations and build table rows
        LET cur CURSOR FOR
            SELECT WAREHOUSE_NAME, CURRENT_SIZE, CLASSIFICATION, RECOMMENDED_ACTION,
                   WEEK_CREDITS, PREV_4WK_AVG_CREDITS, ESTIMATED_CREDIT_IMPACT, GENERATED_SQL,
                   JUSTIFICATION:p50_exec_sec::VARCHAR AS p50,
                   JUSTIFICATION:p95_exec_sec::VARCHAR AS p95,
                   JUSTIFICATION:oversized_pct::VARCHAR AS o_pct,
                   JUSTIFICATION:undersized_pct::VARCHAR AS u_pct,
                   JUSTIFICATION:remote_spill_bytes::NUMBER AS remote_spill,
                   JUSTIFICATION:queued_overload_count::VARCHAR AS q_count,
                   JUSTIFICATION:trend_vs_4wk_avg_pct::VARCHAR AS trend
            FROM WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
            WHERE RUN_DATE = CURRENT_DATE()
            ORDER BY ESTIMATED_CREDIT_IMPACT ASC;

        FOR rec IN cur DO
            LET cls_class VARCHAR := CASE
                WHEN rec.CLASSIFICATION = 'OVERSIZED' THEN 'oversized'
                WHEN rec.CLASSIFICATION = 'UNDERSIZED' THEN 'undersized'
                ELSE 'split' END;
            LET trend_class VARCHAR := CASE
                WHEN rec.trend IS NOT NULL AND rec.trend::FLOAT > 0 THEN 'trend-up'
                WHEN rec.trend IS NOT NULL AND rec.trend::FLOAT < 0 THEN 'trend-down'
                ELSE '' END;
            LET trend_arrow VARCHAR := CASE
                WHEN rec.trend IS NOT NULL AND rec.trend::FLOAT > 0 THEN '&#9650; '
                WHEN rec.trend IS NOT NULL AND rec.trend::FLOAT < 0 THEN '&#9660; '
                ELSE '' END;
            LET spill_display VARCHAR := CASE
                WHEN rec.remote_spill > 1073741824 THEN ROUND(rec.remote_spill / 1073741824.0, 1)::VARCHAR || ' GB'
                WHEN rec.remote_spill > 1048576 THEN ROUND(rec.remote_spill / 1048576.0, 1)::VARCHAR || ' MB'
                WHEN rec.remote_spill > 0 THEN ROUND(rec.remote_spill / 1024.0, 1)::VARCHAR || ' KB'
                ELSE '0' END;
            LET impact_class VARCHAR := CASE
                WHEN rec.ESTIMATED_CREDIT_IMPACT < 0 THEN 'savings'
                WHEN rec.ESTIMATED_CREDIT_IMPACT > 0 THEN 'cost'
                ELSE '' END;

            v_html := v_html || '<tr>'
                || '<td><strong>' || rec.WAREHOUSE_NAME || '</strong></td>'
                || '<td>' || COALESCE(rec.CURRENT_SIZE, '-') || '</td>'
                || '<td><span class="' || cls_class || '">' || rec.CLASSIFICATION || '</span></td>'
                || '<td>' || COALESCE(rec.RECOMMENDED_ACTION, '-') || '</td>'
                || '<td>' || COALESCE(rec.WEEK_CREDITS::VARCHAR, '0') || '</td>'
                || '<td><span class="' || trend_class || '">' || trend_arrow || COALESCE(rec.trend, 'n/a') || CASE WHEN rec.trend IS NOT
  NULL THEN '%' ELSE '' END || '</span></td>'
                || '<td>' || COALESCE(rec.p50, '-') || ' / ' || COALESCE(rec.p95, '-') || '</td>'
                || '<td>' || COALESCE(rec.o_pct, '0') || '%</td>'
                || '<td>' || COALESCE(rec.u_pct, '0') || '%</td>'
                || '<td>' || spill_display || '</td>'
                || '<td>' || COALESCE(rec.q_count, '0') || '</td>'
                || '<td><span class="' || impact_class || '">' || COALESCE(rec.ESTIMATED_CREDIT_IMPACT::VARCHAR, '0') || '</span></td>'
                || '<td><code>' || COALESCE(rec.GENERATED_SQL, '-') || '</code></td>'
                || '</tr>';
        END FOR;

        v_html := v_html || '</table>';

        v_html := v_html || '<h3>Manage Recommendations</h3>'
            || '<p>To snooze a warehouse for N weeks:<br/>'
            || '<code>CALL WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.SNOOZE_WAREHOUSE(''WAREHOUSE_NAME'', N);</code></p>'
            || '<p>To acknowledge:<br/>'
            || '<code>CALL WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.ACKNOWLEDGE_WAREHOUSE(''WAREHOUSE_NAME'');</code></p>'
            || '<p>After applying a change:<br/>'
            || '<code>CALL WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.MARK_APPLIED(''WAREHOUSE_NAME'');</code></p>';

        v_html := v_html || '<hr/><p style="font-size:11px;color:#888;">'
            || 'Generated by WAREHOUSE_OPTIMIZER_SERVICE on ' || CURRENT_DATE()::VARCHAR
            || '. Thresholds: oversized=' || :v_oversized_thresh::VARCHAR || '%, '
            || 'undersized=' || :v_undersized_thresh::VARCHAR || '%, '
            || 'min credits/wk=' || :v_min_credits::VARCHAR
            || '</p></body></html>';

        v_subject := 'Warehouse Right-Sizing Report -- ' || :v_flagged_count::VARCHAR
            || ' flagged (' || CURRENT_DATE()::VARCHAR || ')';

        -- Step 7: Send email
        CALL SYSTEM$SEND_EMAIL(
            'EMAIL_INTEGRATION',
            :v_email,
            :v_subject,
            :v_html,
            'text/html'
        );

        DROP TABLE IF EXISTS WAREHOUSE_OPTIMIZER_SERVICE.SERVICE._WH_ANALYSIS;

        RETURN :v_flagged_count::VARCHAR || ' warehouse(s) flagged. Est. weekly impact: '
            || ROUND(:v_total_savings, 1)::VARCHAR || ' credits. Email sent to ' || :v_email;
    END;
    $$;

    -- =============================================================================
    -- 4. HELPER PROCEDURES
    -- =============================================================================

    CREATE OR REPLACE PROCEDURE WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.SNOOZE_WAREHOUSE(
        P_WAREHOUSE_NAME VARCHAR,
        P_WEEKS NUMBER
    )
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    $$
    BEGIN
        UPDATE WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
        SET STATUS = 'SNOOZED',
            SNOOZED_UNTIL = DATEADD('WEEK', :P_WEEKS, CURRENT_DATE())
        WHERE WAREHOUSE_NAME = :P_WAREHOUSE_NAME
          AND RUN_DATE = (
              SELECT MAX(RUN_DATE)
              FROM WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
              WHERE WAREHOUSE_NAME = :P_WAREHOUSE_NAME
          );
        RETURN 'Snoozed ' || :P_WAREHOUSE_NAME || ' for ' || :P_WEEKS::VARCHAR || ' week(s) until '
            || DATEADD('WEEK', :P_WEEKS, CURRENT_DATE())::VARCHAR;
    END;
    $$;

    CREATE OR REPLACE PROCEDURE WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.ACKNOWLEDGE_WAREHOUSE(
        P_WAREHOUSE_NAME VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    $$
    BEGIN
        UPDATE WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
        SET STATUS = 'ACKNOWLEDGED'
        WHERE WAREHOUSE_NAME = :P_WAREHOUSE_NAME
          AND RUN_DATE = (
              SELECT MAX(RUN_DATE)
              FROM WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
              WHERE WAREHOUSE_NAME = :P_WAREHOUSE_NAME
          );
        RETURN 'Acknowledged recommendation for ' || :P_WAREHOUSE_NAME;
    END;
    $$;

    CREATE OR REPLACE PROCEDURE WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.MARK_APPLIED(
        P_WAREHOUSE_NAME VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    $$
    BEGIN
        UPDATE WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
        SET STATUS = 'APPLIED'
        WHERE WAREHOUSE_NAME = :P_WAREHOUSE_NAME
          AND RUN_DATE = (
              SELECT MAX(RUN_DATE)
              FROM WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.RECOMMENDATION_LOG
              WHERE WAREHOUSE_NAME = :P_WAREHOUSE_NAME
          );
        RETURN 'Marked ' || :P_WAREHOUSE_NAME || ' recommendation as APPLIED';
    END;
    $$;

    -- =============================================================================
    -- 5. SCHEDULED TASK
    -- =============================================================================

    CREATE OR REPLACE TASK WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.DAILY_RIGHTSIZING_TASK
        WAREHOUSE = WAREHOUSE_OPTIMIZER_WH
        SCHEDULE = 'USING CRON 0 9 * * * UTC'
    AS
        CALL WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.GENERATE_RIGHTSIZING_REPORT();

    ALTER TASK WAREHOUSE_OPTIMIZER_SERVICE.SERVICE.DAILY_RIGHTSIZING_TASK RESUME;