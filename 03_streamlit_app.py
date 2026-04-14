import streamlit as st
st.set_page_config(layout="wide")
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

session = get_active_session()

SIZES = ['X-SMALL', 'SMALL', 'MEDIUM', 'LARGE', 'X-LARGE', '2X-LARGE', '3X-LARGE', '4X-LARGE', '5X-LARGE', '6X-LARGE']
ACTIONS = ['SIZE UP', 'SIZE DOWN', 'RIGHT SIZE']
TIMEFRAMES = {'7 Days': 7, '14 Days': 14, '30 Days': 30, '90 Days': 90}

SYSTEM_WH_FILTER = """
          AND WAREHOUSE_NAME NOT LIKE 'COMPUTE_SERVICE_WH%'
          AND WAREHOUSE_NAME NOT LIKE 'SYSTEM$%'
"""

def pill(label, value, color):
    return f"""
    <div style="background:{color}22;border:1px solid {color};border-radius:20px;padding:6px 16px;display:inline-block;margin:2px 4px;">
        <span style="color:{color};font-weight:600;font-size:13px;">{label}:</span>
        <span style="color:{color};font-size:13px;margin-left:4px;">{value}</span>
    </div>"""

with st.sidebar:
    st.markdown("##### Filters")
    tf_label = st.selectbox("Lookback Period", list(TIMEFRAMES.keys()), index=1, key='timeframe')
    lookback_days = TIMEFRAMES[tf_label]
    min_credits = st.number_input("Min Credit Usage", min_value=0.0, value=0.0, step=0.1, key='min_credits')
    undersized_threshold = st.slider("Undersized % Threshold", min_value=1, max_value=50, value=5, key='undersized_thresh')
    action_filter = st.multiselect("Action", ACTIONS, default=ACTIONS, key='action_filter')

@st.cache_data(ttl=600)
def load_current_sizes():
    session.sql("SHOW WAREHOUSES").collect()
    df = session.sql("""
        SELECT "name" AS WAREHOUSE_NAME, UPPER("size") AS WAREHOUSE_SIZE
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    """).to_pandas()
    return df

@st.cache_data(ttl=600)
def load_hourly_data(days):
    df = session.sql(f"""
        SELECT
            WAREHOUSE_NAME,
            UPPER(WAREHOUSE_SIZE) AS WAREHOUSE_SIZE,
            DATE_TRUNC('HOUR', START_TIME) AS HOUR_BUCKET,
            COUNT(*) AS TOTAL_QUERIES,
            SUM(CASE WHEN EXECUTION_TIME < 1000 AND UPPER(WAREHOUSE_SIZE) != 'X-SMALL' THEN 1 ELSE 0 END) AS OVERSIZED_COUNT,
            SUM(CASE WHEN BYTES_SPILLED_TO_REMOTE_STORAGE > 0 OR QUEUED_OVERLOAD_TIME > 5000 THEN 1 ELSE 0 END) AS UNDERSIZED_COUNT,
            SUM(CASE
                WHEN EXECUTION_TIME < 1000 AND UPPER(WAREHOUSE_SIZE) != 'X-SMALL' THEN 0
                WHEN BYTES_SPILLED_TO_REMOTE_STORAGE > 0 OR QUEUED_OVERLOAD_TIME > 5000 THEN 0
                ELSE 1 END) AS RIGHTSIZED_COUNT,
            AVG(EXECUTION_TIME) / 1000.0 AS AVG_EXEC_TIME_SEC,
            ROUND(SUM(BYTES_SPILLED_TO_LOCAL_STORAGE) / 1024.0, 0) AS LOCAL_SPILL_KB,
            ROUND(SUM(BYTES_SPILLED_TO_REMOTE_STORAGE) / 1024.0, 0) AS REMOTE_SPILL_KB
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD('DAY', -{days}, CURRENT_TIMESTAMP())
          AND WAREHOUSE_NAME IS NOT NULL
          AND WAREHOUSE_SIZE IS NOT NULL
          {SYSTEM_WH_FILTER}
        GROUP BY WAREHOUSE_NAME, UPPER(WAREHOUSE_SIZE), DATE_TRUNC('HOUR', START_TIME)
        ORDER BY HOUR_BUCKET
    """).to_pandas()
    return df

@st.cache_data(ttl=600)
def load_warehouse_summary(days):
    df = session.sql(f"""
        SELECT
            q.WAREHOUSE_NAME,
            COUNT(*) AS TOTAL_QUERIES,
            SUM(CASE WHEN q.EXECUTION_TIME < 1000 THEN 1 ELSE 0 END) AS OVERSIZED_COUNT,
            SUM(CASE WHEN q.BYTES_SPILLED_TO_REMOTE_STORAGE > 0 OR q.QUEUED_OVERLOAD_TIME > 5000 THEN 1 ELSE 0 END) AS UNDERSIZED_COUNT,
            SUM(q.BYTES_SPILLED_TO_REMOTE_STORAGE) AS TOTAL_REMOTE_SPILL,
            ROUND(SUM(q.BYTES_SPILLED_TO_LOCAL_STORAGE + q.BYTES_SPILLED_TO_REMOTE_STORAGE) / 1024.0, 0) AS TOTAL_SPILL_KB
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
        WHERE q.START_TIME >= DATEADD('DAY', -{days}, CURRENT_TIMESTAMP())
          AND q.WAREHOUSE_NAME IS NOT NULL
          AND q.WAREHOUSE_SIZE IS NOT NULL
          {SYSTEM_WH_FILTER}
        GROUP BY q.WAREHOUSE_NAME
        ORDER BY TOTAL_QUERIES DESC
    """).to_pandas()
    current_sizes = load_current_sizes()
    df = df.merge(current_sizes, on='WAREHOUSE_NAME', how='left')
    df = df[df['WAREHOUSE_SIZE'].notna()]
    df['OVERSIZED_COUNT'] = df.apply(
        lambda r: r['OVERSIZED_COUNT'] if r['WAREHOUSE_SIZE'] != 'X-SMALL' else 0, axis=1
    )
    credits = session.sql(f"""
        SELECT WAREHOUSE_NAME, SUM(CREDITS_USED) AS CREDITS_USED
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD('DAY', -{days}, CURRENT_TIMESTAMP())
          {SYSTEM_WH_FILTER}
        GROUP BY WAREHOUSE_NAME
    """).to_pandas()
    df = df.merge(credits, on='WAREHOUSE_NAME', how='left')
    df['CREDITS_USED'] = df['CREDITS_USED'].fillna(0).round(2)
    df['OVERSIZED_PCT'] = (df['OVERSIZED_COUNT'] / df['TOTAL_QUERIES'] * 100).round(1)
    df['UNDERSIZED_PCT'] = (df['UNDERSIZED_COUNT'] / df['TOTAL_QUERIES'] * 100).round(1)
    return df

def get_recommendation(row):
    size = row['WAREHOUSE_SIZE']
    if row['OVERSIZED_PCT'] > 75 and size != 'X-SMALL':
        idx = SIZES.index(size) if size in SIZES else -1
        if idx > 0:
            return "SIZE DOWN"
    if row['UNDERSIZED_PCT'] > undersized_threshold:
        idx = SIZES.index(size) if size in SIZES else -1
        if idx >= 0 and idx < len(SIZES) - 1:
            return "SIZE UP"
    return "RIGHT SIZE"

def get_new_size(row):
    size = row['WAREHOUSE_SIZE']
    if row['ACTION'] == 'SIZE DOWN' and size in SIZES:
        idx = SIZES.index(size)
        if idx > 0:
            return SIZES[idx - 1]
    elif row['ACTION'] == 'SIZE UP' and size in SIZES:
        idx = SIZES.index(size)
        if idx < len(SIZES) - 1:
            return SIZES[idx + 1]
    return None

def get_credit_impact(row):
    if row['ACTION'] == 'SIZE DOWN':
        return round(-row['CREDITS_USED'] * 0.5, 2)
    elif row['ACTION'] == 'SIZE UP':
        return round(row['CREDITS_USED'] * 1.0, 2)
    return 0.0

def get_alter_sql(row):
    new_size = get_new_size(row)
    if new_size:
        return f"ALTER WAREHOUSE {row['WAREHOUSE_NAME']} SET WAREHOUSE_SIZE = '{new_size}';"
    return None

AXIS_FONT = 11
LEGEND_FONT = 10
CHART_H = 160
BAR_SIZE = 3

st.markdown("#### Warehouse Cost Optimizer")

pill_slot = st.empty()
chart_slot1 = st.empty()
chart_slot2 = st.empty()
chart_slot3 = st.empty()

st.divider()

hourly_df = load_hourly_data(lookback_days)
summary_df = load_warehouse_summary(lookback_days)
summary_df['ACTION'] = summary_df.apply(get_recommendation, axis=1)
summary_df = summary_df[summary_df['CREDITS_USED'] >= min_credits].reset_index(drop=True)
summary_df = summary_df[summary_df['ACTION'].isin(action_filter)].reset_index(drop=True)
summary_df['CREDIT_IMPACT'] = summary_df.apply(get_credit_impact, axis=1)

max_q = hourly_df.groupby('WAREHOUSE_NAME')['TOTAL_QUERIES'].max().reset_index()
max_q.columns = ['WAREHOUSE_NAME', 'MAX_QUERIES_HR']
summary_df = summary_df.merge(max_q, on='WAREHOUSE_NAME', how='left')
summary_df['MAX_QUERIES_HR'] = summary_df['MAX_QUERIES_HR'].fillna(0).astype(int)

display_df = pd.DataFrame()
display_df['Warehouse'] = summary_df['WAREHOUSE_NAME'].values
display_df['Select'] = [False] * len(summary_df)
display_df['Size'] = summary_df['WAREHOUSE_SIZE'].values
display_df[f'{tf_label} Credits'] = summary_df['CREDITS_USED'].values
display_df['Credit Impact'] = summary_df['CREDIT_IMPACT'].values
display_df['Spill (KB)'] = summary_df['TOTAL_SPILL_KB'].values
display_df['Max Queries/hr'] = summary_df['MAX_QUERIES_HR'].values
display_df['Oversized %'] = summary_df['OVERSIZED_PCT'].values
display_df['Undersized %'] = summary_df['UNDERSIZED_PCT'].values
display_df['Action'] = summary_df['ACTION'].values
display_df = display_df.sort_values(f'{tf_label} Credits', ascending=False).reset_index(drop=True)
display_df.index = [' '] * len(display_df)

edited_df = st.data_editor(
    display_df,
    use_container_width=True,
    key='wh_editor'
)
selected_warehouses = edited_df.loc[edited_df['Select'] == True, 'Warehouse'].tolist()

if selected_warehouses:
    pill_df = summary_df[summary_df['WAREHOUSE_NAME'].isin(selected_warehouses)]
    pill_label = "Selected"
else:
    pill_df = summary_df
    pill_label = "All"

down_savings = pill_df.loc[pill_df['ACTION'] == 'SIZE DOWN', 'CREDIT_IMPACT'].sum()
up_cost = pill_df.loc[pill_df['ACTION'] == 'SIZE UP', 'CREDIT_IMPACT'].sum()
net = round(down_savings + up_cost, 2)
current_total = pill_df['CREDITS_USED'].sum()
net_color = '#27ae60' if net <= 0 else '#e74c3c'

with pill_slot.container():
    p1, p2, p3, p4 = st.columns(4)
    p1.markdown(pill(f"Credits ({pill_label})", f"{current_total:,.1f}", "#7f8c8d"), unsafe_allow_html=True)
    p2.markdown(pill("Size Down Savings", f"{down_savings:,.1f}", "#27ae60"), unsafe_allow_html=True)
    p3.markdown(pill("Size Up Cost", f"+{up_cost:,.1f}", "#e74c3c"), unsafe_allow_html=True)
    p4.markdown(pill("Net Impact", f"{net:,.1f}", net_color), unsafe_allow_html=True)

gen_sql = st.button("Generate SQL for Selected Warehouses")
if gen_sql and selected_warehouses:
    sql_cmds = []
    for _, row in summary_df[summary_df['WAREHOUSE_NAME'].isin(selected_warehouses)].iterrows():
        sql = get_alter_sql(row)
        if sql:
            sql_cmds.append(sql)
    if sql_cmds:
        st.code("\n".join(sql_cmds), language="sql")
    else:
        st.caption("No sizing changes recommended for selected warehouses.")
elif gen_sql:
    st.warning("Select one or more warehouses first.")

if selected_warehouses:
    chart_df = hourly_df[hourly_df['WAREHOUSE_NAME'].isin(selected_warehouses)]
else:
    filtered_wh = summary_df['WAREHOUSE_NAME'].tolist()
    chart_df = hourly_df[hourly_df['WAREHOUSE_NAME'].isin(filtered_wh)]

chart_agg = chart_df.groupby('HOUR_BUCKET').agg(
    OVERSIZED_COUNT=('OVERSIZED_COUNT', 'sum'),
    UNDERSIZED_COUNT=('UNDERSIZED_COUNT', 'sum'),
    RIGHTSIZED_COUNT=('RIGHTSIZED_COUNT', 'sum'),
    AVG_EXEC_TIME_SEC=('AVG_EXEC_TIME_SEC', 'mean'),
    LOCAL_SPILL_KB=('LOCAL_SPILL_KB', 'sum'),
    REMOTE_SPILL_KB=('REMOTE_SPILL_KB', 'sum'),
).reset_index()

chart_agg['LOCAL_SPILL_KB'] = chart_agg['LOCAL_SPILL_KB'].round(0)
chart_agg['REMOTE_SPILL_KB'] = chart_agg['REMOTE_SPILL_KB'].round(0)
chart_agg['AVG_EXEC_TIME_SEC'] = chart_agg['AVG_EXEC_TIME_SEC'].round(2)

x_ax = alt.Axis(format='%m/%d', labelFontSize=AXIS_FONT, titleFontSize=AXIS_FONT)
y_cfg = dict(labelFontSize=AXIS_FONT, titleFontSize=AXIS_FONT, minExtent=50, maxExtent=50)
leg_top = alt.Legend(labelFontSize=LEGEND_FONT, orient='top')
tt_date = alt.Tooltip('HOUR_BUCKET:T', title='Date', format='%m/%d %H:%M')

if not chart_agg.empty:
    melted = chart_agg.melt(
        id_vars=['HOUR_BUCKET'],
        value_vars=['OVERSIZED_COUNT', 'UNDERSIZED_COUNT', 'RIGHTSIZED_COUNT'],
        var_name='Classification', value_name='Count'
    )
    melted['Classification'] = melted['Classification'].replace({
        'OVERSIZED_COUNT': 'Oversized', 'UNDERSIZED_COUNT': 'Undersized', 'RIGHTSIZED_COUNT': 'Right Size'
    })
    c1 = alt.Chart(melted).mark_bar(size=BAR_SIZE).encode(
        x=alt.X('HOUR_BUCKET:T', title='', axis=x_ax),
        y=alt.Y('Count:Q', title='Queries', stack='zero', axis=alt.Axis(**y_cfg)),
        color=alt.Color('Classification:N',
                         scale=alt.Scale(domain=['Oversized','Undersized','Right Size'], range=['#e74c3c','#9b59b6','#f1c40f']),
                         title='', legend=leg_top),
        tooltip=[tt_date, alt.Tooltip('Classification:N', title='Type'), alt.Tooltip('Count:Q', title='Queries')]
    ).properties(height=CHART_H, title=alt.TitleParams(f'Query Classification ({tf_label})', fontSize=11))
    chart_slot1.altair_chart(c1, use_container_width=True)

    c2 = alt.Chart(chart_agg).mark_bar(size=BAR_SIZE, color='#e67e22').encode(
        x=alt.X('HOUR_BUCKET:T', title='', axis=x_ax),
        y=alt.Y('AVG_EXEC_TIME_SEC:Q', title='Seconds', axis=alt.Axis(**y_cfg)),
        tooltip=[tt_date, alt.Tooltip('AVG_EXEC_TIME_SEC:Q', title='Avg Time (s)', format='.2f')]
    ).properties(height=CHART_H, title=alt.TitleParams('Avg Execution Time (s)', fontSize=11))
    chart_slot2.altair_chart(c2, use_container_width=True)

    spill_melted = chart_agg.melt(
        id_vars=['HOUR_BUCKET'],
        value_vars=['LOCAL_SPILL_KB', 'REMOTE_SPILL_KB'],
        var_name='Spill Type', value_name='KB'
    )
    spill_melted['Spill Type'] = spill_melted['Spill Type'].replace({
        'LOCAL_SPILL_KB': 'Local Spill', 'REMOTE_SPILL_KB': 'Remote Spill'
    })
    c3 = alt.Chart(spill_melted).mark_bar(size=BAR_SIZE).encode(
        x=alt.X('HOUR_BUCKET:T', title='', axis=x_ax),
        y=alt.Y('KB:Q', title='KB Spilled', stack='zero', axis=alt.Axis(**y_cfg)),
        color=alt.Color('Spill Type:N', title='', legend=leg_top),
        tooltip=[tt_date, alt.Tooltip('Spill Type:N', title='Type'), alt.Tooltip('KB:Q', title='KB', format=',.0f')]
    ).properties(height=CHART_H, title=alt.TitleParams('Bytes Spilled (KB)', fontSize=11))
    chart_slot3.altair_chart(c3, use_container_width=True)
else:
    chart_slot1.caption("No chart data available")

st.divider()

st.markdown(f"""
<small>

| Term | Definition |
|---|---|
| **Oversized** | EXECUTION_TIME < 1s AND not X-SMALL |
| **Undersized** | BYTES_SPILLED_TO_REMOTE > 0 OR QUEUED_OVERLOAD_TIME > 5s |
| **RIGHT SIZE** | Neither condition met |
| **SIZE DOWN** | Oversized % > 75% (never below X-SMALL) -- saves ~50% credits |
| **SIZE UP** | Undersized % > {undersized_threshold}% -- adds ~100% credits |

</small>
""", unsafe_allow_html=True)