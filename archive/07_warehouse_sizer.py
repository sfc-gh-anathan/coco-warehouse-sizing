##############################################################################
# Warehouse Sizer — Cost Optimizer
# =================================
# A Streamlit-in-Snowflake application that analyzes virtual warehouse
# utilization from ACCOUNT_USAGE views and generates AI-powered sizing
# recommendations using Snowflake Cortex LLM functions.
#
# Data Sources:
#   - SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY   (5-min interval load)
#   - SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY (hourly credit usage)
#   - SHOW WAREHOUSES                                   (current warehouse sizes)
#
# Cortex AI:
#   - SNOWFLAKE.CORTEX.COMPLETE (llama3.1-70b) for sizing recommendations
##############################################################################

import streamlit as st
import pandas as pd
import json
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# PAGE CONFIG
# Set the page title and layout to wide so the summary table has room to
# display all columns including sparkline charts.
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Warehouse Sizer — Cost Optimizer", layout="wide")
st.title("Warehouse Sizer — Cost Optimizer")

# ---------------------------------------------------------------------------
# SNOWPARK SESSION
# Use Streamlit's built-in Snowflake connection to obtain a Snowpark session.
# This avoids manual credential management and leverages the session context
# of the logged-in user.
# ---------------------------------------------------------------------------
session = st.connection("snowflake").session()

# ---------------------------------------------------------------------------
# SIDEBAR — USER CONTROLS
# The sidebar provides all user-configurable parameters that control
# which warehouses are analyzed, over what time range, and the thresholds
# used by the Cortex AI model to decide sizing actions.
# ---------------------------------------------------------------------------
st.sidebar.header("Filters & Thresholds")

# -- Date Range Picker --
# Defaults to the last 30 days. The WAREHOUSE_LOAD_HISTORY and
# WAREHOUSE_METERING_HISTORY views can have up to 3-hour and 6-month
# latency respectively, so recent data may be slightly delayed.
default_end = date.today()
default_start = default_end - timedelta(days=30)
date_range = st.sidebar.date_input(
    "Date Range",
    value=(default_start, default_end),
    max_value=default_end,
    help="Select the start and end dates for warehouse utilization analysis.",
)

# Validate that the user selected both a start and end date.
# st.date_input with a tuple value returns a tuple; if only one date is
# picked the tuple will have length 1.
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = default_start, default_end

# -- Warehouse Name Filter --
# Dynamically populated from WAREHOUSE_METERING_HISTORY so the user only
# sees warehouses that actually consumed credits in their account.
@st.cache_data(ttl=600)
def get_warehouse_names():
    """Fetch distinct warehouse names from metering history.
    Uses ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY which records every
    warehouse that has consumed credits."""
    df = session.sql(
        "SELECT DISTINCT WAREHOUSE_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY ORDER BY 1"
    ).to_pandas()
    return df["WAREHOUSE_NAME"].tolist()

all_warehouses = get_warehouse_names()
selected_warehouses = st.sidebar.multiselect(
    "Warehouses",
    options=all_warehouses,
    default=[],
    help="Leave empty to include all warehouses.",
)

# -- Queued Load Threshold --
# If a warehouse's average queued load exceeds this value, Cortex AI will
# recommend sizing UP because queries are waiting for compute resources.
queued_threshold = st.sidebar.number_input(
    "Queued Load Threshold",
    min_value=0.0,
    max_value=1.0,
    value=0.05,
    step=0.01,
    help="If avg queued load exceeds this, recommend sizing UP.",
)

# -- Utilization Floor --
# If average running load is below this percentage of capacity AND queued
# load is near zero, the warehouse is over-provisioned and Cortex AI will
# recommend sizing DOWN.
utilization_floor = st.sidebar.number_input(
    "Utilization Floor (%)",
    min_value=0,
    max_value=100,
    value=10,
    step=1,
    help="If avg running load is below this % with no queuing, recommend sizing DOWN.",
)

# ---------------------------------------------------------------------------
# DATA QUERIES
# These cached functions retrieve data from Snowflake's ACCOUNT_USAGE schema
# and the SHOW WAREHOUSES command.  Caching avoids re-running expensive
# queries on every Streamlit rerun (e.g. when the user toggles a checkbox).
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600)
def get_load_history(_start: str, _end: str, warehouses: tuple):
    """Query WAREHOUSE_LOAD_HISTORY for 5-minute interval load metrics.
    Returns AVG_RUNNING, AVG_QUEUED_LOAD per warehouse per interval.
    These metrics are the core inputs for sizing analysis — they show
    how busy each warehouse is and whether queries are queuing."""
    wh_filter = ""
    if warehouses:
        wh_list = ",".join([f"'{w}'" for w in warehouses])
        wh_filter = f"AND WAREHOUSE_NAME IN ({wh_list})"
    query = f"""
        SELECT
            WAREHOUSE_NAME,
            START_TIME,
            AVG_RUNNING,
            AVG_QUEUED_LOAD
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
        WHERE START_TIME >= '{_start}'
          AND START_TIME <= '{_end}'
          {wh_filter}
        ORDER BY WAREHOUSE_NAME, START_TIME
    """
    return session.sql(query).to_pandas()


@st.cache_data(ttl=600)
def get_metering_history(_start: str, _end: str, warehouses: tuple):
    """Query WAREHOUSE_METERING_HISTORY for hourly credit consumption.
    CREDITS_USED is the primary cost metric — it determines how much
    each warehouse costs and is used to sort the summary table so the
    most expensive warehouses appear first."""
    wh_filter = ""
    if warehouses:
        wh_list = ",".join([f"'{w}'" for w in warehouses])
        wh_filter = f"AND WAREHOUSE_NAME IN ({wh_list})"
    query = f"""
        SELECT
            WAREHOUSE_NAME,
            SUM(CREDITS_USED) AS TOTAL_CREDITS_USED
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= '{_start}'
          AND START_TIME <= '{_end}'
          {wh_filter}
        GROUP BY WAREHOUSE_NAME
    """
    return session.sql(query).to_pandas()


@st.cache_data(ttl=600)
def get_current_sizes():
    """Run SHOW WAREHOUSES to retrieve each warehouse's current size.
    This is needed so the Cortex AI recommendation can suggest the next
    size up or down relative to the current configuration."""
    session.sql("SHOW WAREHOUSES").collect()
    df = session.sql(
        'SELECT "name" AS WAREHOUSE_NAME, "size" AS WAREHOUSE_SIZE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))'
    ).to_pandas()
    return df

# ---------------------------------------------------------------------------
# FETCH DATA
# Convert selected warehouses to a tuple for caching (lists are unhashable).
# ---------------------------------------------------------------------------
wh_tuple = tuple(selected_warehouses) if selected_warehouses else ()
load_df = get_load_history(str(start_date), str(end_date), wh_tuple)
metering_df = get_metering_history(str(start_date), str(end_date), wh_tuple)
sizes_df = get_current_sizes()

# ---------------------------------------------------------------------------
# BUILD SUMMARY DATAFRAME
# Aggregate per-warehouse metrics from the load history and join with
# metering (credit) data and current warehouse sizes.  This produces one
# row per warehouse with all the metrics needed for the summary table and
# the Cortex AI recommendation prompt.
# ---------------------------------------------------------------------------
if load_df.empty:
    st.warning("No warehouse load data found for the selected filters.")
    st.stop()

# Compute per-warehouse aggregates from the 5-minute load history intervals.
agg_df = (
    load_df.groupby("WAREHOUSE_NAME")
    .agg(
        AVG_RUNNING=("AVG_RUNNING", "mean"),
        AVG_QUEUED_LOAD=("AVG_QUEUED_LOAD", "mean"),
        PEAK_RUNNING=("AVG_RUNNING", "max"),
    )
    .reset_index()
)

# Build sparkline data: for each warehouse, collect the AVG_RUNNING values
# over time as a list. st.column_config.LineChartColumn renders these inline.
sparkline_data = (
    load_df.sort_values("START_TIME")
    .groupby("WAREHOUSE_NAME")["AVG_RUNNING"]
    .apply(list)
    .reset_index()
    .rename(columns={"AVG_RUNNING": "LOAD_TREND"})
)

# Merge all data sources into one summary DataFrame.
summary_df = agg_df.merge(metering_df, on="WAREHOUSE_NAME", how="left")
summary_df = summary_df.merge(sizes_df, on="WAREHOUSE_NAME", how="left")
summary_df = summary_df.merge(sparkline_data, on="WAREHOUSE_NAME", how="left")

# Fill missing credits (warehouse may have load but no metering in the range).
summary_df["TOTAL_CREDITS_USED"] = summary_df["TOTAL_CREDITS_USED"].fillna(0)
summary_df["WAREHOUSE_SIZE"] = summary_df["WAREHOUSE_SIZE"].fillna("Unknown")

# Sort by credits used descending so the most expensive warehouses appear first.
summary_df = summary_df.sort_values("TOTAL_CREDITS_USED", ascending=False).reset_index(drop=True)

# ---------------------------------------------------------------------------
# CORTEX AI SIZING RECOMMENDATIONS
# For each warehouse, call SNOWFLAKE.CORTEX.COMPLETE with the llama3.1-70b
# model to generate a sizing recommendation.  The prompt includes all
# relevant metrics plus the user-configured thresholds so the model can
# make context-aware decisions.
# ---------------------------------------------------------------------------

# System prompt instructs the LLM to act as a Snowflake optimization expert
# and return structured JSON with the action, recommendation, rationale, and SQL.
SYSTEM_PROMPT = """You are a Snowflake warehouse optimization expert. Given the warehouse metrics below, determine if the warehouse should be sized UP, sized DOWN, or kept the same. Provide:
1. A one-sentence recommendation (e.g. SIZE DOWN for {warehouse_name}).
2. A brief rationale (2-3 sentences max).
3. The exact ALTER WAREHOUSE SQL command if a change is needed, using the fully qualified warehouse name.
Rules:
- If avg_queued_load > threshold, recommend sizing UP.
- If avg_running < utilization_floor AND avg_queued_load is near 0, recommend sizing DOWN.
- Map sizes: XS < S < M < L < XL < 2XL < 3XL < 4XL.
- Never recommend below X-Small.
Respond in JSON with keys: action, recommendation, rationale, sql"""


@st.cache_data(ttl=600)
def get_cortex_recommendation(
    warehouse_name: str,
    current_size: str,
    avg_running: float,
    avg_queued: float,
    peak_running: float,
    credits_used: float,
    _queued_threshold: float,
    _util_floor: int,
):
    """Call Snowflake Cortex COMPLETE to get a sizing recommendation.
    Uses the llama3.1-70b model which is capable of structured JSON output.
    The function escapes single quotes in the prompts to prevent SQL injection."""

    # Build the user prompt with all warehouse metrics for the LLM.
    user_prompt = (
        f"Warehouse: {warehouse_name}\n"
        f"Current Size: {current_size}\n"
        f"Avg Running: {avg_running:.4f}\n"
        f"Avg Queued Load: {avg_queued:.4f}\n"
        f"Peak Running: {peak_running:.4f}\n"
        f"Total Credits Used: {credits_used:.2f}\n"
        f"Queued Load Threshold: {_queued_threshold}\n"
        f"Utilization Floor: {_util_floor}%"
    )

    # Escape single quotes for safe SQL embedding.
    safe_system = SYSTEM_PROMPT.replace("'", "\\'")
    safe_user = user_prompt.replace("'", "\\'")

    # SNOWFLAKE.CORTEX.COMPLETE accepts a model name, a system prompt (via
    # the options object or message array), and the user message.  We use
    # the two-argument form with a prompt string for simplicity.
    query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-70b',
            CONCAT('{safe_system}', '\n\nMetrics:\n', '{safe_user}')
        ) AS RECOMMENDATION
    """
    result = session.sql(query).to_pandas()
    return result["RECOMMENDATION"].iloc[0] if not result.empty else "{}"


def parse_recommendation(raw_response: str):
    """Parse the JSON response from Cortex COMPLETE.
    The LLM may wrap JSON in markdown code fences, so we strip those.
    Returns a dict with keys: action, recommendation, rationale, sql."""
    try:
        # Strip markdown code fences if present (```json ... ```)
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1]
        if cleaned.endswith("```"):
            cleaned = cleaned.rsplit("```", 1)[0]
        cleaned = cleaned.strip()
        return json.loads(cleaned)
    except (json.JSONDecodeError, IndexError):
        return {
            "action": "keep",
            "recommendation": "Unable to parse recommendation.",
            "rationale": raw_response[:200],
            "sql": "",
        }


# Generate recommendations for all warehouses.
# We iterate over the summary DataFrame and call Cortex for each warehouse.
recommendations = {}
with st.spinner("Generating Cortex AI sizing recommendations..."):
    for _, row in summary_df.iterrows():
        wh_name = row["WAREHOUSE_NAME"]
        raw = get_cortex_recommendation(
            warehouse_name=wh_name,
            current_size=row["WAREHOUSE_SIZE"],
            avg_running=float(row["AVG_RUNNING"]),
            avg_queued=float(row["AVG_QUEUED_LOAD"]),
            peak_running=float(row["PEAK_RUNNING"]),
            credits_used=float(row["TOTAL_CREDITS_USED"]),
            _queued_threshold=queued_threshold,
            _util_floor=utilization_floor,
        )
        recommendations[wh_name] = parse_recommendation(raw)

# ---------------------------------------------------------------------------
# CLASSIFY EACH WAREHOUSE
# Map the Cortex AI action to one of three categories used by the filter
# toolbar: "size_down", "size_up", or "keep".
# ---------------------------------------------------------------------------
def classify_action(action_str: str) -> str:
    """Normalize the LLM's action string to a standard category.
    The LLM may return variations like 'SIZE UP', 'size up', 'Size Up', etc."""
    lower = action_str.lower().strip()
    if "up" in lower:
        return "size_up"
    elif "down" in lower:
        return "size_down"
    else:
        return "keep"

summary_df["ACTION"] = summary_df["WAREHOUSE_NAME"].apply(
    lambda wh: classify_action(recommendations.get(wh, {}).get("action", "keep"))
)
summary_df["RECOMMENDATION"] = summary_df["WAREHOUSE_NAME"].apply(
    lambda wh: recommendations.get(wh, {}).get("recommendation", "N/A")
)
summary_df["RATIONALE"] = summary_df["WAREHOUSE_NAME"].apply(
    lambda wh: recommendations.get(wh, {}).get("rationale", "N/A")
)
summary_df["SQL"] = summary_df["WAREHOUSE_NAME"].apply(
    lambda wh: recommendations.get(wh, {}).get("sql", "")
)

# ---------------------------------------------------------------------------
# COMPUTE CATEGORY COUNTS
# Count how many warehouses fall into each recommendation category.
# These counts are displayed in the filter dropdown labels.
# ---------------------------------------------------------------------------
count_all = len(summary_df)
count_down = len(summary_df[summary_df["ACTION"] == "size_down"])
count_up = len(summary_df[summary_df["ACTION"] == "size_up"])
count_keep = len(summary_df[summary_df["ACTION"] == "keep"])

# Count warehouses that have an actionable sizing change (not "keep").
actionable_df = summary_df[summary_df["ACTION"] != "keep"]
num_actionable = len(actionable_df)

# ---------------------------------------------------------------------------
# RECOMMENDED SQL MODAL
# When clicked, shows a modal dialog with all ALTER WAREHOUSE statements
# for warehouses that need a sizing change.  This lets DBAs review and
# copy the SQL before executing it.
# ---------------------------------------------------------------------------
@st.dialog("Recommended SQL")
def show_sql_modal(title_count: int):
    """Display a modal with ALTER WAREHOUSE statements for all actionable
    warehouses. Each statement is preceded by a comment showing the action
    and warehouse name for clarity.
    The title_count parameter is used in the header inside the modal."""
    st.subheader(f"Recommended SQL for {title_count} Insights")
    sql_lines = []
    for _, row in actionable_df.iterrows():
        action_label = row["ACTION"].replace("_", " ").upper()
        wh_name = row["WAREHOUSE_NAME"]
        sql_stmt = row["SQL"]
        if sql_stmt:
            sql_lines.append(f"-- {action_label} for {wh_name}")
            sql_lines.append(f"{sql_stmt};")
            sql_lines.append("")
    if sql_lines:
        st.code("\n".join(sql_lines), language="sql")
    else:
        st.info("No sizing changes recommended.")
    if st.button("Close"):
        st.rerun()

# ---------------------------------------------------------------------------
# ACTION TOOLBAR
# A row of controls above the summary table that let the user:
# 1. Open the recommended SQL modal for all actionable warehouses.
# 2. Filter the table by recommendation category.
# 3. Select/deselect all visible rows.
# 4. See how many rows are currently selected.
# ---------------------------------------------------------------------------

# Initialize session state for row selection and filter.
if "selected_rows" not in st.session_state:
    st.session_state.selected_rows = set()
if "category_filter" not in st.session_state:
    st.session_state.category_filter = "all"

# Build the filter options with counts and descriptions.
filter_options = {
    f"All ({count_all})": "all",
    f"Size down ({count_down})": "size_down",
    f"Size up ({count_up})": "size_up",
    f"Keep ({count_keep})": "keep",
}
filter_descriptions = {
    f"All ({count_all})": "Show all insights",
    f"Size down ({count_down})": "Warehouses where most queries are not using all compute power available",
    f"Size up ({count_up})": "Warehouses where queries are frequently queuing for compute",
    f"Keep ({count_keep})": "Warehouses that are appropriately sized",
}

# Toolbar layout: four columns for the controls.
toolbar_cols = st.columns([2, 3, 2, 3])

# -- Action All Button --
# Opens the modal showing all recommended ALTER WAREHOUSE statements.
with toolbar_cols[0]:
    if st.button("Action All", type="primary"):
        show_sql_modal(title_count=num_actionable)
    if st.button(f"Recommended SQL for {num_actionable} Insights"):
        show_sql_modal(title_count=num_actionable)

# -- Filter Dropdown --
# Filters the summary table rows by the selected recommendation category.
# Each option shows the count and a description as help text.
with toolbar_cols[1]:
    selected_filter_label = st.selectbox(
        "Filters",
        options=list(filter_options.keys()),
        index=0,
        help="\n".join([f"**{k}**: {v}" for k, v in filter_descriptions.items()]),
    )
    active_filter = filter_options[selected_filter_label]

# -- Select All Toggle --
# Toggles selection of all currently visible (filtered) rows.
with toolbar_cols[2]:
    if st.button("Select All"):
        filtered_temp = (
            summary_df
            if active_filter == "all"
            else summary_df[summary_df["ACTION"] == active_filter]
        )
        visible_names = set(filtered_temp["WAREHOUSE_NAME"].tolist())
        if visible_names.issubset(st.session_state.selected_rows):
            st.session_state.selected_rows -= visible_names
        else:
            st.session_state.selected_rows |= visible_names
        st.rerun()

# -- Selected Row Count --
with toolbar_cols[3]:
    st.markdown(f"**{len(st.session_state.selected_rows)} row(s) selected**")

# ---------------------------------------------------------------------------
# APPLY FILTER
# Subset the summary DataFrame based on the active toolbar filter.
# ---------------------------------------------------------------------------
if active_filter == "all":
    display_df = summary_df.copy()
else:
    display_df = summary_df[summary_df["ACTION"] == active_filter].copy()

# ---------------------------------------------------------------------------
# WAREHOUSE SUMMARY TABLE
# Display the filtered summary as an interactive dataframe with:
# - Checkbox column for row selection
# - Sparkline chart showing load trend over time
# - Numeric columns for key sizing metrics
# - Recommendation text from Cortex AI
#
# Uses st.column_config for rich column types including LineChartColumn
# for inline sparklines.
# ---------------------------------------------------------------------------

# Add a checkbox column based on session state selections.
display_df["SELECTED"] = display_df["WAREHOUSE_NAME"].apply(
    lambda wh: wh in st.session_state.selected_rows
)

# Reorder columns for display.
table_df = display_df[
    [
        "SELECTED",
        "WAREHOUSE_NAME",
        "LOAD_TREND",
        "WAREHOUSE_SIZE",
        "AVG_RUNNING",
        "AVG_QUEUED_LOAD",
        "TOTAL_CREDITS_USED",
        "PEAK_RUNNING",
        "RECOMMENDATION",
    ]
].copy()

# Round numeric columns for readability.
table_df["AVG_RUNNING"] = table_df["AVG_RUNNING"].round(4)
table_df["AVG_QUEUED_LOAD"] = table_df["AVG_QUEUED_LOAD"].round(4)
table_df["TOTAL_CREDITS_USED"] = table_df["TOTAL_CREDITS_USED"].round(2)
table_df["PEAK_RUNNING"] = table_df["PEAK_RUNNING"].round(4)

# Configure column display using Streamlit's column_config API.
# LineChartColumn renders the LOAD_TREND list as a mini sparkline chart.
column_config = {
    "SELECTED": st.column_config.CheckboxColumn(
        "Select",
        help="Select this warehouse for batch actions",
        default=False,
    ),
    "WAREHOUSE_NAME": st.column_config.TextColumn(
        "Warehouse",
        help="Virtual warehouse name",
    ),
    "LOAD_TREND": st.column_config.LineChartColumn(
        "Load Trend",
        help="AVG_RUNNING over time (sparkline from WAREHOUSE_LOAD_HISTORY)",
        width="medium",
    ),
    "WAREHOUSE_SIZE": st.column_config.TextColumn(
        "Size",
        help="Current warehouse size from SHOW WAREHOUSES",
    ),
    "AVG_RUNNING": st.column_config.NumberColumn(
        "Avg Running",
        help="Average AVG_RUNNING across all 5-min intervals",
        format="%.4f",
    ),
    "AVG_QUEUED_LOAD": st.column_config.NumberColumn(
        "Avg Queued Load",
        help="Average AVG_QUEUED_LOAD — high values indicate need for more compute",
        format="%.4f",
    ),
    "TOTAL_CREDITS_USED": st.column_config.NumberColumn(
        "Credits Used",
        help="Total credits consumed from WAREHOUSE_METERING_HISTORY",
        format="%.2f",
    ),
    "PEAK_RUNNING": st.column_config.NumberColumn(
        "Peak Running",
        help="Maximum AVG_RUNNING seen in any single interval",
        format="%.4f",
    ),
    "RECOMMENDATION": st.column_config.TextColumn(
        "Recommend",
        help="Cortex AI sizing recommendation (llama3.1-70b)",
        width="large",
    ),
}

# Render the interactive data table.
# on_select callback is not available in SiS; we use data_editor for checkboxes.
edited_df = st.data_editor(
    table_df,
    column_config=column_config,
    use_container_width=True,
    hide_index=True,
    disabled=[
        "WAREHOUSE_NAME",
        "LOAD_TREND",
        "WAREHOUSE_SIZE",
        "AVG_RUNNING",
        "AVG_QUEUED_LOAD",
        "TOTAL_CREDITS_USED",
        "PEAK_RUNNING",
        "RECOMMENDATION",
    ],
    key="warehouse_table",
)

# Update session state based on checkbox changes from the data editor.
if edited_df is not None:
    for _, row in edited_df.iterrows():
        wh = row["WAREHOUSE_NAME"]
        if row["SELECTED"]:
            st.session_state.selected_rows.add(wh)
        else:
            st.session_state.selected_rows.discard(wh)

# ---------------------------------------------------------------------------
# PER-WAREHOUSE RECOMMENDATION DETAILS
# Show expandable sections for each warehouse with the full Cortex AI
# recommendation including rationale and SQL.  Users can click "Recommend"
# to see the individual ALTER WAREHOUSE statement.
# ---------------------------------------------------------------------------
st.subheader("Detailed Recommendations")

for _, row in display_df.iterrows():
    wh_name = row["WAREHOUSE_NAME"]
    rec = recommendations.get(wh_name, {})
    action = rec.get("action", "keep").upper()
    rationale = rec.get("rationale", "N/A")
    sql = rec.get("sql", "")

    # Color-code the action badge for quick visual scanning.
    if "UP" in action:
        badge = "🔴"
    elif "DOWN" in action:
        badge = "🟡"
    else:
        badge = "🟢"

    with st.expander(f"{badge} {wh_name} — {action}"):
        st.markdown(f"**Recommendation:** {rec.get('recommendation', 'N/A')}")
        st.markdown(f"**Rationale:** {rationale}")
        if sql:
            st.code(sql, language="sql")
        else:
            st.info("No SQL change needed — warehouse is appropriately sized.")
