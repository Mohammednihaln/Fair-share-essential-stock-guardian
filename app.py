import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# --------------------------------------------------
# Page setup
# --------------------------------------------------
st.set_page_config(
    page_title="Fair-Share Essential Stock Guardian",
    layout="wide"
)

st.title("🛡️ Fair-Share Essential Stock Guardian")
st.caption("AI-for-Good: Fair, explainable allocation of essential supplies")

session = get_active_session()

# --------------------------------------------------
# Load priority data
# --------------------------------------------------
@st.cache_data
def load_priority_data():
    query = """
    SELECT
        date,
        location_name,
        item_name,
        days_to_stockout,
        vulnerability_score,
        fair_priority_score
    FROM FAIR_STOCK_PRIORITY
    """
    return session.sql(query).to_pandas()

df_priority = load_priority_data()

# --------------------------------------------------
# Sidebar filters
# --------------------------------------------------
st.sidebar.header("Filters")

available_dates = sorted(df_priority["DATE"].unique(), reverse=True)

selected_date = st.sidebar.selectbox(
    "Select Date",
    available_dates,
    index=0
)

selected_items = st.sidebar.multiselect(
    "Select Items",
    df_priority["ITEM_NAME"].unique(),
    default=list(df_priority["ITEM_NAME"].unique())
)

filtered_df = df_priority[
    (df_priority["DATE"] == selected_date) &
    (df_priority["ITEM_NAME"].isin(selected_items))
]

# --------------------------------------------------
# AI Summary (show early)
# --------------------------------------------------
@st.cache_data
def load_ai_summary_data():
    query = """
    SELECT
        location_name,
        item_name,
        days_to_stockout,
        vulnerability_score,
        fair_priority_score
    FROM FAIR_STOCK_PRIORITY
    WHERE risk_flag = 'HIGH'
    ORDER BY fair_priority_score DESC
    LIMIT 5
    """
    return session.sql(query).to_pandas()

summary_df = load_ai_summary_data()

def build_summary_text(df):
    if df.empty:
        return "All locations currently have sufficient stock levels."

    locations = ", ".join(df["LOCATION_NAME"].unique())
    items = ", ".join(df["ITEM_NAME"].unique())
    min_days = round(df["DAYS_TO_STOCKOUT"].min(), 1)

    return (
        f"{locations} are the highest priority locations due to "
        f"high vulnerability scores and projected stockouts of {items} "
        f"within approximately {min_days} days. "
        "Immediate redistribution or reorder is recommended."
    )

st.subheader("🤖 Today’s AI Summary")
st.info(build_summary_text(summary_df))

# --------------------------------------------------
# Priority overview
# --------------------------------------------------
st.subheader("📊 Priority Overview")
st.caption(f"Showing {len(filtered_df)} records for {selected_date}")

st.dataframe(filtered_df, use_container_width=True)

# --------------------------------------------------
# Heatmap
# --------------------------------------------------
st.subheader("🔥 Fair Priority Heatmap")

if filtered_df.empty:
    st.warning("No data available for the selected filters.")
else:
    heatmap_data = filtered_df.groupby(
        ["LOCATION_NAME", "ITEM_NAME"],
        as_index=False
    )["FAIR_PRIORITY_SCORE"].max()

    heatmap = alt.Chart(heatmap_data).mark_rect().encode(
        x=alt.X("ITEM_NAME:N", title="Item"),
        y=alt.Y("LOCATION_NAME:N", title="Location"),
        color=alt.Color(
            "FAIR_PRIORITY_SCORE:Q",
            scale=alt.Scale(scheme="reds"),
            title="Priority Score"
        ),
        tooltip=[
            "LOCATION_NAME",
            "ITEM_NAME",
            "FAIR_PRIORITY_SCORE"
        ]
    ).properties(height=400)

    st.altair_chart(heatmap, use_container_width=True)

# --------------------------------------------------
# Action recommendations
# --------------------------------------------------
@st.cache_data
def load_actions():
    query = """
    SELECT
        location_name,
        item_name,
        action_type,
        action_quantity,
        action_reason
    FROM ACTION_RECOMMENDATIONS
    """
    return session.sql(query).to_pandas()

df_actions = load_actions()

st.subheader("⚠️ Recommended Actions")
st.dataframe(df_actions, use_container_width=True)

csv = df_actions.to_csv(index=False).encode("utf-8")

st.download_button(
    label="⬇️ Download Action Plan (CSV)",
    data=csv,
    file_name="fair_stock_action_plan.csv",
    mime="text/csv"
)

# --------------------------------------------------
# Automation status
# --------------------------------------------------
st.subheader("⚙️ Automation Status")

log_df = session.sql(
    "SELECT * FROM PIPELINE_LOG ORDER BY run_time DESC LIMIT 5"
).to_pandas()

st.dataframe(log_df, use_container_width=True)
