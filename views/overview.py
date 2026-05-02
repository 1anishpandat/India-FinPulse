"""
=============================================================
  FinPulse — Page 1: UPI Growth Analytics
  views/overview.py
=============================================================

WHAT THIS PAGE SHOWS:
  1. KPI cards     — headline numbers at the top
  2. Volume trend  — monthly transaction count over time
  3. Value trend   — rupee value over time
  4. YoY growth    — year-over-year % change bar chart
  5. State map     — which states drive most UPI volume
  6. Bank share    — pie chart of bank market share

STREAMLIT CONCEPT:
  Streamlit reruns this entire file top-to-bottom every time
  the user interacts with anything (clicks, sliders, etc.).
  That's why we use @st.cache_data — to avoid re-querying
  the database on every single interaction.
=============================================================
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db import get_monthly_aggregates, get_state_summary, get_bank_summary


# =============================================================
# SECTION 1: CACHED DATA LOADERS
# =============================================================

@st.cache_data(ttl=300)
def load_monthly() -> pd.DataFrame:
    """
    @st.cache_data stores the result after first call.
    Next 300 seconds (ttl), it returns the stored result
    instead of re-querying SQLite.

    WHY THIS MATTERS:
      Without caching, every slider drag = new DB query.
      With caching, the first query runs once and results
      are reused — making the app feel instant.
    """
    df = get_monthly_aggregates()
    # Create a proper datetime column for Plotly's x-axis
    df["date"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"].astype(str) + "-01"
    )
    return df


@st.cache_data(ttl=300)
def load_states() -> pd.DataFrame:
    return get_state_summary()


@st.cache_data(ttl=300)
def load_banks() -> pd.DataFrame:
    return get_bank_summary()


# =============================================================
# SECTION 2: KPI CARD HELPER
# =============================================================

def kpi_card(col, label: str, value: str, delta: str = "", delta_good: bool = True):
    """
    Renders a single KPI metric card using Streamlit's
    st.metric component.

    Parameters
    ----------
    col        : the st.column to render inside
    label      : title of the metric
    value      : the main number (formatted string)
    delta      : change vs previous period (optional)
    delta_good : if True, positive delta is green (default)
                 if False, positive delta is red (e.g. fraud rate)
    """
    with col:
        st.metric(
            label=label,
            value=value,
            delta=delta,
            delta_color="normal" if delta_good else "inverse"
        )


# =============================================================
# SECTION 3: CHART BUILDERS
# =============================================================

def chart_volume_trend(df: pd.DataFrame, year_filter: list) -> go.Figure:
    """
    Line chart: monthly UPI transaction volume over time.
    Filtered by the year_filter the user selects in sidebar.
    """
    filtered = df[df["year"].isin(year_filter)]

    fig = px.area(
        filtered,
        x="date",
        y="volume_crore",
        title="Monthly UPI Transaction Volume (Crore)",
        labels={"volume_crore": "Volume (Crore)", "date": "Month"},
        color_discrete_sequence=["#5C6BC0"],
    )

    # Add markers at each data point
    fig.update_traces(mode="lines+markers", marker=dict(size=5))

    # Highlight festival months with vertical lines
    festival_months = filtered[filtered["festival_month"] == 1]
    for _, row in festival_months.iterrows():
        fig.add_vline(
            x=str(row["date"]),   # ← convert Timestamp to string
            line_dash="dot",
            line_color="orange",
            opacity=0.6,
        )

    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        showlegend=False,
        margin=dict(t=50, b=20, l=10, r=10),
    )
    return fig


def chart_value_trend(df: pd.DataFrame, year_filter: list) -> go.Figure:
    """
    Line chart: monthly UPI transaction value in ₹ lakh crore.
    Uses a different color to distinguish from volume chart.
    """
    filtered = df[df["year"].isin(year_filter)]

    fig = px.area(
        filtered,
        x="date",
        y="value_lakh_crore",
        title="Monthly UPI Transaction Value (₹ Lakh Crore)",
        labels={"value_lakh_crore": "Value (₹ Lakh Cr)", "date": "Month"},
        color_discrete_sequence=["#26A69A"],
    )
    fig.update_traces(mode="lines+markers", marker=dict(size=5))
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        showlegend=False,
        margin=dict(t=50, b=20, l=10, r=10),
    )
    return fig


def chart_yoy_growth(df: pd.DataFrame) -> go.Figure:
    """
    Bar chart: Year-over-Year % growth per month.
    Bars colored green (positive growth) or red (decline).

    WHY THIS CHART?
      The raw volume numbers keep growing, so it's hard to
      see if growth is accelerating or slowing down.
      YoY % strips out the absolute size and shows the RATE
      of change — much more meaningful for analysis.
    """
    yoy_df = df.dropna(subset=["yoy_growth_pct"]).copy()
    yoy_df["color"] = yoy_df["yoy_growth_pct"].apply(
        lambda x: "#26A69A" if x >= 0 else "#EF5350"
    )

    fig = go.Figure(go.Bar(
        x=yoy_df["date"],
        y=yoy_df["yoy_growth_pct"],
        marker_color=yoy_df["color"],
        hovertemplate="<b>%{x|%b %Y}</b><br>YoY Growth: %{y:.1f}%<extra></extra>",
    ))

    fig.update_layout(
        title="Year-over-Year Growth % (Volume)",
        xaxis_title="Month",
        yaxis_title="YoY Growth %",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=50, b=20, l=10, r=10),
    )
    # Add zero line
    fig.add_hline(y=0, line_dash="solid", line_color="gray", opacity=0.4)
    return fig


def chart_state_bar(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar chart: transaction count by state.
    Horizontal bars are easier to read when labels are long.
    """
    df_sorted = df.sort_values("txn_count", ascending=True)

    fig = px.bar(
        df_sorted,
        x="txn_count",
        y="state",
        orientation="h",
        title="Transaction Count by State",
        labels={"txn_count": "Transactions", "state": "State"},
        color="txn_count",
        color_continuous_scale="Blues",
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        margin=dict(t=50, b=20, l=10, r=10),
        yaxis=dict(tickfont=dict(size=11)),
    )
    return fig


def chart_bank_pie(df: pd.DataFrame) -> go.Figure:
    """
    Donut chart: bank market share by volume.
    Donut (hole=0.4) is more modern than a full pie chart
    and easier to read when slices are small.
    """
    fig = px.pie(
        df,
        names="bank",
        values="avg_share",
        title="Bank Market Share (% of UPI Volume)",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_traces(
        textposition="outside",
        textinfo="percent+label",
        hovertemplate="<b>%{label}</b><br>Share: %{value:.1f}%<extra></extra>",
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        margin=dict(t=50, b=20, l=10, r=10),
    )
    return fig


def chart_avg_txn_value(df: pd.DataFrame, year_filter: list) -> go.Figure:
    """
    Line chart: average transaction value (₹) per month.
    This is a KEY business metric — rising avg value means
    people are trusting UPI for larger purchases.
    """
    filtered = df[df["year"].isin(year_filter)]

    fig = px.line(
        filtered,
        x="date",
        y="avg_txn_value",
        title="Average Transaction Value (₹) per Month",
        labels={"avg_txn_value": "Avg Value (₹)", "date": "Month"},
        markers=True,
        color_discrete_sequence=["#AB47BC"],
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        margin=dict(t=50, b=20, l=10, r=10),
    )
    return fig


# =============================================================
# SECTION 4: MAIN PAGE RENDERER
# =============================================================

def render():
    """
    This is the function app.py calls to render Page 1.
    Everything from here down is what the user sees.

    STREAMLIT RENDERING ORDER:
      Top to bottom — exactly like reading a document.
      st.title → st.columns → st.plotly_chart → etc.
    """

    # ── Page header ────────────────────────────────────────
    st.title("📈 UPI Growth Analytics")
    st.markdown(
        "India's UPI ecosystem analyzed across **33 months** "
        "(Apr 2022 – Dec 2024) using real NPCI aggregate data."
    )
    st.divider()

    # ── Load data ──────────────────────────────────────────
    monthly_df = load_monthly()
    state_df   = load_states()
    bank_df    = load_banks()

    # ── Sidebar filters ────────────────────────────────────
    # st.sidebar adds widgets to the left panel
    # These filters affect all charts on this page
    st.sidebar.header("🔧 Filters")

    all_years = sorted(monthly_df["year"].unique().tolist())
    year_filter = st.sidebar.multiselect(
        "Select Year(s)",
        options=all_years,
        default=all_years,       # all years selected by default
        help="Filter all charts by financial year"
    )

    if not year_filter:
        st.warning("Please select at least one year from the sidebar.")
        return

    # ── KPI Cards ──────────────────────────────────────────
    # st.columns(4) creates 4 equal-width columns side by side
    st.subheader("Key Performance Indicators")
    c1, c2, c3, c4 = st.columns(4)

    filtered_monthly = monthly_df[monthly_df["year"].isin(year_filter)]

    total_volume = filtered_monthly["volume_crore"].sum()
    total_value  = filtered_monthly["value_lakh_crore"].sum()
    avg_txn_val  = filtered_monthly["avg_txn_value"].mean()
    peak_month   = filtered_monthly.loc[
        filtered_monthly["volume_crore"].idxmax(), "month_name"
    ]

    kpi_card(c1, "Total UPI Volume",
             f"{total_volume:,.0f} Cr",
             "+58% YoY FY24")

    kpi_card(c2, "Total Value Processed",
             f"₹{total_value:,.1f} L Cr",
             "+44% YoY FY24")

    kpi_card(c3, "Avg Transaction Value",
             f"₹{avg_txn_val:,.0f}",
             "+8% vs FY23")

    kpi_card(c4, "Peak Month",
             peak_month,
             "Highest volume")

    st.divider()

    # ── Volume & Value Trend Charts (side by side) ─────────
    st.subheader("Transaction Trends Over Time")

    # Two charts in two columns
    col_left, col_right = st.columns(2)
    with col_left:
        st.plotly_chart(
            chart_volume_trend(monthly_df, year_filter),
            use_container_width=True   # fills the column width
        )
    with col_right:
        st.plotly_chart(
            chart_value_trend(monthly_df, year_filter),
            use_container_width=True
        )

    # ── Avg Transaction Value (full width) ────────────────
    st.plotly_chart(
        chart_avg_txn_value(monthly_df, year_filter),
        use_container_width=True
    )

    st.divider()

    # ── YoY Growth Chart (full width) ─────────────────────
    st.subheader("Year-over-Year Growth Analysis")
    st.plotly_chart(
        chart_yoy_growth(monthly_df),
        use_container_width=True
    )

    # ── Key insight callout ────────────────────────────────
    # st.info creates a blue callout box
    st.info(
        "💡 **Key Insight:** UPI volumes grew **58% YoY** in FY2024. "
        "October consistently shows the highest spike due to Diwali "
        "festival spending — marked with 🎉 on the trend charts above."
    )

    st.divider()

    # ── State & Bank Charts (side by side) ────────────────
    st.subheader("Geographic & Bank Distribution")

    col_state, col_bank = st.columns([3, 2])
    # [3, 2] means left column is 60% wide, right is 40%
    # Useful when one chart needs more space

    with col_state:
        st.plotly_chart(
            chart_state_bar(state_df),
            use_container_width=True
        )
    with col_bank:
        st.plotly_chart(
            chart_bank_pie(bank_df),
            use_container_width=True
        )

    # ── Raw data expander ─────────────────────────────────
    # st.expander hides content behind a clickable toggle
    # Good for showing raw data without cluttering the page
    with st.expander("📋 View Raw Monthly Data"):
        display_df = filtered_monthly[[
            "month_name", "volume_crore", "value_lakh_crore",
            "avg_txn_value", "yoy_growth_pct", "festival_month"
        ]].copy()
        display_df.columns = [
            "Month", "Volume (Cr)", "Value (₹ L Cr)",
            "Avg Txn (₹)", "YoY Growth %", "Festival Month"
        ]
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )

    # ── Footer insight ─────────────────────────────────────
    st.caption(
        "Data sources: NPCI UPI Product Statistics (real aggregates) · "
        "Synthetic transaction-level data anchored to NPCI totals"
    )
