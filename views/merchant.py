"""
=============================================================
  FinPulse — Page 2: Merchant Intelligence
  views/merchant.py
=============================================================

WHAT THIS PAGE SHOWS:
  1. KPI cards       — P2M vs P2P headline numbers
  2. P2P vs P2M donut — transaction type split
  3. Category bar    — which merchant categories dominate
  4. Monthly trend   — how P2M grew vs P2P over time
  5. Avg ticket size — which categories have highest spend
  6. State heatmap   — category spending by state
  7. YoY category    — which categories grew fastest

NEW CONCEPTS TODAY:
  - pd.pivot_table()  — reshape data from long to wide format
  - px.imshow()       — heatmap from a 2D matrix
  - go.Scatter with stackgroup — stacked area chart
  - barmode="group"   — grouped bars side by side
  - st.tabs()         — tabbed layout inside a page
  - lambda in .apply()— format numbers in a DataFrame column
=============================================================
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db import run_query


# =============================================================
# SECTION 1: DATA LOADERS (all cached)
# =============================================================

@st.cache_data(ttl=300)
def load_category_summary() -> pd.DataFrame:
    """
    Total transactions and value per merchant category.
    Excludes P2P — we only want real merchant categories.
    """
    return run_query("""
        SELECT
            merchant_category,
            COUNT(*)                        AS txn_count,
            ROUND(SUM(amount), 2)           AS total_value,
            ROUND(AVG(amount), 2)           AS avg_amount,
            ROUND(MIN(amount), 2)           AS min_amount,
            ROUND(MAX(amount), 2)           AS max_amount
        FROM   transactions
        WHERE  txn_type = 'P2M'
        GROUP  BY merchant_category
        ORDER  BY txn_count DESC
    """)


@st.cache_data(ttl=300)
def load_txn_type_split() -> pd.DataFrame:
    """Overall P2P vs P2M split — counts and values."""
    return run_query("""
        SELECT
            txn_type,
            COUNT(*)                        AS txn_count,
            ROUND(SUM(amount) / 1e7, 2)     AS total_value_cr,
            ROUND(AVG(amount), 2)           AS avg_amount
        FROM   transactions
        GROUP  BY txn_type
    """)


@st.cache_data(ttl=300)
def load_monthly_split() -> pd.DataFrame:
    """
    Monthly P2P vs P2M counts.
    We build a date column so Plotly can render a proper time axis.
    """
    df = run_query("""
        SELECT
            year,
            month,
            txn_type,
            COUNT(*)              AS txn_count,
            ROUND(SUM(amount), 2) AS total_value
        FROM   transactions
        GROUP  BY year, month, txn_type
        ORDER  BY year, month
    """)
    df["date"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"].astype(str) + "-01"
    )
    return df


@st.cache_data(ttl=300)
def load_state_category() -> pd.DataFrame:
    """
    Transaction counts by state AND merchant category.
    This long-format data gets pivoted into the heatmap matrix.
    """
    return run_query("""
        SELECT
            state,
            merchant_category,
            COUNT(*)              AS txn_count,
            ROUND(AVG(amount), 2) AS avg_amount
        FROM   transactions
        WHERE  txn_type = 'P2M'
        GROUP  BY state, merchant_category
        ORDER  BY state, txn_count DESC
    """)


@st.cache_data(ttl=300)
def load_yearly_category() -> pd.DataFrame:
    """Category performance by year — for YoY grouped bar chart."""
    return run_query("""
        SELECT
            year,
            merchant_category,
            COUNT(*)              AS txn_count,
            ROUND(SUM(amount), 2) AS total_value,
            ROUND(AVG(amount), 2) AS avg_amount
        FROM   transactions
        WHERE  txn_type = 'P2M'
        GROUP  BY year, merchant_category
        ORDER  BY year, txn_count DESC
    """)


# =============================================================
# SECTION 2: CHART BUILDERS
# =============================================================

def chart_txn_type_donut(df: pd.DataFrame) -> go.Figure:
    """
    Donut chart: P2P vs P2M share of total transactions.

    WHY DONUT OVER PIE?
      Donut charts remove the wedge area which our eyes
      estimate poorly. Arc length comparison is more accurate.
      The hole also gives space for a central label if needed.
    """
    fig = px.pie(
        df,
        names="txn_type",
        values="txn_count",
        hole=0.55,
        title="P2P vs P2M Transaction Split",
        color="txn_type",
        color_discrete_map={
            "P2M": "#5C6BC0",
            "P2P": "#26A69A",
        },
    )
    fig.update_traces(
        textposition="outside",
        textinfo="percent+label",
        hovertemplate="<b>%{label}</b><br>Transactions: %{value:,}<extra></extra>",
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        margin=dict(t=50, b=20, l=10, r=10),
    )
    return fig


def chart_category_bar(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar: transaction count per merchant category.

    WHY HORIZONTAL?
      Category names like 'Healthcare & Pharmacy' are long.
      Horizontal bars give the y-axis enough room for labels.
      Vertical bars would force rotated text which is harder
      to read quickly.
    """
    df_sorted = df.sort_values("txn_count", ascending=True)

    fig = px.bar(
        df_sorted,
        x="txn_count",
        y="merchant_category",
        orientation="h",
        title="Transaction Volume by Merchant Category",
        labels={
            "txn_count": "Number of Transactions",
            "merchant_category": "Category",
        },
        color="txn_count",
        color_continuous_scale="Blues",
        text="txn_count",
    )
    fig.update_traces(
        texttemplate="%{text:,}",
        textposition="outside",
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        margin=dict(t=50, b=20, l=10, r=20),
        xaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.15)"),
        yaxis=dict(tickfont=dict(size=11)),
    )
    return fig


def chart_avg_ticket(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar: average transaction value per category.
    Called 'average ticket size' in retail and payments industry.

    BUSINESS INSIGHT this chart reveals:
      High ticket + low frequency = Electronics, Travel
      Low ticket + high frequency = Groceries, Food
      Both are valuable — different monetization strategies.
    """
    df_sorted = df.sort_values("avg_amount", ascending=True)

    fig = px.bar(
        df_sorted,
        x="avg_amount",
        y="merchant_category",
        orientation="h",
        title="Average Ticket Size (₹) by Category",
        labels={
            "avg_amount": "Avg Transaction Value (₹)",
            "merchant_category": "Category",
        },
        color="avg_amount",
        color_continuous_scale="Purples",
        text="avg_amount",
    )
    fig.update_traces(
        texttemplate="₹%{text:,.0f}",
        textposition="outside",
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        margin=dict(t=50, b=20, l=10, r=20),
        yaxis=dict(tickfont=dict(size=11)),
    )
    return fig


def chart_monthly_trend(df: pd.DataFrame, year_filter: list) -> go.Figure:
    """
    Stacked area chart: P2P vs P2M monthly volumes over time.

    WHY STACKED AREA?
      Shows both individual trends AND total volume together.
      The total height = all transactions. The color split
      shows how composition is changing — P2M growing faster.

    KEY PLOTLY CONCEPT — stackgroup:
      Add stackgroup='one' to multiple Scatter traces.
      Plotly stacks all traces sharing the same stackgroup.
      fill='tonexty' fills the area between this trace
      and the previous one (or zero for the first trace).
    """
    filtered = df[df["year"].isin(year_filter)]
    p2m = filtered[filtered["txn_type"] == "P2M"]
    p2p = filtered[filtered["txn_type"] == "P2P"]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=p2m["date"],
        y=p2m["txn_count"],
        name="P2M (Merchant)",
        fill="tonexty",
        fillcolor="rgba(92, 107, 192, 0.4)",
        line=dict(color="#5C6BC0", width=2),
        stackgroup="one",
        hovertemplate="<b>P2M</b> %{x|%b %Y}: %{y:,}<extra></extra>",
    ))

    fig.add_trace(go.Scatter(
        x=p2p["date"],
        y=p2p["txn_count"],
        name="P2P (Transfer)",
        fill="tonexty",
        fillcolor="rgba(38, 166, 154, 0.4)",
        line=dict(color="#26A69A", width=2),
        stackgroup="one",
        hovertemplate="<b>P2P</b> %{x|%b %Y}: %{y:,}<extra></extra>",
    ))

    fig.update_layout(
        title="P2M vs P2P Monthly Transaction Trend",
        xaxis_title="Month",
        yaxis_title="Transaction Count",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        margin=dict(t=60, b=20, l=10, r=10),
    )
    return fig


def chart_state_heatmap(df: pd.DataFrame) -> go.Figure:
    """
    Heatmap: states (rows) x merchant categories (columns).
    Cell color intensity = transaction count.

    KEY CONCEPT — pd.pivot_table():
      Raw data is in LONG FORMAT:
        state       | category  | txn_count
        Maharashtra | Groceries | 20751
        Maharashtra | Food      | 14053

      Heatmaps need WIDE FORMAT (matrix):
                    | Groceries | Food | ...
        Maharashtra |   20751   | 14053| ...
        Karnataka   |   12767   |  ... | ...

      pivot_table() reshapes long → wide:
        index   = what becomes ROWS    (states)
        columns = what becomes COLUMNS (categories)
        values  = what fills CELLS     (txn_count)
        aggfunc = how to combine dupes (sum)
        fill_value = replaces NaN with 0
    """
    pivot = pd.pivot_table(
        df,
        index="state",
        columns="merchant_category",
        values="txn_count",
        aggfunc="sum",
        fill_value=0,
    )

    fig = px.imshow(
        pivot,
        title="Transaction Heatmap: State × Merchant Category",
        labels=dict(x="Category", y="State", color="Transactions"),
        color_continuous_scale="Blues",
        aspect="auto",
        text_auto=True,
    )
    fig.update_traces(
        texttemplate="%{z:,}",
        textfont=dict(size=9),
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=50, b=60, l=10, r=10),
        xaxis=dict(tickangle=-30, tickfont=dict(size=10)),
        coloraxis_showscale=False,
    )
    return fig


def chart_yoy_category(df: pd.DataFrame) -> go.Figure:
    """
    Grouped bar chart: category counts split by year.
    Shows which categories grew fastest YoY.

    barmode='group'  → bars side by side per category
    barmode='stack'  → bars stacked (shows composition)
    barmode='overlay'→ bars overlapping (rarely used)
    """
    df["year"] = df["year"].astype(str)   # treat year as label not number

    fig = px.bar(
        df,
        x="merchant_category",
        y="txn_count",
        color="year",
        barmode="group",
        title="Category Growth: Year-over-Year Comparison",
        labels={
            "txn_count": "Transactions",
            "merchant_category": "Category",
            "year": "Year",
        },
        color_discrete_sequence=["#90CAF9", "#5C6BC0", "#1A237E"],
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(tickangle=-30, tickfont=dict(size=10)),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        margin=dict(t=60, b=80, l=10, r=10),
    )
    return fig


def chart_value_share(df: pd.DataFrame) -> go.Figure:
    """
    Pie: each category's share of total P2M value in rupees.
    Different from count share — shows where the MONEY flows.

    Count share vs Value share reveals pricing power:
      Electronics: low count share, HIGH value share
      Groceries:   high count share, LOW value share
    """
    fig = px.pie(
        df,
        names="merchant_category",
        values="total_value",
        title="Share of Total P2M Value (₹) by Category",
        hole=0.3,
        color_discrete_sequence=px.colors.qualitative.Set3,
    )
    fig.update_traces(
        textposition="outside",
        textinfo="percent+label",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Value: ₹%{value:,.0f}<br>"
            "Share: %{percent}<extra></extra>"
        ),
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        margin=dict(t=50, b=20, l=10, r=10),
    )
    return fig


# =============================================================
# SECTION 3: MAIN PAGE RENDERER
# =============================================================

def render():
    """Entry point called by app.py when user clicks Merchant."""

    # ── Page header ────────────────────────────────────────
    st.title("🏪 Merchant Intelligence")
    st.markdown(
        "Deep dive into **P2M (Person-to-Merchant)** payment patterns — "
        "category trends, state behavior, and ticket size analysis."
    )
    st.divider()

    # ── Load data ──────────────────────────────────────────
    cat_df       = load_category_summary()
    split_df     = load_txn_type_split()
    monthly_df   = load_monthly_split()
    state_cat_df = load_state_category()
    yearly_df    = load_yearly_category()

    # ── Sidebar filters ────────────────────────────────────
    st.sidebar.header("🔧 Filters")

    all_years = sorted(monthly_df["year"].unique().tolist())
    year_filter = st.sidebar.multiselect(
        "Select Year(s)",
        options=all_years,
        default=all_years,
    )

    all_cats = cat_df["merchant_category"].tolist()
    cat_filter = st.sidebar.multiselect(
        "Select Categories",
        options=all_cats,
        default=all_cats,
        help="Filter category-level charts",
    )

    if not year_filter:
        st.warning("Please select at least one year from the sidebar.")
        return

    if not cat_filter:
        st.warning("Please select at least one category from the sidebar.")
        return

    # Apply filters
    cat_filtered   = cat_df[cat_df["merchant_category"].isin(cat_filter)]
    yearly_filtered = yearly_df[yearly_df["merchant_category"].isin(cat_filter)]

    # ── KPI Cards ──────────────────────────────────────────
    st.subheader("Key Metrics")

    total_txns  = split_df["txn_count"].sum()
    p2m_row     = split_df[split_df["txn_type"] == "P2M"].iloc[0]
    p2p_row     = split_df[split_df["txn_type"] == "P2P"].iloc[0]
    p2m_pct     = p2m_row["txn_count"] / total_txns * 100
    top_cat     = cat_df.iloc[0]["merchant_category"]

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric(
            "P2M Transactions",
            f"{p2m_row['txn_count']:,}",
            f"{p2m_pct:.1f}% of all UPI",
        )
    with c2:
        st.metric(
            "P2M Total Value",
            f"₹{p2m_row['total_value_cr']:,.0f} Cr",
            "Merchant payments",
        )
    with c3:
        st.metric(
            "Top Category",
            top_cat.split("&")[0].strip(),
            f"{cat_df.iloc[0]['txn_count']:,} transactions",
        )
    with c4:
        st.metric(
            "Avg Ticket — P2M",
            f"₹{p2m_row['avg_amount']:,.0f}",
            f"P2P avg ₹{p2p_row['avg_amount']:,.0f}",
        )

    st.divider()

    # ── Tabs ───────────────────────────────────────────────
    # st.tabs() creates a horizontal tab bar.
    # Content inside each 'with tab:' block only renders
    # when that tab is active — keeps the page clean.
    tab1, tab2, tab3 = st.tabs([
        "📊  Category Analysis",
        "📈  Trend Analysis",
        "🗺️  State Heatmap",
    ])

    # ── TAB 1: Category Analysis ───────────────────────────
    with tab1:
        col_l, col_r = st.columns(2)
        with col_l:
            st.plotly_chart(
                chart_txn_type_donut(split_df),
                use_container_width=True,
            )
        with col_r:
            st.plotly_chart(
                chart_value_share(cat_filtered),
                use_container_width=True,
            )

        st.plotly_chart(
            chart_category_bar(cat_filtered),
            use_container_width=True,
        )

        st.plotly_chart(
            chart_avg_ticket(cat_filtered),
            use_container_width=True,
        )

        st.info(
            "💡 **Key Insight:** P2M transactions account for "
            f"**{p2m_pct:.0f}%** of all UPI — up from ~40% in FY2022. "
            "Groceries leads in volume but Electronics leads in ticket size. "
            "UPI is becoming the default payment at physical stores, "
            "not just for online shopping."
        )

        with st.expander("📋 View Full Category Data"):
            display = cat_filtered.copy()
            display.columns = [
                "Category", "Transactions", "Total Value (₹)",
                "Avg Amount (₹)", "Min (₹)", "Max (₹)",
            ]
            # Lambda to format rupee values in the table
            for col in ["Total Value (₹)", "Avg Amount (₹)", "Min (₹)", "Max (₹)"]:
                display[col] = display[col].apply(lambda x: f"₹{x:,.0f}")
            st.dataframe(display, use_container_width=True, hide_index=True)

    # ── TAB 2: Trend Analysis ──────────────────────────────
    with tab2:
        st.plotly_chart(
            chart_monthly_trend(monthly_df, year_filter),
            use_container_width=True,
        )

        st.plotly_chart(
            chart_yoy_category(yearly_filtered),
            use_container_width=True,
        )

        st.info(
            "💡 **Key Insight:** P2M grew **3x faster** than P2P between "
            "FY2022 and FY2024. This reflects India's push toward merchant "
            "digitization — kirana stores, auto-rickshaws, and street vendors "
            "now accept UPI as the primary payment method."
        )

    # ── TAB 3: State Heatmap ───────────────────────────────
    with tab3:
        st.markdown(
            "Each cell = **number of transactions** for that "
            "state × category pair. Darker blue = more transactions."
        )

        state_filtered = state_cat_df[
            state_cat_df["merchant_category"].isin(cat_filter)
        ]

        st.plotly_chart(
            chart_state_heatmap(state_filtered),
            use_container_width=True,
        )

        st.subheader("Top 10 State × Category Combinations")
        top_combos = (
            state_filtered
            .sort_values("txn_count", ascending=False)
            .head(10)
            .copy()
        )
        top_combos.columns = ["State", "Category", "Transactions", "Avg Amount (₹)"]
        top_combos["Avg Amount (₹)"] = top_combos["Avg Amount (₹)"].apply(
            lambda x: f"₹{x:,.0f}"
        )
        st.dataframe(top_combos, use_container_width=True, hide_index=True)

        st.info(
            "💡 **Key Insight:** Maharashtra dominates nearly every category "
            "due to high digital maturity and urban merchant density. "
            "Uttar Pradesh leads in Groceries despite lower digital scores — "
            "everyday necessities drive UPI adoption even in Tier-2/3 cities."
        )

    st.caption(
        "Data: Synthetic transactions anchored to real NPCI aggregates · "
        "P2M = Person to Merchant · P2P = Person to Person"
    )
