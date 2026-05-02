"""
=============================================================
  FinPulse — Main Streamlit App
  app.py
=============================================================

HOW STREAMLIT APPS WORK:
  You run: streamlit run app.py
  Streamlit starts a local web server on port 8501.
  Open browser → http://localhost:8501
  Every time you interact, Streamlit reruns this file.

THIS FILE'S JOB:
  1. Configure the page (title, icon, layout)
  2. Build the sidebar navigation
  3. Route to the correct view based on what user clicked
  4. Show a welcome screen on first load

MULTI-PAGE PATTERN:
  We don't use Streamlit's built-in multi-page (pages/ folder)
  because we want full control over the sidebar design.
  Instead, we import each view and call its render() function.
  This is the standard pattern for production Streamlit apps.
=============================================================
"""

import streamlit as st
import sys
import os

# ── make sure Python can find our modules ──────────────────
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


# =============================================================
# SECTION 1: PAGE CONFIGURATION
# Must be the FIRST Streamlit call in the script.
# If any other st. call happens before this, you get an error.
# =============================================================

st.set_page_config(
    page_title="FinPulse — India UPI Analytics",
    page_icon="💳",
    layout="wide",          # use full browser width
    initial_sidebar_state="expanded",
    menu_items={
        "About": "FinPulse — India UPI & Fintech Analytics Platform\n"
                 "Built with Python, Streamlit, Plotly & SQLite\n"
                 "Data: NPCI Public Reports + Synthetic Transactions"
    }
)


# =============================================================
# SECTION 2: CUSTOM CSS
# st.markdown with unsafe_allow_html=True lets you inject CSS.
# We use this to style the sidebar and KPI cards.
# =============================================================

st.markdown("""
<style>
    /* Make metric cards look like proper KPI boxes */
    div[data-testid="metric-container"] {
        background-color: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 14px 18px;
        border-radius: 10px;
    }

    /* Sidebar styling */
    .sidebar-title {
        font-size: 22px;
        font-weight: 700;
        margin-bottom: 4px;
        color: inherit;
    }
    .sidebar-sub {
        font-size: 12px;
        opacity: 0.6;
        margin-bottom: 20px;
    }

    /* Remove default top padding */
    .block-container {
        padding-top: 1.5rem;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================
# SECTION 3: SIDEBAR NAVIGATION
# =============================================================

with st.sidebar:
    # Logo / title area
    st.markdown('<div class="sidebar-title">💳 FinPulse</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-sub">India UPI Analytics Platform</div>', unsafe_allow_html=True)
    st.divider()

    # Navigation radio buttons
    # st.radio returns the currently selected option as a string
    page = st.radio(
        "Navigate",
        options=[
            "🏠  Overview",
            "🏪  Merchant Intelligence",
            "🚨  Fraud Detection",
            "🔮  Forecasting Engine",
            "🏦  Bank Scorecard",
        ],
        label_visibility="collapsed"   # hides the "Navigate" label
    )

    st.divider()

    # About section at bottom of sidebar
    st.markdown("**About this project**")
    st.caption(
        "Built on India's UPI ecosystem. "
        "Combines real NPCI data with synthetic "
        "transaction-level analysis.\n\n"
        "**Stack:** Python · Pandas · Scikit-learn · "
        "Streamlit · Plotly · SQLite"
    )


# =============================================================
# SECTION 4: PAGE ROUTING
# Based on what the user clicked in the sidebar,
# we import and call the correct view's render() function.
#
# WHY IMPORT INSIDE THE IF BLOCK?
#   Each view imports its own libraries (plotly, sklearn etc).
#   Importing only the needed view keeps startup fast.
#   This pattern is called lazy importing.
# =============================================================

if "Overview" in page:
    from views.overview import render
    render()

elif "Merchant" in page:
    # Day 3 — not built yet
    st.title("🏪 Merchant Intelligence")
    st.info("🚧 Coming on Day 3 — Merchant category spending patterns, "
            "P2P vs P2M split analysis, top merchant categories by state.")
    st.image("https://via.placeholder.com/800x400?text=Merchant+Intelligence+%E2%80%94+Coming+Day+3",
             use_column_width=True)

elif "Fraud" in page:
    # Day 4 — not built yet
    st.title("🚨 Fraud Signal Detection")
    st.info("🚧 Coming on Day 4 — IQR + Z-score anomaly detection, "
            "fraud heatmaps by state and bank, threshold tuning.")

elif "Forecasting" in page:
    # Day 5 — not built yet
    st.title("🔮 Forecasting Engine")
    st.info("🚧 Coming on Day 5 — ARIMA model predicting next 6 months "
            "of UPI volume with confidence intervals.")

elif "Scorecard" in page:
    # Day 6 — not built yet
    st.title("🏦 Bank Performance Scorecard")
    st.info("🚧 Coming on Day 6 — Ranking all banks on UPI metrics "
            "with trend lines and benchmark comparisons.")
