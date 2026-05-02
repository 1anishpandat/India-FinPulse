"""
=============================================================
  FinPulse — Database Helper
  database/db.py
=============================================================

WHY A SEPARATE DB FILE?
  Every view (overview, merchant, fraud, forecast, scorecard)
  needs to query the database. Instead of writing the same
  sqlite3.connect() code 5 times, we put it here once.
  All views import from this single file.

  This is called the DRY principle — Don't Repeat Yourself.
  It's one of the most important software engineering rules.
=============================================================
"""

import sqlite3
import pandas as pd
import os

# ── path is relative to project root ──────────────────────
DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "database", "finpulse.db"
)


def get_connection() -> sqlite3.Connection:
    """
    Returns a live SQLite connection.
    WAL mode allows Streamlit's multiple sessions to read
    simultaneously without blocking each other.
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """
    Run any SELECT query and return a pandas DataFrame.

    This is the function every view will call.
    One line in each view instead of 4 lines of boilerplate.

    Parameters
    ----------
    sql    : the SQL query string
    params : tuple of values to safely inject into the query
             (prevents SQL injection attacks)

    Example
    -------
    df = run_query(
        "SELECT * FROM monthly_aggregates WHERE year = ?",
        (2024,)
    )
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()   # always close — even if query crashes
    return df


# ── pre-written queries used across multiple views ─────────

def get_monthly_aggregates() -> pd.DataFrame:
    """Full monthly time series — used by Overview & Forecast."""
    return run_query("""
        SELECT *
        FROM   monthly_aggregates
        ORDER  BY year, month
    """)


def get_state_summary() -> pd.DataFrame:
    """State-wise transaction totals — used by Overview."""
    return run_query("""
        SELECT
            state,
            COUNT(*)         AS txn_count,
            SUM(amount)      AS total_value,
            AVG(amount)      AS avg_amount,
            SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS anomalies
        FROM   transactions
        GROUP  BY state
        ORDER  BY txn_count DESC
    """)


def get_bank_summary() -> pd.DataFrame:
    """Bank market share summary — used by Overview & Scorecard."""
    return run_query("""
        SELECT
            bank,
            ROUND(AVG(market_share_pct), 2)   AS avg_share,
            ROUND(SUM(volume_crore), 1)        AS total_volume,
            ROUND(SUM(value_lakh_crore), 2)    AS total_value,
            ROUND(AVG(avg_txn_value), 0)       AS avg_txn_value
        FROM   bank_scorecard
        GROUP  BY bank
        ORDER  BY avg_share DESC
    """)
