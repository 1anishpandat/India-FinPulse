"""
=============================================================
  FinPulse — Database Loader
  Day 1: pipeline/loader.py
=============================================================

WHY SQLITE?
  SQLite is a file-based database — no server needed.
  One file (finpulse.db) contains ALL our data.
  
  In interviews you say: "I used SQLite as the backend
  database, designed a normalized schema with 3 tables,
  and wrote SQL queries for aggregation and analysis."

SCHEMA DESIGN:
  Table 1: monthly_aggregates  — time series of UPI volumes
  Table 2: transactions        — transaction-level data  
  Table 3: bank_scorecard      — bank performance metrics

  This is a "star schema lite" — common in analytics DBs.
=============================================================
"""

import sqlite3
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# =============================================================
# SECTION 1: SCHEMA DEFINITIONS
# =============================================================
# These are SQL CREATE TABLE statements.
# "IF NOT EXISTS" means: don't crash if table already exists.

SQL_CREATE_MONTHLY = """
CREATE TABLE IF NOT EXISTS monthly_aggregates (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    year               INTEGER NOT NULL,
    month              INTEGER NOT NULL,
    month_name         TEXT,
    volume_crore       REAL,
    value_lakh_crore   REAL,
    avg_txn_value      REAL,
    festival_month     INTEGER DEFAULT 0,
    yoy_growth_pct     REAL,
    UNIQUE(year, month)
);
"""

SQL_CREATE_TRANSACTIONS = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      TEXT PRIMARY KEY,
    date                TEXT,
    year                INTEGER,
    month               INTEGER,
    state               TEXT,
    bank                TEXT,
    txn_type            TEXT,
    merchant_category   TEXT,
    amount              REAL,
    is_anomaly          INTEGER DEFAULT 0
);
"""

SQL_CREATE_BANK = """
CREATE TABLE IF NOT EXISTS bank_scorecard (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    year                INTEGER,
    month               INTEGER,
    bank                TEXT,
    volume_crore        REAL,
    value_lakh_crore    REAL,
    market_share_pct    REAL,
    avg_txn_value       REAL,
    UNIQUE(year, month, bank)
);
"""

SQL_CREATE_VIEWS = [
    # View 1: State-wise annual summary
    """
    CREATE VIEW IF NOT EXISTS v_state_annual AS
    SELECT
        state,
        year,
        COUNT(*)                          AS txn_count,
        SUM(amount)                       AS total_value,
        AVG(amount)                       AS avg_amount,
        SUM(CASE WHEN txn_type='P2M' THEN 1 ELSE 0 END) AS p2m_count,
        SUM(CASE WHEN is_anomaly=1   THEN 1 ELSE 0 END) AS anomaly_count
    FROM transactions
    GROUP BY state, year;
    """,
    # View 2: Merchant category performance
    """
    CREATE VIEW IF NOT EXISTS v_merchant_perf AS
    SELECT
        merchant_category,
        year,
        month,
        COUNT(*)        AS txn_count,
        SUM(amount)     AS total_value,
        AVG(amount)     AS avg_amount
    FROM transactions
    WHERE txn_type = 'P2M'
    GROUP BY merchant_category, year, month;
    """,
]


# =============================================================
# SECTION 2: DATABASE CONNECTION HELPER
# =============================================================

def get_connection(db_path: str = "database/finpulse.db") -> sqlite3.Connection:
    """
    Returns a SQLite connection with optimized settings.
    
    WAL mode = Write-Ahead Logging: allows simultaneous reads
    while writing. Important for Streamlit (multi-user reads).
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


# =============================================================
# SECTION 3: SCHEMA CREATION
# =============================================================

def create_schema(conn: sqlite3.Connection) -> None:
    """Creates all tables and views."""
    cursor = conn.cursor()
    cursor.execute(SQL_CREATE_MONTHLY)
    cursor.execute(SQL_CREATE_TRANSACTIONS)
    cursor.execute(SQL_CREATE_BANK)
    for view_sql in SQL_CREATE_VIEWS:
        cursor.execute(view_sql)
    conn.commit()
    print("  ✅ Schema created: 3 tables + 2 views")


# =============================================================
# SECTION 4: DATA LOADERS
# =============================================================

def load_csv_to_table(conn: sqlite3.Connection,
                      csv_path: str,
                      table_name: str,
                      chunk_size: int = 500) -> int:
    """
    Loads a CSV into a SQLite table in chunks.
    
    WHY CHUNKS?
      If we have 500,000 rows, loading all at once uses too much RAM.
      Chunks of 10,000 rows = efficient and safe.
    
    'if_exists=append' means: add to existing data, don't overwrite.
    'index=False' means: don't write pandas row numbers as a column.
    """
    df = pd.read_csv(csv_path)
    total_rows = len(df)

    # Load in chunks
    rows_loaded = 0
    for start in range(0, total_rows, chunk_size):
        chunk = df.iloc[start : start + chunk_size]
        chunk.to_sql(
             table_name,
             conn,
             if_exists="append",
            index=False,     # faster bulk insert
        )
        rows_loaded += len(chunk)

    return rows_loaded


def clear_and_reload(conn: sqlite3.Connection,
                     table_name: str,
                     csv_path: str) -> int:
    """
    Clears existing data from a table and reloads from CSV.
    Used when re-running the pipeline — avoids duplicates.
    """
    conn.execute(f"DELETE FROM {table_name};")
    conn.commit()
    return load_csv_to_table(conn, csv_path, table_name)


# =============================================================
# SECTION 5: VALIDATION QUERIES
# =============================================================

def validate_database(conn: sqlite3.Connection) -> None:
    """
    Runs quick sanity checks on the loaded data.
    This is called 'data quality validation' in industry.
    """
    checks = {
        "Monthly records":     "SELECT COUNT(*) FROM monthly_aggregates",
        "Transactions":        "SELECT COUNT(*) FROM transactions",
        "Bank records":        "SELECT COUNT(*) FROM bank_scorecard",
        "Anomalies flagged":   "SELECT COUNT(*) FROM transactions WHERE is_anomaly=1",
        "States covered":      "SELECT COUNT(DISTINCT state) FROM transactions",
        "Date range start":    "SELECT MIN(date) FROM transactions",
        "Date range end":      "SELECT MAX(date) FROM transactions",
        "Total UPI (crore)":   "SELECT ROUND(SUM(volume_crore),0) FROM monthly_aggregates",
        "Total value (₹ L Cr)":"SELECT ROUND(SUM(value_lakh_crore),1) FROM monthly_aggregates",
    }

    print("\n  📋 DATABASE VALIDATION")
    print("  " + "-" * 40)
    for label, query in checks.items():
        result = conn.execute(query).fetchone()[0]
        print(f"  {label:<28}: {result}")


# =============================================================
# SECTION 6: MAIN RUNNER
# =============================================================

def run_loader(data_dir: str = "data/processed",
               db_path:  str = "database/finpulse.db") -> None:
    """
    Complete pipeline: create schema → load CSVs → validate.
    """
    print("=" * 60)
    print("  FinPulse Database Loader — Day 1")
    print("=" * 60)

    conn = get_connection(db_path)

    print("\n[1/4] Creating schema...")
    create_schema(conn)

    print("\n[2/4] Loading monthly aggregates...")
    n = clear_and_reload(conn, "monthly_aggregates",
                         f"{data_dir}/monthly_agg.csv")
    print(f"      ✅ {n} rows loaded")

    print("\n[3/4] Loading transactions (this may take a moment)...")
    n = clear_and_reload(conn, "transactions",
                         f"{data_dir}/transactions.csv")
    print(f"      ✅ {n:,} rows loaded")

    print("\n[4/4] Loading bank scorecard...")
    n = clear_and_reload(conn, "bank_scorecard",
                         f"{data_dir}/bank_scorecard.csv")
    print(f"      ✅ {n} rows loaded")

    validate_database(conn)
    conn.close()

    file_size_mb = os.path.getsize(db_path) / (1024 * 1024)
    print(f"\n  💾 Database size: {file_size_mb:.1f} MB → {db_path}")
    print("\n" + "=" * 60)
    print("  DATABASE READY ✅")
    print("=" * 60)


if __name__ == "__main__":
    run_loader()
