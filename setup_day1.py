"""
=============================================================
  FinPulse — Day 1 Master Setup Script
  Run this file to complete Day 1 in one command:
  
      python setup_day1.py
  
  What it does:
    1. Checks dependencies (installs if missing)
    2. Generates all synthetic data
    3. Loads data into SQLite database
    4. Prints a completion summary
=============================================================
"""

import subprocess
import sys
import os


# =============================================================
# STEP 0: DEPENDENCY CHECK
# =============================================================

REQUIRED_PACKAGES = [
    "pandas",
    "numpy",
    "scipy",
    "scikit-learn",
    "statsmodels",
    "plotly",
    "streamlit",
]

def check_and_install_packages():
    """Install missing packages automatically."""
    print("[0/2] Checking dependencies...")
    for pkg in REQUIRED_PACKAGES:
        try:
            __import__(pkg.replace("-", "_"))
            print(f"      ✅ {pkg}")
        except ImportError:
            print(f"      📦 Installing {pkg}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])
            print(f"      ✅ {pkg} installed")


# =============================================================
# STEP 1: ENSURE WE'RE IN THE RIGHT DIRECTORY
# =============================================================

def ensure_project_root():
    """Make sure we run from the finpulse/ directory."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    print(f"\n  📁 Working directory: {os.getcwd()}")


# =============================================================
# STEP 2: RUN PIPELINE
# =============================================================

def run_pipeline():
    """Run data generation and database loading."""
    from pipeline.data_generator import run_data_generation
    from pipeline.loader import run_loader

    print("\n" + "=" * 60)
    print("  STEP 1: DATA GENERATION")
    print("=" * 60)
    run_data_generation(output_dir="data/processed")

    print("\n" + "=" * 60)
    print("  STEP 2: DATABASE LOADING")
    print("=" * 60)
    run_loader(data_dir="data/processed", db_path="database/finpulse.db")


# =============================================================
# STEP 3: FINAL SUMMARY
# =============================================================

def print_day1_summary():
    print("""
╔══════════════════════════════════════════════════════════╗
║           🎉 DAY 1 COMPLETE — FinPulse Foundation        ║
╠══════════════════════════════════════════════════════════╣
║                                                          ║
║  ✅ Project structure created                            ║
║  ✅ Synthetic data generated (real NPCI anchors)         ║
║  ✅ SQLite database loaded (3 tables, 2 views)           ║
║                                                          ║
║  FILES CREATED:                                          ║
║    data/processed/monthly_agg.csv                        ║
║    data/processed/transactions.csv                       ║
║    data/processed/bank_scorecard.csv                     ║
║    database/finpulse.db                                  ║
║                                                          ║
║  WHAT YOU LEARNED TODAY:                                 ║
║    → Synthetic data generation with NumPy                ║
║    → Lognormal distribution for financial data           ║
║    → SQLite schema design (DDL)                          ║
║    → Chunked data loading for large files                ║
║    → SQL Views for reusable queries                      ║
║                                                          ║
║  NEXT: Day 2 — Streamlit App + Overview Dashboard        ║
╚══════════════════════════════════════════════════════════╝
""")


# =============================================================
# MAIN
# =============================================================

if __name__ == "__main__":
    ensure_project_root()
    check_and_install_packages()
    run_pipeline()
    print_day1_summary()
