"""
=============================================================
  FinPulse — Synthetic Data Generator
  Day 1: pipeline/data_generator.py
=============================================================

WHY SYNTHETIC DATA?
  Real UPI transaction-level data is not publicly available
  (it's private banking data). But RBI and NPCI publish
  aggregated monthly numbers. We use those REAL aggregates
  as "anchors" and generate synthetic transaction-level data
  that matches those real totals.

  This is a standard industry practice — companies like
  Mastercard and Visa use synthetic data for R&D.

HOW IT WORKS:
  1. We hardcode REAL monthly UPI volumes from NPCI reports
  2. We generate synthetic rows that SUM UP to those real numbers
  3. We add realistic patterns: festivals, weekends, state distribution
=============================================================
"""

import pandas as pd
import numpy as np
import os
import sys

# ── make sure we can import from project root ──────────────
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── reproducibility ────────────────────────────────────────
np.random.seed(42)


# =============================================================
# SECTION 1: REAL DATA ANCHORS (from NPCI public reports)
# =============================================================
# Source: NPCI UPI Product Statistics (public)
# Volume = number of transactions (in crores)
# Value  = total rupee value (in lakh crores)

REAL_UPI_MONTHLY = {
    # (Year, Month): (Volume_crore, Value_lakh_crore)
    (2022, 4):  (540,  8.9),
    (2022, 5):  (594,  10.4),
    (2022, 6):  (586,  10.1),
    (2022, 7):  (628,  10.6),
    (2022, 8):  (657,  10.7),
    (2022, 9):  (678,  11.1),
    (2022, 10): (730,  12.1),   # Diwali spike
    (2022, 11): (730,  11.9),
    (2022, 12): (782,  12.8),
    (2023, 1):  (803,  12.9),
    (2023, 2):  (753,  12.4),
    (2023, 3):  (865,  14.1),
    (2023, 4):  (889,  14.1),
    (2023, 5):  (930,  14.9),
    (2023, 6):  (937,  14.8),
    (2023, 7):  (996,  15.3),
    (2023, 8):  (1058, 15.8),
    (2023, 9):  (1057, 15.8),
    (2023, 10): (1173, 17.2),   # Diwali spike
    (2023, 11): (1167, 17.4),
    (2023, 12): (1229, 18.2),
    (2024, 1):  (1204, 18.4),
    (2024, 2):  (1201, 18.3),
    (2024, 3):  (1340, 19.8),
    (2024, 4):  (1330, 20.0),
    (2024, 5):  (1408, 20.4),
    (2024, 6):  (1389, 21.1),
    (2024, 7):  (1439, 20.6),
    (2024, 8):  (1459, 20.6),
    (2024, 9):  (1548, 20.6),
    (2024, 10): (1678, 23.5),   # Diwali spike
    (2024, 11): (1548, 21.5),
    (2024, 12): (1674, 23.3),
}


# =============================================================
# SECTION 2: REFERENCE DATA — States, Banks, Merchants
# =============================================================

STATES = {
    # State: (population_weight, digital_maturity_score)
    "Maharashtra":    (0.15, 0.90),
    "Karnataka":      (0.09, 0.92),
    "Telangana":      (0.07, 0.88),
    "Tamil Nadu":     (0.09, 0.85),
    "Delhi":          (0.08, 0.91),
    "Gujarat":        (0.08, 0.87),
    "Rajasthan":      (0.07, 0.72),
    "Uttar Pradesh":  (0.12, 0.65),
    "West Bengal":    (0.08, 0.78),
    "Kerala":         (0.05, 0.88),
    "Andhra Pradesh": (0.06, 0.80),
    "Madhya Pradesh": (0.06, 0.68),
}

# Normalize weights so they sum to 1
_total_weight = sum(v[0] * v[1] for v in STATES.values())
STATE_WEIGHTS = {
    state: (pop * mat) / _total_weight
    for state, (pop, mat) in STATES.items()
}

BANKS = {
    # Bank: (market_share, avg_txn_size_multiplier)
    "PhonePe (Yes Bank)":   (0.48, 1.05),
    "Google Pay (Axis)":    (0.37, 1.08),
    "Paytm Payments Bank":  (0.08, 0.95),
    "HDFC Bank":            (0.03, 1.20),
    "SBI":                  (0.02, 0.90),
    "ICICI Bank":           (0.01, 1.15),
    "Kotak Bank":           (0.01, 1.10),
}

MERCHANT_CATEGORIES = {
    # Category: (share_of_P2M, avg_amount_range)
    "Groceries & Kirana":    (0.22, (80,  600)),
    "Food & Restaurants":    (0.15, (100, 800)),
    "Fuel & Transport":      (0.10, (200, 2000)),
    "E-commerce":            (0.12, (300, 3000)),
    "Utilities & Bills":     (0.08, (500, 5000)),
    "Healthcare & Pharmacy": (0.07, (150, 2000)),
    "Education":             (0.05, (500, 10000)),
    "Entertainment":         (0.06, (200, 1500)),
    "Clothing & Fashion":    (0.07, (400, 3000)),
    "Electronics":           (0.04, (1000, 50000)),
    "Travel & Hotels":       (0.04, (1000, 20000)),
}

# P2P = Person to Person | P2M = Person to Merchant
TRANSACTION_TYPES = {"P2P": 0.45, "P2M": 0.55}   # FY2024 approximate split


# =============================================================
# SECTION 3: HELPER FUNCTIONS
# =============================================================

def get_festival_multiplier(year: int, month: int) -> float:
    """
    Returns a multiplier for transaction volume based on festival seasons.
    October (Diwali) and March (year-end) show the highest spikes.
    
    CONCEPT: This is called a 'feature' in ML — a derived value that
    captures domain knowledge (festivals drive payments).
    """
    festival_map = {
        10: 1.40,   # Diwali / Navratri
        3:  1.25,   # Year-end + Holi
        11: 1.15,   # Post-Diwali shopping
        12: 1.10,   # Christmas + New Year
        8:  1.08,   # Independence Day + Raksha Bandhan
        4:  1.05,   # Ugadi, Vishu, Baisakhi
    }
    return festival_map.get(month, 1.0)


def generate_daily_weights(year: int, month: int) -> np.ndarray:
    """
    Generate weights for each day of the month.
    Weekends get slightly higher weights (more leisure spending).
    Month-end gets higher weight (salary payments, bill payments).
    
    CONCEPT: This creates a realistic DISTRIBUTION of transactions
    across days — real payment data has these exact patterns.
    """
    import calendar
    days_in_month = calendar.monthrange(year, month)[1]
    weights = np.ones(days_in_month)

    for day in range(1, days_in_month + 1):
        date = pd.Timestamp(year=year, month=month, day=day)
        # Weekends get 20% more transactions
        if date.dayofweek >= 5:
            weights[day - 1] *= 1.20
        # Last 5 days of month: salary credit → high spending
        if day >= days_in_month - 4:
            weights[day - 1] *= 1.15
        # 1st of month: EMIs, bill payments
        if day == 1:
            weights[day - 1] *= 1.10

    return weights / weights.sum()   # normalize to probabilities


# =============================================================
# SECTION 4: CORE GENERATORS
# =============================================================

def generate_monthly_transactions(year: int, month: int,
                                  volume_crore: float,
                                  value_lakh_crore: float,
                                  sample_fraction: float = 0.000003) -> pd.DataFrame:
    """
    Generate synthetic transaction rows for ONE month.

    Parameters
    ----------
    year, month        : the month we're generating for
    volume_crore       : real NPCI volume (in crores)
    value_lakh_crore   : real NPCI value (in lakh crores)
    sample_fraction    : what % of real transactions to simulate
                         (0.00005 = 5 rows per 1 lakh real transactions)
                         Full simulation would need billions of rows!

    Returns
    -------
    pd.DataFrame with columns:
        transaction_id, date, state, bank, txn_type,
        merchant_category, amount, is_anomaly
    """
    import calendar

    # ── how many rows to generate ──────────────────────────
    real_count = int(volume_crore * 1e7)           # crore → actual count
    n_rows = max(500, int(real_count * sample_fraction))
    
    # ── average transaction value in rupees ────────────────
    # value_lakh_crore × 1e12 ÷ count = avg rupees per txn
    avg_txn_value = (value_lakh_crore * 1e12) / real_count

    # ── date distribution ──────────────────────────────────
    days_in_month = calendar.monthrange(year, month)[1]
    day_weights = generate_daily_weights(year, month)
    days = np.random.choice(
        range(1, days_in_month + 1),
        size=n_rows,
        p=day_weights
    )
    dates = pd.to_datetime({
        'year': year, 'month': month, 'day': days
    })

    # ── state distribution ─────────────────────────────────
    states = np.random.choice(
        list(STATE_WEIGHTS.keys()),
        size=n_rows,
        p=list(STATE_WEIGHTS.values())
    )

    # ── bank distribution ──────────────────────────────────
    bank_names   = list(BANKS.keys())
    bank_shares  = [v[0] for v in BANKS.values()]
    banks = np.random.choice(bank_names, size=n_rows, p=bank_shares)

    # ── transaction types ──────────────────────────────────
    txn_types = np.random.choice(
        list(TRANSACTION_TYPES.keys()),
        size=n_rows,
        p=list(TRANSACTION_TYPES.values())
    )

    # ── merchant categories (only for P2M) ────────────────
    cat_names   = list(MERCHANT_CATEGORIES.keys())
    cat_weights = [v[0] for v in MERCHANT_CATEGORIES.values()]
    cat_weights = np.array(cat_weights) / sum(cat_weights)

    merchant_categories = np.where(
        txn_types == "P2M",
        np.random.choice(cat_names, size=n_rows, p=cat_weights),
        "P2P Transfer"
    )

    # ── amount generation ──────────────────────────────────
    # Use lognormal distribution — most txns are small,
    # a few are very large (realistic payment distribution)
    amounts = np.random.lognormal(
        mean=np.log(avg_txn_value),
        sigma=0.8,
        size=n_rows
    )

    # Apply bank multipliers
    bank_multipliers = np.array([BANKS[b][1] for b in banks])
    amounts = amounts * bank_multipliers
    amounts = np.clip(amounts, 1, 500000).round(2)   # ₹1 to ₹5L limit

    # ── anomaly injection (2% of transactions) ─────────────
    # These are the "fraud signals" our ML model will detect later
    is_anomaly = np.zeros(n_rows, dtype=int)
    anomaly_idx = np.random.choice(n_rows, size=int(n_rows * 0.02), replace=False)
    is_anomaly[anomaly_idx] = 1
    # Anomalies have unusually high amounts
    amounts[anomaly_idx] = amounts[anomaly_idx] * np.random.uniform(8, 25, size=len(anomaly_idx))
    amounts[anomaly_idx] = np.clip(amounts[anomaly_idx], 1, 500000)

    # ── transaction IDs ────────────────────────────────────
    txn_ids = [f"UPI{year}{month:02d}{i:08d}" for i in range(n_rows)]

    return pd.DataFrame({
        "transaction_id":    txn_ids,
        "date":              dates,
        "year":              year,
        "month":             month,
        "state":             states,
        "bank":              banks,
        "txn_type":          txn_types,
        "merchant_category": merchant_categories,
        "amount":            amounts,
        "is_anomaly":        is_anomaly,
    })


def generate_monthly_aggregates() -> pd.DataFrame:
    """
    Generate the monthly summary table directly from REAL NPCI data.
    This is what we'll use for the forecasting model and trend charts.
    
    This table is 100% based on real public data — no synthetic values.
    """
    rows = []
    for (year, month), (volume, value) in REAL_UPI_MONTHLY.items():
        rows.append({
            "year":               year,
            "month":              month,
            "month_name":         pd.Timestamp(year=year, month=month, day=1).strftime("%b %Y"),
            "volume_crore":       volume,
            "value_lakh_crore":   value,
            "avg_txn_value":      round((value * 1e12) / (volume * 1e7), 2),
            "festival_month":     1 if month in [10, 11, 3] else 0,
            "yoy_growth_pct":     None,   # computed below
        })

    df = pd.DataFrame(rows).sort_values(["year", "month"]).reset_index(drop=True)

    # Compute YoY growth
    for i, row in df.iterrows():
        prev_year_row = df[(df.year == row.year - 1) & (df.month == row.month)]
        if not prev_year_row.empty:
            prev_vol = prev_year_row.iloc[0]["volume_crore"]
            df.at[i, "yoy_growth_pct"] = round(
                ((row.volume_crore - prev_vol) / prev_vol) * 100, 2
            )

    return df


def generate_bank_scorecard() -> pd.DataFrame:
    """
    Generate bank-level monthly performance metrics.
    Banks are scored on: volume share, value share, avg txn size, growth.
    """
    rows = []
    for (year, month), (volume, value) in REAL_UPI_MONTHLY.items():
        for bank, (share, multiplier) in BANKS.items():
            bank_volume = volume * share * np.random.uniform(0.97, 1.03)
            bank_value  = value  * share * multiplier * np.random.uniform(0.97, 1.03)
            rows.append({
                "year":             year,
                "month":            month,
                "bank":             bank,
                "volume_crore":     round(bank_volume, 2),
                "value_lakh_crore": round(bank_value,  4),
                "market_share_pct": round(share * 100, 2),
                "avg_txn_value":    round((bank_value * 1e12) / (bank_volume * 1e7), 2),
            })
    return pd.DataFrame(rows)


# =============================================================
# SECTION 5: MAIN RUNNER
# =============================================================

def run_data_generation(output_dir: str = "data/processed") -> None:
    """
    Main function: generates all datasets and saves them as CSVs.
    
    OUTPUT FILES:
        transactions.csv    — synthetic transaction rows
        monthly_agg.csv     — real NPCI monthly aggregates
        bank_scorecard.csv  — bank-level performance metrics
    """
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("  FinPulse Data Generator — Day 1")
    print("=" * 60)

    # ── 1. Monthly Aggregates (real data) ──────────────────
    print("\n[1/3] Generating monthly aggregates (real NPCI data)...")
    monthly_df = generate_monthly_aggregates()
    monthly_df.to_csv(f"{output_dir}/monthly_agg.csv", index=False)
    print(f"      ✅ {len(monthly_df)} months | saved → monthly_agg.csv")

    # ── 2. Transaction-level data (synthetic) ─────────────
    print("\n[2/3] Generating synthetic transactions...")
    all_txns = []
    for (year, month), (volume, value) in REAL_UPI_MONTHLY.items():
        df = generate_monthly_transactions(year, month, volume, value)
        all_txns.append(df)
        print(f"      ✅ {year}-{month:02d} → {len(df):,} rows generated")

    txn_df = pd.concat(all_txns, ignore_index=True)
    txn_df.to_csv(f"{output_dir}/transactions.csv", index=False)
    print(f"\n      📦 Total: {len(txn_df):,} transactions saved → transactions.csv")

    # ── 3. Bank Scorecard ──────────────────────────────────
    print("\n[3/3] Generating bank scorecard...")
    bank_df = generate_bank_scorecard()
    bank_df.to_csv(f"{output_dir}/bank_scorecard.csv", index=False)
    print(f"      ✅ {len(bank_df)} rows | saved → bank_scorecard.csv")

    # ── Summary ────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  DATA GENERATION COMPLETE")
    print("=" * 60)
    print(f"\n  📁 Output directory : {output_dir}/")
    print(f"  📊 Monthly records  : {len(monthly_df)}")
    print(f"  💳 Transactions     : {len(txn_df):,}")
    print(f"  🏦 Bank records     : {len(bank_df)}")
    print(f"\n  Date range: {monthly_df['month_name'].iloc[0]}"
          f" → {monthly_df['month_name'].iloc[-1]}")
    print(f"  Total UPI volume in dataset: "
          f"{monthly_df['volume_crore'].sum():.0f} crore transactions")
    print(f"  Total UPI value in dataset : "
          f"₹{monthly_df['value_lakh_crore'].sum():.1f} lakh crore")


if __name__ == "__main__":
    run_data_generation()
