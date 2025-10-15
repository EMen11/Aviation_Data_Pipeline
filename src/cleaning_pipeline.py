# src/cleaning_pipeline.py
# -------------------------------------------------------
# Airport Operations Analytics — Cleaning Pipeline
# Input : data/raw/airline_delay.csv
# Output: data/processed/airline_delay_cleaned.csv
#         data/processed/airline_delay_cleaned_sample.csv
#         data/processed/airline_delay_data_dictionary.csv
# -------------------------------------------------------

import pandas as pd
import numpy as np
from pathlib import Path

# --- Define file paths relative to project root ---
ROOT = Path("/Users/eliemenassa/Desktop/aviation-analytics")
RAW = ROOT / "data" / "raw" / "airline_delay.csv"
PROC_DIR = ROOT / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)

CLEAN = PROC_DIR / "airline_delay_cleaned.csv"
SAMPLE = PROC_DIR / "airline_delay_cleaned_sample.csv"
DICT = PROC_DIR / "airline_delay_data_dictionary.csv"

# --- Helper functions ---
def to_snake(s: str) -> str:
    """Convert column names to snake_case for consistency."""
    s = s.strip().replace("/", " ").replace("-", " ").replace(".", " ")
    s = "".join(ch if ch.isalnum() or ch == " " else "_" for ch in s)
    parts = [p for p in s.replace("__","_").lower().split() if p]
    return "_".join(parts)

def safe_div(num, den):
    """Perform safe division: returns NaN if denominator == 0."""
    num = num.astype(float)
    den = den.astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(den == 0, np.nan, num / den)

# =======================================================
# 1) Load raw data
# =======================================================
df = pd.read_csv(RAW)

# =======================================================
# 2) Standardize column names
# =======================================================
# Convert all column names to snake_case for easier reference
df.columns = [to_snake(c) for c in df.columns]

# =======================================================
# 3) Convert data types and handle missing values
# =======================================================

# Define groups of numeric columns
count_cols = [
    "arr_flights","arr_del15","carrier_ct","weather_ct","nas_ct",
    "security_ct","late_aircraft_ct","arr_cancelled","arr_diverted"
]
delay_min_cols = [
    "arr_delay","carrier_delay","weather_delay","nas_delay",
    "security_delay","late_aircraft_delay"
]

# Convert numeric-like columns to numeric and fill NaN with 0
# because this dataset represents aggregated monthly totals
for col in count_cols + delay_min_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# Ensure year/month are integers (nullable Int64 type)
for col in ["year","month"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

# =======================================================
# 4) Feature engineering — KPI creation
# =======================================================

# --- Create a proper datetime column (1st of each month) ---
if {"year","month"}.issubset(df.columns):
    df["year_month"] = pd.to_datetime(
        df["year"].astype("int").astype(str) + "-" +
        df["month"].astype("int").astype(str) + "-01",
        errors="coerce"
    )
    df["year_month_key"] = df["year_month"].dt.strftime("%Y-%m")

# --- Delay rate: share of flights delayed more than 15 min ---
if {"arr_del15","arr_flights"}.issubset(df.columns):
    df["delayed_rate"] = safe_div(df["arr_del15"], df["arr_flights"])

# --- Cancellation rate: cancelled flights / total handled ---
if {"arr_flights","arr_cancelled","arr_diverted"}.issubset(df.columns):
    total_handled = df["arr_flights"] + df["arr_cancelled"] + df["arr_diverted"]
    df["cancellation_rate"] = safe_div(df["arr_cancelled"], total_handled)

# --- Average delay (min) per delayed flight ---
if {"arr_delay","arr_del15"}.issubset(df.columns):
    df["avg_delay_min_per_delayed_flight"] = safe_div(df["arr_delay"], df["arr_del15"])

# --- Cause shares (count-based): % of delayed flights by cause ---
for cause in ["carrier","weather","nas","security","late_aircraft"]:
    ct = f"{cause}_ct"
    if {"arr_del15", ct}.issubset(df.columns):
        df[f"{cause}_cause_share_ct"] = safe_div(df[ct], df["arr_del15"])

# --- Cause shares (minute-based): % of total delay minutes by cause ---
for cause in ["carrier","weather","nas","security","late_aircraft"]:
    dmin = f"{cause}_delay"
    if {"arr_delay", dmin}.issubset(df.columns):
        df[f"{cause}_cause_share_min"] = safe_div(df[dmin], df["arr_delay"])

# =======================================================
# 5) Data quality checks (non-blocking)
# =======================================================

# Example: check if the sum of cause counts exceeds delayed flights
checks = {}
req = ["carrier_ct","weather_ct","nas_ct","security_ct","late_aircraft_ct","arr_del15"]
if all(c in df.columns for c in req):
    cause_sum = df[["carrier_ct","weather_ct","nas_ct","security_ct","late_aircraft_ct"]].sum(axis=1)
    df["cause_counts_exceed_delayed_flag"] = cause_sum > df["arr_del15"]
    checks["rows_where_cause_counts_exceed_delayed"] = int(df["cause_counts_exceed_delayed_flag"].sum())

# =======================================================
# 6) Save outputs
# =======================================================

# Save full cleaned dataset
df.to_csv(CLEAN, index=False)

# Save a smaller sample (useful for Power BI tests or quick inspection)
df.sample(min(len(df), 500), random_state=42).to_csv(SAMPLE, index=False)

# Generate a basic data dictionary (for documentation)
data_dict = pd.DataFrame({
    "column": df.columns,
    "dtype": [df[c].dtype for c in df.columns],
    "null_pct": [round(df[c].isna().mean()*100, 2) for c in df.columns],
    "example": [df[c].dropna().iloc[0] if df[c].notna().any() else None for c in df.columns]
}).sort_values("column")
data_dict.to_csv(DICT, index=False)

# =======================================================
# 7) Console summary
# =======================================================
print(" Cleaning completed successfully")
print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")
print("Saved files:")
print(" -", CLEAN)
print(" -", SAMPLE)
print(" -", DICT)
if checks:
    print("Quality checks summary:", checks)
    
#=======================================================
# 8) Dataset validation and quality check
# =======================================================

df = pd.read_csv(CLEAN)

# General overview — check shape and first few rows
print(df.shape)
print(df.head())

# Missing values — calculate % of missing values per column
missing = df.isna().mean().sort_values(ascending=False)
print("\nMissing values (%):\n", (missing*100).round(2).head(10))

# Data types — verify that each column has the expected type (int, float, object)
print("\nData types:\n", df.dtypes.head(10))

# Descriptive statistics — check min, max, mean, std for numeric columns
print("\nSummary statistics:\n", df.describe().T.head(10))

#KPI verification — inspect calculated metrics for potential outliers or errors
kpi_cols = ["delayed_rate", "cancellation_rate", "avg_delay_min_per_delayed_flight"]
print("\nKPI ranges:\n", df[kpi_cols].describe())


    

