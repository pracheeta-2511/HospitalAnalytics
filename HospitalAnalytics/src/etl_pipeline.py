"""
=============================================================
AIIMS Bhubaneswar Patient Analytics — ETL Pipeline
=============================================================
Project     : Hospital Patient Data Analysis
Author      : [Your Name] | Roll No: [Your Roll No]
Batch       : [Your Batch]
Description : ETL Pipeline — Extract from CSV (SAP Export),
              Transform (Clean + Validate), Load to SQLite DB
=============================================================
"""

import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime

# ── Logging Setup ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("output/etl_pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────
CSV_PATH   = "data/patients.csv"
DB_PATH    = "output/hospital.db"
TABLE_NAME = "patients"

# =============================================================
# STEP 1: EXTRACT — Read CSV (Simulated SAP FB60 Export)
# =============================================================
def extract(path):
    log.info("EXTRACT: Reading patient data from CSV (SAP FB60 Export)...")
    df = pd.read_csv(path)
    log.info(f"EXTRACT: {len(df)} records loaded successfully.")
    log.info(f"EXTRACT: Columns → {list(df.columns)}")
    return df

# =============================================================
# STEP 2: TRANSFORM — Clean + Validate + Enrich
# =============================================================
def transform(df):
    log.info("TRANSFORM: Starting data cleaning and validation...")

    original_count = len(df)

    # ── 2a. Check for missing values ──────────────────────
    missing = df.isnull().sum()
    if missing.any():
        log.warning(f"TRANSFORM: Missing values found:\n{missing[missing > 0]}")
        df = df.dropna()
        log.info(f"TRANSFORM: Dropped {original_count - len(df)} rows with missing values.")
    else:
        log.info("TRANSFORM: No missing values found. Data is clean.")

    # ── 2b. Remove duplicates ─────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=["patient_id"])
    log.info(f"TRANSFORM: Removed {before - len(df)} duplicate records.")

    # ── 2c. Data type conversions ─────────────────────────
    df["admission_date"]  = pd.to_datetime(df["admission_date"])
    df["discharge_date"]  = pd.to_datetime(df["discharge_date"])
    df["treatment_cost"]  = pd.to_numeric(df["treatment_cost"], errors="coerce")
    df["insurance_covered"] = pd.to_numeric(df["insurance_covered"], errors="coerce")
    df["age"]             = pd.to_numeric(df["age"], errors="coerce")
    log.info("TRANSFORM: Data types converted successfully.")

    # ── 2d. Data Quality Checks ───────────────────────────
    log.info("TRANSFORM: Running Data Quality Checks...")

    # Check: treatment_cost must be positive
    invalid_cost = df[df["treatment_cost"] <= 0]
    if len(invalid_cost) > 0:
        log.warning(f"TRANSFORM: {len(invalid_cost)} records with invalid treatment cost removed.")
        df = df[df["treatment_cost"] > 0]

    # Check: age must be between 0 and 120
    invalid_age = df[(df["age"] < 0) | (df["age"] > 120)]
    if len(invalid_age) > 0:
        log.warning(f"TRANSFORM: {len(invalid_age)} records with invalid age removed.")
        df = df[(df["age"] >= 0) & (df["age"] <= 120)]

    # Check: discharge must be after admission
    invalid_dates = df[df["discharge_date"] < df["admission_date"]]
    if len(invalid_dates) > 0:
        log.warning(f"TRANSFORM: {len(invalid_dates)} records with invalid dates removed.")
        df = df[df["discharge_date"] >= df["admission_date"]]

    log.info("TRANSFORM: All data quality checks passed.")

    # ── 2e. Feature Engineering ───────────────────────────
    df["length_of_stay"]    = (df["discharge_date"] - df["admission_date"]).dt.days
    df["out_of_pocket"]     = df["treatment_cost"] - df["insurance_covered"]
    df["insurance_pct"]     = round((df["insurance_covered"] / df["treatment_cost"]) * 100, 2)
    df["admission_month"]   = df["admission_date"].dt.month_name()
    df["age_group"]         = pd.cut(df["age"],
                                     bins=[0, 18, 35, 50, 65, 120],
                                     labels=["Child", "Young Adult", "Middle Aged", "Senior", "Elderly"])

    log.info("TRANSFORM: Feature engineering complete.")
    log.info(f"TRANSFORM: Final dataset has {len(df)} clean records and {len(df.columns)} columns.")
    return df

# =============================================================
# STEP 3: LOAD — Store into SQLite Database
# =============================================================
def load(df, db_path, table_name):
    log.info(f"LOAD: Connecting to SQLite database at {db_path}...")
    os.makedirs("output", exist_ok=True)

    conn = sqlite3.connect(db_path)
    df_load = df.copy()
    df_load["admission_date"] = df_load["admission_date"].astype(str)
    df_load["discharge_date"] = df_load["discharge_date"].astype(str)
    df_load["age_group"]      = df_load["age_group"].astype(str)

    df_load.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

    log.info(f"LOAD: {len(df)} records successfully loaded into table '{table_name}'.")
    log.info(f"LOAD: Database saved at {db_path}")

# =============================================================
# STEP 4: VALIDATE LOAD
# =============================================================
def validate_load(db_path, table_name):
    log.info("VALIDATE: Verifying loaded data...")
    conn   = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
    sample = cursor.fetchall()
    conn.close()
    log.info(f"VALIDATE: {count} records found in database.")
    log.info(f"VALIDATE: Sample records loaded successfully.")
    return count

# =============================================================
# MAIN — Run ETL Pipeline
# =============================================================
def run_pipeline():
    log.info("=" * 60)
    log.info("AIIMS Bhubaneswar — Hospital Analytics ETL Pipeline")
    log.info(f"Pipeline Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    raw_df    = extract(CSV_PATH)
    clean_df  = transform(raw_df)
    load(clean_df, DB_PATH, TABLE_NAME)
    count     = validate_load(DB_PATH, TABLE_NAME)

    log.info("=" * 60)
    log.info(f"ETL Pipeline Completed Successfully! {count} records in DB.")
    log.info(f"Pipeline End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)
    return clean_df

if __name__ == "__main__":
    run_pipeline()
