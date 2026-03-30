import pandas as pd
import mysql.connector
import os

# ===== CONFIG =====
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Nhatminh2006#",
    "database": "vn_firm_panel"
}

os.makedirs("outputs", exist_ok=True)

conn = mysql.connector.connect(**DB_CONFIG)

# ===== QUERY (LATEST SNAPSHOT PER TABLE) =====
query = """
WITH fy_clean AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY firm_id, fiscal_year
                   ORDER BY snapshot_id DESC
               ) AS rn
        FROM fact_financial_year
    ) t
    WHERE rn = 1
),

cf_clean AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY firm_id, fiscal_year
                   ORDER BY snapshot_id DESC
               ) AS rn
        FROM fact_cashflow_year
    ) t
    WHERE rn = 1
),

mk_clean AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY firm_id, fiscal_year
                   ORDER BY snapshot_id DESC
               ) AS rn
        FROM fact_market_year
    ) t
    WHERE rn = 1
),

ow_clean AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY firm_id, fiscal_year
                   ORDER BY snapshot_id DESC
               ) AS rn
        FROM fact_ownership_year
    ) t
    WHERE rn = 1
),

meta_clean AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY firm_id, fiscal_year
                   ORDER BY snapshot_id DESC
               ) AS rn
        FROM fact_firm_year_meta
    ) t
    WHERE rn = 1
),

inn_clean AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY firm_id, fiscal_year
                   ORDER BY snapshot_id DESC
               ) AS rn
        FROM fact_innovation_year
    ) t
    WHERE rn = 1
)

SELECT
    f.ticker,
    fy.fiscal_year,

    -- FINANCIAL
    fy.total_assets,
    fy.current_liabilities,
    fy.growth_ratio,

    -- MARKET
    mk.shares_outstanding,
    mk.market_value_equity,
    mk.share_price,

    -- OWNERSHIP
    ow.managerial_inside_own,
    ow.state_own,
    ow.institutional_own,
    ow.foreign_own,

    -- INNOVATION
    inn.evidence_note

FROM fy_clean fy

JOIN dim_firm f 
    ON fy.firm_id = f.firm_id

LEFT JOIN cf_clean cf
    ON fy.firm_id = cf.firm_id AND fy.fiscal_year = cf.fiscal_year

LEFT JOIN mk_clean mk
    ON fy.firm_id = mk.firm_id AND fy.fiscal_year = mk.fiscal_year

LEFT JOIN ow_clean ow
    ON fy.firm_id = ow.firm_id AND fy.fiscal_year = ow.fiscal_year

LEFT JOIN meta_clean meta
    ON fy.firm_id = meta.firm_id AND fy.fiscal_year = meta.fiscal_year

LEFT JOIN inn_clean inn
    ON fy.firm_id = inn.firm_id AND fy.fiscal_year = inn.fiscal_year

ORDER BY f.ticker, fy.fiscal_year
"""

# ===== LOAD DATA =====
df = pd.read_sql(query, conn)
print(f"Loaded {df.shape[0]} rows for QC")

# ===== QC CHECKS =====
errors = []
passed_count = 0

for _, row in df.iterrows():

    ticker = row["ticker"]
    year = row["fiscal_year"]

    row_errors = []

    # ===== 1. OWNERSHIP [0,1] =====
    for col in [
        "managerial_inside_own",
        "state_own",
        "institutional_own",
        "foreign_own"
    ]:
        val = row[col]
        if pd.notna(val) and (val < 0 or val > 1):
            row_errors.append({
                "ticker": ticker,
                "fiscal_year": year,
                "field_name": col,
                "error_type": "range_error",
                "message": "ownership must be in [0,1]"
            })

    # ===== 2. SHARES > 0 =====
    if pd.notna(row["shares_outstanding"]) and row["shares_outstanding"] <= 0:
        row_errors.append({
            "ticker": ticker,
            "fiscal_year": year,
            "field_name": "shares_outstanding",
            "error_type": "invalid_value",
            "message": "shares must be > 0"
        })

    # ===== 3. TOTAL ASSETS >= 0 =====
    if pd.notna(row["total_assets"]) and row["total_assets"] < 0:
        row_errors.append({
            "ticker": ticker,
            "fiscal_year": year,
            "field_name": "total_assets",
            "error_type": "invalid_value",
            "message": "must be >= 0"
        })

    # ===== 4. CURRENT LIABILITIES >= 0 =====
    if pd.notna(row["current_liabilities"]) and row["current_liabilities"] < 0:
        row_errors.append({
            "ticker": ticker,
            "fiscal_year": year,
            "field_name": "current_liabilities",
            "error_type": "invalid_value",
            "message": "must be >= 0"
        })

    # ===== 5. GROWTH RANGE =====
    if pd.notna(row["growth_ratio"]) and not (-0.95 <= row["growth_ratio"] <= 5):
        row_errors.append({
            "ticker": ticker,
            "fiscal_year": year,
            "field_name": "growth_ratio",
            "error_type": "range_error",
            "message": "out of expected range (-0.95 to 5)"
        })

    # ===== 6. MARKET VALUE CHECK =====
    if pd.notna(row["shares_outstanding"]) and pd.notna(row["market_value_equity"]) and pd.notna(row["share_price"]):
        expected = row["shares_outstanding"] * row["share_price"]
        actual = row["market_value_equity"]

        if expected > 0 and abs(actual - expected) / expected > 0.2:
            row_errors.append({
                "ticker": ticker,
                "fiscal_year": year,
                "field_name": "market_value_equity",
                "error_type": "consistency_error",
                "message": "market cap mismatch (>20%)"
            })

    # ===== 7. TEXT LENGTH =====
    note = row.get("evidence_note")
    if note is not None and len(str(note)) > 500:
        row_errors.append({
            "ticker": ticker,
            "fiscal_year": year,
            "field_name": "evidence_note",
            "error_type": "length_error",
            "message": "exceeds 500 characters"
        })

    # ===== FINAL CHECK =====
    if len(row_errors) == 0:
        passed_count += 1
    else:
        errors.extend(row_errors)

# ===== EXPORT =====
total_rows = len(df)

if len(errors) == 0:
    qc_df = df[["ticker", "fiscal_year"]].copy()
    qc_df["status"] = "OK"

    qc_df.to_csv("outputs/qc_report.csv", index=False)

    print("\n===== QC SUMMARY =====")
    print(f"Total rows checked : {total_rows}")
    print(f"Rows passed QC     : {total_rows}")
    print("Total errors       : 0")
    print("Data quality tốt – toàn bộ dữ liệu hợp lệ")
    print("File outputs/qc_report.csv = danh sách data OK")

else:
    qc_df = pd.DataFrame(errors)
    qc_df.to_csv("outputs/qc_report.csv", index=False)

    print("\n===== QC SUMMARY =====")
    print(f"Total rows checked : {total_rows}")
    print(f"Rows passed QC     : {total_rows - len(qc_df)}")
    print(f"Total errors       : {len(qc_df)}")
    print(f"Data có lỗi – xem outputs/qc_report.csv")

    # breakdown lỗi
    print("\nTop error types:")
    print(qc_df["error_type"].value_counts())

    print("\nTop error fields:")
    print(qc_df["field_name"].value_counts())
