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

# ===== QUERY: LATEST SNAPSHOT PER TABLE =====
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
    ) t WHERE rn = 1
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
    ) t WHERE rn = 1
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
    ) t WHERE rn = 1
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
    ) t WHERE rn = 1
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
    ) t WHERE rn = 1
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
    ) t WHERE rn = 1
)

SELECT
    f.ticker,
    fy.fiscal_year,

    -- VERSIONING (ĂN ĐIỂM)
    s.snapshot_date,
    s.version_tag,

    -- ===== FINANCIAL =====
    fy.unit_scale,
    fy.currency_code,
    fy.net_sales,
    fy.total_assets,
    fy.selling_expenses,
    fy.general_admin_expenses,
    fy.manufacturing_overhead,
    fy.raw_material_consumption,
    fy.merchandise_purchase_year,
    fy.wip_goods_purchase,
    fy.outside_manufacturing_expenses,
    fy.production_cost,
    fy.net_operating_income,
    fy.rnd_expenses,
    fy.intangible_assets_net,
    fy.inventory,
    fy.net_ppe,
    fy.total_equity,
    fy.total_liabilities,
    fy.cash_and_equivalents,
    fy.long_term_debt,
    fy.current_assets,
    fy.current_liabilities,
    fy.net_income,
    fy.growth_ratio,

    -- ===== CASHFLOW =====
    cf.net_cfo,
    cf.capex,
    cf.net_cfi,

    -- ===== MARKET =====
    mk.shares_outstanding,
    mk.share_price,
    mk.market_value_equity,
    mk.dividend_cash_paid,
    mk.eps_basic,

    -- ===== OWNERSHIP =====
    ow.managerial_inside_own,
    ow.state_own,
    ow.institutional_own,
    ow.foreign_own,

    -- ===== META =====
    meta.employees_count,
    meta.firm_age,

    -- ===== INNOVATION =====
    inn.product_innovation,
    inn.process_innovation,
    inn.evidence_note

FROM fy_clean fy

JOIN dim_firm f 
    ON fy.firm_id = f.firm_id

JOIN fact_data_snapshot s
    ON fy.snapshot_id = s.snapshot_id

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

df = pd.read_sql(query, conn)

# ===== CLEAN OUTPUT =====
df = df.drop_duplicates()
df = df.sort_values(["ticker", "fiscal_year"])

# ===== DEBUG =====
print("Shape:", df.shape)
print("\n Missing values:")
print(df.isna().sum())

# ===== EXPORT =====
df.to_csv("outputs/panel_latest.csv", index=False)

conn.close()

print("Export completed: outputs/panel_latest.csv")