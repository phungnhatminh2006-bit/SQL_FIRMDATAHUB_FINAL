import pandas as pd
import numpy as np
import mysql.connector

# ===== CONFIG =====
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Nhatminh2006#",
    "database": "vn_firm_panel"
}

# ===== CONNECT =====
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# ===== LOAD DATA =====
df = pd.read_excel("data/panel_2020_2024.xlsx")
df.columns = df.columns.str.strip().str.lower()

print(f"Loaded: {df.shape}")

# ===== CLEAN FUNCTIONS =====
def clean(x):
    if pd.isna(x):
        return None
    if isinstance(x, str) and x.strip().lower() == "nan":
        return None
    return x

def clean_text(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    if x == "":
        return None
    return x[:500]

# ===== FIX TICKER =====
df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

# ===== MONEY SCALE =====
money_cols = [
    "net_sales","total_assets","selling_expenses","general_admin_expenses",
    "intangible_assets_net","manufacturing_overhead","net_operating_income",
    "raw_material_consumption","merchandise_purchase_year","wip_goods_purchase",
    "outside_manufacturing_expenses","production_cost","rnd_expenses",
    "net_income","total_equity","market_value_equity","total_liabilities",
    "net_cfo","capex","net_cfi","cash_and_equivalents","long_term_debt",
    "current_assets","current_liabilities","inventory","net_ppe",
    "dividend_cash_paid"
]

for col in money_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = (df[col] / 1e9).round(2)

# ===== OWNERSHIP SCALE =====
own_cols = ["state_own", "managerial_inside_own", "institutional_own", "foreign_own"]

for col in own_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].apply(lambda x: x/100 if pd.notna(x) and x > 1 else x)

# ===== GROWTH FIX =====
if "growth_ratio" in df.columns:
    df["growth_ratio"] = pd.to_numeric(df["growth_ratio"], errors="coerce")
    df["growth_ratio"] = df["growth_ratio"].apply(
    lambda x: x/100 if pd.notna(x) and x > 1 else x
)
    df["growth_ratio"] = df["growth_ratio"].clip(-0.95, 5)

# ===== FINAL CLEAN =====
df = df.replace({np.nan: None})

# ===== LOOP =====
for _, row in df.iterrows():

    ticker = clean(row.get("ticker"))
    year = clean(row.get("fiscal_year"))

    if ticker is None or year is None:
        continue

    year = int(year)

    # ===== GET firm_id =====
    cursor.execute(
        "SELECT firm_id FROM dim_firm WHERE ticker=%s",
        (ticker,)
    )
    result = cursor.fetchone()

    if result is None:
        raise ValueError(f"❌ Ticker not found: {ticker}")

    firm_id = result[0]

    # ===== GET snapshot_id (LATEST) =====
    cursor.execute("""
    SELECT snapshot_id
    FROM fact_data_snapshot
    WHERE fiscal_year=%s
    ORDER BY snapshot_id DESC
    LIMIT 1
    """, (year,))

    snap = cursor.fetchone()

    if snap is None:
        raise ValueError(f"❌ No snapshot for year {year}")

    snapshot_id = snap[0]

    # ================= FINANCIAL =================
    cursor.execute("""
    INSERT INTO fact_financial_year (
        firm_id,fiscal_year,snapshot_id,
        unit_scale,currency_code,
        net_sales,total_assets,
        selling_expenses,general_admin_expenses,
        manufacturing_overhead,raw_material_consumption,
        merchandise_purchase_year,wip_goods_purchase,
        outside_manufacturing_expenses,production_cost,
        net_operating_income,rnd_expenses,
        intangible_assets_net,inventory,net_ppe,
        total_equity,total_liabilities,
        cash_and_equivalents,long_term_debt,
        current_assets,current_liabilities,
        net_income,growth_ratio
    )
    VALUES (%s,%s,%s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s,%s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s)
    """, (
        firm_id, year, snapshot_id,
        "billion", "VND",

        clean(row.get("net_sales")),
        clean(row.get("total_assets")),

        clean(row.get("selling_expenses")),
        clean(row.get("general_admin_expenses")),

        clean(row.get("manufacturing_overhead")),
        clean(row.get("raw_material_consumption")),

        clean(row.get("merchandise_purchase_year")),
        clean(row.get("wip_goods_purchase")),

        clean(row.get("outside_manufacturing_expenses")),
        clean(row.get("production_cost")),

        clean(row.get("net_operating_income")),
        clean(row.get("rnd_expenses")),

        clean(row.get("intangible_assets_net")),
        clean(row.get("inventory")),
        clean(row.get("net_ppe")),

        clean(row.get("total_equity")),
        clean(row.get("total_liabilities")),

        clean(row.get("cash_and_equivalents")),
        clean(row.get("long_term_debt")),

        clean(row.get("current_assets")),
        clean(row.get("current_liabilities")),

        clean(row.get("net_income")),
        clean(row.get("growth_ratio"))
    ))

    # ================= MARKET =================
    cursor.execute("""
    INSERT INTO fact_market_year (
        firm_id,fiscal_year,snapshot_id,
        shares_outstanding,market_value_equity,dividend_cash_paid,eps_basic
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        firm_id, year, snapshot_id,
        clean(row.get("shares_outstanding")),
        clean(row.get("market_value_equity")),
        clean(row.get("dividend_cash_paid")),
        clean(row.get("eps_basic"))
    ))

    # ================= CASHFLOW =================
    cursor.execute("""
    INSERT INTO fact_cashflow_year (
        firm_id,fiscal_year,snapshot_id,
        net_cfo,capex,net_cfi
    )
    VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        firm_id, year, snapshot_id,
        clean(row.get("net_cfo")),
        clean(row.get("capex")),
        clean(row.get("net_cfi"))
    ))

    # ================= OWNERSHIP =================
    cursor.execute("""
    INSERT INTO fact_ownership_year (
        firm_id,fiscal_year,snapshot_id,
        state_own,managerial_inside_own,
        institutional_own,foreign_own
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        firm_id, year, snapshot_id,
        clean(row.get("state_own")),
        clean(row.get("managerial_inside_own")),
        clean(row.get("institutional_own")),
        clean(row.get("foreign_own"))
    ))

    # ================= META =================
    cursor.execute("""
    INSERT INTO fact_firm_year_meta (
        firm_id,fiscal_year,snapshot_id,
        employees_count,firm_age
    )
    VALUES (%s,%s,%s,%s,%s)
    """, (
        firm_id, year, snapshot_id,
        clean(row.get("employees_count")),
        clean(row.get("firm_age"))
    ))

    # ================= INNOVATION =================
    cursor.execute("""
    INSERT INTO fact_innovation_year (
        firm_id,fiscal_year,snapshot_id,
        product_innovation,process_innovation,evidence_note
    )
    VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        firm_id, year, snapshot_id,
        clean(row.get("product_innovation")),
        clean(row.get("process_innovation")),
        clean_text(row.get("evidence_note"))
    ))

# ===== SAVE =====
conn.commit()
cursor.close()
conn.close()

print("IMPORT PANEL SUCCESS")