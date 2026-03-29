import pandas as pd
import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Nhatminh2006#",
    "database": "vn_firm_panel"
}

conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

df = pd.read_excel("data/firms.xlsx")
df.columns = df.columns.str.strip().str.lower()

# FIX ticker
df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

for _, row in df.iterrows():

    ticker = row["ticker"]
    company_name = row.get("company_name")

    # ===== EXCHANGE =====
    cursor.execute(
        "SELECT exchange_id FROM dim_exchange WHERE exchange_code=%s",
        (row.get("exchange_code"),)
    )
    exchange_result = cursor.fetchone()

    if exchange_result is None:
        print("⚠️ Unknown exchange → set NULL:", row.get("exchange_code"))
        exchange_id = None
    else:
        exchange_id = exchange_result[0]

    # ===== INDUSTRY =====
    cursor.execute(
        "SELECT industry_l2_id FROM dim_industry_l2 WHERE industry_l2_name=%s",
        (row.get("industry_l2"),)
    )
    industry_result = cursor.fetchone()

    if industry_result is None:
        print("⚠️ Unknown industry → set NULL:", row.get("industry_l2"))
        industry_id = None
    else:
        industry_id = industry_result[0]

    # ===== INSERT =====
    cursor.execute("""
    INSERT IGNORE INTO dim_firm
    (ticker, company_name, exchange_id, industry_l2_id)
    VALUES (%s,%s,%s,%s)
    """, (
        ticker,
        company_name,
        exchange_id,
        industry_id
    ))

conn.commit()
cursor.close()
conn.close()

print("✅ Firms imported")