import mysql.connector
import datetime

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Nhatminh2006#",
    "database": "vn_firm_panel"
}

def create_snapshot(source_name, fiscal_year, version_tag):

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # ===== GET source_id =====
    cursor.execute(
        "SELECT source_id FROM dim_data_source WHERE source_name=%s",
        (source_name,)
    )
    result = cursor.fetchone()

    if result is None:
        raise ValueError(f"Source not found: {source_name}")

    source_id = result[0]

    snapshot_date = datetime.date.today()

    # ===== INSERT =====
    sql = """
    INSERT INTO fact_data_snapshot
    (snapshot_date, fiscal_year, source_id, version_tag)
    VALUES (%s,%s,%s,%s)
    """

    cursor.execute(sql, (
        snapshot_date,
        fiscal_year,
        source_id,
        version_tag
    ))

    conn.commit()

    snapshot_id = cursor.lastrowid

    cursor.close()
    conn.close()

    print("Snapshot created:", snapshot_id)

    return snapshot_id


if __name__ == "__main__":

    create_snapshot(
        source_name="BCTC_Audited",
        fiscal_year=2024,
        version_tag="v1"
    )