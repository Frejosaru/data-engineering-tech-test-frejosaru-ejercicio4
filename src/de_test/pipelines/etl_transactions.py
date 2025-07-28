import os
import csv
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

CSV_PATH = os.getenv("CSV_PATH", "data/sample_transactions.csv")
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://user:password@localhost:5432/dwh"
)

def load_to_staging(conn, csv_path: str):
    with conn.cursor() as cur, open(csv_path, "r", newline="") as f:
        # Limpia staging
        cur.execute("TRUNCATE staging.transactions_raw;")

        reader = csv.DictReader(f)
        rows = [
            (
                r["transaction_id"],
                r["customer_id"],
                r["merchant_id"],
                r["transaction_ts"],
                r["amount"],
                r["currency"],
                r["status"],
                r.get("country"),
                r.get("city"),
                r.get("payment_method"),
                r.get("card_type"),
                r.get("category"),
            )
            for r in reader
        ]

        execute_values(cur, """
            INSERT INTO staging.transactions_raw
            (transaction_id, customer_id, merchant_id, transaction_ts, amount, currency,
             status, country, city, payment_method, card_type, category)
            VALUES %s
        """, rows)
    conn.commit()

def upsert_dim_currency(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dwh.dim_currency (currency_code)
            SELECT DISTINCT currency
            FROM staging.transactions_raw s
            LEFT JOIN dwh.dim_currency d
              ON s.currency = d.currency_code
            WHERE d.currency_code IS NULL
        """)
    conn.commit()

def upsert_dim_payment_method(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dwh.dim_payment_method (method_code, card_type)
            SELECT DISTINCT payment_method, card_type
            FROM staging.transactions_raw s
            LEFT JOIN dwh.dim_payment_method d
              ON s.payment_method = d.method_code
             AND COALESCE(s.card_type,'') = COALESCE(d.card_type,'')
            WHERE d.payment_method_sk IS NULL
        """)
    conn.commit()

def scd2_upsert_dimension(conn, dim_table: str, bk_col: str, attrs: list):
    """
    Generic SCD2 upsert for dim_customer / dim_merchant.
    attrs: columns to compare (change detection).
    """
    now = datetime.utcnow()
    with conn.cursor() as cur:
        # Extract distinct BK rows from staging
        if dim_table.endswith("customer"):
            src_sql = """
                SELECT DISTINCT customer_id AS bk, country, city
                FROM staging.transactions_raw
            """
        elif dim_table.endswith("merchant"):
            src_sql = """
                SELECT DISTINCT merchant_id AS bk, category, country, city
                FROM staging.transactions_raw
            """
        else:
            raise ValueError("Unsupported dim table")

        cur.execute(src_sql)
        candidates = cur.fetchall()

        for row in candidates:
            # Map attrs
            if dim_table.endswith("customer"):
                bk, country, city = row
                current_attrs = {"country": country, "city": city}
            else:
                bk, category, country, city = row
                current_attrs = {"category": category, "country": country, "city": city}

            # Get current record
            cur.execute(f"""
                SELECT *
                FROM dwh.{dim_table}
                WHERE {bk_col} = %s AND is_current = TRUE
            """, (bk,))
            existing = cur.fetchone()

            if not existing:
                # Insert brand-new current row
                cols = [bk_col] + list(current_attrs.keys()) + ["valid_from", "is_current"]
                values = [bk] + list(current_attrs.values()) + [now, True]
                placeholders = ", ".join(["%s"] * len(values))
                cur.execute(
                    f"INSERT INTO dwh.{dim_table} ({', '.join(cols)}) VALUES ({placeholders})",
                    values
                )
            else:
                # Compare attributes (simple compare; puedes mejorar con hashing)
                # existing is a tuple; for clarity fetch by column name using DictCursor
                # but for brevity, do diff checking with another query:
                set_clauses = []
                changed = False
                for k, v in current_attrs.items():
                    cur.execute(
                        f"SELECT {k} FROM dwh.{dim_table} WHERE {bk_col}=%s AND is_current=TRUE",
                        (bk,)
                    )
                    (old_val,) = cur.fetchone()
                    if old_val != v:
                        changed = True
                        break

                if changed:
                    # Close old row
                    cur.execute(f"""
                        UPDATE dwh.{dim_table}
                           SET valid_to = %s, is_current = FALSE, updated_at = NOW()
                         WHERE {bk_col} = %s AND is_current = TRUE
                    """, (now, bk))

                    # Insert new version
                    cols = [bk_col] + list(current_attrs.keys()) + ["valid_from", "is_current"]
                    values = [bk] + list(current_attrs.values()) + [now, True]
                    placeholders = ", ".join(["%s"] * len(values))
                    cur.execute(
                        f"INSERT INTO dwh.{dim_table} ({', '.join(cols)}) VALUES ({placeholders})",
                        values
                    )
    conn.commit()

def upsert_dim_date(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dwh.dim_date (date_key, date, year, quarter, month, day, weekday)
            SELECT DISTINCT
                CAST(to_char(transaction_ts::date, 'YYYYMMDD') AS INTEGER) AS date_key,
                transaction_ts::date AS date,
                EXTRACT(YEAR FROM transaction_ts)::smallint AS year,
                EXTRACT(QUARTER FROM transaction_ts)::smallint AS quarter,
                EXTRACT(MONTH FROM transaction_ts)::smallint AS month,
                EXTRACT(DAY FROM transaction_ts)::smallint AS day,
                EXTRACT(DOW FROM transaction_ts)::smallint AS weekday
            FROM staging.transactions_raw s
            LEFT JOIN dwh.dim_date d ON CAST(to_char(s.transaction_ts::date, 'YYYYMMDD') AS INTEGER) = d.date_key
            WHERE d.date_key IS NULL
        """)
    conn.commit()

def insert_fact(conn):
    with conn.cursor() as cur:
        # Resolve SKs and insert
        cur.execute("""
            INSERT INTO dwh.fact_transactions
            (transaction_id, customer_sk, merchant_sk, payment_method_sk, currency_sk, date_key,
             amount, status, transaction_ts)
            SELECT
                s.transaction_id,
                dc.customer_sk,
                dm.merchant_sk,
                dpm.payment_method_sk,
                dcurr.currency_sk,
                CAST(to_char(s.transaction_ts::date, 'YYYYMMDD') AS INTEGER) AS date_key,
                s.amount,
                s.status,
                s.transaction_ts
            FROM staging.transactions_raw s
            JOIN dwh.dim_customer dc
              ON dc.customer_bk = s.customer_id AND dc.is_current
            JOIN dwh.dim_merchant dm
              ON dm.merchant_bk = s.merchant_id AND dm.is_current
            JOIN dwh.dim_payment_method dpm
              ON dpm.method_code = s.payment_method
             AND COALESCE(dpm.card_type,'') = COALESCE(s.card_type,'')
            JOIN dwh.dim_currency dcurr
              ON dcurr.currency_code = s.currency
            ON CONFLICT (transaction_id) DO NOTHING;
        """)
    conn.commit()

def main():
    conn = psycopg2.connect(PG_DSN)
    try:
        load_to_staging(conn, CSV_PATH)
        upsert_dim_currency(conn)
        upsert_dim_payment_method(conn)
        scd2_upsert_dimension(conn, "dim_customer", "customer_bk", ["country", "city"])
        scd2_upsert_dimension(conn, "dim_merchant", "merchant_bk", ["category", "country", "city"])
        upsert_dim_date(conn)
        insert_fact(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
