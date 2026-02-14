from pathlib import Path
from database.connection import get_connection
import sys
import subprocess

BASE_DIR = Path(__file__).resolve().parents[1]

def run_sql_file(path, name):
    print(f"=== {name} ===")

    sql_path = BASE_DIR / path
    sql = sql_path.read_text(encoding="utf-8")

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    print("STEP 1: generate raw data")
    subprocess.run([sys.executable, "-m", "data_generator.run"], check=True)

    run_sql_file("database/etl/raw_to_core_users.sql",
                "STEP 2.1: raw → core.users")

    run_sql_file("database/etl/raw_to_core_transactions.sql",
                "STEP 2.2: raw → core.transactions")

    run_sql_file("database/etl/core_to_transaction_feature_1h.sql",
                "STEP 3.1: features 1h")

    run_sql_file("database/etl/core_to_transaction_feature_24h.sql",
                "STEP 3.2: features 24h")

    run_sql_file("database/etl/core_to_user_behavior_feature.sql",
                "STEP 3.3: user behavior features")

    print("PIPELINE FINISHED SUCCESSFULLY")


if __name__ == "__main__": 
    main()
