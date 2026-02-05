from datetime import datetime
from data_generator.generate_users import generate_users
from data_generator.generate_devices import generate_devices
from data_generator.generate_merchants import generate_merchants
from data_generator.generate_transactions import generate_transactions
from data_generator.label_transactions import label_transactions
import os
from database.connection import get_connection
from data_generator.config import GENERATOR_MODE
# python -m data_generator.run


if GENERATOR_MODE not in {"DEV", "INCREMENTAL"}:
    raise ValueError("GENERATOR_MODE must be DEV or INCREMENTAL")

print(f"Running data generator in {GENERATOR_MODE} mode")

def reset_raw_tables(cur):
    cur.execute("""
        TRUNCATE TABLE
            raw.transactions,
            raw.users
        CASCADE;
    """)


def insert_users(cur, users):
    cur.executemany(
        """
        INSERT INTO raw.users (user_id, registration_date, home_country, risk_segment)
        VALUES (%s, %s, %s, %s)
        """,
        users
    )


def insert_devices(cur, devices_by_user):
    rows = []
    for user_devices in devices_by_user.values():
        for d in user_devices:
            rows.append((
                d["device_id"],
                d["device_type"],
                d["first_seen_ts"]
            ))
    
    cur.executemany(
        """
        INSERT INTO core.devices (device_id, device_type, first_seen_ts)
        VALUES (%s, %s, %s)
        ON CONFLICT (device_id) DO NOTHING;
        """,
        rows
    )


def insert_merchants(cur, merchants):
    cur.executemany(
        """
        INSERT INTO core.merchants (merchant_id, merchant_category)
        VALUES (%s, %s)
        ON CONFLICT (merchant_id) DO NOTHING;
        """,
        merchants
    )


def insert_transactions(cur, labeled_transactions):
    cur.executemany(
        """
        INSERT INTO raw.transactions (
            transaction_id,
            user_id,
            amount,
            currency,
            merchant_id,
            merchant_category,
            transaction_country,
            device_id,
            transaction_ts,
            ingestion_ts
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                tx[0],  # transaction_id
                tx[1],  # user_id
                tx[2],  # amount
                tx[3],  # currency
                tx[4],  # merchant_id
                tx[5],  # merchant_category
                tx[6],  # transaction_country
                tx[7],  # device_id
                tx[8],  # transaction_ts (event time)
                datetime.utcnow()  # ingestion_ts (system time)
            )
            for tx in labeled_transactions
        ]
    )


def insert_fraud_predictions(cur, labeled_transactions):
    cur.executemany(
    """
    INSERT INTO mart.fraud_predictions (
        transaction_id,
        fraud_probability,
        is_fraud,
        model_version,
        score_source,
        prediction_ts
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
    """,
    [
        (
            tx[0],          # transaction_id
            tx[-3],         # fraud_score → fraud_probability
            tx[-2],         # is_fraud
            "rules_v1",
            "rules",
            datetime.utcnow()
        )
        for tx in labeled_transactions
    ]
)


def main():
    print(f"Starting data generation ({GENERATOR_MODE} mode)")

    users = generate_users()
    devices_by_user = generate_devices(users)
    merchants = generate_merchants(10)

    transactions = generate_transactions(
        users=users,
        devices_by_user=devices_by_user,
        merchants=merchants
    )

    labeled_transactions = label_transactions(
        transactions=transactions,
        users=users
    )

    fraud_rate = sum(tx[-2] for tx in labeled_transactions) / len(labeled_transactions)
    print(f"Fraud rate: {fraud_rate:.4f}")
    assert 0.0 <= fraud_rate <= 0.05, "Fraud rate out of expected range"

    conn = get_connection()
    cur = conn.cursor()

    try:
        if GENERATOR_MODE == "DEV":
            print("Resetting raw tables (DEV mode)")
            reset_raw_tables(cur)

        insert_users(cur, users)
        insert_devices(cur, devices_by_user)
        insert_merchants(cur, merchants)
        insert_transactions(cur, labeled_transactions)
        insert_fraud_predictions(cur, labeled_transactions)

        conn.commit()
        print("Data successfully written to database")

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()

    print("Pipeline finished")


if __name__ == "__main__":
    main()