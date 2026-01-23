from data_generator.fraud_logic import compute_fraud_score, is_fraud
from collections import defaultdict
from datetime import timedelta

user_tx_history = defaultdict(list)
user_devices_seen = defaultdict(set)

def label_transactions(transactions, users):
    user_map = {
        u[0]: {
            "registration_date": u[1],
            "home_country": u[2],
            "risk_segment": u[3]
        }
        for u in users
    }

    labeled = []

    for tx in sorted(transactions, key=lambda x: x[-1]):
        (
            transaction_id,
            user_id,
            amount,
            currency,
            merchant_id,
            merchant_category,
            transaction_country,
            device_id,
            transaction_ts
        ) = tx

        history = user_tx_history[user_id]

        tx_count_last_1h = sum(
            1 for h in history
            if transaction_ts - h <= timedelta(hours=1)
        )

        is_new_device = device_id not in user_devices_seen[user_id]

        context = {
            "transaction_ts": transaction_ts,
            "transaction_country": transaction_country,
            "merchant_category": merchant_category,
            "tx_count_last_1h": tx_count_last_1h,
            "is_new_device": is_new_device,
            "user_home_country": user_map[user_id]["home_country"],
            "user_registration_date": user_map[user_id]["registration_date"],
            "risk_segment": user_map[user_id]["risk_segment"]
        }

        score, reasons = compute_fraud_score(context)
        fraud_flag = is_fraud(score)

        labeled.append(tx + (score, fraud_flag, reasons))

        history.append(transaction_ts)
        user_devices_seen[user_id].add(device_id)

    return labeled