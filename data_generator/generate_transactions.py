from typing import List, Dict, Tuple
from faker import Faker
from datetime import datetime, timedelta
import random

from data_generator.config import RANDOM_SEED, TX_DAYS, AVG_TX_PER_DAY


fake = Faker()
fake.seed_instance(RANDOM_SEED)
random.seed(RANDOM_SEED)

CURRENCIES = ["USD", "EUR", "RUB", "KGS"]
AMOUNT_RANGES = {
    "USD": (1, 25000),
    "EUR": (1, 25000),
    "RUB": (1, 300000),
    "KGS": (1, 300000)
}


def generate_transactions(
    users: List[tuple],
    devices_by_user: Dict[str, List[dict]],
    merchants: List[tuple]
) -> List[Tuple]:

    transactions = []
    start_date = datetime.now() - timedelta(days=TX_DAYS)

    for user in users:
        user_id = user[0]
        registration_date = user[1]
        risk_segment = user[3]

        avg_tx = AVG_TX_PER_DAY[risk_segment]

        user_devices = devices_by_user[user_id]

        for day_offset in range(TX_DAYS):
            current_day = start_date + timedelta(days=day_offset)

            tx_count_today = max(0, int(random.gauss(avg_tx, avg_tx * 0.3)))

            for _ in range(tx_count_today):
                transaction_id = fake.uuid4()
                transaction_ts = fake.date_time_between(start_date=current_day, end_date=current_day + timedelta(days=1))
                device = random.choice(user_devices)
                merchant = random.choice(merchants)
                currency = random.choice(CURRENCIES)
                min_amt, max_amt = AMOUNT_RANGES[currency]
                amount = round(random.uniform(min_amt, max_amt), 2)
                transaction_country = fake.country_code()

                transactions.append((
                    transaction_id,
                    user_id,
                    amount,
                    currency,
                    merchant[0],
                    merchant[1],
                    transaction_country,
                    device["device_id"],
                    transaction_ts
                ))

    return transactions
