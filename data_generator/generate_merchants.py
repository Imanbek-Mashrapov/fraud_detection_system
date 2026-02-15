from typing import List, Tuple
from faker import Faker
from data_generator.config import RANDOM_SEED

fake = Faker()
fake.seed_instance(RANDOM_SEED)

CATEGORIES = [
    "food",
    "electronics",
    "travel",
    "subscriptions",
    "fashion",
    "entertainment",
    "health",
    "utilities",
    "gambling",
    "transfer"
]

WEIGHTS = [
    0.25,  # food
    0.12,  # electronics
    0.06,  # travel
    0.10,  # subscriptions
    0.10,  # fashion
    0.07,  # entertainment
    0.05,  # health
    0.10,  # utilities
    0.03,  # gambling
    0.12   # transfer (P2P)
]

def generate_merchants(n_merchants) -> List[Tuple[str, str]]:
    merchants = []

    for _ in range(n_merchants):
        merchant_id = fake.uuid4()
        merchant_category = fake.random.choices(
            CATEGORIES,
            weights = WEIGHTS
        )[0]

        merchants.append((
            merchant_id,
            merchant_category
        ))

    return merchants
