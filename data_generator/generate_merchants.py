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
    "gambling"
]

WEIGHTS = [
    0.30,
    0.15,
    0.10,
    0.10,
    0.10,
    0.08,
    0.07,
    0.07,
    0.03
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
