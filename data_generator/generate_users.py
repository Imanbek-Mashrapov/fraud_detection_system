from typing import List, Tuple
from faker import Faker
from data_generator.config import RANDOM_SEED, N_USERS

fake = Faker()
fake.seed_instance(RANDOM_SEED)

def generate_users() -> List[Tuple]:
    users = []

    for _ in range(N_USERS):
        user_id = fake.uuid4()
        registration_date = fake.date_between(start_date='-3y', end_date='today')
        home_country = fake.country_code()
        risk_segment = fake.random.choices(
            ["low", "medium", "high"],
            [0.7, 0.2, 0.1] 
        )[0]

        users.append((
            user_id,
            registration_date,
            home_country,
            risk_segment
        ))

    return users


