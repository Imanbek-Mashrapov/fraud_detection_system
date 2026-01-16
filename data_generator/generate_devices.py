from typing import List, Dict
from faker import Faker
from data_generator.config import RANDOM_SEED

fake = Faker()
fake.seed_instance(RANDOM_SEED)

def generate_devices(users) -> Dict[str, List[dict]]:
    devices_by_user = {}

    for user in users:
        user_id = user[0]
        registration_date = user[1]

        num_devices = fake.random.choices(
            population=[1, 2, 3],
            weights=[0.7, 0.2, 0.1]
        )[0]

        devices = []

        for _ in range(num_devices):
            device_id = fake.uuid4()
            device_type = fake.random.choices(
                ["mobile", "web"],
                weights=[0.8, 0.2]
            )[0]
            first_seen_ts = fake.date_between(
                start_date=registration_date,
                end_date='today'
            )

            devices.append({
                "device_id": device_id,
                "device_type": device_type,
                "first_seen_ts": first_seen_ts
            })

        devices_by_user[user_id] = devices

    return devices_by_user

    