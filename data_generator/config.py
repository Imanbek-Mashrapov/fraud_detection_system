import os
import time
import random

GENERATOR_MODE = os.getenv("GENERATOR_MODE", "DEV")

if GENERATOR_MODE not in {"DEV", "INCREMENTAL"}:
    raise ValueError("GENERATOR_MODE must be DEV or INCREMENTAL")

# seed 
if GENERATOR_MODE == "INCREMENTAL":
    RANDOM_SEED = int(time.time())
else:
    RANDOM_SEED = 42

random.seed(RANDOM_SEED)

# Generator parameters
N_USERS = 500
MAX_DEVICE_PER_USER = 2

TX_DAYS = 21
AVG_TX_PER_DAY = {
    "low": 3,
    "medium": 5,
    "high": 10
}

FRAUD_BASE_PROB = 0.01
