from dotenv import load_dotenv
from pathlib import Path
import os
import psycopg2

# ЯВНО указываем путь к .env
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(env_path)

def get_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
    except psycopg2.Error as e:
        raise RuntimeError(f"Ошибка подключения к БД: {e}")

