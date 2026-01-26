from dotenv import load_dotenv
from pathlib import Path
import os
import psycopg2

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(env_path, override=True)

def get_connection():
    try:
        print("DB_HOST:", os.getenv("DB_HOST"))
        print("DB_PORT:", os.getenv("DB_PORT"))
        print("DB_NAME:", os.getenv("DB_NAME"))
        print("DB_USER:", os.getenv("DB_USER"))

        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
    except psycopg2.Error as e:
        raise RuntimeError(f"Ошибка подключения к БД: {e}")
    
if __name__ == "__main__":
    get_connection()