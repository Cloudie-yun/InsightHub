import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()  # loads .env into environment variables

def get_db_connection():
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if database_url:
        return psycopg2.connect(database_url)

    connection_kwargs = {
        "host": os.getenv("DB_HOST", "localhost"),
        "database": os.getenv("DB_NAME", "InsightHubDB"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT", "5432"),
    }

    sslmode = (os.getenv("DB_SSLMODE") or "").strip()
    if sslmode:
        connection_kwargs["sslmode"] = sslmode

    return psycopg2.connect(**connection_kwargs)
