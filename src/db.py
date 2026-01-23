import os
import psycopg2

def get_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL não definido (GitHub Secret / env).")
    return psycopg2.connect(db_url)
