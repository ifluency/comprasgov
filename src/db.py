import os
import psycopg2


def get_conn():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL não definido.")
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn
