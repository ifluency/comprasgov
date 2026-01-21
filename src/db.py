import os
import psycopg2

def get_conn():
    db_url = os.environ["DATABASE_URL"].strip()  # <- remove \n e espaços

    # garante sslmode=require
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"

    return psycopg2.connect(db_url)
