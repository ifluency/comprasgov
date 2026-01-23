import os
import psycopg2


def get_conn():
    db_url = os.environ["DATABASE_URL"].strip()

    # Proteção contra colarem "psql postgresql://..."
    if db_url.lower().startswith("psql "):
        raise ValueError(
            "DATABASE_URL está no formato de comando 'psql ...'. "
            "Use somente a URL começando com 'postgresql://...'."
        )

    # Garante SSL
    if "sslmode=" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"

    return psycopg2.connect(db_url)
