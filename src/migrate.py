from pathlib import Path
from db import get_conn

def main():
    sql = Path("migrations/001_init.sql").read_text(encoding="utf-8")
    conn = get_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        print("Migration aplicada com sucesso.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
