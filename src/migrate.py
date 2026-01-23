from pathlib import Path
from db import get_conn

MIGRATIONS_DIR = Path("migrations")

def main():
    conn = get_conn()
    conn.autocommit = True
    try:
        files = sorted(MIGRATIONS_DIR.glob("*.sql"))
        if not files:
            raise RuntimeError("Nenhuma migration encontrada em migrations/*.sql")

        with conn.cursor() as cur:
            for f in files:
                sql = f.read_text(encoding="utf-8")
                cur.execute(sql)
                print(f"[MIGRATE] OK {f.name}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
