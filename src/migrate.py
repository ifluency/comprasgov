from pathlib import Path
from db import get_conn


def main():
    migrations_dir = Path("migrations")
    files = sorted(migrations_dir.glob("*.sql"))

    if not files:
        raise RuntimeError("Nenhuma migration .sql encontrada em /migrations")

    conn = get_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for f in files:
                sql = f.read_text(encoding="utf-8")
                cur.execute(sql)
                print(f"[MIGRATE] OK {f.name}")
        print("[MIGRATE] Todas as migrations aplicadas com sucesso.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
