from __future__ import annotations

from pathlib import Path

from db import get_conn


MIGRATIONS_DIR = Path("migrations")


SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
  filename TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
""".strip()


def _ensure_schema_migrations(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_MIGRATIONS_DDL)


def _get_applied(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT filename FROM schema_migrations")
        return {r[0] for r in cur.fetchall()}


def _apply_file(conn, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(
            "INSERT INTO schema_migrations(filename) VALUES (%s) ON CONFLICT DO NOTHING",
            (path.name,),
        )


def main() -> None:
    if not MIGRATIONS_DIR.exists():
        raise RuntimeError("Diretório migrations/ não encontrado")

    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        raise RuntimeError("Nenhuma migration encontrada em migrations/*.sql")

    conn = get_conn()
    conn.autocommit = True
    try:
        _ensure_schema_migrations(conn)
        applied = _get_applied(conn)

        for f in files:
            if f.name in applied:
                print(f"[MIGRATE] SKIP {f.name}")
                continue
            _apply_file(conn, f)
            print(f"[MIGRATE] OK {f.name}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
