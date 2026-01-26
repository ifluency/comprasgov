import os
from pathlib import Path
from typing import Set, List

from db import get_conn


MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


def ensure_schema_migrations(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists schema_migrations (
              filename text primary key,
              applied_at timestamptz not null default now()
            );
            """
        )
    conn.commit()


def already_applied(conn) -> Set[str]:
    ensure_schema_migrations(conn)
    with conn.cursor() as cur:
        cur.execute("select filename from schema_migrations;")
        return {r[0] for r in cur.fetchall()}


def list_migration_files() -> List[Path]:
    if not MIGRATIONS_DIR.exists():
        raise RuntimeError(f"Pasta de migrations não encontrada: {MIGRATIONS_DIR}")
    return sorted([p for p in MIGRATIONS_DIR.glob("*.sql") if p.is_file()])


def apply_migration(conn, filename: str, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(
            "insert into schema_migrations(filename) values (%s) on conflict (filename) do nothing;",
            (filename,),
        )
    conn.commit()


def main() -> None:
    conn = get_conn()
    try:
        applied = already_applied(conn)
        for path in list_migration_files():
            fname = path.name
            if fname in applied:
                print(f"[MIGRATE] SKIP {fname}")
                continue

            sql = path.read_text(encoding="utf-8").strip()
            if not sql:
                print(f"[MIGRATE] SKIP(empty) {fname}")
                apply_migration(conn, fname, "select 1;")
                continue

            print(f"[MIGRATE] APPLY {fname}")
            apply_migration(conn, fname, sql)
            print(f"[MIGRATE] OK {fname}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
