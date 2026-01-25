from __future__ import annotations

import os
from pathlib import Path
from typing import List

from db import get_conn


MIGRATIONS_DIR = Path("migrations")


def ensure_schema_migrations(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists schema_migrations (
              version text primary key,
              applied_at timestamptz not null default now()
            );
            """
        )


def list_migration_files() -> List[Path]:
    if not MIGRATIONS_DIR.exists():
        raise RuntimeError(f"Pasta de migrations não encontrada: {MIGRATIONS_DIR.resolve()}")
    files = sorted([p for p in MIGRATIONS_DIR.glob("*.sql") if p.is_file()])
    return files


def already_applied(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("select version from schema_migrations;")
        return {r[0] for r in cur.fetchall()}


def apply_one(conn, path: Path) -> None:
    sql = path.read_text(encoding="utf-8").strip()
    if not sql:
        print(f"[MIGRATE] SKIP (empty) {path.name}")
        return

    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("insert into schema_migrations(version) values (%s);", (path.name,))

    print(f"[MIGRATE] OK {path.name}")


def main() -> None:
    conn = get_conn()
    conn.autocommit = True
    try:
        ensure_schema_migrations(conn)

        applied = already_applied(conn)
        files = list_migration_files()

        for f in files:
            if f.name in applied:
                print(f"[MIGRATE] SKIP {f.name}")
                continue
            apply_one(conn, f)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
