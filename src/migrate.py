from __future__ import annotations

import os
from pathlib import Path
from typing import List, Set, Optional

from db import get_conn


MIGRATIONS_DIR = Path("migrations")


def ensure_schema_migrations(conn) -> None:
    """
    Ensures schema_migrations exists with a 'version' column.
    If the table exists without 'version', attempts to migrate from an existing text column.
    """
    with conn.cursor() as cur:
        # Create table if not exists (correct shape)
        cur.execute(
            """
            create table if not exists schema_migrations (
              version text primary key,
              applied_at timestamptz not null default now()
            );
            """
        )

        # Check if 'version' column exists
        cur.execute(
            """
            select 1
            from information_schema.columns
            where table_schema = current_schema()
              and table_name = 'schema_migrations'
              and column_name = 'version'
            limit 1;
            """
        )
        has_version = cur.fetchone() is not None
        if has_version:
            return

        # If table exists but no 'version' column, find a likely existing column to copy from
        cur.execute(
            """
            select column_name, data_type
            from information_schema.columns
            where table_schema = current_schema()
              and table_name = 'schema_migrations'
            order by ordinal_position;
            """
        )
        cols = cur.fetchall()  # [(name, type), ...]
        # pick first text-like column as source
        source_col: Optional[str] = None
        for name, dtype in cols:
            if dtype in ("text", "character varying", "character"):
                source_col = name
                break

        # Add version column
        cur.execute("alter table schema_migrations add column if not exists version text;")

        # Copy from source if found and different
        if source_col and source_col != "version":
            cur.execute(
                f"""
                update schema_migrations
                   set version = {source_col}
                 where version is null;
                """
            )

        # Ensure not null when possible (only if no nulls)
        cur.execute("select count(*) from schema_migrations where version is null;")
        nulls = cur.fetchone()[0]
        if nulls == 0:
            # Try to enforce uniqueness / PK (best effort)
            # 1) unique index
            cur.execute("create unique index if not exists schema_migrations_version_ux on schema_migrations(version);")
            # 2) if table has no pk, try set PK
            cur.execute(
                """
                select conname
                from pg_constraint
                where conrelid = 'schema_migrations'::regclass
                  and contype = 'p'
                limit 1;
                """
            )
            has_pk = cur.fetchone() is not None
            if not has_pk:
                # Best effort: add PK on version
                try:
                    cur.execute("alter table schema_migrations add constraint schema_migrations_pkey primary key (version);")
                except Exception:
                    # If it fails (e.g., duplicates), we keep unique index already created.
                    conn.rollback()
                    conn.autocommit = True


def list_migration_files() -> List[Path]:
    if not MIGRATIONS_DIR.exists():
        return []
    files = sorted([p for p in MIGRATIONS_DIR.glob("*.sql") if p.is_file()])
    return files


def already_applied(conn) -> Set[str]:
    ensure_schema_migrations(conn)
    with conn.cursor() as cur:
        cur.execute("select version from schema_migrations;")
        return {row[0] for row in cur.fetchall()}


def apply_migration(conn, version: str, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("insert into schema_migrations(version) values (%s) on conflict (version) do nothing;", (version,))


def main():
    conn = get_conn()
    conn.autocommit = True

    try:
        ensure_schema_migrations(conn)
        applied = already_applied(conn)

        files = list_migration_files()
        if not files:
            print("[MIGRATE] No migrations found.")
            return

        for path in files:
            version = path.name  # store filename as version
            if version in applied:
                print(f"[MIGRATE] SKIP {version}")
                continue

            sql = path.read_text(encoding="utf-8").strip()
            if not sql:
                print(f"[MIGRATE] EMPTY {version} (skipping)")
                apply_migration(conn, version, "select 1;")
                continue

            apply_migration(conn, version, sql)
            print(f"[MIGRATE] OK {version}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
