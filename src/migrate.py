from __future__ import annotations

import os
from pathlib import Path
from typing import List, Set, Optional

from db import get_conn
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Set, Tuple

from db import get_conn

MIGRATIONS_DIR = Path("migrations")


def list_migration_files() -> List[Path]:
    if not MIGRATIONS_DIR.exists():
        return []
    return sorted([p for p in MIGRATIONS_DIR.glob("*.sql") if p.is_file()])


def get_table_columns(conn, table_name: str) -> Dict[str, Dict[str, object]]:
    """
    Returns:
      { colname: { "data_type": str, "is_nullable": bool, "has_default": bool } }
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              c.column_name,
              c.data_type,
              (c.is_nullable = 'YES') as is_nullable,
              (c.column_default is not null) as has_default
            from information_schema.columns c
            where c.table_schema = current_schema()
              and c.table_name = %s
            order by c.ordinal_position;
            """,
            (table_name,),
        )
        cols = {}
        for name, dtype, is_nullable, has_default in cur.fetchall():
            cols[name] = {
                "data_type": dtype,
                "is_nullable": bool(is_nullable),
                "has_default": bool(has_default),
            }
        return cols


def ensure_schema_migrations(conn) -> None:
    """
    Create a minimal schema_migrations if it doesn't exist.
    If it exists, we DON'T try to reshape it aggressively — we adapt at runtime.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists schema_migrations (
              version text primary key,
              applied_at timestamptz not null default now()
            );
            """
        )


def get_applied_identifier_column(conn) -> str:
    """
    Prefer 'version' if it exists. Otherwise use 'filename' if it exists.
    Fallback: raise.
    """
    cols = get_table_columns(conn, "schema_migrations")
    if "version" in cols:
        return "version"
    if "filename" in cols:
        return "filename"
    raise RuntimeError("schema_migrations exists but has neither 'version' nor 'filename' column.")


def already_applied(conn) -> Set[str]:
    ensure_schema_migrations(conn)
    ident_col = get_applied_identifier_column(conn)

    with conn.cursor() as cur:
        cur.execute(f"select {ident_col} from schema_migrations;")
        return {row[0] for row in cur.fetchall() if row[0] is not None}


def build_insert_for_schema_migrations(conn, migration_id: str) -> Tuple[str, Tuple[object, ...]]:
    """
    Builds an INSERT statement that satisfies NOT NULL constraints for the existing schema_migrations shape.

    Common shapes seen:
      - (version PK, applied_at default)
      - (filename NOT NULL, applied_at default)
      - (filename NOT NULL, applied_at NOT NULL default, version nullable, etc.)

    We will populate:
      - version = migration_id if column exists
      - filename = migration_id if column exists
    """
    cols = get_table_columns(conn, "schema_migrations")

    insert_cols: List[str] = []
    values: List[object] = []

    # Fill both if present
    if "version" in cols:
        insert_cols.append("version")
        values.append(migration_id)

    if "filename" in cols:
        insert_cols.append("filename")
        values.append(migration_id)

    # If neither exists (shouldn't happen due to get_applied_identifier_column), fallback
    if not insert_cols:
        raise RuntimeError("schema_migrations has no usable identifier columns ('version'/'filename').")

    # Decide conflict target
    conflict_target = "version" if "version" in cols else "filename"

    placeholders = ", ".join(["%s"] * len(insert_cols))
    col_list = ", ".join(insert_cols)

    sql = f"""
        insert into schema_migrations({col_list})
        values ({placeholders})
        on conflict ({conflict_target}) do nothing;
    """.strip()

    return sql, tuple(values)


def apply_migration(conn, version: str, sql: str) -> None:
    with conn.cursor() as cur:
        # run migration
        cur.execute(sql)

        # mark applied (compatible with current schema_migrations)
        ins_sql, params = build_insert_for_schema_migrations(conn, version)
        cur.execute(ins_sql, params)


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
            version = path.name  # store filename as the migration id
            if version in applied:
                print(f"[MIGRATE] SKIP {version}")
                continue

            sql = path.read_text(encoding="utf-8").strip()
            if not sql:
                # still mark as applied so it doesn't loop forever
                apply_migration(conn, version, "select 1;")
                print(f"[MIGRATE] OK {version} (empty)")
                continue

            apply_migration(conn, version, sql)
            print(f"[MIGRATE] OK {version}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

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
