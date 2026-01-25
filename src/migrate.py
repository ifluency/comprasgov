from __future__ import annotations

from pathlib import Path
from typing import List, Set, Optional

from db import get_conn

MIGRATIONS_DIR = Path("migrations")

# Colunas comuns que já vimos em tabelas schema_migrations em projetos diferentes
CANDIDATE_VERSION_COLS = ["version", "name", "migration", "filename", "file", "id"]


def ensure_schema_migrations_table(conn) -> None:
    """
    Garante que a tabela exista. Não assume a estrutura.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists schema_migrations (
              version text,
              applied_at timestamptz not null default now()
            );
            """
        )


def get_existing_columns(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
              from information_schema.columns
             where table_schema = 'public'
               and table_name = 'schema_migrations'
             order by ordinal_position;
            """
        )
        return [r[0] for r in cur.fetchall()]


def pick_version_column(cols: List[str]) -> Optional[str]:
    for c in CANDIDATE_VERSION_COLS:
        if c in cols:
            return c
    return None


def ensure_version_column(conn) -> str:
    """
    Retorna a coluna que vamos usar como "identificador da migration".
    Se não existir nenhuma coluna candidata, cria 'version'.
    """
    cols = get_existing_columns(conn)
    picked = pick_version_column(cols)
    if picked:
        return picked

    # Se chegou aqui, não existe nenhuma coluna candidata -> cria "version"
    with conn.cursor() as cur:
        cur.execute("alter table schema_migrations add column if not exists version text;")
    return "version"


def list_migration_files() -> List[Path]:
    if not MIGRATIONS_DIR.exists():
        raise RuntimeError(f"Pasta de migrations não encontrada: {MIGRATIONS_DIR.resolve()}")
    return sorted([p for p in MIGRATIONS_DIR.glob("*.sql") if p.is_file()])


def already_applied(conn, version_col: str) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute(f"select {version_col} from schema_migrations where {version_col} is not null;")
        return {r[0] for r in cur.fetchall()}


def apply_one(conn, path: Path, version_col: str) -> None:
    sql = path.read_text(encoding="utf-8").strip()
    if not sql:
        print(f"[MIGRATE] SKIP (empty) {path.name}")
        return

    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(
            f"insert into schema_migrations({version_col}) values (%s);",
            (path.name,),
        )

    print(f"[MIGRATE] OK {path.name}")


def main() -> None:
    conn = get_conn()
    conn.autocommit = True
    try:
        ensure_schema_migrations_table(conn)
        version_col = ensure_version_column(conn)

        applied = already_applied(conn, version_col)
        files = list_migration_files()

        for f in files:
            if f.name in applied:
                print(f"[MIGRATE] SKIP {f.name}")
                continue
            apply_one(conn, f, version_col)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
