#!/usr/bin/env python
"""Ingest de ITENS das contratações (PNCP 14.133) para a unidade 155125.

Endpoint:
  /modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id?tipo=idCompra&codigo=<idCompra>

Regras:
- Lê todos os id_compra já carregados em contratacao_pncp_14133 (coluna id_compra).
- Para cada id_compra, chama o endpoint e salva:
    1) registro completo em api_raw
    2) itens normalizados em contratacao_pncp_14133_item

Observação:
- Não criamos views agora; objetivo é deixar o ETL funcionando.
"""

import datetime as dt
import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional

import requests

from db import get_conn

BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").rstrip("/")
PATH_ITENS = os.getenv(
    "COMPRAS_ITENS_PATH",
    "/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
)
TIMEOUT = float(os.getenv("COMPRAS_TIMEOUT_S", "60"))
SLEEP_S = float(os.getenv("COMPRAS_SLEEP_S", "0.10"))

# Para rodar em lotes/CI (opcional)
LIMIT_IDS = int(os.getenv("COMPRAS_ITENS_LIMIT_IDS", "0"))  # 0 = sem limite
ONLY_MISSING = os.getenv("COMPRAS_ITENS_ONLY_MISSING", "true").lower() in ("1", "true", "yes", "y")


def sha256_json(obj) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def parse_dt(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        # Ex.: "2025-11-07T07:11:18" (sem timezone) ou "2025-03-18T01:15:28"
        # Mantemos como naive (interpretação local) ou ISO completo. Se vier com Z, converte.
        if isinstance(s, str) and s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9",
            "User-Agent": "comprasgov-ingestor/1.0",
        }
    )
    return s


def get_with_retry(session: requests.Session, url: str, params: Dict[str, Any]) -> requests.Response:
    max_tries = 5
    backoff = 2.0
    for attempt in range(1, max_tries + 1):
        r = session.get(url, params=params, timeout=TIMEOUT)
        print(f"[HTTP] attempt={attempt} status={r.status_code} url={r.url}", flush=True)

        if r.status_code in (429, 500, 502, 503, 504) and attempt < max_tries:
            time.sleep(backoff)
            backoff *= 2
            continue
        return r

    return r


def ensure_tables(conn):
    # Garante a existência, caso alguém rode ingest sem migrate (defensivo)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS contratacao_pncp_14133_item (
              id_compra              TEXT NOT NULL,
              id_compra_item         TEXT NOT NULL,
              data_inclusao_pncp     TIMESTAMPTZ NULL,
              data_atualizacao_pncp  TIMESTAMPTZ NULL,
              fetched_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              raw_json               JSONB NOT NULL,
              PRIMARY KEY (id_compra_item)
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_item_id_compra ON contratacao_pncp_14133_item (id_compra);")
    conn.commit()


def load_id_compras(conn) -> List[str]:
    with conn.cursor() as cur:
        if ONLY_MISSING:
            cur.execute(
                """
                SELECT c.id_compra
                FROM contratacao_pncp_14133 c
                LEFT JOIN contratacao_pncp_14133_item i ON i.id_compra = c.id_compra
                WHERE c.id_compra IS NOT NULL
                  AND i.id_compra IS NULL
                ORDER BY c.data_publicacao_pncp NULLS LAST, c.id_compra;
                """
            )
        else:
            cur.execute(
                """
                SELECT id_compra
                FROM contratacao_pncp_14133
                WHERE id_compra IS NOT NULL
                ORDER BY data_publicacao_pncp NULLS LAST, id_compra;
                """
            )
        rows = cur.fetchall()
    ids = [r[0] for r in rows if r and r[0]]
    if LIMIT_IDS > 0:
        ids = ids[:LIMIT_IDS]
    return ids


def upsert_api_raw(conn, url: str, params: Dict[str, Any], payload: Dict[str, Any]):
    sha = sha256_json(payload)
    now = dt.datetime.utcnow()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO api_raw (source, url, params_json, payload_json, sha256, fetched_at)
            VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s)
            ON CONFLICT (sha256) DO NOTHING;
            """,
            (
                "itens_contratacoes_pncp_14133",
                url,
                json.dumps(params, ensure_ascii=False),
                json.dumps(payload, ensure_ascii=False),
                sha,
                now,
            ),
        )
    conn.commit()


def upsert_items(conn, id_compra: str, itens: List[Dict[str, Any]]):
    if not itens:
        return
    with conn.cursor() as cur:
        for it in itens:
            id_compra_item = it.get("idCompraItem")
            if not id_compra_item:
                continue
            di = parse_dt(it.get("dataInclusaoPncp"))
            da = parse_dt(it.get("dataAtualizacaoPncp"))
            cur.execute(
                """
                INSERT INTO contratacao_pncp_14133_item (
                  id_compra,
                  id_compra_item,
                  data_inclusao_pncp,
                  data_atualizacao_pncp,
                  raw_json
                ) VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (id_compra_item) DO UPDATE SET
                  id_compra = EXCLUDED.id_compra,
                  data_inclusao_pncp = EXCLUDED.data_inclusao_pncp,
                  data_atualizacao_pncp = EXCLUDED.data_atualizacao_pncp,
                  raw_json = EXCLUDED.raw_json,
                  fetched_at = NOW();
                """,
                (
                    id_compra,
                    id_compra_item,
                    di,
                    da,
                    json.dumps(it, ensure_ascii=False),
                ),
            )
    conn.commit()


def main():
    url = f"{BASE_URL}{PATH_ITENS}"

    conn = get_conn()
    ensure_tables(conn)

    ids = load_id_compras(conn)
    print(f"[LOAD] id_compra_count={len(ids)} only_missing={ONLY_MISSING} limit_ids={LIMIT_IDS}", flush=True)

    session = make_session()

    ok = 0
    fail = 0

    for idx, id_compra in enumerate(ids, start=1):
        params = {"tipo": "idCompra", "codigo": id_compra}
        try:
            r = get_with_retry(session, url, params)
            if r.status_code != 200:
                print(f"[ERR] id_compra={id_compra} status={r.status_code} body={r.text[:500]}", flush=True)
                fail += 1
                time.sleep(SLEEP_S)
                continue

            payload = r.json()
            upsert_api_raw(conn, url, params, payload)

            itens = payload.get("resultado") or []
            upsert_items(conn, id_compra, itens)

            ok += 1
            if idx % 25 == 0:
                print(f"[PROGRESS] {idx}/{len(ids)} ok={ok} fail={fail}", flush=True)

        except Exception as e:
            print(f"[EXC] id_compra={id_compra} err={e}", flush=True)
            fail += 1

        time.sleep(SLEEP_S)

    print(f"[DONE] ok={ok} fail={fail}", flush=True)


if __name__ == "__main__":
    main()
