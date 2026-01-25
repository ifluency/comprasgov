"""Ingest itens de contratações PNCP (Lei 14.133) por idCompra.

Endpoint:
  /modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id
Params:
  tipo=idCompra
  codigo=<id_compra>

Fonte do id_compra:
  tabela contratacao_pncp_14133 (coluna id_compra)

Env:
  DATABASE_URL (obrigatório)
  COMPRAS_BASE_URL (default https://dadosabertos.compras.gov.br)
  COMPRAS_SLEEP_S (default 0.10)
  COMPRAS_ITEM_LIMIT (opcional) -> limita quantas compras serão processadas
"""

from __future__ import annotations

import json
import os
import time
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from db import get_conn


BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").rstrip("/")
ITEM_PATH = "/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id"


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _request_json(session: requests.Session, url: str, params: Dict[str, Any], max_attempts: int = 5) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, params=params, timeout=60)
            print(f"[HTTP] attempt={attempt} status={resp.status_code} url={resp.url}")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            # backoff simples
            time.sleep(min(2.0 * attempt, 10.0))
    raise RuntimeError(f"Falha ao chamar {url} params={params}. Último erro: {last_err}")


def insert_raw(cur, endpoint: str, params: Dict[str, Any], payload: Dict[str, Any]) -> None:
    # api_raw exige payload_sha256 (NOT NULL) e há UNIQUE(endpoint, payload_sha256)
    payload_bytes = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()

    cur.execute(
        """
        INSERT INTO api_raw (endpoint, params, payload, payload_sha256, fetched_at)
        VALUES (%s, %s::jsonb, %s::jsonb, %s, now())
        ON CONFLICT (endpoint, payload_sha256) DO NOTHING
        """,
        (
            endpoint,
            json.dumps(params, ensure_ascii=False),
            json.dumps(payload, ensure_ascii=False),
            payload_sha256,
        ),
    )


def upsert_item(cur, item: Dict[str, Any]) -> None:
    # Persistimos o JSON bruto + alguns campos úteis como colunas.
    cur.execute(
        """
        INSERT INTO contratacao_item_pncp_14133 (
          id_compra_item,
          id_compra,
          numero_item_pncp,
          numero_item_compra,
          descricao_resumida,
          material_ou_servico,
          cod_item_catalogo,
          cod_fornecedor,
          nome_fornecedor,
          valor_total,
          valor_total_resultado,
          data_inclusao_pncp,
          data_atualizacao_pncp,
          raw_json
        ) VALUES (
          %(idCompraItem)s,
          %(idCompra)s,
          %(numeroItemPncp)s,
          %(numeroItemCompra)s,
          %(descricaoResumida)s,
          %(materialOuServico)s,
          %(codItemCatalogo)s,
          %(codFornecedor)s,
          %(nomeFornecedor)s,
          %(valorTotal)s,
          %(valorTotalResultado)s,
          %(dataInclusaoPncp)s,
          %(dataAtualizacaoPncp)s,
          %(raw_json)s::jsonb
        )
        ON CONFLICT (id_compra_item) DO UPDATE SET
          id_compra = EXCLUDED.id_compra,
          numero_item_pncp = EXCLUDED.numero_item_pncp,
          numero_item_compra = EXCLUDED.numero_item_compra,
          descricao_resumida = EXCLUDED.descricao_resumida,
          material_ou_servico = EXCLUDED.material_ou_servico,
          cod_item_catalogo = EXCLUDED.cod_item_catalogo,
          cod_fornecedor = EXCLUDED.cod_fornecedor,
          nome_fornecedor = EXCLUDED.nome_fornecedor,
          valor_total = EXCLUDED.valor_total,
          valor_total_resultado = EXCLUDED.valor_total_resultado,
          data_inclusao_pncp = EXCLUDED.data_inclusao_pncp,
          data_atualizacao_pncp = EXCLUDED.data_atualizacao_pncp,
          raw_json = EXCLUDED.raw_json
        """,
        {
            "idCompraItem": item.get("idCompraItem"),
            "idCompra": item.get("idCompra"),
            "numeroItemPncp": item.get("numeroItemPncp"),
            "numeroItemCompra": item.get("numeroItemCompra"),
            "descricaoResumida": item.get("descricaoResumida"),
            "materialOuServico": item.get("materialOuServico"),
            "codItemCatalogo": item.get("codItemCatalogo"),
            "codFornecedor": item.get("codFornecedor"),
            "nomeFornecedor": item.get("nomeFornecedor"),
            "valorTotal": item.get("valorTotal"),
            "valorTotalResultado": item.get("valorTotalResultado"),
            "dataInclusaoPncp": item.get("dataInclusaoPncp"),
            "dataAtualizacaoPncp": item.get("dataAtualizacaoPncp"),
            "raw_json": json.dumps(item, ensure_ascii=False),
        },
    )


def iter_id_compra(conn, limit: Optional[int] = None) -> Iterable[str]:
    sql = "SELECT id_compra FROM contratacao_pncp_14133 ORDER BY data_publicacao_pncp ASC"
    if limit is not None:
        sql += " LIMIT %s"
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            for (id_compra,) in cur.fetchall():
                yield id_compra
    else:
        with conn.cursor() as cur:
            cur.execute(sql)
            for (id_compra,) in cur.fetchall():
                yield id_compra


def main() -> None:
    sleep_s = _env_float("COMPRAS_SLEEP_S", 0.10)
    limit = _env_int("COMPRAS_ITEM_LIMIT", None)

    print("[ITENS] START")
    print(f"[ITENS] BASE_URL={BASE_URL}")
    print(f"[ITENS] ITEM_PATH={ITEM_PATH}")
    if limit is not None:
        print(f"[ITENS] limit={limit}")

    conn = get_conn()
    session = requests.Session()

    ok = 0
    fail = 0
    total_items = 0

    try:
        for id_compra in iter_id_compra(conn, limit=limit):
            url = f"{BASE_URL}{ITEM_PATH}"
            params = {"tipo": "idCompra", "codigo": id_compra}
            try:
                payload = _request_json(session, url, params)
                results = payload.get("resultado") or []
                print(f"[ITENS] id_compra={id_compra} items={len(results)}")

                with conn.cursor() as cur:
                    insert_raw(cur, f"modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id", params, payload)
                    for item in results:
                        upsert_item(cur, item)
                conn.commit()

                ok += 1
                total_items += len(results)
            except Exception as e:
                conn.rollback()
                fail += 1
                print(f"[EXC] id_compra={id_compra} err={e}")
            time.sleep(sleep_s)

        print(f"[DONE] ok={ok} fail={fail} itens={total_items}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
