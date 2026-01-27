import os
import time
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_batch


BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").rstrip("/")
SLEEP_S = float(os.getenv("COMPRAS_SLEEP_S", "0.10"))

ENDPOINT = "modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def sha256_json(obj: Any) -> str:
    s = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def get_conn():
    url = os.environ["DATABASE_URL"]
    return psycopg2.connect(url)


def fetch_itens(id_compra: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/{ENDPOINT}"
    params = {"tipo": "idCompra", "codigo": id_compra}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def upsert_api_raw(
    cur,
    endpoint: str,
    params: Dict[str, Any],
    payload: Dict[str, Any],
    fetched_at: datetime,
) -> None:
    payload_sha = sha256_json(payload)
    cur.execute(
        """
        INSERT INTO api_raw (endpoint, params, payload, payload_sha256, fetched_at)
        VALUES (%s, %s::jsonb, %s::jsonb, %s, %s)
        ON CONFLICT (endpoint, payload_sha256)
        DO UPDATE SET
          params = EXCLUDED.params,
          payload = EXCLUDED.payload,
          fetched_at = EXCLUDED.fetched_at
        """,
        (endpoint, json.dumps(params, ensure_ascii=False), json.dumps(payload, ensure_ascii=False), payload_sha, fetched_at),
    )


def upsert_item(cur, item: Dict[str, Any]) -> None:
    # Persistimos o JSON bruto + alguns campos úteis como colunas.
    # dataResultado às vezes vem como "YYYY-MM-DD 00:00:00.0000000".
    # Para garantir cast seguro para DATE, normalizamos para "YYYY-MM-DD".
    data_resultado = item.get("dataResultado")
    if isinstance(data_resultado, str) and data_resultado:
        # aceita "2024-02-19 00:00:00.0000000" e "2024-02-19T00:00:00"
        item["dataResultado"] = data_resultado[:10]

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
          valor_unitario_estimado,
          valor_unitario_resultado,
          data_resultado,
          situacao_compra_item_nome,
          tem_resultado,
          data_inclusao_pncp,
          data_atualizacao_pncp,
          raw_json
        )
        VALUES (
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
          %(valorUnitarioEstimado)s,
          %(valorUnitarioResultado)s,
          %(dataResultado)s,
          %(situacaoCompraItemNome)s,
          %(temResultado)s,
          %(dataInclusaoPncp)s,
          %(dataAtualizacaoPncp)s,
          %(raw_json)s::jsonb
        )
        ON CONFLICT (id_compra_item)
        DO UPDATE SET
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
          valor_unitario_estimado = EXCLUDED.valor_unitario_estimado,
          valor_unitario_resultado = EXCLUDED.valor_unitario_resultado,
          data_resultado = EXCLUDED.data_resultado,
          situacao_compra_item_nome = EXCLUDED.situacao_compra_item_nome,
          tem_resultado = EXCLUDED.tem_resultado,
          data_inclusao_pncp = EXCLUDED.data_inclusao_pncp,
          data_atualizacao_pncp = EXCLUDED.data_atualizacao_pncp,
          raw_json = EXCLUDED.raw_json
        """,
        {
            **item,
            "raw_json": json.dumps(item, ensure_ascii=False),
        },
    )


def iter_ids_compra(cur) -> List[str]:
    cur.execute(
        """
        SELECT DISTINCT id_compra
        FROM contratacao_pncp_14133
        ORDER BY id_compra
        """
    )
    return [r[0] for r in cur.fetchall()]


def main() -> None:
    ok = 0
    fail = 0
    total_itens = 0

    fetched_at = utcnow()

    with get_conn() as conn:
        with conn.cursor() as cur:
            ids = iter_ids_compra(cur)

    with get_conn() as conn:
        with conn.cursor() as cur:
            for id_compra in ids:
                try:
                    payload = fetch_itens(id_compra)
                    itens = payload.get("resultado") or []
                    print(f"[ITENS] id_compra={id_compra} items={len(itens)}")

                    # Log raw
                    upsert_api_raw(
                        cur=cur,
                        endpoint=ENDPOINT,
                        params={"tipo": "idCompra", "codigo": id_compra},
                        payload=payload,
                        fetched_at=fetched_at,
                    )

                    for item in itens:
                        upsert_item(cur, item)

                    total_itens += len(itens)
                    ok += 1

                    conn.commit()
                    time.sleep(SLEEP_S)

                except Exception as e:
                    conn.rollback()
                    print(f"[EXC] id_compra={id_compra} err={e}")
                    fail += 1
                    time.sleep(SLEEP_S)

    print(f"[DONE] ok={ok} fail={fail} itens={total_itens}")


if __name__ == "__main__":
    main()
