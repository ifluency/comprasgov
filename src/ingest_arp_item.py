import os
import json
import time
import hashlib
import datetime as dt
import requests

from db import get_conn

RAW_ENDPOINT_NAME = "modulo-arp/2.1_consultarARPItem_Id"

UG = int(os.getenv("COMPRAS_UG", "155125"))
TIMEOUT = int(os.getenv("COMPRAS_TIMEOUT", "60"))

BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").strip().rstrip("/")
ITEM_PATH = "/modulo-arp/modulo-arp/2.1_consultarARPItem_Id"

# Quantas ARPs processar por execução (pra controlar custo)
ARP_ITEM_BATCH_LIMIT = int(os.getenv("ARP_ITEM_BATCH_LIMIT", "500"))
SLEEP_BETWEEN_CALLS_S = float(os.getenv("ARP_ITEM_SLEEP_S", "0.15"))

# dataAtualizacao (se a API aceitar como filtro incremental)
DEFAULT_DATA_ATUALIZACAO = os.getenv("ARP_ITEM_DEFAULT_DATA_ATUALIZACAO", "2024-01-01")


def sha256_json(obj) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }
    )
    return s


def get_with_retry(session: requests.Session, url: str, params: dict):
    max_tries = 5
    backoff = 2.0

    for attempt in range(1, max_tries + 1):
        r = session.get(url, params=params, timeout=TIMEOUT)
        print(f"[HTTP] attempt={attempt} status={r.status_code} url={r.url}", flush=True)

        if r.status_code in (429, 500, 502, 503, 504) and attempt < max_tries:
            print(f"[HTTP] retry in {backoff:.1f}s", flush=True)
            time.sleep(backoff)
            backoff *= 2
            continue

        return r

    return r


def get_state(conn, name: str):
    with conn.cursor() as cur:
        cur.execute("select value from etl_state where name = %s", (name,))
        row = cur.fetchone()
        return row[0] if row else None


def set_state(conn, name: str, value: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into etl_state (name, value, updated_at)
            values (%s, %s::jsonb, now())
            on conflict (name)
            do update set value=excluded.value, updated_at=now()
            """,
            (name, json.dumps(value, ensure_ascii=False)),
        )


def insert_raw(conn, endpoint_name: str, params: dict, payload, payload_sha: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into api_raw (endpoint, params, payload, payload_sha256)
            values (%s, %s::jsonb, %s::jsonb, %s)
            """,
            (
                endpoint_name,
                json.dumps(params, ensure_ascii=False),
                json.dumps(payload, ensure_ascii=False),
                payload_sha,
            ),
        )


def upsert_item(conn, ug: int, numero_ata: str, numero_controle: str, item: dict) -> int:
    item_sha = sha256_json(item)

    # Heurísticas para identificar campos comuns (ajustamos depois com base no payload real)
    item_id = item.get("id") or item.get("itemId") or item.get("idItem") or item.get("idItemAta") or item.get("idItemARP")
    numero_item = item.get("numeroItem") or item.get("numero") or item.get("item") or item.get("nItem")

    descricao = item.get("descricao") or item.get("descricaoItem") or item.get("nome") or item.get("descricaoDetalhada")
    unidade = item.get("unidade") or item.get("unidadeFornecimento") or item.get("unidadeMedida")
    quantidade = item.get("quantidade") or item.get("qtde") or item.get("quantidadeTotal")
    valor_unitario = item.get("valorUnitario") or item.get("precoUnitario") or item.get("valorUnit")
    valor_total = item.get("valorTotal") or item.get("precoTotal") or item.get("valor")

    catmat = item.get("catmat") or item.get("codigoCatmat") or item.get("codigoCATMAT")
    catsrv = item.get("catsrv") or item.get("codigoCatsrv") or item.get("codigoCATSRV")

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into arp_item (
              codigo_unidade_gerenciadora,
              numero_ata_registro_preco,
              numero_controle_pncp_ata,
              item_id,
              numero_item,
              descricao,
              unidade,
              quantidade,
              valor_unitario,
              valor_total,
              catmat,
              catsrv,
              payload_sha256,
              payload,
              first_seen_at,
              last_seen_at
            )
            values (
              %s, %s, %s,
              %s, %s,
              %s, %s, %s, %s, %s,
              %s, %s,
              %s, %s::jsonb,
              now(), now()
            )
            on conflict (codigo_unidade_gerenciadora, numero_controle_pncp_ata, coalesce(item_id,''), coalesce(numero_item,-1))
            do update set
              numero_ata_registro_preco = excluded.numero_ata_registro_preco,
              descricao = excluded.descricao,
              unidade = excluded.unidade,
              quantidade = excluded.quantidade,
              valor_unitario = excluded.valor_unitario,
              valor_total = excluded.valor_total,
              catmat = excluded.catmat,
              catsrv = excluded.catsrv,
              payload_sha256 = excluded.payload_sha256,
              payload = excluded.payload,
              last_seen_at = now()
            """,
            (
                ug,
                numero_ata,
                numero_controle,
                str(item_id) if item_id is not None else None,
                int(numero_item) if isinstance(numero_item, int) or (isinstance(numero_item, str) and numero_item.isdigit()) else None,
                descricao,
                unidade,
                quantidade,
                valor_unitario,
                valor_total,
                catmat,
                catsrv,
                item_sha,
                json.dumps(item, ensure_ascii=False),
            ),
        )

    return 1


def select_arp_targets(conn, limit: int):
    """
    Seleciona ARPs para puxar itens.
    Usa numeroControlePncpAta do payload, pra não depender da coluna existir.
    """
    sql = """
    select
      codigo_unidade_gerenciadora,
      coalesce(numero_ata_registro_preco, payload->>'numeroAtaRegistroPreco') as numero_ata_registro_preco,
      payload->>'numeroControlePncpAta' as numero_controle_pncp_ata
    from arp
    where codigo_unidade_gerenciadora = %s
      and (payload->>'numeroControlePncpAta') is not null
      and (payload->>'numeroControlePncpAta') <> ''
    order by last_seen_at desc
    limit %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (UG, limit))
        return cur.fetchall()


def extract_items_from_response(data):
    """
    Normaliza possíveis formatos:
    - { resultado: [...] }
    - [...]
    - { itens: [...] }
    """
    if isinstance(data, dict):
        if isinstance(data.get("resultado"), list):
            return data["resultado"]
        if isinstance(data.get("itens"), list):
            return data["itens"]
        # fallback: se vier dict com chave única list
        for v in data.values():
            if isinstance(v, list):
                return v
    if isinstance(data, list):
        return data
    return []


def main():
    print("[ARP_ITEM] START", flush=True)
    print(f"[ARP_ITEM] BASE_URL={BASE_URL}", flush=True)

    session = make_session()
    url = BASE_URL + ITEM_PATH

    conn = get_conn()
    conn.autocommit = False

    total_calls = 0
    total_items_upserted = 0
    total_raw = 0

    try:
        state = get_state(conn, "arp_item_last_run") or {}
        last_data_atualizacao = state.get("dataAtualizacao") or DEFAULT_DATA_ATUALIZACAO

        print(f"[ARP_ITEM] dataAtualizacao (filtro) = {last_data_atualizacao}", flush=True)

        targets = select_arp_targets(conn, ARP_ITEM_BATCH_LIMIT)
        print(f"[ARP_ITEM] targets={len(targets)} (limit={ARP_ITEM_BATCH_LIMIT})", flush=True)

        for (ug, numero_ata, numero_controle) in targets:
            params = {
                "numeroControlePncpAta": numero_controle,
                # se a API aceitar, filtra incremental:
                "dataAtualizacao": last_data_atualizacao,
            }

            r = get_with_retry(session, url, params)
            r.raise_for_status()
            data = r.json()

            insert_raw(conn, RAW_ENDPOINT_NAME, params, data, sha256_json(data))
            total_raw += 1

            items = extract_items_from_response(data)
            print(f"[ARP_ITEM] controle={numero_controle} items={len(items)}", flush=True)

            for item in items:
                total_items_upserted += upsert_item(conn, ug, numero_ata, numero_controle, item)

            conn.commit()
            total_calls += 1
            time.sleep(SLEEP_BETWEEN_CALLS_S)

        # Atualiza estado: próximo incremental usa "agora" como dataAtualizacao
        now_str = dt.date.today().isoformat()
        set_state(
            conn,
            "arp_item_last_run",
            {
                "ended_at": dt.datetime.utcnow().isoformat(),
                "dataAtualizacao": now_str,
                "calls": total_calls,
                "raw_pages": total_raw,
                "upserts": total_items_upserted,
                "batch_limit": ARP_ITEM_BATCH_LIMIT,
            },
        )
        conn.commit()

        print(f"[ARP_ITEM] DONE calls={total_calls} raw={total_raw} upserts={total_items_upserted}", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
