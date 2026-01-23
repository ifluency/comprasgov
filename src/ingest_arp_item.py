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
ITEM_PATH = "/modulo-arp/2.1_consultarARPItem_Id"

ARP_ITEM_BATCH_LIMIT = int(os.getenv("ARP_ITEM_BATCH_LIMIT", "300"))
SLEEP_BETWEEN_CALLS_S = float(os.getenv("ARP_ITEM_SLEEP_S", "0.15"))


def sha256_json(obj) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def parse_dt(value):
    if value is None or value == "":
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except Exception:
        return None


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


def select_arp_targets(conn, limit: int):
    """
    Puxa ARPs mais recentes que têm numeroControlePncpAta no payload.
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


def extract_items(data):
    if isinstance(data, dict) and isinstance(data.get("resultado"), list):
        return data["resultado"]
    if isinstance(data, list):
        return data
    return []


def upsert_item(conn, ug: int, numero_controle_ata: str, item: dict) -> int:
    """
    Upsert conforme payload real do Swagger.

    OBS: Por enquanto, mantemos colunas normalizadas mínimas e guardamos o JSON inteiro no payload.
    """
    item_sha = sha256_json(item)

    numero_ata = item.get("numeroAtaRegistroPreco")

    # Chaves relevantes no payload real
    codigo_item = item.get("codigoItem")  # usamos como item_id
    descricao_item = item.get("descricaoItem")

    qtd_item = item.get("quantidadeHomologadaItem")
    qtd_vencedor = item.get("quantidadeHomologadaVencedor")
    valor_unit = item.get("valorUnitario")
    valor_total = item.get("valorTotal")

    # Escolhe a quantidade mais útil (vencedor se existir)
    quantidade = qtd_vencedor if qtd_vencedor is not None else qtd_item

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
              quantidade = excluded.quantidade,
              valor_unitario = excluded.valor_unitario,
              valor_total = excluded.valor_total,
              payload_sha256 = excluded.payload_sha256,
              payload = excluded.payload,
              last_seen_at = now()
            """,
            (
                ug,
                numero_ata,
                numero_controle_ata,
                str(codigo_item) if codigo_item is not None else None,  # item_id <- codigoItem
                None,  # numero_item (int) ainda não usamos; numeroItem string fica no payload
                descricao_item,
                None,  # unidade não veio no exemplo
                quantidade,
                valor_unit,
                valor_total,
                None,
                None,
                item_sha,
                json.dumps(item, ensure_ascii=False),
            ),
        )

    return 1


def main():
    print("[ARP_ITEM] START", flush=True)
    print(f"[ARP_ITEM] BASE_URL={BASE_URL}", flush=True)
    print(f"[ARP_ITEM] ITEM_PATH={ITEM_PATH}", flush=True)

    session = make_session()
    url = BASE_URL + ITEM_PATH

    conn = get_conn()
    conn.autocommit = False

    total_calls = 0
    total_items = 0
    total_raw = 0

    try:
        targets = select_arp_targets(conn, ARP_ITEM_BATCH_LIMIT)
        print(f"[ARP_ITEM] targets={len(targets)} (limit={ARP_ITEM_BATCH_LIMIT})", flush=True)

        for (ug, numero_ata, numero_controle_ata) in targets:
            params = {"numeroControlePncpAta": numero_controle_ata}

            r = get_with_retry(session, url, params)
            if r.status_code == 404:
                raise RuntimeError(
                    "404 no endpoint de itens. Confirme se o Request URL do Swagger bate com o ITEM_PATH. "
                    f"url={r.url}"
                )

            r.raise_for_status()
            data = r.json()

            insert_raw(conn, RAW_ENDPOINT_NAME, params, data, sha256_json(data))
            total_raw += 1

            items = extract_items(data)
            print(f"[ARP_ITEM] controle={numero_controle_ata} items={len(items)}", flush=True)

            for item in items:
                # ✅ chamada corrigida (4 args)
                total_items += upsert_item(conn, ug, numero_controle_ata, item)

            conn.commit()
            total_calls += 1
            time.sleep(SLEEP_BETWEEN_CALLS_S)

        # Salva estado simples
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into etl_state (name, value, updated_at)
                values (%s, %s::jsonb, now())
                on conflict (name)
                do update set value=excluded.value, updated_at=now()
                """,
                (
                    "arp_item_last_run",
                    json.dumps(
                        {
                            "ended_at": dt.datetime.utcnow().isoformat(),
                            "calls": total_calls,
                            "raw_pages": total_raw,
                            "upserts": total_items,
                            "batch_limit": ARP_ITEM_BATCH_LIMIT,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
        conn.commit()

        print(f"[ARP_ITEM] DONE calls={total_calls} raw={total_raw} upserts={total_items}", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
