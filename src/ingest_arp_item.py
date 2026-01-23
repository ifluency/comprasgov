import os
import json
import time
import hashlib
import requests

from db import get_conn

RAW_ENDPOINT_NAME = "modulo-arp/2.1_consultarARPItem_Id"

UG = int(os.getenv("COMPRAS_UG", "155125"))
TIMEOUT = int(os.getenv("COMPRAS_TIMEOUT", "60"))

BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").strip().rstrip("/")

# ✅ O seu 404 indica que o PATH com /modulo-arp/modulo-arp/ pode estar errado.
# Então tentamos variações até achar a que o Swagger realmente usa.
ITEM_PATH_CANDIDATES = [
    "/modulo-arp/2.1_consultarARPItem_Id",
    "/modulo-arp/2.1_consultarARPItem_Id/",
    "/modulo-arp/modulo-arp/2.1_consultarARPItem_Id",
    "/modulo-arp/modulo-arp/2.1_consultarARPItem_Id/",
    "/modulo_arp/2.1_consultarARPItem_Id",
    "/modulo_arp/2.1_consultarARPItem_Id/",
]

ARP_ITEM_BATCH_LIMIT = int(os.getenv("ARP_ITEM_BATCH_LIMIT", "300"))
SLEEP_BETWEEN_CALLS_S = float(os.getenv("ARP_ITEM_SLEEP_S", "0.15"))


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

    # Identificadores (vamos refinando depois com base no payload real)
    item_id = item.get("id") or item.get("idItem") or item.get("idItemAta") or item.get("itemId")
    numero_item = item.get("numeroItem") or item.get("numero") or item.get("item")

    descricao = item.get("descricao") or item.get("descricaoItem") or item.get("nome")
    unidade = item.get("unidade") or item.get("unidadeFornecimento") or item.get("unidadeMedida")
    quantidade = item.get("quantidade") or item.get("qtde")
    valor_unitario = item.get("valorUnitario") or item.get("precoUnitario")
    valor_total = item.get("valorTotal") or item.get("precoTotal")

    catmat = item.get("catmat") or item.get("codigoCatmat")
    catsrv = item.get("catsrv") or item.get("codigoCatsrv")

    # normaliza numero_item se vier string numérica
    if isinstance(numero_item, str) and numero_item.isdigit():
        numero_item = int(numero_item)
    elif not isinstance(numero_item, int):
        numero_item = None

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
                numero_item,
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
    Seleciona ARPs com numeroControlePncpAta disponível no payload (conforme schema que você enviou).
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
    Normaliza formatos comuns:
    - { resultado: [...] }
    - [...]
    - { itens: [...] }
    """
    if isinstance(data, dict):
        if isinstance(data.get("resultado"), list):
            return data["resultado"]
        if isinstance(data.get("itens"), list):
            return data["itens"]
        # fallback: primeira lista encontrada
        for v in data.values():
            if isinstance(v, list):
                return v
    if isinstance(data, list):
        return data
    return []


def fetch_items(session: requests.Session, numero_controle_pncp_ata: str):
    """
    Tenta todos os ITEM_PATH_CANDIDATES até achar um que responda != 404.
    """
    params = {"numeroControlePncpAta": numero_controle_pncp_ata}

    last_non_404 = None
    for path in ITEM_PATH_CANDIDATES:
        url = BASE_URL + path
        r = get_with_retry(session, url, params)

        if r.status_code == 404:
            continue

        last_non_404 = r
        r.raise_for_status()
        return r.json(), path

    raise RuntimeError(
        f"[ARP_ITEM] Nenhum ITEM_PATH funcionou (404 em todos). "
        f"base={BASE_URL} tried={ITEM_PATH_CANDIDATES} last_non_404={last_non_404}"
    )


def main():
    print("[ARP_ITEM] START", flush=True)
    print(f"[ARP_ITEM] BASE_URL={BASE_URL}", flush=True)
    print(f"[ARP_ITEM] PATH_CANDIDATES={ITEM_PATH_CANDIDATES}", flush=True)

    session = make_session()
    conn = get_conn()
    conn.autocommit = False

    total_calls = 0
    total_items_upserted = 0
    total_raw = 0

    try:
        targets = select_arp_targets(conn, ARP_ITEM_BATCH_LIMIT)
        print(f"[ARP_ITEM] targets={len(targets)} (limit={ARP_ITEM_BATCH_LIMIT})", flush=True)

        for (ug, numero_ata, numero_controle) in targets:
            data, used_path = fetch_items(session, numero_controle)

            # RAW
            raw_params = {"numeroControlePncpAta": numero_controle, "_path": used_path}
            insert_raw(conn, RAW_ENDPOINT_NAME, raw_params, data, sha256_json(data))
            total_raw += 1

            items = extract_items_from_response(data)
            print(f"[ARP_ITEM] controle={numero_controle} used_path={used_path} items={len(items)}", flush=True)

            for item in items:
                total_items_upserted += upsert_item(conn, ug, numero_ata, numero_controle, item)

            conn.commit()
            total_calls += 1
            time.sleep(SLEEP_BETWEEN_CALLS_S)

        # Estado simples
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
                            "upserts": total_items_upserted,
                            "batch_limit": ARP_ITEM_BATCH_LIMIT,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
        conn.commit()

        print(f"[ARP_ITEM] DONE calls={total_calls} raw={total_raw} upserts={total_items_upserted}", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    import datetime as dt
    main()
