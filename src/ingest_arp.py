import os
import json
import time
import hashlib
import datetime as dt
import requests

from db import get_conn

# =========================
# Config
# =========================
BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").strip().rstrip("/")
UG = int(os.getenv("COMPRAS_UG", "155125"))
PAGE_SIZE = int(os.getenv("COMPRAS_PAGE_SIZE", "500"))
TIMEOUT = int(os.getenv("COMPRAS_TIMEOUT", "60"))
DAILY_LOOKBACK_DAYS = int(os.getenv("DAILY_LOOKBACK_DAYS", "45"))

ARP_PATH_CANDIDATES = [
    "/modulo-arp/1_consultarARP",
    "/modulo-arp/1_consultarARP/",
    "/modulo_arp/1_consultarARP",
]

RAW_ENDPOINT_NAME = "modulo-arp/1_consultarARP"

# =========================
# Helpers
# =========================
def sha256_json(obj) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def parse_date(value):
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value[:10])
    except Exception:
        return None

def parse_dt(value):
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None

def build_url(path: str) -> str:
    return BASE_URL + path

# =========================
# DB ops
# =========================
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

def upsert_arp(conn, item: dict, item_sha: str) -> int:
    numero_ata = item.get("numeroAtaRegistroPreco")
    if not numero_ata:
        return 0

    rec = {
        "codigo_unidade_gerenciadora": item.get("codigoUnidadeGerenciadora") or UG,
        "numero_ata_registro_preco": numero_ata,
        "status_ata": item.get("statusAta"),
        "data_assinatura": parse_date(item.get("dataAssinatura")),
        "data_vigencia_inicio": parse_date(item.get("dataVigenciaInicial")),
        "data_vigencia_fim": parse_date(item.get("dataVigenciaFinal")),
        "valor_total": item.get("valorTotal"),
        "payload_sha256": item_sha,
        "payload": json.dumps(item, ensure_ascii=False),
    }

    sql = """
    insert into arp (
      codigo_unidade_gerenciadora,
      numero_ata_registro_preco,
      status_ata,
      data_assinatura,
      data_vigencia_inicio,
      data_vigencia_fim,
      valor_total,
      payload_sha256,
      payload,
      first_seen_at,
      last_seen_at
    )
    values (
      %(codigo_unidade_gerenciadora)s,
      %(numero_ata_registro_preco)s,
      %(status_ata)s,
      %(data_assinatura)s,
      %(data_vigencia_inicio)s,
      %(data_vigencia_fim)s,
      %(valor_total)s,
      %(payload_sha256)s,
      %(payload)s::jsonb,
      now(),
      now()
    )
    on conflict (codigo_unidade_gerenciadora, numero_ata_registro_preco)
    do update set
      status_ata = excluded.status_ata,
      data_assinatura = excluded.data_assinatura,
      data_vigencia_inicio = excluded.data_vigencia_inicio,
      data_vigencia_fim = excluded.data_vigencia_fim,
      valor_total = excluded.valor_total,
      payload_sha256 = excluded.payload_sha256,
      payload = excluded.payload,
      last_seen_at = now();
    """

    with conn.cursor() as cur:
        cur.execute(sql, rec)

    return 1

# =========================
# HTTP
# =========================
def fetch_page(session, pagina: int, params: dict):
    headers = {"accept": "*/*"}

    for path in ARP_PATH_CANDIDATES:
        url = build_url(path)
        r = session.get(
            url,
            params={**params, "pagina": pagina, "tamanhoPagina": PAGE_SIZE},
            headers=headers,
            timeout=TIMEOUT,
        )

        print(f"[HTTP] {r.status_code} {r.url}", flush=True)

        if r.status_code == 404:
            continue

        r.raise_for_status()
        return r.json(), path

    raise RuntimeError("Nenhum endpoint ARP válido respondeu (404 em todos).")

# =========================
# Main
# =========================
def main():
    print("[ARP] START", flush=True)

    start_fixed = dt.date(2024, 1, 1)
    today = dt.date.today()

    assinatura_ini = max(start_fixed, today - dt.timedelta(days=DAILY_LOOKBACK_DAYS))

    params = {
        "codigoUnidadeGerenciadora": UG,
        "dataVigenciaInicial": start_fixed.isoformat(),
        "dataVigenciaFinal": today.isoformat(),
        "dataAssinaturaInicial": assinatura_ini.isoformat(),
        "dataAssinaturaFinal": today.isoformat(),
    }

    print(f"[ARP] params={params}", flush=True)

    session = requests.Session()
    conn = get_conn()
    conn.autocommit = False

    total = 0
    pagina = 1

    try:
        # Smoke test
        data_test, path = fetch_page(session, 1, params)
        insert_raw(
            conn,
            RAW_ENDPOINT_NAME,
            {**params, "pagina": 1, "_smoke": True},
            data_test,
            sha256_json(data_test),
        )
        conn.commit()
        print("[SMOKE] RAW OK", flush=True)

        while True:
            data, path = fetch_page(session, pagina, params)

            insert_raw(
                conn,
                RAW_ENDPOINT_NAME,
                {**params, "pagina": pagina},
                data,
                sha256_json(data),
            )

            items = data.get("resultado") if isinstance(data, dict) else None
            if not items:
                conn.commit()
                print(f"[ARP] page={pagina} itens=0 -> fim", flush=True)
                break

            for item in items:
                total += upsert_arp(conn, item, sha256_json(item))

            conn.commit()
            print(f"[ARP] page={pagina} upserts={total}", flush=True)
            pagina += 1
            time.sleep(0.2)

        set_state(conn, "arp_last_run", {"ended_at": dt.datetime.utcnow().isoformat()})
        conn.commit()
        print(f"[ARP] DONE total_upserts={total}", flush=True)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
