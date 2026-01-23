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
RAW_ENDPOINT_NAME = "modulo-arp/1_consultarARP"

UG = int(os.getenv("COMPRAS_UG", "155125"))
PAGE_SIZE = int(os.getenv("COMPRAS_PAGE_SIZE", "500"))
TIMEOUT = int(os.getenv("COMPRAS_TIMEOUT", "60"))

# limite de janela (API costuma limitar diferença de datas)
MAX_WINDOW_DAYS = int(os.getenv("COMPRAS_MAX_WINDOW_DAYS", "365"))

BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").strip().rstrip("/")
ARP_PATH = "/modulo-arp/1_consultarARP"


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


def date_chunks(start: dt.date, end: dt.date, max_days: int):
    """Gera janelas [ini, fim] com tamanho máximo max_days (inclusive)."""
    if end < start:
        return
    cur = start
    step = dt.timedelta(days=max_days - 1)
    one_day = dt.timedelta(days=1)

    while cur <= end:
        chunk_end = min(end, cur + step)
        yield cur, chunk_end
        cur = chunk_end + one_day


def make_session() -> requests.Session:
    s = requests.Session()
    # Headers "browser-like" ajudam a evitar respostas estranhas em alguns gateways
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
    """
    Mantém um subset mínimo de colunas + payload completo.
    Se o seu schema de arp tem mais colunas, não tem problema: estas precisam existir.
    """
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
# HTTP (retries)
# =========================
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


# =========================
# Main
# =========================
def main():
    print("[ARP] START", flush=True)
    print(f"[ARP] BASE_URL={BASE_URL}", flush=True)
    print(f"[ARP] UG={UG} PAGE_SIZE={PAGE_SIZE} MAX_WINDOW_DAYS={MAX_WINDOW_DAYS}", flush=True)

    start_fixed = dt.date(2024, 1, 1)
    today = dt.date.today()

    # full no domingo ou se FORCE_FULL=true
    weekday = today.weekday()  # 6 = domingo
    force_full = os.getenv("FORCE_FULL", "false").lower() == "true"
    mode = "full" if (force_full or weekday == 6) else "daily"

    # 🔥 Otimização: daily busca só últimos 730 dias de "vigência inicial"
    if mode == "daily":
        vig_start = max(start_fixed, today - dt.timedelta(days=730))
    else:
        vig_start = start_fixed

    url = BASE_URL + ARP_PATH
    session = make_session()

    conn = get_conn()
    conn.autocommit = False

    total_upserts = 0
    total_raw_pages = 0
    total_windows = 0

    try:
        for vig_ini, vig_fim in date_chunks(vig_start, today, MAX_WINDOW_DAYS):
            total_windows += 1
            print(f"[ARP] window_vigInicial={vig_ini}->{vig_fim}", flush=True)

            base_params = {
                "codigoUnidadeGerenciadora": UG,
                "dataVigenciaInicialMin": vig_ini.isoformat(),
                "dataVigenciaInicialMax": vig_fim.isoformat(),
            }

            pagina = 1
            while True:
                params = {**base_params, "pagina": pagina, "tamanhoPagina": PAGE_SIZE}
                r = get_with_retry(session, url, params)

                if r.status_code == 404:
                    raise RuntimeError(
                        "Endpoint retornou 404. Verifique se o path está correto no Swagger "
                        f"ou se houve bloqueio momentâneo. url={r.url}"
                    )

                r.raise_for_status()
                data = r.json()

                insert_raw(conn, RAW_ENDPOINT_NAME, params, data, sha256_json(data))
                total_raw_pages += 1

                items = data.get("resultado") if isinstance(data, dict) else None
                if not items:
                    conn.commit()
                    print(f"[ARP] window={vig_ini}->{vig_fim} page={pagina} itens=0 -> next window", flush=True)
                    break

                for item in items:
                    total_upserts += upsert_arp(conn, item, sha256_json(item))

                conn.commit()
                print(
                    f"[ARP] window={vig_ini}->{vig_fim} page={pagina} items={len(items)} upserts_total={total_upserts}",
                    flush=True
                )

                pagina += 1
                time.sleep(0.25)

        set_state(
            conn,
            "arp_last_run",
            {
                "ended_at": dt.datetime.utcnow().isoformat(),
                "mode": mode,
                "vig_start": vig_start.isoformat(),
                "vig_end": today.isoformat(),
                "windows": total_windows,
                "raw_pages": total_raw_pages,
                "upserts": total_upserts,
            },
        )
        conn.commit()

        print(f"[ARP] DONE mode={mode} windows={total_windows} raw_pages={total_raw_pages} upserts={total_upserts}", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
