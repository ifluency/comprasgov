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
DAILY_LOOKBACK_DAYS = int(os.getenv("DAILY_LOOKBACK_DAYS", "45"))

# A API limita diferença entre data inicial e final em 365 dias.
MAX_WINDOW_DAYS = int(os.getenv("COMPRAS_MAX_WINDOW_DAYS", "365"))

PRIMARY_BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").strip().rstrip("/")

BASE_URL_CANDIDATES = [PRIMARY_BASE_URL]
if "dadosabertos.compras.gov.br" in PRIMARY_BASE_URL:
    BASE_URL_CANDIDATES.extend([
        PRIMARY_BASE_URL + "/api",
        PRIMARY_BASE_URL + "/dadosabertos",
    ])

ARP_PATH_CANDIDATES = [
    "/modulo-arp/1_consultarARP",
    "/modulo-arp/1_consultarARP/",
    "/modulo_arp/1_consultarARP",
]

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

def normalize_base(base: str) -> str:
    return base.strip().rstrip("/")

def build_url(base: str, path: str) -> str:
    return normalize_base(base) + path

def date_chunks(start: dt.date, end: dt.date, max_days: int):
    """
    Gera janelas [ini, fim] com tamanho máximo = max_days (inclusive).
    Ex.: max_days=365 -> fim = ini + 364 dias.
    """
    if end < start:
        return
    cur = start
    step = dt.timedelta(days=max_days - 1)
    one_day = dt.timedelta(days=1)

    while cur <= end:
        chunk_end = min(end, cur + step)
        yield cur, chunk_end
        cur = chunk_end + one_day

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
# HTTP (browser-like UA + retries)
# =========================
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "accept": "*/*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    })
    return s

def get_with_retry(session: requests.Session, url: str, params: dict):
    max_tries = 5
    backoff = 2.0

    for attempt in range(1, max_tries + 1):
        r = session.get(url, params=params, timeout=TIMEOUT)
        print(f"[HTTP] attempt={attempt} status={r.status_code} url={r.url}", flush=True)

        if r.status_code in (429, 500, 502, 503, 504):
            if attempt < max_tries:
                print(f"[HTTP] retry in {backoff:.1f}s (status={r.status_code})", flush=True)
                time.sleep(backoff)
                backoff *= 2
                continue

        return r

    return r

def fetch_page(session: requests.Session, pagina: int, params: dict):
    """
    Tenta combinações base+path. Retorna (json, used_base, used_path, status_code).
    """
    last_err = None
    for base in BASE_URL_CANDIDATES:
        for path in ARP_PATH_CANDIDATES:
            url = build_url(base, path)
            r = get_with_retry(session, url, {**params, "pagina": pagina, "tamanhoPagina": PAGE_SIZE})

            if r.status_code == 404:
                continue

            try:
                r.raise_for_status()
                return r.json(), base, path, r.status_code
            except Exception as e:
                last_err = e
                # 400 aqui geralmente é "intervalo de datas maior que 365"
                # mas também pode ser parâmetro inválido. Vamos logar e subir.
                print(f"[HTTP] error base={base} path={path}: {repr(e)}", flush=True)
                raise

    raise RuntimeError(
        f"Nenhum endpoint ARP válido respondeu (404 em todos). "
        f"BASE_URL_CANDIDATES={BASE_URL_CANDIDATES} PATHS={ARP_PATH_CANDIDATES} last_err={repr(last_err)}"
    )

# =========================
# Main
# =========================
def main():
    print("[ARP] START", flush=True)
    print(f"[ARP] BASE_URL_CANDIDATES={BASE_URL_CANDIDATES}", flush=True)
    print(f"[ARP] PATH_CANDIDATES={ARP_PATH_CANDIDATES}", flush=True)
    print(f"[ARP] MAX_WINDOW_DAYS={MAX_WINDOW_DAYS}", flush=True)

    start_fixed = dt.date(2024, 1, 1)
    today = dt.date.today()

    force_full = os.getenv("FORCE_FULL", "false").lower() == "true"
    weekday = today.weekday()  # 0=seg ... 6=dom
    weekly_full = (weekday == 6)  # domingo

    mode = "full" if (force_full or weekly_full) else "daily"

    # daily: janela móvel por assinatura, mas sempre respeitando o limite da VIGÊNCIA (365) via chunks
    if mode == "full":
        assinatura_ini = start_fixed
    else:
        assinatura_ini = max(start_fixed, today - dt.timedelta(days=DAILY_LOOKBACK_DAYS))

    print(f"[ARP] mode={mode} assinatura_ini={assinatura_ini} today={today}", flush=True)

    session = make_session()
    conn = get_conn()
    conn.autocommit = False

    total_upserts = 0
    total_raw_pages = 0
    total_windows = 0

    try:
        # Quebra a VIGÊNCIA em janelas <= 365 dias:
        for vig_ini, vig_fim in date_chunks(start_fixed, today, MAX_WINDOW_DAYS):
            total_windows += 1
            print(f"[ARP] window_vig={vig_ini}->{vig_fim}", flush=True)

            # Parâmetros obrigatórios
            base_params = {
                "codigoUnidadeGerenciadora": UG,
                "dataVigenciaInicial": vig_ini.isoformat(),
                "dataVigenciaFinal": vig_fim.isoformat(),
                "dataAssinaturaInicial": assinatura_ini.isoformat(),
                "dataAssinaturaFinal": today.isoformat(),
            }

            # Smoke test por janela: tenta pagina 1
            pagina = 1
            while True:
                data, used_base, used_path, status = fetch_page(session, pagina, base_params)

                # RAW sempre
                raw_params = {**base_params, "pagina": pagina, "tamanhoPagina": PAGE_SIZE, "_base": used_base, "_path": used_path}
                insert_raw(conn, RAW_ENDPOINT_NAME, raw_params, data, sha256_json(data))
                total_raw_pages += 1

                items = data.get("resultado") if isinstance(data, dict) else None
                if not items:
                    conn.commit()
                    print(f"[ARP] window_vig={vig_ini}->{vig_fim} page={pagina} itens=0 -> next window", flush=True)
                    break

                for item in items:
                    total_upserts += upsert_arp(conn, item, sha256_json(item))

                conn.commit()
                print(
                    f"[ARP] window_vig={vig_ini}->{vig_fim} page={pagina} items={len(items)} upserts_total={total_upserts}",
                    flush=True
                )

                pagina += 1
                time.sleep(0.25)

        set_state(conn, "arp_last_run", {
            "ended_at": dt.datetime.utcnow().isoformat(),
            "mode": mode,
            "windows": total_windows,
            "raw_pages": total_raw_pages,
            "upserts": total_upserts,
        })
        conn.commit()

        print(f"[ARP] DONE mode={mode} windows={total_windows} raw_pages={total_raw_pages} upserts={total_upserts}", flush=True)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
