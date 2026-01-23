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

# Alguns gateways mudam path/barra final, então tentamos variações
ARP_PATH_CANDIDATES = [
    "/modulo-arp/1_consultarARP",
    "/modulo-arp/1_consultarARP/",
    "/modulo_arp/1_consultarARP",
]

# Mantém só como identificador do endpoint no RAW (não precisa ser URL completa)
RAW_ENDPOINT_NAME = "modulo-arp/1_consultarARP"


# =========================
# Helpers
# =========================
def sha256_json(obj) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
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
    # Campos conforme retorno esperado (mantemos payload completo também)
    rec = {
        "codigo_unidade_gerenciadora": item.get("codigoUnidadeGerenciadora") or UG,
        "numero_ata_registro_preco": item.get("numeroAtaRegistroPreco"),

        "codigo_orgao": item.get("codigoOrgao"),
        "nome_orgao": item.get("nomeOrgao"),
        "nome_unidade_gerenciadora": item.get("nomeUnidadeGerenciadora"),

        "codigo_modalidade_compra": item.get("codigoModalidadeCompra"),
        "nome_modalidade_compra": item.get("nomeModalidadeCompra"),

        "numero_compra": item.get("numeroCompra"),
        "ano_compra": item.get("anoCompra"),

        "status_ata": item.get("statusAta"),
        "objeto": item.get("objeto"),

        "data_assinatura": parse_date(item.get("dataAssinatura")),
        "data_vigencia_inicio": parse_date(item.get("dataVigenciaInicial")),
        "data_vigencia_fim": parse_date(item.get("dataVigenciaFinal")),

        "valor_total": item.get("valorTotal"),
        "quantidade_itens": item.get("quantidadeItens"),

        "link_ata_pncp": item.get("linkAtaPNCP"),
        "link_compra_pncp": item.get("linkCompraPNCP"),
        "numero_controle_pncp_ata": item.get("numeroControlePncpAta"),
        "numero_controle_pncp_compra": item.get("numeroControlePncpCompra"),
        "id_compra": item.get("idCompra"),

        "data_hora_atualizacao": parse_dt(item.get("dataHoraAtualizacao")),
        "data_hora_inclusao": parse_dt(item.get("dataHoraInclusao")),
        "data_hora_exclusao": parse_dt(item.get("dataHoraExclusao")),
        "ata_excluido": item.get("ataExcluido"),

        "payload_sha256": item_sha,
        "payload": json.dumps(item, ensure_ascii=False),
    }

    if not rec["numero_ata_registro_preco"]:
        return 0

    sql = """
    insert into arp (
      codigo_unidade_gerenciadora, numero_ata_registro_preco,
      codigo_orgao, nome_orgao, nome_unidade_gerenciadora,
      codigo_modalidade_compra, nome_modalidade_compra,
      numero_compra, ano_compra,
      status_ata, objeto,
      data_assinatura, data_vigencia_inicio, data_vigencia_fim,
      valor_total, quantidade_itens,
      link_ata_pncp, link_compra_pncp,
      numero_controle_pncp_ata, numero_controle_pncp_compra,
      id_compra,
      data_hora_atualizacao, data_hora_inclusao, data_hora_exclusao, ata_excluido,
      payload_sha256, payload,
      first_seen_at, last_seen_at
    )
    values (
      %(codigo_unidade_gerenciadora)s, %(numero_ata_registro_preco)s,
      %(codigo_orgao)s, %(nome_orgao)s, %(nome_unidade_gerenciadora)s,
      %(codigo_modalidade_compra)s, %(nome_modalidade_compra)s,
      %(numero_compra)s, %(ano_compra)s,
      %(status_ata)s, %(objeto)s,
      %(data_assinatura)s, %(data_vigencia_inicio)s, %(data_vigencia_fim)s,
      %(valor_total)s, %(quantidade_itens)s,
      %(link_ata_pncp)s, %(link_compra_pncp)s,
      %(numero_controle_pncp_ata)s, %(numero_controle_pncp_compra)s,
      %(id_compra)s,
      %(data_hora_atualizacao)s, %(data_hora_inclusao)s, %(data_hora_exclusao)s, %(ata_excluido)s,
      %(payload_sha256)s, %(payload)s::jsonb,
      now(), now()
    )
    on conflict (codigo_unidade_gerenciadora, numero_ata_registro_preco)
    do update set
      codigo_orgao = excluded.codigo_orgao,
      nome_orgao = excluded.nome_orgao,
      nome_unidade_gerenciadora = excluded.nome_unidade_gerenciadora,
      codigo_modalidade_compra = excluded.codigo_modalidade_compra,
      nome_modalidade_compra = excluded.nome_modalidade_compra,
      numero_compra = excluded.numero_compra,
      ano_compra = excluded.ano_compra,
      status_ata = excluded.status_ata,
      objeto = excluded.objeto,
      data_assinatura = excluded.data_assinatura,
      data_vigencia_inicio = excluded.data_vigencia_inicio,
      data_vigencia_fim = excluded.data_vigencia_fim,
      valor_total = excluded.valor_total,
      quantidade_itens = excluded.quantidade_itens,
      link_ata_pncp = excluded.link_ata_pncp,
      link_compra_pncp = excluded.link_compra_pncp,
      numero_controle_pncp_ata = excluded.numero_controle_pncp_ata,
      numero_controle_pncp_compra = excluded.numero_controle_pncp_compra,
      id_compra = excluded.id_compra,
      data_hora_atualizacao = excluded.data_hora_atualizacao,
      data_hora_inclusao = excluded.data_hora_inclusao,
      data_hora_exclusao = excluded.data_hora_exclusao,
      ata_excluido = excluded.ata_excluido,
      payload_sha256 = excluded.payload_sha256,
      payload = excluded.payload,
      last_seen_at = now();
    """

    with conn.cursor() as cur:
        cur.execute(sql, rec)

    return 1


# =========================
# HTTP fetch with retries + fallback
# =========================
def http_get_with_retry(session: requests.Session, url: str, params: dict, headers: dict, timeout: int):
    max_tries = 5
    backoff = 2.0

    for attempt in range(1, max_tries + 1):
        r = session.get(url, params=params, headers=headers, timeout=timeout)
        print(f"[HTTP] attempt={attempt} status={r.status_code} url={r.url}", flush=True)

        # retry conditions
        if r.status_code in (429, 502, 503, 504):
            if attempt < max_tries:
                sleep_s = backoff
                print(f"[HTTP] retrying in {sleep_s:.1f}s (status={r.status_code})", flush=True)
                time.sleep(sleep_s)
                backoff *= 2
                continue

        return r

    return r


def fetch_page(session: requests.Session, pagina: int, params: dict):
    headers = {"accept": "*/*"}  # mais compatível com exemplos/servidores

    # testamos variações de path; se 404, tenta a próxima
    last_error = None
    for path in ARP_PATH_CANDIDATES:
        url = build_url(path)
        try:
            r = http_get_with_retry(
                session,
                url,
                params={**params, "pagina": pagina, "tamanhoPagina": PAGE_SIZE},
                headers=headers,
                timeout=TIMEOUT,
            )

            if r.status_code == 404:
                print(f"[HTTP] 404 for path={path} -> trying next", flush=True)
                continue

            r.raise_for_status()
            return r.json(), path

        except Exception as e:
            last_error = e
            print(f"[HTTP] error for path={path}: {repr(e)}", flush=True)
            continue

    raise RuntimeError(
        f"ARP endpoint not found/failed for all paths={ARP_PATH_CANDIDATES}. last_error={repr(last_error)}"
    )


# =========================
# Main
# =========================
def main():
    print("[ARP] START main()", flush=True)
    print(f"[ARP] BASE_URL={BASE_URL}", flush=True)
    print(f"[ARP] UG={UG} PAGE_SIZE={PAGE_SIZE} TIMEOUT={TIMEOUT}", flush=True)
    print(f"[ARP] PATH_CANDIDATES={ARP_PATH_CANDIDATES}", flush=True)

    start_fixed = dt.date(2024, 1, 1)
    today = dt.date.today()

    force_full = os.getenv("FORCE_FULL", "false").lower() == "true"
    weekday = today.weekday()  # 0=seg ... 6=dom
    weekly_full = (weekday == 6)  # domingo

    mode = "full" if (force_full or weekly_full) else "daily"

    if mode == "full":
        assinatura_ini = start_fixed
    else:
        assinatura_ini = max(start_fixed, today - dt.timedelta(days=DAILY_LOOKBACK_DAYS))

    params = {
        "codigoUnidadeGerenciadora": UG,
        "dataVigenciaInicial": start_fixed.isoformat(),
        "dataVigenciaFinal": today.isoformat(),
        "dataAssinaturaInicial": assinatura_ini.isoformat(),
        "dataAssinaturaFinal": today.isoformat(),
    }

    print(
        f"[ARP] mode={mode} vig={start_fixed}->{today} ass={assinatura_ini}->{today}",
        flush=True,
    )
    print(f"[ARP] params={params}", flush=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "comprasgov-arp-ingestor/1.0"})

    conn = get_conn()
    conn.autocommit = False

    started = dt.datetime.utcnow()
    pages = 0
    total_upserts = 0

    try:
        # =========================
        # SMOKE TEST: 1 request + grava RAW
        # =========================
        print("[SMOKE] Fetching page=1 to ensure API + RAW insert works", flush=True)
        data_test, used_path = fetch_page(session, 1, params)
        print(f"[SMOKE] ok used_path={used_path} type={type(data_test)}", flush=True)

        test_sha = sha256_json(data_test)
        insert_raw(
            conn,
            RAW_ENDPOINT_NAME,
            {**params, "pagina": 1, "tamanhoPagina": PAGE_SIZE, "_smoke": True, "_path": used_path},
            data_test,
            test_sha,
        )
        conn.commit()
        print("[SMOKE] inserted into api_raw + committed", flush=True)

        # =========================
        # Loop pages
        # =========================
        pagina = 1
        while True:
            print(f"[ARP] Fetching page={pagina}", flush=True)
            data, used_path = fetch_page(session, pagina, params)

            page_sha = sha256_json(data)
            insert_raw(
                conn,
                RAW_ENDPOINT_NAME,
                {**params, "pagina": pagina, "tamanhoPagina": PAGE_SIZE, "_path": used_path},
                da
