import os
import json
import time
import hashlib
import datetime as dt
import requests

from db import get_conn

BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").strip().rstrip("/")
PATH = "/modulo-legado/1_consultarLicitacao"

UASG = int(os.getenv("COMPRAS_UASG", "155125"))
PAGE_SIZE = int(os.getenv("COMPRAS_PAGE_SIZE", "500"))
TIMEOUT = int(os.getenv("COMPRAS_TIMEOUT", "60"))

START_DATE = os.getenv("COMPRAS_START_DATE", "2024-01-01")
MAX_WINDOW_DAYS = int(os.getenv("COMPRAS_MAX_WINDOW_DAYS", "365"))

SLEEP_S = float(os.getenv("COMPRAS_SLEEP_S", "0.10"))

RAW_ENDPOINT_NAME = "modulo-legado/1_consultarLicitacao"


def sha256_json(obj) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def parse_date(s):
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s)
    except Exception:
        return None


def parse_dt(s):
    if not s:
        return None
    try:
        # "2026-01-23T18:12:50.415Z" -> trocar Z por +00:00
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def make_session():
    s = requests.Session()
    s.headers.update(
        {
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9",
            "User-Agent": "comprasgov-ingestor/1.0",
        }
    )
    return s


def get_with_retry(session, url, params):
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


def insert_raw(conn, endpoint, params, payload):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into api_raw (endpoint, params, payload, payload_sha256)
            values (%s, %s::jsonb, %s::jsonb, %s)
            """,
            (
                endpoint,
                json.dumps(params, ensure_ascii=False),
                json.dumps(payload, ensure_ascii=False),
                sha256_json(payload),
            ),
        )


def upsert_licitacao(conn, row: dict):
    payload_sha = sha256_json(row)

    id_compra = row.get("id_compra")
    if not id_compra:
        # sem id_compra não dá pra garantir chave; ignora
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into licitacao (
              id_compra,
              identificador,
              numero_processo,
              uasg,
              modalidade,
              nome_modalidade,
              numero_aviso,
              situacao_aviso,
              tipo_pregao,
              tipo_recurso,
              nome_responsavel,
              funcao_responsavel,
              numero_itens,
              valor_estimado_total,
              valor_homologado_total,
              informacoes_gerais,
              objeto,
              endereco_entrega_edital,
              codigo_municipio_uasg,
              data_abertura_proposta,
              data_entrega_edital,
              data_entrega_proposta,
              data_publicacao,
              dt_alteracao,
              pertence14133,
              payload_sha256,
              payload,
              first_seen_at,
              last_seen_at
            )
            values (
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
              %s,%s::jsonb, now(), now()
            )
            on conflict (id_compra) do update set
              identificador=excluded.identificador,
              numero_processo=excluded.numero_processo,
              uasg=excluded.uasg,
              modalidade=excluded.modalidade,
              nome_modalidade=excluded.nome_modalidade,
              numero_aviso=excluded.numero_aviso,
              situacao_aviso=excluded.situacao_aviso,
              tipo_pregao=excluded.tipo_pregao,
              tipo_recurso=excluded.tipo_recurso,
              nome_responsavel=excluded.nome_responsavel,
              funcao_responsavel=excluded.funcao_responsavel,
              numero_itens=excluded.numero_itens,
              valor_estimado_total=excluded.valor_estimado_total,
              valor_homologado_total=excluded.valor_homologado_total,
              informacoes_gerais=excluded.informacoes_gerais,
              objeto=excluded.objeto,
              endereco_entrega_edital=excluded.endereco_entrega_edital,
              codigo_municipio_uasg=excluded.codigo_municipio_uasg,
              data_abertura_proposta=excluded.data_abertura_proposta,
              data_entrega_edital=excluded.data_entrega_edital,
              data_entrega_proposta=excluded.data_entrega_proposta,
              data_publicacao=excluded.data_publicacao,
              dt_alteracao=excluded.dt_alteracao,
              pertence14133=excluded.pertence14133,
              payload_sha256=excluded.payload_sha256,
              payload=excluded.payload,
              last_seen_at=now()
            """,
            (
                id_compra,
                row.get("identificador"),
                row.get("numero_processo"),
                int(row.get("uasg")) if row.get("uasg") is not None else UASG,
                row.get("modalidade"),
                row.get("nome_modalidade"),
                row.get("numero_aviso"),
                row.get("situacao_aviso"),
                row.get("tipo_pregao"),
                row.get("tipo_recurso"),
                row.get("nome_responsavel"),
                row.get("funcao_responsavel"),
                row.get("numero_itens"),
                row.get("valor_estimado_total"),
                row.get("valor_homologado_total"),
                row.get("informacoes_gerais"),
                row.get("objeto"),
                row.get("endereco_entrega_edital"),
                row.get("codigo_municipio_uasg"),
                parse_date(row.get("data_abertura_proposta")),
                parse_date(row.get("data_entrega_edital")),
                parse_date(row.get("data_entrega_proposta")),
                parse_date(row.get("data_publicacao")),
                parse_dt(row.get("dt_alteracao")),
                row.get("pertence14133"),
                payload_sha,
                json.dumps(row, ensure_ascii=False),
            ),
        )
    return 1


def daterange_windows(start: dt.date, end: dt.date, window_days: int):
    cur = start
    while cur <= end:
        w_end = min(end, cur + dt.timedelta(days=window_days - 1))
        yield cur, w_end
        cur = w_end + dt.timedelta(days=1)


def main():
    today = dt.date.today()
    start = dt.date.fromisoformat(START_DATE)

    print("[LICITACAO] START", flush=True)
    print(f"[LICITACAO] uasg={UASG} start={start} end={today} page_size={PAGE_SIZE} max_window_days={MAX_WINDOW_DAYS}", flush=True)

    session = make_session()
    url = BASE_URL + PATH

    conn = get_conn()
    conn.autocommit = False

    total_upserts = 0
    total_pages = 0
    total_windows = 0

    try:
        for w_start, w_end in daterange_windows(start, today, MAX_WINDOW_DAYS):
            total_windows += 1
            pagina = 1
            print(f"[LICITACAO] window={w_start}->{w_end}", flush=True)

            while True:
                params = {
                    "pagina": pagina,
                    "tamanhoPagina": PAGE_SIZE,
                    "uasg": UASG,
                    "data_publicacao_inicial": w_start.isoformat(),
                    "data_publicacao_final": w_end.isoformat(),
                }

                r = get_with_retry(session, url, params)
                r.raise_for_status()
                data = r.json()

                insert_raw(conn, RAW_ENDPOINT_NAME, params, data)
                total_pages += 1

                items = data.get("resultado") or []
                if not items:
                    print(f"[LICITACAO] page={pagina} items=0 -> next window", flush=True)
                    conn.commit()
                    break

                for row in items:
                    total_upserts += upsert_licitacao(conn, row)

                conn.commit()
                print(f"[LICITACAO] page={pagina} items={len(items)} upserts_total={total_upserts}", flush=True)

                pagina += 1
                time.sleep(SLEEP_S)

        # estado
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into etl_state (name, value, updated_at)
                values (%s, %s::jsonb, now())
                on conflict (name)
                do update set value=excluded.value, updated_at=now()
                """,
                (
                    "licitacao_last_run",
                    json.dumps(
                        {
                            "ended_at": dt.datetime.utcnow().isoformat(),
                            "uasg": UASG,
                            "start": START_DATE,
                            "end": today.isoformat(),
                            "windows": total_windows,
                            "pages": total_pages,
                            "upserts": total_upserts,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
        conn.commit()

        print(f"[LICITACAO] DONE windows={total_windows} pages={total_pages} upserts={total_upserts}", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
