import os
import json
import time
import hashlib
import datetime as dt
import requests

from db import get_conn

BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").strip().rstrip("/")
PATH = "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133"

UNIDADE = os.getenv("COMPRAS_UNIDADE", "155125").strip()
CODIGO_MODALIDADE = int(os.getenv("COMPRAS_CODIGO_MODALIDADE", "5"))

PAGE_SIZE = int(os.getenv("COMPRAS_PAGE_SIZE", "500"))
TIMEOUT = int(os.getenv("COMPRAS_TIMEOUT", "60"))

START_DATE = os.getenv("COMPRAS_START_DATE", "2024-01-01")
MAX_WINDOW_DAYS = int(os.getenv("COMPRAS_MAX_WINDOW_DAYS", "365"))
SLEEP_S = float(os.getenv("COMPRAS_SLEEP_S", "0.10"))

RAW_ENDPOINT_NAME = "modulo-contratacoes/1_consultarContratacoes_PNCP_14133"


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
        if isinstance(s, str) and s.endswith("Z"):
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


def pick_first(d: dict, keys):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def candidate_id(row: dict) -> str:
    # tentamos vários nomes comuns de identificador PNCP
    v = pick_first(
        row,
        [
            "numeroControlePncp",
            "numeroControlePNCP",
            "numeroControlePncpCompra",
            "numeroControlePncpContratacao",
            "numero_controle_pncp",
            "idPncp",
            "id_pncp",
            "id",
            "idContratacao",
            "id_contratacao",
        ],
    )
    if v is not None and str(v).strip() != "":
        return str(v).strip()
    # fallback determinístico
    return sha256_json(row)


def main():
    today = dt.date.today()
    start = dt.date.fromisoformat(START_DATE)

    print("[CONTRATACOES] START", flush=True)
    print(
        f"[CONTRATACOES] unidade={UNIDADE} modalidade={CODIGO_MODALIDADE} start={start} end={today} page_size={PAGE_SIZE} max_window_days={MAX_WINDOW_DAYS}",
        flush=True,
    )

    session = make_session()
    url = BASE_URL + PATH

    conn = get_conn()
    conn.autocommit = False

    total_upserts = 0
    total_pages = 0
    total_windows = 0

    try:
        cur_date = start
        while cur_date <= today:
            w_end = min(today, cur_date + dt.timedelta(days=MAX_WINDOW_DAYS - 1))
            total_windows += 1
            pagina = 1
            print(f"[CONTRATACOES] window={cur_date}->{w_end}", flush=True)

            while True:
                params = {
                    "pagina": pagina,
                    "tamanhoPagina": PAGE_SIZE,
                    "unidadeOrgaoCodigoUnidade": UNIDADE,
                    "codigoModalidade": CODIGO_MODALIDADE,
                    "dataPublicacaoPncpInicial": cur_date.isoformat(),
                    "dataPublicacaoPncpFinal": w_end.isoformat(),
                }

                r = get_with_retry(session, url, params)
                r.raise_for_status()
                data = r.json()

                insert_raw(conn, RAW_ENDPOINT_NAME, params, data)
                total_pages += 1

                items = data.get("resultado") or []
                if not items:
                    print(f"[CONTRATACOES] page={pagina} items=0 -> next window", flush=True)
                    conn.commit()
                    break

                for row in items:
                    payload_sha = sha256_json(row)
                    cid = candidate_id(row)

                    # normalização leve (sem assumir nomes além do que você informou)
                    data_pub = parse_date(
                        pick_first(row, ["dataPublicacaoPncp", "data_publicacao_pncp", "dataPublicacao"])
                    )
                    dt_atual = parse_dt(
                        pick_first(row, ["dataAualizacaoPncp", "dataAtualizacaoPncp", "data_atualizacao_pncp"])
                    )
                    excluida = pick_first(row, ["contratacaoExcluida", "contratacao_excluida"])

                    orgao_cnpj = pick_first(row, ["orgaoEntidadeCnpj", "orgao_cnpj", "cnpjOrgao"])
                    codigo_orgao = pick_first(row, ["codigoOrgao", "codigo_orgao"])
                    uf = pick_first(row, ["unidadeOrgaoUfSigla", "unidade_orgao_uf_sigla"])
                    ibge = pick_first(row, ["unidadeOrgaoCodigoIbge", "unidade_orgao_codigo_ibge"])

                    objeto = pick_first(row, ["objeto", "descricaoObjeto", "descricao_objeto"])
                    situacao = pick_first(row, ["situacao", "status", "situacaoContratacao"])

                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            insert into contratacao_pncp_14133 (
                              id_pncp,
                              numero_controle_pncp,
                              id_compra,
                              unidade_orgao_codigo_unidade,
                              codigo_modalidade,
                              data_publicacao_pncp,
                              data_atualizacao_pncp,
                              contratacao_excluida,
                              orgao_cnpj,
                              codigo_orgao,
                              unidade_orgao_uf_sigla,
                              unidade_orgao_codigo_ibge,
                              objeto,
                              situacao,
                              payload_sha256,
                              payload,
                              first_seen_at,
                              last_seen_at
                            )
                            values (
                              %s,%s,%s,%s,%s,
                              %s,%s,%s,
                              %s,%s,%s,%s,
                              %s,%s,
                              %s,%s::jsonb,
                              now(), now()
                            )
                            on conflict (id_pncp)
                            do update set
                              numero_controle_pncp=excluded.numero_controle_pncp,
                              id_compra=excluded.id_compra,
                              unidade_orgao_codigo_unidade=excluded.unidade_orgao_codigo_unidade,
                              codigo_modalidade=excluded.codigo_modalidade,
                              data_publicacao_pncp=excluded.data_publicacao_pncp,
                              data_atualizacao_pncp=excluded.data_atualizacao_pncp,
                              contratacao_excluida=excluded.contratacao_excluida,
                              orgao_cnpj=excluded.orgao_cnpj,
                              codigo_orgao=excluded.codigo_orgao,
                              unidade_orgao_uf_sigla=excluded.unidade_orgao_uf_sigla,
                              unidade_orgao_codigo_ibge=excluded.unidade_orgao_codigo_ibge,
                              objeto=excluded.objeto,
                              situacao=excluded.situacao,
                              payload_sha256=excluded.payload_sha256,
                              payload=excluded.payload,
                              last_seen_at=now()
                            """,
                            (
                                cid,
                                pick_first(row, ["numeroControlePncp", "numeroControlePNCP", "numeroControlePncpContratacao"]),
                                pick_first(row, ["idCompra", "id_compra"]),
                                UNIDADE,
                                CODIGO_MODALIDADE,
                                data_pub,
                                dt_atual,
                                excluida,
                                orgao_cnpj,
                                int(codigo_orgao) if codigo_orgao not in (None, "") else None,
                                uf,
                                int(ibge) if ibge not in (None, "") else None,
                                objeto,
                                situacao,
                                payload_sha,
                                json.dumps(row, ensure_ascii=False),
                            ),
                        )
                    total_upserts += 1

                conn.commit()
                print(f"[CONTRATACOES] page={pagina} items={len(items)} upserts_total={total_upserts}", flush=True)

                pagina += 1
                time.sleep(SLEEP_S)

            cur_date = w_end + dt.timedelta(days=1)

        with conn.cursor() as cur:
            cur.execute(
                """
                insert into etl_state (name, value, updated_at)
                values (%s, %s::jsonb, now())
                on conflict (name)
                do update set value=excluded.value, updated_at=now()
                """,
                (
                    "contratacoes_last_run",
                    json.dumps(
                        {
                            "ended_at": dt.datetime.utcnow().isoformat(),
                            "unidade": UNIDADE,
                            "codigoModalidade": CODIGO_MODALIDADE,
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

        print(f"[CONTRATACOES] DONE windows={total_windows} pages={total_pages} upserts={total_upserts}", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
