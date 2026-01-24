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

# Agora suportamos múltiplas modalidades: "5,6,7"
MODALIDADES_RAW = os.getenv("COMPRAS_MODALIDADES", "5,6,7")
MODALIDADES = [int(x.strip()) for x in MODALIDADES_RAW.split(",") if x.strip()]

PAGE_SIZE = int(os.getenv("COMPRAS_PAGE_SIZE", "500"))
TIMEOUT = int(os.getenv("COMPRAS_TIMEOUT", "60"))

START_DATE = os.getenv("COMPRAS_START_DATE", "2024-01-01")
MAX_WINDOW_DAYS = int(os.getenv("COMPRAS_MAX_WINDOW_DAYS", "365"))
SLEEP_S = float(os.getenv("COMPRAS_SLEEP_S", "0.10"))

RAW_ENDPOINT_NAME = "modulo-contratacoes/1_consultarContratacoes_PNCP_14133"


def sha256_json(obj) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


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


def daterange_windows(start: dt.date, end: dt.date, window_days: int):
    cur = start
    while cur <= end:
        w_end = min(end, cur + dt.timedelta(days=window_days - 1))
        yield cur, w_end
        cur = w_end + dt.timedelta(days=1)


def to_int(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


def to_num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def upsert_contratacao(conn, row: dict):
    numero_controle = row.get("numeroControlePNCP")
    if not numero_controle:
        # sem chave, ignora (não deve acontecer nesse endpoint)
        return 0

    payload_sha = sha256_json(row)

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into contratacao_pncp_14133 (
              numero_controle_pncp,
              id_compra,
              ano_compra_pncp,
              sequencial_compra_pncp,

              orgao_entidade_cnpj,
              orgao_subrogado_cnpj,
              codigo_orgao,
              orgao_entidade_razao_social,
              orgao_subrogado_razao_social,
              orgao_entidade_esfera_id,
              orgao_subrogado_esfera_id,
              orgao_entidade_poder_id,
              orgao_subrogado_poder_id,

              unidade_orgao_codigo_unidade,
              unidade_subrogada_codigo_unidade,
              unidade_orgao_nome_unidade,
              unidade_subrogada_nome_unidade,
              unidade_orgao_uf_sigla,
              unidade_subrogada_uf_sigla,
              unidade_orgao_municipio_nome,
              unidade_subrogada_municipio_nome,
              unidade_orgao_codigo_ibge,
              unidade_subrogada_codigo_ibge,

              numero_compra,
              modalidade_id_pncp,
              codigo_modalidade,
              modalidade_nome,
              srp,

              modo_disputa_id_pncp,
              codigo_modo_disputa,
              modo_disputa_nome,

              amparo_legal_codigo_pncp,
              amparo_legal_nome,
              amparo_legal_descricao,

              processo,
              objeto_compra,
              informacao_complementar,

              existe_resultado,

              orcamento_sigiloso_codigo,
              orcamento_sigiloso_descricao,

              situacao_compra_id_pncp,
              situacao_compra_nome_pncp,

              tipo_instrumento_convocatorio_codigo_pncp,
              tipo_instrumento_convocatorio_nome,

              valor_total_estimado,
              valor_total_homologado,

              data_inclusao_pncp,
              data_atualizacao_pncp,
              data_publicacao_pncp,
              data_abertura_proposta_pncp,
              data_encerramento_proposta_pncp,

              contratacao_excluida,

              payload_sha256,
              payload,
              first_seen_at,
              last_seen_at
            )
            values (
              %s,%s,%s,%s,
              %s,%s,%s,%s,%s,%s,%s,%s,%s,
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
              %s,%s,%s,%s,%s,
              %s,%s,%s,
              %s,%s,%s,
              %s,%s,%s,
              %s,
              %s,%s,
              %s,%s,
              %s,%s,
              %s,%s,
              %s,%s,%s,%s,%s,
              %s,
              %s,%s::jsonb,
              now(), now()
            )
            on conflict (numero_controle_pncp)
            do update set
              id_compra=excluded.id_compra,
              ano_compra_pncp=excluded.ano_compra_pncp,
              sequencial_compra_pncp=excluded.sequencial_compra_pncp,

              orgao_entidade_cnpj=excluded.orgao_entidade_cnpj,
              orgao_subrogado_cnpj=excluded.orgao_subrogado_cnpj,
              codigo_orgao=excluded.codigo_orgao,
              orgao_entidade_razao_social=excluded.orgao_entidade_razao_social,
              orgao_subrogado_razao_social=excluded.orgao_subrogado_razao_social,
              orgao_entidade_esfera_id=excluded.orgao_entidade_esfera_id,
              orgao_subrogado_esfera_id=excluded.orgao_subrogado_esfera_id,
              orgao_entidade_poder_id=excluded.orgao_entidade_poder_id,
              orgao_subrogado_poder_id=excluded.orgao_subrogado_poder_id,

              unidade_orgao_codigo_unidade=excluded.unidade_orgao_codigo_unidade,
              unidade_subrogada_codigo_unidade=excluded.unidade_subrogada_codigo_unidade,
              unidade_orgao_nome_unidade=excluded.unidade_orgao_nome_unidade,
              unidade_subrogada_nome_unidade=excluded.unidade_subrogada_nome_unidade,
              unidade_orgao_uf_sigla=excluded.unidade_orgao_uf_sigla,
              unidade_subrogada_uf_sigla=excluded.unidade_subrogada_uf_sigla,
              unidade_orgao_municipio_nome=excluded.unidade_orgao_municipio_nome,
              unidade_subrogada_municipio_nome=excluded.unidade_subrogada_municipio_nome,
              unidade_orgao_codigo_ibge=excluded.unidade_orgao_codigo_ibge,
              unidade_subrogada_codigo_ibge=excluded.unidade_subrogada_codigo_ibge,

              numero_compra=excluded.numero_compra,
              modalidade_id_pncp=excluded.modalidade_id_pncp,
              codigo_modalidade=excluded.codigo_modalidade,
              modalidade_nome=excluded.modalidade_nome,
              srp=excluded.srp,

              modo_disputa_id_pncp=excluded.modo_disputa_id_pncp,
              codigo_modo_disputa=excluded.codigo_modo_disputa,
              modo_disputa_nome=excluded.modo_disputa_nome,

              amparo_legal_codigo_pncp=excluded.amparo_legal_codigo_pncp,
              amparo_legal_nome=excluded.amparo_legal_nome,
              amparo_legal_descricao=excluded.amparo_legal_descricao,

              processo=excluded.processo,
              objeto_compra=excluded.objeto_compra,
              informacao_complementar=excluded.informacao_complementar,

              existe_resultado=excluded.existe_resultado,

              orcamento_sigiloso_codigo=excluded.orcamento_sigiloso_codigo,
              orcamento_sigiloso_descricao=excluded.orcamento_sigiloso_descricao,

              situacao_compra_id_pncp=excluded.situacao_compra_id_pncp,
              situacao_compra_nome_pncp=excluded.situacao_compra_nome_pncp,

              tipo_instrumento_convocatorio_codigo_pncp=excluded.tipo_instrumento_convocatorio_codigo_pncp,
              tipo_instrumento_convocatorio_nome=excluded.tipo_instrumento_convocatorio_nome,

              valor_total_estimado=excluded.valor_total_estimado,
              valor_total_homologado=excluded.valor_total_homologado,

              data_inclusao_pncp=excluded.data_inclusao_pncp,
              data_atualizacao_pncp=excluded.data_atualizacao_pncp,
              data_publicacao_pncp=excluded.data_publicacao_pncp,
              data_abertura_proposta_pncp=excluded.data_abertura_proposta_pncp,
              data_encerramento_proposta_pncp=excluded.data_encerramento_proposta_pncp,

              contratacao_excluida=excluded.contratacao_excluida,

              payload_sha256=excluded.payload_sha256,
              payload=excluded.payload,
              last_seen_at=now()
            """,
            (
                numero_controle,
                row.get("idCompra"),
                to_int(row.get("anoCompraPncp")),
                to_int(row.get("sequencialCompraPncp")),

                row.get("orgaoEntidadeCnpj"),
                row.get("orgaoSubrogadoCnpj"),
                to_int(row.get("codigoOrgao")),
                row.get("orgaoEntidadeRazaoSocial"),
                row.get("orgaoSubrogadoRazaoSocial"),
                row.get("orgaoEntidadeEsferaId"),
                row.get("orgaoSubrogadoEsferaId"),
                row.get("orgaoEntidadePoderId"),
                row.get("orgaoSubrogadoPoderId"),

                row.get("unidadeOrgaoCodigoUnidade") or UNIDADE,
                row.get("unidadeSubrogadaCodigoUnidade"),
                row.get("unidadeOrgaoNomeUnidade"),
                row.get("unidadeSubrogadaNomeUnidade"),
                row.get("unidadeOrgaoUfSigla"),
                row.get("unidadeSubrogadaUfSigla"),
                row.get("unidadeOrgaoMunicipioNome"),
                row.get("unidade_subrogada_municipio_nome") or row.get("unidadeSubrogadaMunicipioNome"),
                to_int(row.get("unidadeOrgaoCodigoIbge")),
                to_int(row.get("unidadeSubrogadaCodigoIbge")),

                row.get("numeroCompra"),
                to_int(row.get("modalidadeIdPncp")),
                to_int(row.get("codigoModalidade")),
                row.get("modalidadeNome"),
                row.get("srp"),

                to_int(row.get("modoDisputaIdPncp")),
                to_int(row.get("codigoModoDisputa")),
                row.get("modoDisputaNomePncp"),

                to_int(row.get("amparoLegalCodigoPncp")),
                row.get("amparoLegalNome"),
                row.get("amparoLegalDescricao"),

                row.get("processo"),
                row.get("objetoCompra"),
                row.get("informacaoComplementar"),

                row.get("existeResultado"),

                to_int(row.get("orcamentoSigilosoCodigo")),
                row.get("orcamentoSigilosoDescricao"),

                to_int(row.get("situacaoCompraIdPncp")),
                row.get("situacaoCompraNomePncp"),

                to_int(row.get("tipoInstrumentoConvocatorioCodigoPncp")),
                row.get("tipoInstrumentoConvocatorioNome"),

                to_num(row.get("valorTotalEstimado")),
                to_num(row.get("valorTotalHomologado")),

                parse_dt(row.get("dataInclusaoPncp")),
                parse_dt(row.get("dataAtualizacaoPncp")),
                parse_dt(row.get("dataPublicacaoPncp")),
                parse_dt(row.get("dataAberturaPropostaPncp")),
                parse_dt(row.get("dataEncerramentoPropostaPncp")),

                row.get("contratacaoExcluida"),

                payload_sha,
                json.dumps(row, ensure_ascii=False),
            ),
        )

    return 1


def main():
    today = dt.date.today()
    start = dt.date.fromisoformat(START_DATE)

    print("[CONTRATACOES] START", flush=True)
    print(f"[CONTRATACOES] unidade={UNIDADE} modalidades={MODALIDADES} start={start} end={today} page_size={PAGE_SIZE} max_window_days={MAX_WINDOW_DAYS}", flush=True)

    session = make_session()
    url = BASE_URL + PATH

    conn = get_conn()
    conn.autocommit = False

    total_upserts = 0
    total_pages = 0
    total_windows = 0

    try:
        for cod_modalidade in MODALIDADES:
            print(f"[CONTRATACOES] modalidade={cod_modalidade}", flush=True)

            for w_start, w_end in daterange_windows(start, today, MAX_WINDOW_DAYS):
                total_windows += 1
                pagina = 1
                print(f"[CONTRATACOES] window={w_start}->{w_end}", flush=True)

                while True:
                    params = {
                        "pagina": pagina,
                        "tamanhoPagina": PAGE_SIZE,
                        "unidadeOrgaoCodigoUnidade": UNIDADE,
                        "codigoModalidade": cod_modalidade,
                        "dataPublicacaoPncpInicial": w_start.isoformat(),
                        "dataPublicacaoPncpFinal": w_end.isoformat(),
                    }

                    r = get_with_retry(session, url, params)
                    r.raise_for_status()
                    data = r.json()

                    insert_raw(conn, RAW_ENDPOINT_NAME, params, data)
                    total_pages += 1

                    items = data.get("resultado") or []
                    if not items:
                        conn.commit()
                        print(f"[CONTRATACOES] page={pagina} items=0 -> next window", flush=True)
                        break

                    for row in items:
                        total_upserts += upsert_contratacao(conn, row)

                    conn.commit()
                    print(f"[CONTRATACOES] page={pagina} items={len(items)} upserts_total={total_upserts}", flush=True)

                    pagina += 1
                    time.sleep(SLEEP_S)

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
                            "modalidades": MODALIDADES,
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
