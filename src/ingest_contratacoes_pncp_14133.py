import json
import os
import time
import hashlib
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import get_conn


BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").rstrip("/")
PATH = "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133"
ENDPOINT_RAW = "modulo-contratacoes/1_consultarContratacoes_PNCP_14133"


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _parse_modalidades(s: str) -> List[int]:
    out = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            pass
    return out or [5, 6, 7]


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _request_json(session: requests.Session, url: str, params: Dict[str, Any], max_attempts: int = 5) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, params=params, timeout=90)
            print(f"[HTTP] attempt={attempt} status={resp.status_code} url={resp.url}")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(min(2.0 * attempt, 10.0))
    raise RuntimeError(f"Falha ao chamar {url} params={params}. Último erro: {last_err}")


def insert_raw(cur, endpoint: str, params: Dict[str, Any], payload: Dict[str, Any]) -> None:
    payload_bytes = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()

    cur.execute(
        """
        insert into api_raw(endpoint, params, payload, payload_sha256, fetched_at)
        values (%s, %s::jsonb, %s::jsonb, %s, now())
        on conflict (endpoint, payload_sha256) do nothing
        """,
        (
            endpoint,
            json.dumps(params, ensure_ascii=False),
            json.dumps(payload, ensure_ascii=False),
            payload_sha256,
        ),
    )


def upsert_contratacao(cur, item: Dict[str, Any]) -> None:
    cur.execute(
        """
        insert into contratacao_pncp_14133 (
          id_compra,
          numero_controle_pncp,
          codigo_modalidade,
          modalidade_nome,
          unidade_orgao_codigo_unidade,
          unidade_orgao_nome_unidade,
          unidade_orgao_uf_sigla,
          unidade_orgao_municipio_nome,
          unidade_orgao_codigo_ibge,
          orgao_entidade_cnpj,
          codigo_orgao,
          orgao_entidade_razao_social,
          numero_compra,
          processo,
          srp,
          objeto_compra,
          data_inclusao_pncp,
          data_atualizacao_pncp,
          data_publicacao_pncp,
          data_abertura_proposta_pncp,
          data_encerramento_proposta_pncp,
          valor_total_estimado,
          valor_total_homologado,
          contratacao_excluida,
          raw_json,
          updated_at
        ) values (
          %(idCompra)s,
          %(numeroControlePNCP)s,
          %(codigoModalidade)s,
          %(modalidadeNome)s,
          %(unidadeOrgaoCodigoUnidade)s,
          %(unidadeOrgaoNomeUnidade)s,
          %(unidadeOrgaoUfSigla)s,
          %(unidadeOrgaoMunicipioNome)s,
          %(unidadeOrgaoCodigoIbge)s,
          %(orgaoEntidadeCnpj)s,
          %(codigoOrgao)s,
          %(orgaoEntidadeRazaoSocial)s,
          %(numeroCompra)s,
          %(processo)s,
          %(srp)s,
          %(objetoCompra)s,
          %(dataInclusaoPncp)s,
          %(dataAtualizacaoPncp)s,
          %(dataPublicacaoPncp)s,
          %(dataAberturaPropostaPncp)s,
          %(dataEncerramentoPropostaPncp)s,
          %(valorTotalEstimado)s,
          %(valorTotalHomologado)s,
          %(contratacaoExcluida)s,
          %(raw_json)s::jsonb,
          now()
        )
        on conflict (id_compra) do update set
          numero_controle_pncp = excluded.numero_controle_pncp,
          codigo_modalidade = excluded.codigo_modalidade,
          modalidade_nome = excluded.modalidade_nome,
          unidade_orgao_codigo_unidade = excluded.unidade_orgao_codigo_unidade,
          unidade_orgao_nome_unidade = excluded.unidade_orgao_nome_unidade,
          unidade_orgao_uf_sigla = excluded.unidade_orgao_uf_sigla,
          unidade_orgao_municipio_nome = excluded.unidade_orgao_municipio_nome,
          unidade_orgao_codigo_ibge = excluded.unidade_orgao_codigo_ibge,
          orgao_entidade_cnpj = excluded.orgao_entidade_cnpj,
          codigo_orgao = excluded.codigo_orgao,
          orgao_entidade_razao_social = excluded.orgao_entidade_razao_social,
          numero_compra = excluded.numero_compra,
          processo = excluded.processo,
          srp = excluded.srp,
          objeto_compra = excluded.objeto_compra,
          data_inclusao_pncp = excluded.data_inclusao_pncp,
          data_atualizacao_pncp = excluded.data_atualizacao_pncp,
          data_publicacao_pncp = excluded.data_publicacao_pncp,
          data_abertura_proposta_pncp = excluded.data_abertura_proposta_pncp,
          data_encerramento_proposta_pncp = excluded.data_encerramento_proposta_pncp,
          valor_total_estimado = excluded.valor_total_estimado,
          valor_total_homologado = excluded.valor_total_homologado,
          contratacao_excluida = excluded.contratacao_excluida,
          raw_json = excluded.raw_json,
          updated_at = now()
        """,
        {
            **item,
            "raw_json": json.dumps(item, ensure_ascii=False),
        },
    )


def main() -> None:
    unidade = os.getenv("COMPRAS_UNIDADE", "155125")
    modalidades = _parse_modalidades(os.getenv("COMPRAS_MODALIDADES", "5,6,7"))
    page_size = _env_int("COMPRAS_PAGE_SIZE", 500)
    sleep_s = _env_float("COMPRAS_SLEEP_S", 0.10)

    start_date = _parse_date(os.getenv("COMPRAS_START_DATE", "2021-01-01"))
    max_window_days = _env_int("COMPRAS_MAX_WINDOW_DAYS", 365)

    print("[CONTRATACOES] START")
    print(f"[CONTRATACOES] BASE_URL={BASE_URL}")
    print(f"[CONTRATACOES] PATH={PATH}")
    print(f"[CONTRATACOES] unidade={unidade}")
    print(f"[CONTRATACOES] modalidades={modalidades}")
    print(f"[CONTRATACOES] start_date={start_date.isoformat()} max_window_days={max_window_days} page_size={page_size}")

    conn = get_conn()
    session = requests.Session()

    upserts = 0
    windows = 0
    pages = 0

    try:
        win_start = start_date
        today = date.today()

        while win_start <= today:
            win_end = min(win_start + timedelta(days=max_window_days - 1), today)
            windows += 1

            for modalidade in modalidades:
                page = 1
                while True:
                    url = f"{BASE_URL}{PATH}"
                    params = {
                        "pagina": page,
                        "tamanhoPagina": page_size,
                        "unidadeOrgaoCodigoUnidade": unidade,
                        "dataPublicacaoPncpInicial": win_start.isoformat(),
                        "dataPublicacaoPncpFinal": win_end.isoformat(),
                        "codigoModalidade": modalidade,
                    }

                    payload = _request_json(session, url, params)
                    results = payload.get("resultado") or []
                    pages += 1

                    if not results:
                        print(f"[CONTRATACOES] page={page} items=0 modalidade={modalidade} window={win_start}..{win_end}")
                        break

                    with conn.cursor() as cur:
                        insert_raw(cur, ENDPOINT_RAW, params, payload)
                        for item in results:
                            upsert_contratacao(cur, item)
                            upserts += 1
                    conn.commit()

                    print(f"[CONTRATACOES] page={page} items={len(results)} modalidade={modalidade} window={win_start}..{win_end}")

                    total_pages = payload.get("totalPaginas")
                    if total_pages and isinstance(total_pages, int) and page >= total_pages:
                        break

                    page += 1
                    time.sleep(sleep_s)

                time.sleep(sleep_s)

            win_start = win_end + timedelta(days=1)

        print(f"[CONTRATACOES] DONE windows={windows} pages={pages} upserts={upserts}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
