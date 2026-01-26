import json
import os
import time
import hashlib
from typing import Any, Dict, Iterable, Optional

import requests

from db import get_conn


BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").rstrip("/")
ITEM_PATH = "/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id"
ENDPOINT_RAW = "modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id"


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


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


def upsert_item(cur, item: Dict[str, Any]) -> None:
    cur.execute(
        """
        insert into contratacao_item_pncp_14133 (
          id_compra_item,
          id_compra,
          numero_item_pncp,
          numero_item_compra,
          numero_grupo,
          descricao_resumida,
          material_ou_servico,
          material_ou_servico_nome,
          codigo_classe,
          codigo_grupo,
          cod_item_catalogo,
          unidade_medida,
          orcamento_sigiloso,
          item_categoria_id_pncp,
          item_categoria_nome,
          criterio_julgamento_id_pncp,
          criterio_julgamento_nome,
          situacao_compra_item,
          situacao_compra_item_nome,
          tipo_beneficio,
          tipo_beneficio_nome,
          incentivo_produtivo_basico,
          quantidade,
          valor_unitario_estimado,
          valor_total,
          tem_resultado,
          cod_fornecedor,
          nome_fornecedor,
          quantidade_resultado,
          valor_unitario_resultado,
          valor_total_resultado,
          data_inclusao_pncp,
          data_atualizacao_pncp,
          numero_controle_pncp_compra,
          raw_json,
          updated_at
        ) values (
          %(idCompraItem)s,
          %(idCompra)s,
          %(numeroItemPncp)s,
          %(numeroItemCompra)s,
          %(numeroGrupo)s,
          %(descricaoResumida)s,
          %(materialOuServico)s,
          %(materialOuServicoNome)s,
          %(codigoClasse)s,
          %(codigoGrupo)s,
          %(codItemCatalogo)s,
          %(unidadeMedida)s,
          %(orcamentoSigiloso)s,
          %(itemCategoriaIdPncp)s,
          %(itemCategoriaNome)s,
          %(criterioJulgamentoIdPncp)s,
          %(criterioJulgamentoNome)s,
          %(situacaoCompraItem)s,
          %(situacaoCompraItemNome)s,
          %(tipoBeneficio)s,
          %(tipoBeneficioNome)s,
          %(incentivoProdutivoBasico)s,
          %(quantidade)s,
          %(valorUnitarioEstimado)s,
          %(valorTotal)s,
          %(temResultado)s,
          %(codFornecedor)s,
          %(nomeFornecedor)s,
          %(quantidadeResultado)s,
          %(valorUnitarioResultado)s,
          %(valorTotalResultado)s,
          %(dataInclusaoPncp)s,
          %(dataAtualizacaoPncp)s,
          %(numeroControlePNCPCompra)s,
          %(raw_json)s::jsonb,
          now()
        )
        on conflict (id_compra_item) do update set
          id_compra = excluded.id_compra,
          numero_item_pncp = excluded.numero_item_pncp,
          numero_item_compra = excluded.numero_item_compra,
          numero_grupo = excluded.numero_grupo,
          descricao_resumida = excluded.descricao_resumida,
          material_ou_servico = excluded.material_ou_servico,
          material_ou_servico_nome = excluded.material_ou_servico_nome,
          codigo_classe = excluded.codigo_classe,
          codigo_grupo = excluded.codigo_grupo,
          cod_item_catalogo = excluded.cod_item_catalogo,
          unidade_medida = excluded.unidade_medida,
          orcamento_sigiloso = excluded.orcamento_sigiloso,
          item_categoria_id_pncp = excluded.item_categoria_id_pncp,
          item_categoria_nome = excluded.item_categoria_nome,
          criterio_julgamento_id_pncp = excluded.criterio_julgamento_id_pncp,
          criterio_julgamento_nome = excluded.criterio_julgamento_nome,
          situacao_compra_item = excluded.situacao_compra_item,
          situacao_compra_item_nome = excluded.situacao_compra_item_nome,
          tipo_beneficio = excluded.tipo_beneficio,
          tipo_beneficio_nome = excluded.tipo_beneficio_nome,
          incentivo_produtivo_basico = excluded.incentivo_produtivo_basico,
          quantidade = excluded.quantidade,
          valor_unitario_estimado = excluded.valor_unitario_estimado,
          valor_total = excluded.valor_total,
          tem_resultado = excluded.tem_resultado,
          cod_fornecedor = excluded.cod_fornecedor,
          nome_fornecedor = excluded.nome_fornecedor,
          quantidade_resultado = excluded.quantidade_resultado,
          valor_unitario_resultado = excluded.valor_unitario_resultado,
          valor_total_resultado = excluded.valor_total_resultado,
          data_inclusao_pncp = excluded.data_inclusao_pncp,
          data_atualizacao_pncp = excluded.data_atualizacao_pncp,
          numero_controle_pncp_compra = excluded.numero_controle_pncp_compra,
          raw_json = excluded.raw_json,
          updated_at = now()
        """,
        {
            **item,
            "raw_json": json.dumps(item, ensure_ascii=False),
        },
    )


def iter_id_compra(conn, limit: Optional[int] = None) -> Iterable[str]:
    sql = "select id_compra from contratacao_pncp_14133 order by data_publicacao_pncp asc nulls last"
    if limit is not None:
        sql += " limit %s"
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            for (id_compra,) in cur.fetchall():
                yield id_compra
    else:
        with conn.cursor() as cur:
            cur.execute(sql)
            for (id_compra,) in cur.fetchall():
                yield id_compra


def main() -> None:
    sleep_s = _env_float("COMPRAS_SLEEP_S", 0.10)
    limit = _env_int("COMPRAS_ITEM_LIMIT", None)

    print("[ITENS] START")
    print(f"[ITENS] BASE_URL={BASE_URL}")
    print(f"[ITENS] ITEM_PATH={ITEM_PATH}")
    if limit is not None:
        print(f"[ITENS] limit={limit}")

    conn = get_conn()
    session = requests.Session()

    ok = 0
    fail = 0
    total_items = 0

    try:
        for id_compra in iter_id_compra(conn, limit=limit):
            url = f"{BASE_URL}{ITEM_PATH}"
            params = {"tipo": "idCompra", "codigo": id_compra}

            try:
                payload = _request_json(session, url, params)
                results = payload.get("resultado") or []
                print(f"[ITENS] id_compra={id_compra} items={len(results)}")

                with conn.cursor() as cur:
                    insert_raw(cur, ENDPOINT_RAW, params, payload)
                    for item in results:
                        upsert_item(cur, item)

                conn.commit()
                ok += 1
                total_items += len(results)

            except Exception as e:
                conn.rollback()
                fail += 1
                print(f"[EXC] id_compra={id_compra} err={e}")

            time.sleep(sleep_s)

        print(f"[DONE] ok={ok} fail={fail} itens={total_items}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
