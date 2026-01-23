import os
import json
import time
import hashlib
import datetime as dt
import requests

from db import get_conn

RAW_ENDPOINT_NAME = "modulo-arp/2.1_consultarARPItem_Id"

UG = int(os.getenv("COMPRAS_UG", "155125"))
TIMEOUT = int(os.getenv("COMPRAS_TIMEOUT", "60"))

BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").strip().rstrip("/")
ITEM_PATH = "/modulo-arp/2.1_consultarARPItem_Id"

ARP_ITEM_BATCH_LIMIT = int(os.getenv("ARP_ITEM_BATCH_LIMIT", "300"))
SLEEP_BETWEEN_CALLS_S = float(os.getenv("ARP_ITEM_SLEEP_S", "0.15"))


def sha256_json(obj) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def parse_dt(value):
    if value is None or value == "":
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except Exception:
        return None


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


def select_arp_targets(conn, limit: int):
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


def extract_items(data):
    if isinstance(data, dict) and isinstance(data.get("resultado"), list):
        return data["resultado"]
    if isinstance(data, list):
        return data
    return []


def upsert_item(conn, ug: int, numero_controle_ata: str, item: dict) -> int:
    item_sha = sha256_json(item)

    numero_ata = item.get("numeroAtaRegistroPreco")

    numero_item_str = item.get("numeroItem")  # ex: "00005"
    codigo_item = item.get("codigoItem")
    descricao_item = item.get("descricaoItem")
    tipo_item = item.get("tipoItem")

    qtd_item = item.get("quantidadeHomologadaItem")
    qtd_vencedor = item.get("quantidadeHomologadaVencedor")
    valor_unit = item.get("valorUnitario")
    valor_total = item.get("valorTotal")
    maximo_adesao = item.get("maximoAdesao")

    classificacao_fornecedor = item.get("classificacaoFornecedor")
    ni_fornecedor = item.get("niFornecedor")
    nome_fornecedor = item.get("nomeRazaoSocialFornecedor")
    situacao_sicaf = item.get("situacaoSicaf")

    codigo_pdm = item.get("codigoPdm")
    nome_pdm = item.get("nomePdm")

    numero_compra = item.get("numeroCompra")
    ano_compra = item.get("anoCompra")
    codigo_modalidade = item.get("codigoModalidadeCompra")

    id_compra = item.get("idCompra")
    numero_controle_compra = item.get("numeroControlePncpCompra")

    data_hora_inclusao = parse_dt(item.get("dataHoraInclusao"))
    data_hora_atualizacao = parse_dt(item.get("dataHoraAtualizacao"))
    data_hora_exclusao = parse_dt(item.get("dataHoraExclusao"))
    item_excluido = item.get("itemExcluido")

    quantidade = qtd_vencedor if qtd_vencedor is not None else qtd_item

    # Evita null na chave (index usa coalesce para '')
    numero_item_str = "" if numero_item_str is None else str(numero_item_str)

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into arp_item (
              codigo_unidade_gerenciadora,
              numero_ata_registro_preco,
              numero_controle_pncp_ata,

              -- chave nova
              numero_item_str,

              -- outros campos
              codigo_item,
              tipo_item,
              descricao,
              quantidade,
              valor_unitario,
              valor_total,
              maximo_adesao,

              classificacao_fornecedor,
              ni_fornecedor,
              nome_fornecedor,
              situacao_sicaf,

              codigo_pdm,
              nome_pdm,

              numero_compra,
              ano_compra,
              codigo_modalidade_compra,
              id_compra,
              numero_controle_pncp_compra,

              data_hora_inclusao,
              data_hora_atualizacao,
              data_hora_exclusao,
              item_excluido,

              payload_sha256,
              payload,
              first_seen_at,
              last_seen_at
            )
            values (
              %s, %s, %s,
              %s,
              %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s,
              %s, %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s::jsonb,
              now(), now()
            )
            on conflict (codigo_unidade_gerenciadora, numero_controle_pncp_ata, numero_item_str)
            do update set
              numero_ata_registro_preco = excluded.numero_ata_registro_preco,

              codigo_item = excluded.codigo_item,
              tipo_item = excluded.tipo_item,
              descricao = excluded.descricao,
              quantidade = excluded.quantidade,
              valor_unitario = excluded.valor_unitario,
              valor_total = excluded.valor_total,
              maximo_adesao = excluded.maximo_adesao,

              classificacao_fornecedor = excluded.classificacao_fornecedor,
              ni_fornecedor = excluded.ni_fornecedor,
              nome_fornecedor = excluded.nome_fornecedor,
              situacao_sicaf = excluded.situacao_sicaf,

              codigo_pdm = excluded.codigo_pdm,
              nome_pdm = excluded.nome_pdm,

              numero_compra = excluded.numero_compra,
              ano_compra = excluded.ano_compra,
              codigo_modalidade_compra = excluded.codigo_modalidade_compra,
              id_compra = excluded.id_compra,
              numero_controle_pncp_compra = excluded.numero_controle_pncp_compra,

              data_hora_inclusao = excluded.data_hora_inclusao,
              data_hora_atualizacao = excluded.data_hora_atualizacao,
              data_hora_exclusao = excluded.data_hora_exclusao,
              item_excluido = excluded.item_excluido,

              payload_sha256 = excluded.payload_sha256,
              payload = excluded.payload,
              last_seen_at = now()
            """,
            (
                ug,
                numero_ata,
                numero_controle_ata,
                numero_item_str,
                codigo_item,
                tipo_item,
                descricao_item,
                quantidade,
                valor_unit,
                valor_total,
                maximo_adesao,
                classificacao_fornecedor,
                ni_fornecedor,
                nome_fornecedor,
                situacao_sicaf,
                codigo_pdm,
                nome_pdm,
                numero_compra,
                ano_compra,
                codigo_modalidade,
                id_compra,
                numero_controle_compra,
                data_hora_inclusao,
                data_hora_atualizacao,
                data_hora_exclusao,
                item_excluido,
                item_sha,
                json.dumps(item, ensure_ascii=False),
            ),
        )

    return 1


def main():
    print("[ARP_ITEM] START", flush=True)
    print(f"[ARP_ITEM] BASE_URL={BASE_URL}", flush=True)
    print(f"[ARP_ITEM] ITEM_PATH={ITEM_PATH}", flush=True)

    session = make_session()
    url = BASE_URL + ITEM_PATH

    conn = get_conn()
    conn.autocommit = False

    total_calls = 0
    total_items = 0
    total_raw = 0

    try:
        targets = select_arp_targets(conn, ARP_ITEM_BATCH_LIMIT)
        print(f"[ARP_ITEM] targets={len(targets)} (limit={ARP_ITEM_BATCH_LIMIT})", flush=True)

        for (ug, _numero_ata, numero_controle_ata) in targets:
            params = {"numeroControlePncpAta": numero_controle_ata}

            r = get_with_retry(session, url, params)
            if r.status_code == 404:
                raise RuntimeError(
                    "404 no endpoint de itens. Confirme se o Request URL do Swagger bate com o ITEM_PATH. "
                    f"url={r.url}"
                )

            r.raise_for_status()
            data = r.json()

            insert_raw(conn, RAW_ENDPOINT_NAME, params, data, sha256_json(data))
            total_raw += 1

            items = extract_items(data)
            print(f"[ARP_ITEM] controle={numero_controle_ata} items={len(items)}", flush=True)

            for item in items:
                total_items += upsert_item(conn, ug, numero_controle_ata, item)

            conn.commit()
            total_calls += 1
            time.sleep(SLEEP_BETWEEN_CALLS_S)

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
                            "upserts": total_items,
                            "batch_limit": ARP_ITEM_BATCH_LIMIT,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
        conn.commit()

        print(f"[ARP_ITEM] DONE calls={total_calls} raw={total_raw} upserts={total_items}", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
