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

ARP_ITEM_BATCH_LIMIT = int(os.getenv("ARP_ITEM_BATCH_LIMIT", "1000"))
SLEEP_BETWEEN_CALLS_S = float(os.getenv("ARP_ITEM_SLEEP_S", "0.15"))

# Se quiser, você pode limitar a recaptura para ARPs vistas nos últimos X dias
ARP_ITEM_ARP_LOOKBACK_DAYS = int(os.getenv("ARP_ITEM_ARP_LOOKBACK_DAYS", "0"))  # 0 = sem filtro


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


def extract_items(data):
    if isinstance(data, dict) and isinstance(data.get("resultado"), list):
        return data["resultado"]
    if isinstance(data, list):
        return data
    return []


def select_arp_targets_pending(conn, limit: int):
    """
    Prioriza:
    1) ARPs que ainda NÃO têm nenhum item carregado
    2) ARPs cuja last_seen_at (arp) é mais recente que o last_seen_at dos itens daquele controle

    Isso evita reprocessar sempre as mesmas.
    """
    lookback_clause = ""
    params = [UG]

    if ARP_ITEM_ARP_LOOKBACK_DAYS and ARP_ITEM_ARP_LOOKBACK_DAYS > 0:
        lookback_clause = "and a.last_seen_at >= now() - (%s || ' days')::interval"
        params.append(str(ARP_ITEM_ARP_LOOKBACK_DAYS))

    params.append(limit)

    sql = f"""
    with arps as (
      select
        a.codigo_unidade_gerenciadora,
        a.last_seen_at as arp_last_seen,
        coalesce(a.numero_ata_registro_preco, a.payload->>'numeroAtaRegistroPreco') as numero_ata_registro_preco,
        a.payload->>'numeroControlePncpAta' as numero_controle_pncp_ata
      from arp a
      where a.codigo_unidade_gerenciadora = %s
        and (a.payload->>'numeroControlePncpAta') is not null
        and (a.payload->>'numeroControlePncpAta') <> ''
        {lookback_clause}
    ),
    items_agg as (
      select
        numero_controle_pncp_ata,
        max(last_seen_at) as items_last_seen,
        count(*) as itens
      from arp_item
      where codigo_unidade_gerenciadora = %s
      group by 1
    )
    select
      arps.codigo_unidade_gerenciadora,
      arps.numero_ata_registro_preco,
      arps.numero_controle_pncp_ata
    from arps
    left join items_agg ia
      on ia.numero_controle_pncp_ata = arps.numero_controle_pncp_ata
    where ia.numero_controle_pncp_ata is null
       or ia.items_last_seen is null
       or arps.arp_last_seen > ia.items_last_seen
    order by
      (ia.numero_controle_pncp_ata is null) desc,
      arps.arp_last_seen desc
    limit %s;
    """

    # params: UG, [lookback], limit
    # mas items_agg também precisa UG
    # então duplicamos UG no meio
    # Se lookback ativo: params = [UG, days, limit] -> inserir UG antes do limit
    # Se lookback não: params = [UG, limit] -> inserir UG antes do limit
    if ARP_ITEM_ARP_LOOKBACK_DAYS and ARP_ITEM_ARP_LOOKBACK_DAYS > 0:
        # [UG, days, limit] -> [UG, days, UG, limit]
        final_params = [params[0], params[1], params[0], params[2]]
    else:
        # [UG, limit] -> [UG, UG, limit]
        final_params = [params[0], params[0], params[1]]

    with conn.cursor() as cur:
        cur.execute(sql, final_params)
        return cur.fetchall()


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

    # chave nova não aceita NULL
    numero_item_str = "" if numero_item_str is None else str(numero_item_str)

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into arp_item (
              codigo_unidade_gerenciadora,
              numero_ata_registro_preco,
              numero_controle_pncp_ata,
              numero_item_str,

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
              %s, %s, %s, %s,
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
    print(f"[ARP_ITEM] BATCH_LIMIT={ARP_ITEM_BATCH_LIMIT} LOOKBACK_DAYS={ARP_ITEM_ARP_LOOKBACK_DAYS}", flush=True)

    session = make_session()
    url = BASE_URL + ITEM_PATH

    conn = get_conn()
    conn.autocommit = False

    total_calls = 0
    total_items = 0
    total_raw = 0

    try:
        targets = select_arp_targets_pending(conn, ARP_ITEM_BATCH_LIMIT)
        print(f"[ARP_ITEM] targets={len(targets)} (pending/updated)", flush=True)

        for (ug, _numero_ata, numero_controle_ata) in targets:
            params = {"numeroControlePncpAta": numero_controle_ata}

            r = get_with_retry(session, url, params)
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
                            "mode": "pending_or_updated",
                            "lookback_days": ARP_ITEM_ARP_LOOKBACK_DAYS,
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
