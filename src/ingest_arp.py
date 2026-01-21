import os
import json
import time
import hashlib
import datetime as dt
import requests
from db import get_conn

BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").rstrip("/")
ENDPOINT = "/modulo-arp/1_consultarARP"

UG = int(os.getenv("COMPRAS_UG", "155125"))
PAGE_SIZE = int(os.getenv("COMPRAS_PAGE_SIZE", "500"))
TIMEOUT = int(os.getenv("COMPRAS_TIMEOUT", "60"))
DAILY_LOOKBACK_DAYS = int(os.getenv("DAILY_LOOKBACK_DAYS", "45"))

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

def set_state(conn, name: str, value: dict):
    with conn.cursor() as cur:
        cur.execute("""
            insert into etl_state (name, value, updated_at)
            values (%s, %s::jsonb, now())
            on conflict (name)
            do update set value=excluded.value, updated_at=now()
        """, (name, json.dumps(value, ensure_ascii=False)))

def insert_raw(conn, endpoint, params, payload, payload_sha):
    with conn.cursor() as cur:
        cur.execute("""
            insert into api_raw (endpoint, params, payload, payload_sha256)
            values (%s, %s::jsonb, %s::jsonb, %s)
        """, (
            endpoint,
            json.dumps(params, ensure_ascii=False),
            json.dumps(payload, ensure_ascii=False),
            payload_sha
        ))

def upsert_arp(conn, item: dict, item_sha: str):
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

def fetch_page(session, pagina, params):
    url = f"{BASE_URL}{ENDPOINT}"
    p = dict(params)
    p["pagina"] = pagina
    p["tamanhoPagina"] = PAGE_SIZE
    r = session.get(url, params=p, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def main():
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

    print(f"[ARP] mode={mode} UG={UG} vig={start_fixed}->{today} ass={assinatura_ini}->{today} page_size={PAGE_SIZE}")

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    conn = get_conn()
    conn.autocommit = False

    total = 0
    pagina = 1

    try:
        while True:
            data = fetch_page(session, pagina, params)
            page_sha = sha256_json(data)
            insert_raw(conn, ENDPOINT, {**params, "pagina": pagina, "tamanhoPagina": PAGE_SIZE}, data, page_sha)

            items = None
            if isinstance(data, dict) and isinstance(data.get("resultado"), list):
                items = data["resultado"]
            elif isinstance(data, list):
                items = data

            if not items:
                print(f"[ARP] pagina={pagina} itens=0 -> fim")
                break

            print(f"[ARP] pagina={pagina} itens={len(items)}")
            for item in items:
                total += upsert_arp(conn, item, sha256_json(item))

            conn.commit()
            pagina += 1
            time.sleep(0.2)

        set_state(conn, "arp_last_run", {
            "mode": mode,
            "ended_at_utc": dt.datetime.utcnow().isoformat()
        })
        conn.commit()

        print(f"[ARP] done upserts={total}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
