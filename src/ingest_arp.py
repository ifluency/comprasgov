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

# Base URL principal vindo do workflow/env
PRIMARY_BASE_URL = os.getenv("COMPRAS_BASE_URL", "https://dadosabertos.compras.gov.br").strip().rstrip("/")

# Algumas vezes o gateway muda prefixo/roteamento.
# A gente tenta variações seguras SEM depender de você alterar workflow.
BASE_URL_CANDIDATES = [PRIMARY_BASE_URL]

if "dadosabertos.compras.gov.br" in PRIMARY_BASE_URL:
    # Variações comuns quando tem proxy/gateway/route
    BASE_URL_CANDIDATES.extend([
        PRIMARY_BASE_URL + "/api",
        PRIMARY_BASE_URL + "/dadosabertos",
    ])

# Paths do módulo ARP (você já viu que isso pode variar)
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
        "payload":
