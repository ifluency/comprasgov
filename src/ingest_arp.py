ARP_PATH_CANDIDATES = [
    "/modulo-arp/1_consultarARP",
    "/modulo-arp/1_consultarARP/",   # alguns gateways exigem barra final
    "/modulo_arp/1_consultarARP",    # variação comum em APIs
]

def build_arp_url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + path

def fetch_page(session, pagina: int, params: dict):
    last_exc = None

    # header mais “neutro” (o manual usa accept */*)
    headers = {"accept": "*/*"}

    for path in ARP_PATH_CANDIDATES:
        url = build_arp_url(BASE_URL, path)

        try:
            r = session.get(url, params={**params, "pagina": pagina}, headers=headers, timeout=60)

            # Se não for 404, esse path existe (pode ser 200, 400, 422 etc.)
            if r.status_code != 404:
                r.raise_for_status()
                return r.json()

        except Exception as e:
            last_exc = e
            # se foi 404, tenta o próximo path; se foi outro erro, você pode decidir retry também
            continue

    # Se chegou aqui, todas as variações deram 404
    raise RuntimeError(
        f"ARP endpoint não encontrado (404) para todos os paths testados: {ARP_PATH_CANDIDATES}. "
        f"BASE_URL={BASE_URL}. Último erro: {last_exc}"
    )
