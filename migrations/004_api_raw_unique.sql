-- Necessário para permitir: ON CONFLICT (endpoint, payload_sha256) no api_raw

create unique index if not exists ux_api_raw_endpoint_payload_sha256
  on api_raw (endpoint, payload_sha256);
