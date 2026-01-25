-- 003_contratacao_itens.sql
-- Itens das contratações (PNCP 14.133)
-- Endpoint: /modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id?tipo=idCompra&codigo=<idCompra>

CREATE TABLE IF NOT EXISTS contratacao_pncp_14133_item (
  id_compra              TEXT NOT NULL,
  id_compra_item         TEXT NOT NULL,
  data_inclusao_pncp     TIMESTAMPTZ NULL,
  data_atualizacao_pncp  TIMESTAMPTZ NULL,
  fetched_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  raw_json               JSONB NOT NULL,

  CONSTRAINT pk_contratacao_pncp_14133_item PRIMARY KEY (id_compra_item)
);

CREATE INDEX IF NOT EXISTS idx_contratacao_pncp_14133_item_id_compra
  ON contratacao_pncp_14133_item (id_compra);

CREATE INDEX IF NOT EXISTS idx_contratacao_pncp_14133_item_data_atualizacao
  ON contratacao_pncp_14133_item (data_atualizacao_pncp);
