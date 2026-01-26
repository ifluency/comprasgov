-- 005_item_valores.sql
-- Adiciona colunas para valores estimados e de resultado nos itens de contratações (PNCP 14.133)
-- + backfill a partir do raw_json

BEGIN;

ALTER TABLE contratacao_item_pncp_14133
  ADD COLUMN IF NOT EXISTS quantidade NUMERIC,
  ADD COLUMN IF NOT EXISTS valor_unitario_estimado NUMERIC,
  ADD COLUMN IF NOT EXISTS valor_total_estimado NUMERIC,
  ADD COLUMN IF NOT EXISTS tem_resultado BOOLEAN,
  ADD COLUMN IF NOT EXISTS quantidade_resultado NUMERIC,
  ADD COLUMN IF NOT EXISTS valor_unitario_resultado NUMERIC,
  ADD COLUMN IF NOT EXISTS valor_total_resultado NUMERIC,
  ADD COLUMN IF NOT EXISTS data_resultado TIMESTAMPTZ;

-- Backfill: preencher somente quando estiver NULL, lendo do raw_json
UPDATE contratacao_item_pncp_14133
SET
  quantidade = COALESCE(
    quantidade,
    NULLIF(raw_json->>'quantidade','')::NUMERIC
  ),
  valor_unitario_estimado = COALESCE(
    valor_unitario_estimado,
    NULLIF(raw_json->>'valorUnitarioEstimado','')::NUMERIC
  ),
  valor_total_estimado = COALESCE(
    valor_total_estimado,
    NULLIF(raw_json->>'valorTotal','')::NUMERIC
  ),
  tem_resultado = COALESCE(
    tem_resultado,
    CASE
      WHEN raw_json ? 'temResultado' THEN (raw_json->>'temResultado')::BOOLEAN
      ELSE NULL
    END
  ),
  quantidade_resultado = COALESCE(
    quantidade_resultado,
    NULLIF(raw_json->>'quantidadeResultado','')::NUMERIC
  ),
  valor_unitario_resultado = COALESCE(
    valor_unitario_resultado,
    NULLIF(raw_json->>'valorUnitarioResultado','')::NUMERIC
  ),
  valor_total_resultado = COALESCE(
    valor_total_resultado,
    NULLIF(raw_json->>'valorTotalResultado','')::NUMERIC
  ),
  data_resultado = COALESCE(
    data_resultado,
    CASE
      WHEN (raw_json->>'dataResultado') IS NULL OR (raw_json->>'dataResultado') = '' THEN NULL
      ELSE (raw_json->>'dataResultado')::TIMESTAMPTZ
    END
  )
WHERE raw_json IS NOT NULL;

-- Índices (idempotentes)
CREATE INDEX IF NOT EXISTS idx_contratacao_item_pncp_14133_id_compra
  ON contratacao_item_pncp_14133 (id_compra);

CREATE INDEX IF NOT EXISTS idx_contratacao_item_pncp_14133_numero_controle
  ON contratacao_item_pncp_14133 (numero_controle_pncp_compra);

CREATE INDEX IF NOT EXISTS idx_contratacao_item_pncp_14133_data_atualizacao
  ON contratacao_item_pncp_14133 (data_atualizacao_pncp);

CREATE INDEX IF NOT EXISTS ix_item_cod_fornecedor
  ON contratacao_item_pncp_14133 (cod_fornecedor);

CREATE INDEX IF NOT EXISTS ix_item_cod_item_catalogo
  ON contratacao_item_pncp_14133 (cod_item_catalogo);

COMMIT;
