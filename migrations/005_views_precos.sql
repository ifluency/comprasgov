-- 005_views_precos.sql
--
-- Objetivo:
-- 1) Persistir também os valores *estimados* e *de resultado* (unitário e total)
--    na tabela de itens (contratacao_item_pncp_14133).
-- 2) Backfill (preencher retroativamente) a partir do raw_json já armazenado.
-- 3) Criar 2 views para consumo pelo app (por CATMAT = cod_item_catalogo).

-- =========================================================
-- 1) Novas colunas (sem quebrar cargas já existentes)
-- =========================================================
ALTER TABLE IF EXISTS contratacao_item_pncp_14133
  ADD COLUMN IF NOT EXISTS quantidade                numeric,
  ADD COLUMN IF NOT EXISTS valor_unitario_estimado    numeric,
  ADD COLUMN IF NOT EXISTS valor_total_estimado       numeric,
  ADD COLUMN IF NOT EXISTS quantidade_resultado       numeric,
  ADD COLUMN IF NOT EXISTS valor_unitario_resultado   numeric,
  ADD COLUMN IF NOT EXISTS data_resultado             timestamptz;

-- =========================================================
-- 2) Backfill (puxa do raw_json para colunas) para o histórico já ingerido
-- =========================================================
UPDATE contratacao_item_pncp_14133
SET
  quantidade              = COALESCE(quantidade,             NULLIF(raw_json->>'quantidade','')::numeric),
  valor_unitario_estimado = COALESCE(valor_unitario_estimado,NULLIF(raw_json->>'valorUnitarioEstimado','')::numeric),
  valor_total_estimado    = COALESCE(valor_total_estimado,   NULLIF(raw_json->>'valorTotal','')::numeric),
  quantidade_resultado    = COALESCE(quantidade_resultado,   NULLIF(raw_json->>'quantidadeResultado','')::numeric),
  valor_unitario_resultado= COALESCE(valor_unitario_resultado,NULLIF(raw_json->>'valorUnitarioResultado','')::numeric),
  data_resultado          = COALESCE(
    data_resultado,
    CASE
      WHEN raw_json ? 'dataResultado'
       AND raw_json->>'dataResultado' IS NOT NULL
       AND length(raw_json->>'dataResultado') >= 19
      THEN (substring(raw_json->>'dataResultado' from 1 for 19)::timestamp AT TIME ZONE 'UTC')
      ELSE NULL
    END
  )
WHERE
  raw_json IS NOT NULL;

-- =========================================================
-- 3) Índices para consulta por CATMAT e por data
-- =========================================================
CREATE INDEX IF NOT EXISTS ix_item_cod_item_catalogo
  ON contratacao_item_pncp_14133 (cod_item_catalogo);

CREATE INDEX IF NOT EXISTS ix_item_data_resultado
  ON contratacao_item_pncp_14133 (data_resultado);

-- =========================================================
-- 4) Views
--    CATMAT = cod_item_catalogo (inteiro)
-- =========================================================

-- View 1: último preço conhecido por CATMAT
-- Preferência de ordenação:
--   1) data_resultado (quando existir)
--   2) data_atualizacao_pncp
--   3) data_inclusao_pncp
CREATE OR REPLACE VIEW vw_catmat_preco_ultimo AS
SELECT DISTINCT ON (i.cod_item_catalogo)
  i.cod_item_catalogo,
  i.descricao_resumida,
  i.material_ou_servico,
  i.unidade_medida,
  i.id_compra,
  i.numero_controle_pncp_compra,
  c.codigo_modalidade,
  c.data_publicacao_pncp,
  i.data_resultado,
  i.quantidade,
  i.valor_unitario_estimado,
  i.valor_total_estimado,
  i.quantidade_resultado,
  i.valor_unitario_resultado,
  i.valor_total_resultado,
  i.data_atualizacao_pncp,
  i.data_inclusao_pncp
FROM contratacao_item_pncp_14133 i
LEFT JOIN contratacao_pncp_14133 c
  ON c.id_compra = i.id_compra
WHERE i.cod_item_catalogo IS NOT NULL
ORDER BY
  i.cod_item_catalogo,
  i.data_resultado DESC NULLS LAST,
  i.data_atualizacao_pncp DESC NULLS LAST,
  i.data_inclusao_pncp DESC NULLS LAST;

-- View 2: histórico completo por CATMAT (todas as ocorrências)
CREATE OR REPLACE VIEW vw_catmat_preco_historico AS
SELECT
  i.cod_item_catalogo,
  i.descricao_resumida,
  i.material_ou_servico,
  i.unidade_medida,
  i.id_compra,
  i.id_compra_item,
  i.numero_controle_pncp_compra,
  c.codigo_modalidade,
  c.data_publicacao_pncp,
  i.data_resultado,
  i.quantidade,
  i.valor_unitario_estimado,
  i.valor_total_estimado,
  i.quantidade_resultado,
  i.valor_unitario_resultado,
  i.valor_total_resultado,
  i.data_atualizacao_pncp,
  i.data_inclusao_pncp
FROM contratacao_item_pncp_14133 i
LEFT JOIN contratacao_pncp_14133 c
  ON c.id_compra = i.id_compra
WHERE i.cod_item_catalogo IS NOT NULL;
