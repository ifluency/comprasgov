-- Cria a tabela de itens das contratações PNCP 14.133, consultados por idCompra.
-- Fonte: /modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id

create table if not exists contratacao_item_pncp_14133 (
  -- IDs
  id_compra text not null,
  id_compra_item text primary key,
  id_contratacao_pncp text null,
  numero_controle_pncp_compra text null,

  -- Contexto órgão/unidade
  unidade_orgao_codigo_unidade text null,
  orgao_entidade_cnpj text null,

  -- Identificação do item
  numero_item_pncp integer null,
  numero_item_compra integer null,
  numero_grupo integer null,

  -- Descrições / natureza
  descricao_resumida text null,
  descricao_detalhada text null,
  material_ou_servico text null,
  material_ou_servico_nome text null,

  -- Códigos catálogo/classificação
  codigo_classe integer null,
  codigo_grupo integer null,
  cod_item_catalogo integer null,

  -- Unidade / flags
  unidade_medida text null,
  orcamento_sigiloso boolean null,

  -- Categoria / julgamento / situação
  item_categoria_id_pncp integer null,
  item_categoria_nome text null,
  criterio_julgamento_id_pncp integer null,
  criterio_julgamento_nome text null,
  situacao_compra_item text null,
  situacao_compra_item_nome text null,
  tipo_beneficio text null,
  tipo_beneficio_nome text null,
  incentivo_produtivo_basico boolean null,

  -- Quantidades e valores (estimado / resultado)
  quantidade numeric null,
  valor_unitario_estimado numeric null,
  valor_total numeric null,
  tem_resultado boolean null,
  cod_fornecedor text null,
  nome_fornecedor text null,
  quantidade_resultado numeric null,
  valor_unitario_resultado numeric null,
  valor_total_resultado numeric null,

  -- Datas da API (strings/timestamp conforme retorno)
  data_inclusao_pncp timestamptz null,
  data_atualizacao_pncp timestamptz null,
  data_resultado timestamptz null,

  -- Margens / NCM
  margem_preferencia_normal boolean null,
  percentual_margem_preferencia_normal numeric null,
  margem_preferencia_adicional boolean null,
  percentual_margem_preferencia_adicional numeric null,
  codigo_ncm text null,
  descricao_ncm text null,

  -- Payload cru (para auditoria / reprocesso)
  raw_json jsonb not null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_contratacao_item_pncp_14133_id_compra
  on contratacao_item_pncp_14133 (id_compra);

create index if not exists idx_contratacao_item_pncp_14133_numero_controle
  on contratacao_item_pncp_14133 (numero_controle_pncp_compra);

create index if not exists idx_contratacao_item_pncp_14133_data_atualizacao
  on contratacao_item_pncp_14133 (data_atualizacao_pncp);
