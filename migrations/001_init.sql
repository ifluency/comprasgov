-- 001_init.sql
-- Schema limpo para:
-- - schema_migrations
-- - api_raw
-- - contratacao_pncp_14133
-- - contratacao_item_pncp_14133

create table if not exists schema_migrations (
  filename text primary key,
  applied_at timestamptz not null default now()
);

create table if not exists api_raw (
  id bigserial primary key,
  endpoint text not null,
  params jsonb not null default '{}'::jsonb,
  payload jsonb not null,
  payload_sha256 text not null,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create table if not exists contratacao_pncp_14133 (
  id_compra text primary key,

  numero_controle_pncp text,
  codigo_modalidade integer,
  modalidade_nome text,

  unidade_orgao_codigo_unidade text,
  unidade_orgao_nome_unidade text,
  unidade_orgao_uf_sigla text,
  unidade_orgao_municipio_nome text,
  unidade_orgao_codigo_ibge integer,

  orgao_entidade_cnpj text,
  codigo_orgao integer,
  orgao_entidade_razao_social text,

  numero_compra text,
  processo text,

  srp boolean,
  objeto_compra text,

  data_inclusao_pncp timestamptz,
  data_atualizacao_pncp timestamptz,
  data_publicacao_pncp timestamptz,
  data_abertura_proposta_pncp timestamptz,
  data_encerramento_proposta_pncp timestamptz,

  valor_total_estimado numeric,
  valor_total_homologado numeric,

  contratacao_excluida boolean,

  raw_json jsonb not null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists contratacao_item_pncp_14133 (
  id_compra_item text primary key,
  id_compra text not null references contratacao_pncp_14133(id_compra) on delete cascade,

  numero_item_pncp integer,
  numero_item_compra integer,
  numero_grupo integer,

  descricao_resumida text,
  material_ou_servico text,
  material_ou_servico_nome text,

  codigo_classe integer,
  codigo_grupo integer,
  cod_item_catalogo integer,

  unidade_medida text,
  orcamento_sigiloso boolean,

  item_categoria_id_pncp integer,
  item_categoria_nome text,

  criterio_julgamento_id_pncp integer,
  criterio_julgamento_nome text,

  situacao_compra_item text,
  situacao_compra_item_nome text,

  tipo_beneficio text,
  tipo_beneficio_nome text,

  incentivo_produtivo_basico boolean,

  quantidade numeric,
  valor_unitario_estimado numeric,
  valor_total numeric,

  tem_resultado boolean,

  cod_fornecedor text,
  nome_fornecedor text,

  quantidade_resultado numeric,
  valor_unitario_resultado numeric,
  valor_total_resultado numeric,

  data_inclusao_pncp timestamptz,
  data_atualizacao_pncp timestamptz,

  numero_controle_pncp_compra text,

  raw_json jsonb not null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
