create table if not exists api_raw (
  id bigserial primary key,
  endpoint text not null,
  params jsonb not null,
  payload jsonb not null,
  payload_sha256 text not null,
  fetched_at timestamptz not null default now()
);

create index if not exists idx_api_raw_endpoint on api_raw (endpoint);
create index if not exists idx_api_raw_fetched_at on api_raw (fetched_at);

create table if not exists etl_state (
  name text primary key,
  value jsonb not null,
  updated_at timestamptz not null default now()
);

-- Contratações PNCP 14.133 (endpoint: modulo-contratacoes/1_consultarContratacoes_PNCP_14133)
create table if not exists contratacao_pncp_14133 (
  id bigserial primary key,

  -- chave natural (estável no payload real)
  numero_controle_pncp text not null unique,

  -- compra / PNCP
  id_compra text null,
  ano_compra_pncp integer null,
  sequencial_compra_pncp integer null,

  -- órgão / unidade
  orgao_entidade_cnpj text null,
  orgao_subrogado_cnpj text null,
  codigo_orgao integer null,
  orgao_entidade_razao_social text null,
  orgao_subrogado_razao_social text null,
  orgao_entidade_esfera_id text null,
  orgao_subrogado_esfera_id text null,
  orgao_entidade_poder_id text null,
  orgao_subrogado_poder_id text null,

  unidade_orgao_codigo_unidade text not null,
  unidade_subrogada_codigo_unidade text null,
  unidade_orgao_nome_unidade text null,
  unidade_subrogada_nome_unidade text null,
  unidade_orgao_uf_sigla text null,
  unidade_subrogada_uf_sigla text null,
  unidade_orgao_municipio_nome text null,
  unidade_subrogada_municipio_nome text null,
  unidade_orgao_codigo_ibge integer null,
  unidade_subrogada_codigo_ibge integer null,

  -- compra (campos usuais)
  numero_compra text null,
  modalidade_id_pncp integer null,
  codigo_modalidade integer not null,
  modalidade_nome text null,
  srp boolean null,

  modo_disputa_id_pncp integer null,
  codigo_modo_disputa integer null,
  modo_disputa_nome text null,

  amparo_legal_codigo_pncp integer null,
  amparo_legal_nome text null,
  amparo_legal_descricao text null,

  processo text null,
  objeto_compra text null,
  informacao_complementar text null,

  existe_resultado boolean null,

  orcamento_sigiloso_codigo integer null,
  orcamento_sigiloso_descricao text null,

  situacao_compra_id_pncp integer null,
  situacao_compra_nome_pncp text null,

  tipo_instrumento_convocatorio_codigo_pncp integer null,
  tipo_instrumento_convocatorio_nome text null,

  valor_total_estimado numeric(18,2) null,
  valor_total_homologado numeric(18,2) null,

  data_inclusao_pncp timestamptz null,
  data_atualizacao_pncp timestamptz null,
  data_publicacao_pncp timestamptz null,
  data_abertura_proposta_pncp timestamptz null,
  data_encerramento_proposta_pncp timestamptz null,

  contratacao_excluida boolean null,

  payload_sha256 text not null,
  payload jsonb not null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now()
);
