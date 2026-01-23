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

create table if not exists contratacao_pncp_14133 (
  id bigserial primary key,

  id_pncp text not null,
  numero_controle_pncp text null,
  id_compra text null,

  unidade_orgao_codigo_unidade text not null,
  codigo_modalidade integer not null,

  data_publicacao_pncp date null,
  data_atualizacao_pncp timestamptz null,
  contratacao_excluida boolean null,

  orgao_cnpj text null,
  codigo_orgao integer null,
  unidade_orgao_uf_sigla text null,
  unidade_orgao_codigo_ibge integer null,

  objeto text null,
  situacao text null,

  payload_sha256 text not null,
  payload jsonb not null,

  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),

  constraint uq_contratacao_id_pncp unique (id_pncp)
);

create index if not exists idx_contratacao_uo_modalidade_data_pub
  on contratacao_pncp_14133 (unidade_orgao_codigo_unidade, codigo_modalidade, data_publicacao_pncp);

create index if not exists idx_contratacao_data_atualizacao
  on contratacao_pncp_14133 (data_atualizacao_pncp);
