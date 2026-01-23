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

-- Contratações PNCP 14.133
create table if not exists contratacao_pncp_14133 (
  id bigserial primary key,

  -- identificador/keys (vamos guardar alguns candidates e também o payload completo)
  id_pncp text null,
  numero_controle_pncp text null,
  id_compra text null,

  unidade_orgao_codigo_unidade text not null,  -- "155125" vem como string na API
  codigo_modalidade integer not null,          -- fixo 5 no seu caso (pregão)

  data_publicacao_pncp date null,
  data_atualizacao_pncp timestamptz null,
  contratacao_excluida boolean null,

  -- campos comuns que costumam existir em contratações PNCP (mantém null se não vier)
  orgao_cnpj text null,
  codigo_orgao integer null,
  unidade_orgao_uf_sigla text null,
  unidade_orgao_codigo_ibge integer null,

  objeto text null,
  situacao text null,

  payload_sha256 text not null,
  payload jsonb not null,

  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now()
);

-- Índice único "flexível": vamos criar depois que confirmarmos qual campo é realmente único no payload.
-- Por enquanto, garantimos desempenho por filtros básicos.
create index if not exists idx_contratacao_uo_modalidade_data_pub
  on contratacao_pncp_14133 (unidade_orgao_codigo_unidade, codigo_modalidade, data_publicacao_pncp);

create index if not exists idx_contratacao_data_atualizacao
  on contratacao_pncp_14133 (data_atualizacao_pncp);
