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

-- Tabela normalizada: licitação (módulo-legado)
create table if not exists licitacao (
  id bigserial primary key,

  -- chave natural recomendada pela API
  id_compra text not null unique,

  identificador text null,
  numero_processo text null,

  uasg integer not null,
  modalidade integer null,
  nome_modalidade text null,
  numero_aviso integer null,

  situacao_aviso text null,
  tipo_pregao text null,
  tipo_recurso text null,

  nome_responsavel text null,
  funcao_responsavel text null,

  numero_itens integer null,
  valor_estimado_total numeric(18,2) null,
  valor_homologado_total numeric(18,2) null,

  informacoes_gerais text null,
  objeto text null,
  endereco_entrega_edital text null,

  codigo_municipio_uasg integer null,

  data_abertura_proposta date null,
  data_entrega_edital date null,
  data_entrega_proposta date null,
  data_publicacao date null,

  dt_alteracao timestamptz null,
  pertence14133 boolean null,

  payload_sha256 text not null,
  payload jsonb not null,

  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now()
);
