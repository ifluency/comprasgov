-- RAW (guarda JSON cru)
create table if not exists api_raw (
  id             bigserial primary key,
  endpoint       text not null,
  params         jsonb not null,
  collected_at   timestamptz not null default now(),
  payload        jsonb not null,
  payload_sha256 text not null
);

create index if not exists idx_api_raw_endpoint_collected_at
  on api_raw (endpoint, collected_at desc);

-- Estado do ETL (para incremental/híbrido)
create table if not exists etl_state (
  name text primary key,
  value jsonb not null,
  updated_at timestamptz not null default now()
);

-- Tabela normalizada ARP
create table if not exists arp (
  id bigserial primary key,

  codigo_unidade_gerenciadora integer not null,
  numero_ata_registro_preco   text not null,

  codigo_orgao              integer null,
  nome_orgao                text null,
  nome_unidade_gerenciadora text null,

  codigo_modalidade_compra  text null,
  nome_modalidade_compra    text null,

  numero_compra             text null,
  ano_compra                text null,

  status_ata                text null,
  objeto                    text null,

  data_assinatura           date null,
  data_vigencia_inicio      date null,
  data_vigencia_fim         date null,

  valor_total               numeric(18,2) null,
  quantidade_itens          integer null,

  link_ata_pncp             text null,
  link_compra_pncp          text null,
  numero_controle_pncp_ata  text null,
  numero_controle_pncp_compra text null,
  id_compra                 text null,

  data_hora_atualizacao     timestamptz null,
  data_hora_inclusao        timestamptz null,
  data_hora_exclusao        timestamptz null,
  ata_excluido              boolean null,

  first_seen_at             timestamptz not null default now(),
  last_seen_at              timestamptz not null default now(),
  payload_sha256            text not null,
  payload                   jsonb not null,

  constraint uq_arp unique (codigo_unidade_gerenciadora, numero_ata_registro_preco)
);

create index if not exists idx_arp_last_seen_at on arp (last_seen_at desc);
create index if not exists idx_arp_data_assinatura on arp (data_assinatura);
create index if not exists idx_arp_data_vigencia_fim on arp (data_vigencia_fim);
