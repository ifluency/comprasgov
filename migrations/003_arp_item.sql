-- Itens da ARP (coletados via numeroControlePncpAta)

create table if not exists arp_item (
  id bigserial primary key,

  codigo_unidade_gerenciadora integer not null,
  numero_ata_registro_preco text null,
  numero_controle_pncp_ata text not null,

  -- identificadores do item (podem variar conforme payload)
  item_id text null,
  numero_item integer null,

  descricao text null,
  unidade text null,
  quantidade numeric(18,4) null,
  valor_unitario numeric(18,2) null,
  valor_total numeric(18,2) null,

  catmat text null,
  catsrv text null,

  payload_sha256 text not null,
  payload jsonb not null,

  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now()
);

-- ✅ UNIQUE por expressão precisa ser INDEX (não constraint)
create unique index if not exists uq_arp_item_expr
  on arp_item (
    codigo_unidade_gerenciadora,
    numero_controle_pncp_ata,
    coalesce(item_id,''),
    coalesce(numero_item,-1)
  );

create index if not exists idx_arp_item_controle
  on arp_item (numero_controle_pncp_ata);

create index if not exists idx_arp_item_num_ata
  on arp_item (numero_ata_registro_preco);

create index if not exists idx_arp_item_last_seen
  on arp_item (last_seen_at);

-- busca textual (opcional, mas ajuda muito)
create index if not exists idx_arp_item_desc
  on arp_item using gin (to_tsvector('portuguese', coalesce(descricao,'')));
