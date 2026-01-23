-- Normalização de campos do payload real de ARP Item

alter table arp_item
  add column if not exists numero_item_str text,
  add column if not exists codigo_item integer,
  add column if not exists tipo_item text,

  add column if not exists quantidade_homologada_item numeric(18,4),
  add column if not exists quantidade_homologada_vencedor numeric(18,4),
  add column if not exists maximo_adesao numeric(18,4),

  add column if not exists classificacao_fornecedor text,
  add column if not exists ni_fornecedor text,
  add column if not exists nome_fornecedor text,
  add column if not exists situacao_sicaf text,

  add column if not exists codigo_pdm integer,
  add column if not exists nome_pdm text,

  add column if not exists numero_compra text,
  add column if not exists ano_compra text,
  add column if not exists codigo_modalidade_compra text,

  add column if not exists id_compra text,
  add column if not exists numero_controle_pncp_compra text,

  add column if not exists data_hora_inclusao timestamptz,
  add column if not exists data_hora_atualizacao timestamptz,
  add column if not exists data_hora_exclusao timestamptz,
  add column if not exists item_excluido boolean;

-- Índices úteis
create index if not exists idx_arp_item_numero_item_str
  on arp_item (numero_item_str);

create index if not exists idx_arp_item_codigo_item
  on arp_item (codigo_item);

create index if not exists idx_arp_item_ni_fornecedor
  on arp_item (ni_fornecedor);

create index if not exists idx_arp_item_nome_fornecedor
  on arp_item (nome_fornecedor);

create index if not exists idx_arp_item_id_compra
  on arp_item (id_compra);

create index if not exists idx_arp_item_controle_compra
  on arp_item (numero_controle_pncp_compra);

create index if not exists idx_arp_item_data_hora_atualizacao
  on arp_item (data_hora_atualizacao);
