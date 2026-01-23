-- Chave ideal para itens: UG + numeroControlePncpAta + numeroItem (string)

-- garante coluna
alter table arp_item
  add column if not exists numero_item_str text;

-- normaliza nulls para string vazia (pra permitir NOT NULL)
update arp_item
set numero_item_str = ''
where numero_item_str is null;

-- impõe NOT NULL (necessário pra UNIQUE funcionar de forma previsível)
alter table arp_item
  alter column numero_item_str set not null;

-- remove duplicados na chave (mantém o mais recente)
with ranked as (
  select
    id,
    row_number() over (
      partition by codigo_unidade_gerenciadora, numero_controle_pncp_ata, numero_item_str
      order by last_seen_at desc, id desc
    ) as rn
  from arp_item
)
delete from arp_item
where id in (select id from ranked where rn > 1);

-- ✅ UNIQUE INDEX direto (sem expressão) -> compatível com ON CONFLICT (colunas)
create unique index if not exists uq_arp_item_ug_controle_numitem
  on arp_item (codigo_unidade_gerenciadora, numero_controle_pncp_ata, numero_item_str);
