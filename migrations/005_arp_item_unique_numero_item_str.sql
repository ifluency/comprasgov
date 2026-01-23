-- Chave ideal para itens: UG + numeroControlePncpAta + numeroItem (string, preserva zeros)

-- Garante que a coluna existe (caso alguém pule a 004 por engano)
alter table arp_item
  add column if not exists numero_item_str text;

-- Para evitar erro na criação do unique index, removemos duplicados mantendo o mais recente.
-- (isso só roda se existirem duplicados)
with ranked as (
  select
    id,
    row_number() over (
      partition by codigo_unidade_gerenciadora, numero_controle_pncp_ata, coalesce(numero_item_str,'')
      order by last_seen_at desc, id desc
    ) as rn
  from arp_item
)
delete from arp_item
where id in (select id from ranked where rn > 1);

-- Unique index correto (por numero_item_str)
create unique index if not exists uq_arp_item_ug_controle_numitem
  on arp_item (
    codigo_unidade_gerenciadora,
    numero_controle_pncp_ata,
    coalesce(numero_item_str,'')
  );
