create index if not exists idx_arp_ug on arp (codigo_unidade_gerenciadora);
create index if not exists idx_arp_vig_ini on arp (data_vigencia_inicio);
create index if not exists idx_arp_vig_fim on arp (data_vigencia_fim);
create index if not exists idx_arp_status on arp (status_ata);
create index if not exists idx_arp_last_seen on arp (last_seen_at);

create index if not exists idx_raw_collected_at on api_raw (collected_at);
