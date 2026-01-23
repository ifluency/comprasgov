create index if not exists idx_licitacao_uasg_data_publicacao
  on licitacao (uasg, data_publicacao);

create index if not exists idx_licitacao_dt_alteracao
  on licitacao (dt_alteracao);

create index if not exists idx_licitacao_numero_aviso
  on licitacao (numero_aviso);

create index if not exists idx_licitacao_modalidade
  on licitacao (modalidade);

create index if not exists idx_licitacao_objeto_gin
  on licitacao using gin (to_tsvector('portuguese', coalesce(objeto,'')));
