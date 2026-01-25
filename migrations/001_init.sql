create index if not exists idx_contratacao_unidade_modalidade_pub
  on contratacao_pncp_14133 (unidade_orgao_codigo_unidade, codigo_modalidade, data_publicacao_pncp);

create index if not exists idx_contratacao_data_atualizacao
  on contratacao_pncp_14133 (data_atualizacao_pncp);

create index if not exists idx_contratacao_id_compra
  on contratacao_pncp_14133 (id_compra);

create index if not exists idx_contratacao_numero_compra
  on contratacao_pncp_14133 (numero_compra);

create index if not exists idx_contratacao_processo
  on contratacao_pncp_14133 (processo);

create index if not exists idx_contratacao_objeto_gin
  on contratacao_pncp_14133 using gin (to_tsvector('portuguese', coalesce(objeto_compra,'')));
