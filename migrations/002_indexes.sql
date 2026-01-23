create index if not exists idx_contratacao_objeto_gin
  on contratacao_pncp_14133 using gin (to_tsvector('portuguese', coalesce(objeto,'')));

create index if not exists idx_contratacao_codigo_orgao
  on contratacao_pncp_14133 (codigo_orgao);

create index if not exists idx_contratacao_orgao_cnpj
  on contratacao_pncp_14133 (orgao_cnpj);

create index if not exists idx_contratacao_excluida
  on contratacao_pncp_14133 (contratacao_excluida);
