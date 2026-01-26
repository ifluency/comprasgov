-- 002_indexes.sql

-- api_raw: necessário pro ON CONFLICT (endpoint, payload_sha256)
create unique index if not exists ux_api_raw_endpoint_sha
on api_raw(endpoint, payload_sha256);

create index if not exists ix_api_raw_endpoint
on api_raw(endpoint);

create index if not exists ix_api_raw_fetched_at
on api_raw(fetched_at);

-- contratacao_pncp_14133
create unique index if not exists ux_contratacao_numero_controle
on contratacao_pncp_14133(numero_controle_pncp)
where numero_controle_pncp is not null;

create index if not exists ix_contratacao_codigo_modalidade
on contratacao_pncp_14133(codigo_modalidade);

create index if not exists ix_contratacao_data_publicacao
on contratacao_pncp_14133(data_publicacao_pncp);

create index if not exists ix_contratacao_unidade
on contratacao_pncp_14133(unidade_orgao_codigo_unidade);

-- contratacao_item_pncp_14133
create index if not exists ix_item_id_compra
on contratacao_item_pncp_14133(id_compra);

create index if not exists ix_item_cod_item_catalogo
on contratacao_item_pncp_14133(cod_item_catalogo);

create index if not exists ix_item_cod_fornecedor
on contratacao_item_pncp_14133(cod_fornecedor);
