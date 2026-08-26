-- PASSAGEM do seed para o object storage — mesmo motivo/mecanismo de
-- warehouse_province_map.sql (query.py so escaneia parquet sob silver/, nunca tabela
-- nativa do .duckdb local).
--
-- warehouse_province_map = ONDE o armazem fica (1 municipio por wh).
-- warehouse_service_area = QUAIS municipios estao na mesma Area Urbana Funcional (INE)
-- do armazem — N municipios por wh, incluindo o proprio municipio-sede
-- (is_home_municipality = true). Derivado e cross-validado contra o Callejero real em
-- scripts/derive_warehouse_service_area.py — ver o docstring de la para a fonte (AUF do
-- INE) e a limitacao conhecida (municipios de fronteira de Madrid/Barcelona fora das
-- provincias 08/28/41/46 nao entram, porque nao foram baixados).
--
-- Nao pertence a mercadona/ nem a ine_population/ nem a ine_callejero/: e uma tabela de
-- referencia (dimensao) ortogonal as sources, por isso fica direto sob silver/, igual
-- warehouse_province_map.
{{ config(
    location = 's3://retail-lakehouse/silver/warehouse_service_area.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('warehouse_service_area_seed') }}
