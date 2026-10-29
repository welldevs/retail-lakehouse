-- PASS-THROUGH of the seed to object storage — same reason/mechanism as
-- warehouse_province_map.sql (query.py only scans parquet under silver/, never a native
-- table of the local .duckdb).
--
-- warehouse_province_map = WHERE the warehouse is (1 municipality per wh).
-- warehouse_service_area = WHICH municipalities are in the same Functional Urban Area (INE)
-- as the warehouse — N municipalities per wh, including the home municipality itself
-- (is_home_municipality = true). Derived and cross-validated against the real Callejero in
-- scripts/derive_warehouse_service_area.py — see the docstring there for the source (INE
-- FUA) and the known limitation (border municipalities of Madrid/Barcelona outside
-- provinces 08/28/41/46 are not included, because they were not downloaded).
--
-- It does not belong in mercadona/ nor ine_population/ nor ine_callejero/: it is a
-- reference table (dimension) orthogonal to the sources, so it sits directly under silver/,
-- like warehouse_province_map.
{{ config(
    location = 's3://retail-lakehouse/silver/warehouse_service_area.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('warehouse_service_area_seed') }}
