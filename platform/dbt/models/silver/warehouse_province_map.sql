-- PASS-THROUGH of the seed to object storage. The seed (platform/dbt/seeds/) is the
-- VERSIONED source of truth — but `retail_platform query`/`connect_lakehouse()` never reads
-- a native table of the local .duckdb, it only scans parquet under s3://.../silver/ (see
-- query.py). Without this model, the seed loads fine in `dbt build` but stays invisible
-- to any query made the way the rest of the platform queries Silver — measured:
-- `make query SQL="select * from warehouse_province_map"` gave "table does not exist"
-- even with the seed loaded, because the table only existed LOCALLY, never in the
-- bucket. This model is just a "select *" of the seed, materialized like the other 5
-- Silver models, so it is visible the same way they are.
--
-- It does not belong in mercadona/ nor in ine/: it is a reference table (dimension),
-- orthogonal to both sources, so it sits directly under silver/ like raw_manifest.sql
-- did before it became Mercadona-specific.
{{ config(
    location = 's3://retail-lakehouse/silver/warehouse_province_map.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('warehouse_province_map_seed') }}
