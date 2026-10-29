-- THE MANIFEST AS A MODELED SOURCE.
--
-- It exists so that "did Silver lose a row?" is a TEST, and not a loose script: the totals
-- the Source declared become queryable next to the derived data. See schema.yml,
-- reconciliation tests.
--
-- CONTRACT.md section 1: a missing list is equivalent to an empty list. The 2026-08-15
-- partition was written before anomalies[] existed, which is why union_by_name is
-- mandatory here — without it, the schema of the first file read would dictate everyone's.
{{ config(
    location = 's3://retail-lakehouse/silver/raw_manifest.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select
    ingestion_date,
    wh                                  as warehouse,
    run_id,
    complete,
    source.name                         as source_name,
    source.lang                         as source_lang,
    source.wh                           as source_warehouse,
    manifest_version,
    started_at_utc,
    finished_at_utc,
    duration_seconds,
    totals.product_rows                 as declared_product_rows,
    totals.unique_product_ids           as declared_unique_product_ids,
    totals.catalog_files                as declared_catalog_files,
    totals.categories_level_1           as declared_categories_level_1,
    totals.categories_level_2           as declared_categories_level_2,
    totals.http_requests                as http_requests,
    totals.http_retries                 as http_retries,
    totals.bytes                        as declared_bytes,
    schema_fingerprint.sha256           as schema_fingerprint,
    len(coalesce(files, []))            as declared_files,
    len(coalesce(failures, []))         as failure_count,
    len(coalesce(anomalies, []))        as anomaly_count

from read_json(
    '{{ var("raw_prefix") }}/ingestion_date=*/wh=*/_manifest.json',
    hive_partitioning = 1,
    union_by_name = true
)
