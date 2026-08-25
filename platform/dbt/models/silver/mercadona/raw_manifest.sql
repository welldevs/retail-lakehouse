-- O MANIFESTO COMO FONTE MODELADA.
--
-- Existe para que "o Silver perdeu linha?" seja um TESTE, e nao um script solto: os
-- totais que a Source declarou ficam consultaveis ao lado dos dados derivados. Ver
-- schema.yml, testes de reconciliacao.
--
-- CONTRACT.md secao 1: uma lista ausente equivale a lista vazia. A particao de
-- 2026-08-15 foi gravada antes de anomalies[] existir, e por isso union_by_name e
-- obrigatorio aqui — sem ele, o schema do primeiro arquivo lido ditaria o de todos.
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
