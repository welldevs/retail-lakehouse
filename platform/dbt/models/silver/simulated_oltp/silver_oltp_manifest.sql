-- THE OLTP SOURCE'S MANIFEST AS A MODELED SOURCE. Sibling of raw_manifest.sql, with the
-- same purpose: to make "did Silver lose a row?" a TEST, and not a loose script.
--
-- GRAIN: (ingestion_date, wh) — one manifest per partition.
--
-- Here it carries one more thing than Mercadona's: the LINEAGE OF THE INPUTS. This source
-- is derived — it consumes the Silver the other three produced — so "which Callejero and
-- which population did these customers come from" is a legitimate question and the answer
-- only exists in the manifest. The `reference` block is the only place where the three
-- input ingestion_dates are recorded.
--
-- previous_runs counts the `history`, which the Source accumulates on each --overwrite by
-- keeping the seed and count of each previous run (partition.py:130-153). Without it, a
-- regeneration with another seed would swap the people behind the same customer_id
-- without leaving a trace. With it, "this base was regenerated 3 times" is queryable.
--
-- Since Phase 6 the REGISTRATION scope travels along: minimum age, penetration rate and
-- allocation rule. All three have the same nature as the seed — they swap the people behind
-- the same ids — and without them two partitions with the same count would be
-- indistinguishable. `customer_target` next to `requested_count` answers "does the base
-- have the size the population called for?" without redoing the math.
--
-- CONTRACT.md section 1: a missing list is equivalent to an empty list — hence the
-- coalesce before len(), same pattern as raw_manifest.sql.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_oltp_manifest.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select
    ingestion_date,
    wh,
    run_id,
    complete,

    source.name                             as source_name,
    source.wh                               as source_warehouse,
    source.generator_version                as generator_version,
    manifest_version,

    started_at_utc,
    finished_at_utc,
    duration_seconds,

    -- What reproduces this partition exactly. seed + reference + ingestion_date + registration
    -- scope are the FOUR conditions of the reproducibility guarantee (CONTRACT.md section 3).
    config.seed                             as seed,
    config.count                            as requested_count,
    -- 'reference' when the size came from the population, 'cli' when someone passed --count.
    -- Without this column, a base generated with a manual override would be indistinguishable
    -- from one derived from the population, and the only way to find out would be to redo the
    -- math.
    config.count_source                     as count_source,
    config.reference                        as reference_path,

    -- Lineage of the inputs: which Silver these customers were derived from.
    reference.callejero_ingestion_date       as callejero_ingestion_date,
    reference.population_ingestion_date      as population_ingestion_date,
    reference.population_series_ingestion_date as population_series_ingestion_date,
    reference.address_candidates             as reference_address_candidates,
    reference.orphan_tramos_excluded         as reference_orphan_tramos_excluded,
    reference.population_year                as population_year,
    reference.population_reference_date      as population_reference_date,
    reference.age_year                       as age_year,
    reference.age_fk_periodo                 as age_fk_periodo,
    reference.age_reference_date             as age_reference_date,

    -- THE REGISTRATION SCOPE. The first three swap the people behind the same customer_id
    -- as much as the seed does, which is why they travel next to it. Without them, two
    -- partitions with the same count and the same look would be indistinguishable.
    reference.min_customer_age               as min_customer_age,
    reference.penetration_pct                as penetration_pct,
    reference.allocation_rule                as allocation_rule,
    reference.penetration_source             as penetration_source,
    reference.customer_target                as customer_target,

    totals.customer_rows                    as declared_customer_rows,
    totals.municipalities_used              as declared_municipalities_used,
    totals.house_number_null                as declared_house_number_null,
    totals.pseudo_address_rows              as declared_pseudo_address_rows,
    totals.bytes                            as declared_bytes,

    schema_fingerprint.sha256               as schema_fingerprint,
    len(coalesce(files, []))                as declared_files,
    len(coalesce(failures, []))             as failure_count,
    len(coalesce(anomalies, []))            as anomaly_count,
    len(coalesce(history, []))              as previous_runs

from read_json(
    '{{ var("oltp_raw_prefix") }}/ingestion_date=*/wh=*/_manifest.json',
    hive_partitioning = 1,
    union_by_name = true
)
