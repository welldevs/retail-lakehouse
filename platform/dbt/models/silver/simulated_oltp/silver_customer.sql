-- FAITHFUL PROJECTION of the synthetic customers. No interpretation beyond typing and
-- marking which ingestion_date is the current one.
--
-- GRAIN: (ingestion_date, customer_id). Not customer_id alone: each Source re-run creates
-- a new ingestion_date, and the same ids coexist across different dates. The `wh` is
-- embedded in the id itself (cust_mad1_000042), so there is no collision between
-- warehouses within a date — CONTRACT.md section 3, guarantee 3.
--
-- THE CUSTOMER IS SYNTHETIC, THE ADDRESS IS NOT. Municipality, street, postcode and
-- numbering range always come from a real Callejero row. This model neither adds nor
-- corrects any of that: what the Source wrote is what comes out of here.
--
-- `wh` PRESERVED, not renamed to `warehouse`. The two tables this model joins with
-- (warehouse_service_area, warehouse_province_map) use `wh`, and the source's vocabulary
-- rules — the same criterion that kept `numbering_type` instead of inventing
-- `address_precision` (ARCHITECTURE.md, "Vocabulary choices: preserve, do not rename").
-- silver_product_price uses `warehouse` because that is what Mercadona calls it there.
--
-- partition_wh EXISTS TO BE RECONCILED. Measured: the record's `wh` field and the hive
-- segment `wh=` have the SAME name, and DuckDB deduplicates both into a single column —
-- the record's value wins and the path's disappears. Without a second column, a partition
-- landed under the wrong directory would go unnoticed because there would be nothing to
-- compare against. Extracting from the filename with a regexp returns the partition axis
-- under its own name; the equality test is in schema.yml. Same reason raw_manifest.sql
-- exposes `warehouse` (hive) and `source_warehouse` (from the manifest) side by side.
--
-- candidate_index is AUDIT, not a foreign key. It points to the exact row of
-- address_candidates.json, which is an ephemeral generator input and is not landed in RAW.
-- It only has meaning against the reference identified in the manifest (see
-- silver_oltp_manifest). Resolving it back to the segment would require re-materializing
-- the reference; the trigger to do that is a mart needing section_code.
--
-- This model assumes at least one landed file (read_json fails on an empty glob).
-- When the source has not landed anything yet, `make silver` excludes it from the build via
-- `retail-platform has-data simulated_oltp` — the same treatment as the other three.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_customer.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        wh,
        regexp_extract(filename, 'wh=([^/]+)', 1)       as partition_wh,
        customer_id,
        province_code,
        province_name,
        municipality_code,
        municipality_name,
        candidate_index,
        street_name,
        postal_code,
        numbering_type,
        house_number,
        first_name,
        last_name,
        sex_label,
        birth_year
    from read_json(
        '{{ var("oltp_raw_prefix") }}/ingestion_date=*/wh=*/customers.json',
        hive_partitioning = 1,
        union_by_name = true,
        filename = true
    )

)

select
    ingestion_date,

    -- A regeneration of the base creates another ingestion_date, and this model stacks them
    -- all (the history is deliberate: it is what lets you watch the base grow). Without an
    -- explicit marker, whoever queries without filtering sums customers from different
    -- generations. `where is_latest_ingestion` is the right way to read the current base —
    -- same convention as the 7 INE reference models.
    ingestion_date = max(ingestion_date) over ()         as is_latest_ingestion,

    wh,
    partition_wh,
    customer_id,

    province_code,
    province_name,
    municipality_code,
    municipality_name,

    candidate_index,
    street_name,
    postal_code,
    numbering_type,
    house_number,

    first_name,
    last_name,
    sex_label,
    birth_year,

    -- AGE IS THE STABLE ATTRIBUTE, birth_year is the derived one. The generator draws an age
    -- and writes `birth_year = year(ingestion_date) - age` (customers_generator.py:189),
    -- so this subtraction returns EXACTLY the sampled age, with no approximation. It matters
    -- because it is what survives a regeneration on another date: the person keeps the age
    -- and changes the birth year (CONTRACT.md section 3).
    year(ingestion_date) - birth_year                    as age_at_ingestion

from raw
