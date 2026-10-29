-- STREETS of the Callejero (VIAS file), projected with no interpretation beyond
-- decomposing the code and extracting the name/type fields.
--
-- GRAIN: (ingestion_date, street_code). street_code (10 digits) is already unique within
-- an ingestion_date.
--
-- Measured layout (the source's CONTRACT.md, section 2): 132-char line, code at [0:10]
-- (province 2 + municipality 3 + street id 5), short name at [10:38], date at [38:46],
-- street type (CALLE/PLAZA/CMNO/PRAJE...) at [52:57], full name at [57:107], alternative
-- short name at [107:132]. Validated against real addresses: "Alcala" in municipality
-- 079 (Madrid), "Gran Via" in 3 different municipalities.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_callejero_streets.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        province                                        as province_code,
        regexp_extract(filename, '([^/]+)$', 1)         as source_file,
        line
    from read_csv(
        '{{ var("callejero_raw_prefix") }}/ingestion_date=*/provinces/province=*/VIAS.*',
        columns = {'line': 'VARCHAR'},
        delim = E'\x01',
        header = false,
        encoding = 'latin-1',
        quote = '',
        hive_partitioning = 1,
        filename = true
    )

)

select
    ingestion_date,
    -- A re-extraction of the SAME INE publication creates another ingestion_date with
    -- equivalent rows, and this model stacks them all (the history is deliberate). Without an
    -- explicit marker, whoever queries without filtering double-counts — it already happened
    -- with silver_ine_population_series, which ended up with 1,547,496 rows in EACH of two
    -- ingestion_dates. `where is_latest_ingestion` is the right way to read the current
    -- state; without the filter, the whole history is read, and that becomes a choice rather
    -- than an accident.
    ingestion_date = max(ingestion_date) over () as is_latest_ingestion,
    'VIAS'                                         as source_dataset,
    source_file,
    substr(line, 1, 10)                            as street_code,
    province_code,
    substr(line, 3, 3)                              as municipality_code,
    substr(line, 6, 5)                              as street_id,
    strptime(substr(line, 39, 8), '%Y%m%d')::date  as reference_date,
    trim(substr(line, 53, 5))                       as street_type,
    trim(substr(line, 11, 28))                      as street_name_short,
    trim(substr(line, 58, 50))                      as street_name_full,
    trim(substr(line, 108, 25))                     as street_name_alt

from raw
