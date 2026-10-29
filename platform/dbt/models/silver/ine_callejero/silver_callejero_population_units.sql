-- POPULATION UNITS (nuclei) of the Callejero (UP file), projected with no interpretation
-- beyond decomposing the code and extracting the two useful names.
--
-- GRAIN: (ingestion_date, unit_code). unit_code (12 digits) is already unique within an
-- ingestion_date.
--
-- Measured layout (the source's CONTRACT.md, section 2), NOT officially documented by
-- INE in the download: 604-char line, code at [0:12] (province 2 + municipality 3 +
-- suffix 7; suffix "0000000" is the AGGREGATE row for the whole municipality, the others
-- are nuclei/entities within it), date at [15:23], MUNICIPALITY name at [94:314], name
-- of the POPULATION UNIT (the nucleus -- e.g. "*DISEMINADO*" for scattered population) at
-- [459:529]. There are more repetitions of the same text at other widths (abbreviated
-- forms), not extracted here -- confirmed by comparing the real content, not assumed
-- from positional similarity.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_callejero_population_units.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        province                                        as province_code,
        regexp_extract(filename, '([^/]+)$', 1)         as source_file,
        line
    from read_csv(
        '{{ var("callejero_raw_prefix") }}/ingestion_date=*/provinces/province=*/UP.*',
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
    'UP'                                          as source_dataset,
    source_file,
    substr(line, 1, 12)                           as unit_code,
    province_code,
    substr(line, 3, 3)                             as municipality_code,
    substr(line, 6, 7)                             as entity_suffix,
    substr(line, 6, 7) = '0000000'                 as is_municipality_aggregate,
    strptime(substr(line, 16, 8), '%Y%m%d')::date  as reference_date,
    trim(substr(line, 95, 220))                    as municipality_name,
    trim(substr(line, 460, 70))                    as population_unit_name

from raw
