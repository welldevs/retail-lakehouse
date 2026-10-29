-- CENSUS SECTIONS of the Callejero (SECC file), projected with no interpretation beyond
-- decomposing the code, which the source concatenates without a separator.
--
-- GRAIN: (ingestion_date, section_code). section_code (10 digits) is already unique within
-- an ingestion_date -- it embeds province+municipality+district+section, so there is no
-- collision between different provinces.
--
-- SECC has no field other than the code -- measured against the 4 real files (the
-- source's CONTRACT.md, section 2). Line width is 11 (10 digits + 1 trailing space), not
-- 10 -- the space is part of the line, not of the code.
--
-- The file is fixed-width text in ISO-8859-1 (Latin-1) with CRLF, not JSON: each whole
-- line is read as a single column (a delimiter that never appears in the data) and fields
-- are extracted with substr(), the same technique silver_ine_population_series uses for
-- "Nombre" -- here applied to byte offsets, not to segments separated by ". ".
{{ config(
    location = 's3://retail-lakehouse/silver/silver_callejero_sections.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        province                                        as province_code,
        regexp_extract(filename, '([^/]+)$', 1)         as source_file,
        trim(line)                                       as section_code
    from read_csv(
        '{{ var("callejero_raw_prefix") }}/ingestion_date=*/provinces/province=*/SECC.*',
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
    'SECC'                            as source_dataset,
    source_file,
    section_code,
    province_code,
    substr(section_code, 3, 3)        as municipality_code,
    substr(section_code, 6, 2)        as district_code,
    substr(section_code, 8, 3)        as section_number

from raw
