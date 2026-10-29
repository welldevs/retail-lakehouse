-- PSEUDO-STREETS of the Callejero (PSEU file): addressable points outside a conventional
-- street -- grouped mailboxes ("BUZON..."), car parks, named building complexes. It is
-- not an auxiliary of VIAS: they are their own identities and codes, with no 1:1
-- correspondence with any street -- which is why it becomes its own Silver model, not
-- just an extra column in silver_callejero_streets.
--
-- GRAIN: (ingestion_date, pseudo_address_code). The code (10 digits) is already unique
-- within an ingestion_date.
--
-- Measured layout (the source's CONTRACT.md, section 2): 127-char line, code at [0:10]
-- (province 2 + municipality 3 + id 5), name at [10:63], date at [63:71], full name
-- at [77:127]. No street-type field -- pseudo-streets are not classified by type.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_callejero_pseudo_addresses.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        province                                        as province_code,
        regexp_extract(filename, '([^/]+)$', 1)         as source_file,
        line
    from read_csv(
        '{{ var("callejero_raw_prefix") }}/ingestion_date=*/provinces/province=*/PSEU.*',
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
    'PSEU'                                          as source_dataset,
    source_file,
    substr(line, 1, 10)                             as pseudo_address_code,
    province_code,
    substr(line, 3, 3)                               as municipality_code,
    substr(line, 6, 5)                               as pseudo_address_id,
    strptime(substr(line, 64, 8), '%Y%m%d')::date   as reference_date,
    trim(substr(line, 11, 53))                       as name_short,
    trim(substr(line, 78, 50))                       as name_full

from raw
