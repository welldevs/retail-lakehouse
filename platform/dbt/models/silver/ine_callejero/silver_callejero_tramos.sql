-- Street SEGMENTS (tramos) of the Callejero (TRAM file): the only one of INE's 4 geography
-- sources that links, in a single record, census section + entity/nucleus + street OR
-- pseudo-street + POSTAL CODE + numbering range. It is what makes it possible to assemble
-- Street+City+Postcode+Neighbourhood (nucleus, where it exists) in a single query, joining
-- with the other 3 models of this source by code (there is no physical FK in the files,
-- only the codes match).
--
-- GRAIN: (ingestion_date, section_code, entity_suffix, street_id, pseudo_address_id,
-- postal_code, numbering_type, number_from, number_from_qualifier, number_to,
-- number_to_qualifier). There is no single "segment id" column in the bytes — composite
-- grain verified without exception against the 304,952 real rows of the 4 provinces (see
-- assert_callejero_tramos_grain_is_unique.sql).
--
-- Measured layout (the source's CONTRACT.md, section 2): 273-char line. [0:10] SECTION
-- code (same format as SECC) · [13:20] CUN, entity/nucleus suffix (same format as UP) ·
-- [20:25] CVIA, street id (same format as VIAS; "00000" = not applicable) ·
-- [25:30] CPSVIA, pseudo-street id (same format as PSEU; "00000" = not applicable) —
-- CVIA and CPSVIA are mutually exclusive, confirmed without exception in the 4 provinces ·
-- [42:47] CPOS, postal code (5 digits, always with the province's correct prefix,
-- confirmed in 304,905/304,952 rows — 47 exceptions, 0.015%, only in Barcelona) · [47:48]
-- TINUM, numbering type (0=no numbering, 1=odd, 2=even) · [48:52]+[52:53] EIN+CEIN,
-- lower bound of the range + duplicate qualifier · [53:57]+[57:58] ESN+CESN, upper bound
-- + qualifier · [61:69] reference date. Confirmed against the official INE/IDA-Padron
-- "Diseno de registro" (it names the "Tramero" fields in the same order observed in the
-- bytes) — see CONTRACT.md section 6 for the reference.
--
-- street_code/pseudo_address_code are rebuilt (province+municipality+id) for a direct JOIN
-- with silver_callejero_streets/silver_callejero_pseudo_addresses; when the id is
-- "00000" (the "not applicable" sentinel), the rebuilt code simply matches no real
-- street/pseudo-street — no special handling is needed for that.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_callejero_tramos.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw as (

    select
        ingestion_date,
        province                                        as province_code,
        regexp_extract(filename, '([^/]+)$', 1)         as source_file,
        line
    from read_csv(
        '{{ var("callejero_raw_prefix") }}/ingestion_date=*/provinces/province=*/TRAM.*',
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
    'TRAM'                                                  as source_dataset,
    source_file,
    substr(line, 1, 10)                                     as section_code,
    province_code,
    substr(line, 3, 3)                                      as municipality_code,
    substr(line, 6, 2)                                      as district_code,
    substr(line, 8, 3)                                       as section_number,
    substr(line, 14, 7)                                     as entity_suffix,
    province_code || substr(line, 3, 3) || substr(line, 14, 7)  as unit_code,
    substr(line, 21, 5)                                     as street_id,
    province_code || substr(line, 3, 3) || substr(line, 21, 5)  as street_code,
    substr(line, 26, 5)                                     as pseudo_address_id,
    province_code || substr(line, 3, 3) || substr(line, 26, 5)  as pseudo_address_code,
    substr(line, 43, 5)                                     as postal_code,
    substr(line, 48, 1)                                     as numbering_type,
    substr(line, 49, 4)                                     as number_from,
    trim(substr(line, 53, 1))                               as number_from_qualifier,
    substr(line, 54, 4)                                     as number_to,
    trim(substr(line, 58, 1))                               as number_to_qualifier,
    strptime(substr(line, 62, 8), '%Y%m%d')::date          as reference_date

from raw
