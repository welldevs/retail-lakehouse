-- POPULATION by MUNICIPALITY from INE (Tempus3, table_id 29005 — "Cifras oficiales del
-- padron por municipio"), projected with no interpretation beyond splitting
-- municipality/sex, which the source concatenates in "Nombre", and resolving the
-- municipality CODE via the ine_municipality_codes_seed seed — table_id=29005 carries no
-- code, only the full name (see the source's CONTRACT.md, section 2, and
-- scripts/derive_municipality_codes.py).
--
-- GRAIN: (ingestion_date, series_code, year, fk_periodo). fk_periodo=28 is the only value
-- measured in the real payload (unlike 31304, which publishes 2 estimates/year —
-- fk_periodo 26 and 27); kept in the grain for symmetry and as a safeguard in case INE
-- starts publishing more than one estimate/year here in the future, not because it was
-- observed.
--
-- MEASURED AGAINST THE REAL PAYLOAD (sample of 4,509 complete series — the unfiltered
-- response has history since 1996, and is large enough to get truncated on the network
-- during this investigation; the sample is the intact prefix up to the cut):
--   * "Nombre" ALWAYS has exactly 4 segments separated by ". ":
--     "<Municipality>. <Sex>. Total habitantes. Personas." — unlike 31304, the
--     ORDER HERE IS FIXED (no exception in the sample), so municipality and sex are
--     extracted by POSITION (split_part 1 and 2), not by a closed vocabulary.
--   * sex is always one of 'Total'/'Hombres'/'Mujeres' — a DIFFERENT vocabulary from
--     31304, which uses 'Ambos sexos' for the same role. Different sources, not
--     normalized here: each model exposes its own source's vocabulary.
--   * Secreto=false in 100% of the sample's points (no statistical-confidentiality
--     suppression observed) — exposed as its own column (is_secret) instead of
--     transformed, because that absence was NOT verified exhaustively (the sample
--     does not cover all ~8,200 municipalities) and there is no observed payload with
--     Secreto=true to know what the source puts in "Valor" in that case.
--
-- Municipality name IS NOT A SAFE KEY: 18 names repeat across different provinces in
-- Spain (see ine_municipality_codes_seed). The seed is scoped to 08/28/41/46 and has NO
-- internal collision — measured — but that alone is NOT enough, and an earlier version of
-- this comment wrongly concluded that it was: the RAW side is NATIONAL (~8,200
-- municipalities, nothing is filtered at extraction), so a seed row matches both the
-- series from our province and the homonymous one from elsewhere. Fanout measured in 3
-- municipalities:
--
--     "Torrent. Total. Total habitantes. Personas. " -> DPOP21778 = 90.928 (46244)
--     "Torrent. Total. Total habitantes. Personas. " -> DPOP7960  =    182 (17197, Girona)
--
-- The grain test (ingestion_date, series_code, year, fk_periodo) did not catch it, because
-- the two series have different series_code — hence the additional test
-- assert_ine_population_by_municipality_has_one_series_per_municipality.
--
-- FIX: `ine_ambiguous_series_seed` carries, for each series with an ambiguous name, the
-- municipality's OFFICIAL CODE (province+municipality) obtained from INE's own API
-- (VALORES_SERIE/{COD} — see scripts/derive_ambiguous_series.py). Ambiguous-name series
-- only pass when the official code matches the seed's; unambiguous names are still
-- resolved by name, as before. It is not a "keep the largest value" heuristic: it is the
-- source saying which series belongs to which municipality.
--
-- The INNER JOIN with the seed remains the deliberate interpretation of scope: this Silver
-- model only resolves codes — and only serves cross-queries with the Callejero — for what
-- this platform actually covers, the same reasoning warehouse_service_area applies.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_ine_population_by_municipality.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with raw_series as (

    select
        ingestion_date,
        "COD"    as series_code,
        "Nombre" as series_name,
        "Data"   as data_points
    from read_json(
        '{{ var("ine_raw_prefix") }}/ingestion_date=*/tables/table_id=29005.json',
        hive_partitioning = 1,
        union_by_name = true
    )

),

segmented as (

    select
        ingestion_date,
        series_code,
        series_name,
        data_points,
        trim(split_part(series_name, '. ', 1)) as municipality_name,
        trim(split_part(series_name, '. ', 2)) as sex_label
    from raw_series

),

exploded as (

    select
        ingestion_date,
        series_code,
        series_name,
        municipality_name,
        sex_label,
        unnest(data_points) as point
    from segmented

)

select
    e.ingestion_date,
    -- A re-extraction of the SAME INE publication creates another ingestion_date with
    -- equivalent rows, and this model stacks them all (the history is deliberate). Without an
    -- explicit marker, whoever queries without filtering double-counts — it already happened
    -- with silver_ine_population_series, which ended up with 1,547,496 rows in EACH of two
    -- ingestion_dates. `where is_latest_ingestion` is the right way to read the current
    -- state; without the filter, the whole history is read, and that becomes a choice rather
    -- than an accident.
    e.ingestion_date = max(e.ingestion_date) over () as is_latest_ingestion,
    e.series_code,
    e.series_name,
    c.province_code,
    c.municipality_code,
    e.municipality_name,
    e.sex_label,

    e.point."Anyo"                            as year,
    e.point."FK_Periodo"                      as fk_periodo,
    to_timestamp(e.point."Fecha" / 1000)::date as reference_date,
    e.point."Secreto"                         as is_secret,
    e.point."Valor"                           as population_value

from exploded e
inner join {{ ref('ine_municipality_codes_seed') }} c
    on e.municipality_name = c.municipality_name
-- Only the ambiguous-name series appear in this seed. For them, the source's official
-- code must match the seed's; for all others, `a.series_code is null` and the name
-- resolves on its own.
left join {{ ref('ine_ambiguous_series_seed') }} a
    on e.series_code = a.series_code
where a.series_code is null
   or (a.province_code = c.province_code and a.municipality_code = c.municipality_code)
