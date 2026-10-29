-- PROJECTION of the population-by-province series, exactly as INE returns it. No
-- interpretation beyond separating the fields "Nombre" already concatenates (age,
-- territory, sex).
--
-- GRAIN: (ingestion_date, table_id, series_code, year, fk_periodo). Measured against the
-- real payload (table_id=31304, 16,377 series, 1,547,496 points): each series has TWO
-- points per year, one per fk_periodo (27 = reference 30/06, 26 = reference 31/12 —
-- INE publishes two estimates per year). "year" alone is NOT unique within a series;
-- (series, Anyo, FK_Periodo) confirmed unique for all 16,377 series, 0 exceptions.
--
-- year (the source's Anyo) and reference_date MAY disagree at fk_periodo=26: measured that
-- Anyo=2022/fk_periodo=26 has reference_date=2021-12-31, one year EARLIER. It is not a
-- calculation error here — it is INE's convention for "population on 1 January <Anyo>",
-- recorded with Fecha on 31/12 of the previous year. year stays as the source's label;
-- reference_date stays as the exact date derived from Fecha; neither is "corrected" to
-- match the other.
--
-- table_id does NOT come as a hive column: unlike Mercadona's wh=/ingestion_date=, which
-- are DIRECTORY segments, "table_id=<id>" is the FILE NAME
-- (tables/table_id=31304.json) — DuckDB's hive_partitioning=1 only recognizes key=value in
-- directories, never in file names. Measured: without filename=true, the column does not
-- appear. That is why it is extracted from the file path with a regexp, not from hive
-- partitioning.
--
-- The glob below targets table_id=31304.json SPECIFICALLY, not table_id=*.json: ever since
-- table_id=29005 (population by MUNICIPALITY, see silver_ine_population_by_municipality)
-- started landing in the same partition, a generic glob would mix both tables in this
-- classifier — which only knows 31304's vocabulary. Real collision, measured: "Sevilla"
-- is at the same time the name of a province (list below) AND the name of that province's
-- capital municipality — a 29005 series like "Sevilla. Total. Total habitantes. Personas."
-- would match province_name="Sevilla" by vocabulary, and become a ghost row in this
-- PROVINCE series with wrong sex_label/age_label (the classifier does not recognize
-- "Total habitantes"/"Personas" as sex or age). Each table_id has its own projection,
-- each one reads only its own file — no glob shared between the two.
--
-- Province, age and sex come concatenated as TEXT inside "Nombre", not as separate
-- columns or codes: "<Age>. <Territory>. <Sex>. Población. Número.",
-- e.g.: "Total. Albacete. Ambos sexos. Población. Número.". MEASURED AGAINST THE REAL
-- PAYLOAD: the ORDER of the first three segments is NOT fixed —
--   * 15,912 series (97.2%): "Age. Territory. Sex." (the documented order)
--   * 156 series (the whole "85 y más años" bucket): "Sex. Age. Territory."
--   * 309 series: they are the NATIONAL TOTAL, not a province — "Total Nacional. Age.
--     Sex." or (only the "100 y más años" band) "Age. Total Nacional. Sex."
-- That is why territory and sex are identified by known VOCABULARY (closed list of
-- 52 provinces + "Total Nacional"; 3 sex values), not by position — extracting by fixed
-- index (e.g. split_part(..., 2)) produces a wrong "province_name" for ~2.8% of the
-- series (age or sex, recorded as if it were a province). "Total Nacional" stays as a
-- legitimate province_name value (it is not a real province, but no real province is
-- called that, so a future join by province name ignores it on its own).
-- Age is the segment left over after identifying territory and sex — never read by
-- fixed position.
--
-- This model assumes there is at least one landed file (read_json fails with "no
-- files found" on an empty glob). When this source has not landed anything yet — e.g. a
-- fresh clone of the repository, before the first `make ine-refresh` — `make silver`
-- excludes this model from `dbt build` via `retail-platform has-data`, so as not to
-- bring down the whole build (which also serves Mercadona) because of an empty source.
-- See Makefile, target `silver`.
--
-- location is FLAT under silver/ (silver/silver_ine_population_series.parquet), not
-- silver/ine_population/silver_ine_population_series.parquet: measured that
-- retail_platform/query.py derives the view name from the first segment after "silver/" —
-- a file under a subdirectory becomes a partitioned view named after the SUBDIRECTORY,
-- not after the file. The organization in models/silver/ine_population/ (the .sql folder,
-- which only affects where dbt FINDS the source file) is independent of the output layout
-- in the bucket, which follows the convention already used by the Mercadona models: one
-- .parquet file per model, directly under silver/.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_ine_population_series.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with provincias (province_name) as (

    -- The 52 provinces of Spain (50 + Ceuta and Melilla), as they appear in "Nombre".
    -- Closed list, measured against the real payload of table_id=31304 — not a general
    -- geographic catalog; if a future table_id uses another spelling, this CTE must change.
    values
        ('Albacete'), ('Alicante/Alacant'), ('Almería'), ('Araba/Álava'), ('Asturias'),
        ('Ávila'), ('Badajoz'), ('Balears, Illes'), ('Barcelona'), ('Bizkaia'), ('Burgos'),
        ('Cáceres'), ('Cádiz'), ('Cantabria'), ('Castellón/Castelló'), ('Ceuta'),
        ('Ciudad Real'), ('Córdoba'), ('Coruña, A'), ('Cuenca'), ('Gipuzkoa'), ('Girona'),
        ('Granada'), ('Guadalajara'), ('Huelva'), ('Huesca'), ('Jaén'), ('León'),
        ('Lleida'), ('Lugo'), ('Madrid'), ('Málaga'), ('Melilla'), ('Murcia'),
        ('Navarra'), ('Ourense'), ('Palencia'), ('Palmas, Las'), ('Pontevedra'),
        ('Rioja, La'), ('Salamanca'), ('Santa Cruz de Tenerife'), ('Segovia'),
        ('Sevilla'), ('Soria'), ('Tarragona'), ('Teruel'), ('Toledo'),
        ('Valencia/València'), ('Valladolid'), ('Zamora'), ('Zaragoza')

),

raw_series as (

    select
        ingestion_date,
        regexp_extract(filename, 'table_id=([^.]+)\.json$', 1) as table_id,
        "COD"                                                  as series_code,
        "Nombre"                                                as series_name,
        "Data"                                                  as data_points
    from read_json(
        '{{ var("ine_raw_prefix") }}/ingestion_date=*/tables/table_id=31304.json',
        hive_partitioning = 1,
        union_by_name = true,
        filename = true
    )

),

segmented as (

    select
        *,
        trim(split_part(series_name, '. ', 1)) as seg1,
        trim(split_part(series_name, '. ', 2)) as seg2,
        trim(split_part(series_name, '. ', 3)) as seg3
    from raw_series

),

classified as (

    select
        ingestion_date,
        table_id,
        series_code,
        series_name,
        data_points,
        seg1,
        seg2,
        seg3,

        -- territory: matches the known province OR "Total Nacional", in any of the 3
        -- segments. coalesce takes the first one that matches.
        coalesce(
            case when seg1 = 'Total Nacional' or seg1 in (select province_name from provincias) then seg1 end,
            case when seg2 = 'Total Nacional' or seg2 in (select province_name from provincias) then seg2 end,
            case when seg3 = 'Total Nacional' or seg3 in (select province_name from provincias) then seg3 end
        ) as province_name,

        -- sex: matches one of the 3 fixed values, in any of the 3 segments.
        coalesce(
            case when seg1 in ('Ambos sexos', 'Hombres', 'Mujeres') then seg1 end,
            case when seg2 in ('Ambos sexos', 'Hombres', 'Mujeres') then seg2 end,
            case when seg3 in ('Ambos sexos', 'Hombres', 'Mujeres') then seg3 end
        ) as sex_label

    from segmented

),

dimensioned as (

    select
        ingestion_date,
        table_id,
        series_code,
        series_name,
        data_points,
        province_name,
        sex_label,

        -- age: the segment left over after province_name and sex_label are identified.
        case
            when seg1 is distinct from province_name and seg1 is distinct from sex_label then seg1
            when seg2 is distinct from province_name and seg2 is distinct from sex_label then seg2
            else seg3
        end as age_label

    from classified

),

exploded as (

    select
        ingestion_date,
        table_id,
        series_code,
        series_name,
        province_name,
        age_label,
        sex_label,
        unnest(data_points) as point
    from dimensioned

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
    table_id,
    series_code,
    series_name,
    province_name,
    age_label,
    sex_label,

    point."Anyo"                                                as year,
    point."FK_Periodo"                                          as fk_periodo,
    to_timestamp(point."Fecha" / 1000)::date                    as reference_date,
    point."FK_TipoDato"                                         as fk_tipo_dato,
    point."Valor"                                               as population_value

from exploded
