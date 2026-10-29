-- OBSERVED POPULATION by municipality, sex and year. The model's DENOMINATOR.
--
-- GRAIN: (province_code, municipality_code, sex_label, year). 21,410 rows, 1996-2025.
-- TYPE: observed (INE, table 29005, cifras oficiales).
--
-- WHY A FACT AND NOT A DIMENSION. It has a temporal grain (29 years measured), it is
-- additive by municipality and by sex, and it is the denominator of every per-capita or
-- penetration ratio. That is the definition of a fact. What goes to DIM_GEOGRAPHY is only
-- the CURRENT YEAR's value, as a size attribute — a slicing convenience, not the series.
--
-- THE 'Total' AGGREGATE WAS ALREADY EXCLUDED IN THE CUT, and it is worth saying why here
-- too: the source mixes the aggregate with the detail in the SAME sex_label column.
-- Summing the three labels gives exactly double the population. It is the same trap that
-- in Phase 1 inflated the age pyramid by 2.03x with age_label, and it does not fail — it
-- produces a plausible number. A test in schema.yml re-checks Hombres + Mujeres against the
-- source's own 'Total'.
--
-- DECLARED LIMITATION: this fact is ANNUAL and municipal. There is no age band per
-- municipality in the tables this platform ingests — 29005 has no age column and
-- 31304 only has province. The synthetic customers' age pyramid uses the PROVINCIAL
-- distribution as a proxy, and that approximation is declared in the Source's contract,
-- not hidden here.
{{ config(materialized = 'table') }}

select
    md5(concat_ws('|', province_code, municipality_code))   as municipality_key,
    province_code,
    municipality_code,
    municipality_name,
    wh,

    sex_label,
    year,
    reference_date,

    population_value,
    -- is_secret comes from the source: INE suppresses values for very small municipalities
    -- for statistical confidentiality. Null here is NOT data missing due to our error, and
    -- the companion column is what distinguishes the two cases.
    is_secret,

    -- year and reference_date MAY disagree, and it is not a calculation error: it is INE's
    -- convention (year=2025 recorded with Fecha on 2024-12-31). Neither is "corrected" to
    -- match the other; whoever needs the exact date uses reference_date.
    (year = (select max(year) from {{ source('stage', 'STG_POPULATION_MUNICIPALITY') }}))
                                                            as is_latest_year
from {{ source('stage', 'STG_POPULATION_MUNICIPALITY') }}
