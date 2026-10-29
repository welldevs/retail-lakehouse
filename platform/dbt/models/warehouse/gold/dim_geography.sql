-- THE ANALYTICAL GEOGRAPHY: where the customer lives and where deliveries go.
--
-- GRAIN: (province_code, municipality_code, postal_code) — 699 rows, distilled from
-- 304,952 Callejero segments.
-- KEY: geography_sk (surrogate). Natural key: the three columns above.
-- TYPE: observed (INE Callejero + INE 29005).
--
-- WHY THE GRAIN IS THIS ONE, measured:
--   * postal_code ALONE DOES NOT WORK — 29 postcodes cross a municipality border. Keying
--     only by postcode would merge two municipalities into one row and the per-municipality
--     sum would be wrong.
--   * municipality alone loses the delivery granularity, which is where routing happens.
--   * (municipality, postcode) is the smallest grain that answers both questions. And `wh`
--     is a FUNCTION of that pair, not another dimension: measured that 0 postcodes cross a
--     warehouse, so the assignment is unambiguous.
--
-- THE TWO MUNICIPALITY NAMES travel together with distinct labels because the two INE
-- products spell the SAME municipality differently in 370 of 370 cases: the Callejero uses
-- upper case with the article in parentheses ("BRUC (EL)"), table 29005 uses mixed case
-- with a postponed article ("Bruc, El"). This was discovered when Phase 1 validation failed
-- 200/200 customers. Keeping only one name would hide the divergence; giving both the same
-- label would be a trap. What is compared across sources is always the CODE.
--
-- WHAT THIS DIMENSION DELIBERATELY DOES NOT HAVE: section_code, street_id, numbering
-- range. Those are 517 thousand address-resolution rows that serve the customer
-- GENERATOR, not the analyst, and they stay in S3. Trigger for a DIM_CENSUS_SECTION: a
-- real question per census section (it would be 10,030 rows in scope).
--
-- municipality_population is the CURRENT YEAR's value, a SIZE attribute. The historical
-- series (1996-2025) is a fact and lives in FACT_POPULATION_MUNICIPALITY. Summing this
-- column over several rows of the same municipality counts the same population several
-- times — reduce to the distinct municipality first.
{{ config(materialized = 'table') }}

select
    md5(concat_ws('|', province_code, municipality_code, postal_code)) as geography_sk,

    province_code,
    municipality_code,
    postal_code,

    municipality_name_ine                                   as municipality_name,
    municipality_name_callejero,
    wh,
    is_home_municipality,

    municipality_population,
    tramo_count,
    section_count,
    street_count
from {{ source('stage', 'STG_GEOGRAPHY') }}
