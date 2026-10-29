-- GRAIN: (customer_id) — the CURRENT base, one row per customer. Previous versions
--        stay in DIM_CUSTOMER; this is the current picture.
--
-- QUESTION IT ANSWERS: who the customers are, where they live and in what demographic context.
--
-- SYNTHETIC AND OBSERVED ON THE SAME ROW, with the labels kept apart on purpose:
--   * the PERSON (name, sex, age) is synthetic — sampled, not observed;
--   * the ADDRESS (municipality, street, postal code, numbering range) is real, from the Callejero;
--   * the municipality POPULATION is observed, from INE.
-- Whoever queries this mart needs to know which half is which. That is why
-- municipality_population is here: without context, "3 customers in Ajalvir" says nothing;
-- with it, it says 3 customers cover a municipality of 4,500 inhabitants.
{{ config(materialized = 'table') }}

select
    c.customer_id,
    c.customer_sk,
    c.valid_from                                            as generated_at,

    c.wh,
    w.province_name,

    c.province_code,
    c.municipality_code,
    c.municipality_name,
    c.postal_code,
    g.is_home_municipality,
    g.municipality_population,

    -- Address: synthetic assignment over a real street segment. address_is_street_level says
    -- the Callejero declares that the street has no numbering — not that the number was lost.
    c.street_name,
    c.house_number,
    c.numbering_type,
    c.address_is_street_level,

    c.full_name,
    c.sex_label,
    c.birth_year,
    c.age_at_ingestion,
    c.age_band
from {{ ref('dim_customer') }} c
join {{ ref('dim_warehouse') }} w  on w.wh = c.wh
left join {{ ref('dim_geography') }} g on g.geography_sk = c.geography_sk
where c.is_current
