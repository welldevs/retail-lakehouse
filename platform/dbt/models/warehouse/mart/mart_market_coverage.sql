-- GRAIN: (wh, province_code, municipality_code) — one AUF municipality per row, ALL
--        of them, including those that have not received any customer yet.
--
-- QUESTION IT ANSWERS: where the simulation has density and where it does not.
--
-- IT IS THE MOST USEFUL MART OF THE PHASE, and the reason is the LEFT JOIN. Starting from the 370
-- AUF municipalities and hanging the customers on them — instead of grouping the customers — makes
-- a municipality with ZERO customers show up as a row with zero, instead of vanishing from the table.
-- A mart that started from the customers would show 100% coverage by construction, always, whatever
-- the size of the base. Measured today: with 5,000 customers per warehouse, mad1 covers 125
-- of 128 municipalities; with the original 200 it covered 46. This table is where that shows.
--
-- customers_per_10k_inhabitants crosses the SYNTHETIC with the OBSERVED, which is why it is the
-- column that needs the most careful reading: the numerator is invented, the denominator is from INE.
-- It serves to compare the simulation's density ACROSS municipalities, never as an estimate of
-- real market penetration.
{{ config(materialized = 'table') }}

with clientes as (

    select
        wh,
        province_code,
        municipality_code,
        count(*)                                            as customers,
        count(distinct postal_code)                         as postal_codes_used,
        {{ count_if("sex_label = 'Mujeres'") }}               as customers_female,
        {{ count_if('address_is_street_level') }}             as customers_without_house_number,
        round(avg(age_at_ingestion), 2)                     as avg_age
    from {{ ref('dim_customer') }}
    where is_current
    group by 1, 2, 3

),

-- Reduces to the municipality BEFORE summing: STG_GEOGRAPHY is (municipality, postal code), and
-- municipality_population is a municipality attribute repeated in each of its postal codes.
geografia as (

    select
        wh,
        province_code,
        municipality_code,
        max(municipality_name)                              as municipality_name,
        max(municipality_population)                        as municipality_population,
        count(distinct postal_code)                         as postal_codes_available,
        sum(tramo_count)                                    as tramos,
        {{ max_bool('is_home_municipality') }}                as is_home_municipality
    from {{ ref('dim_geography') }}
    group by 1, 2, 3

)

select
    g.wh,
    w.province_name,
    g.province_code,
    g.municipality_code,
    g.municipality_name,
    g.is_home_municipality,

    g.municipality_population,
    g.postal_codes_available,
    g.tramos,

    coalesce(c.customers, 0)                                as customers,
    coalesce(c.postal_codes_used, 0)                        as postal_codes_used,
    coalesce(c.customers_female, 0)                         as customers_female,
    coalesce(c.customers_without_house_number, 0)           as customers_without_house_number,
    c.avg_age,

    -- INE denominator, synthetic numerator. Compare density ACROSS municipalities, never
    -- read it as market penetration.
    case
        when g.municipality_population is null or g.municipality_population = 0 then null
        -- Postgres only has `round(numeric, int)`, not `round(double precision, int)"
        -- ("function round(double precision, integer) does not exist", measured):
        -- municipality_population crosses over as double precision. Snowflake accepts
        -- FLOAT directly, and hard-coding ::numeric there would truncate the default NUMBER to scale 0
        -- before the round — which is why the cast only applies on the Postgres side.
        else round(
            (10000.0 * coalesce(c.customers, 0) / g.municipality_population)
                {%- if target.type == 'postgres' %}::numeric{% endif -%}, 3)
    end                                                     as customers_per_10k_inhabitants,

    (c.customers is null)                                   as has_no_customers
from geografia g
join {{ ref('dim_warehouse') }} w on w.wh = g.wh
-- LEFT: a municipality with no customer MUST appear. It is the only way for the table to show
-- what is missing instead of only what exists.
left join clientes c
       on  c.wh                = g.wh
      and  c.province_code     = g.province_code
      and  c.municipality_code = g.municipality_code
