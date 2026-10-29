-- THE FOUR WAREHOUSES and their service area.
--
-- GRAIN: one warehouse. KEY: wh.
-- TYPE: the key is OBSERVED (`wh` is a real Mercadona API parameter, not a label this
-- project invented); the link to a province is DERIVED — it was built from INE's
-- Functional Urban Areas in scripts/derive_warehouse_province_map.py.
--
-- The four were not chosen by market size, but by ASSORTMENT DIVERGENCE measured against
-- the source: warehouses in the same city have identical catalogs and differ only in
-- price, so a second Madrid would pay 152 requests to add one axis. See ARCHITECTURE.md
-- section 2.1.
--
-- SOURCE TRAP, recorded here because it is where someone would come looking: an invalid
-- `wh` does NOT fail at the origin. The API returns 200 falling back to vlc1, so a typo
-- produces a partition labeled with the wrong code containing Valencia data — wrong and
-- internally consistent. That is why the accepted_values in schema.yml is not bureaucracy.
{{ config(materialized = 'table') }}

with area as (

    select
        wh,
        count(*)                                            as municipalities_served,
        {{ count_if('is_home_municipality') }}                as home_municipalities
    from {{ source('stage', 'STG_SERVICE_AREA') }}
    group by 1

),

cobertura as (

    select wh, count(distinct postal_code) as postal_codes_served
    from {{ source('stage', 'STG_GEOGRAPHY') }}
    group by 1

),

-- POPULATION CANNOT BE SUMMED OVER THE STG_GEOGRAPHY GRAIN. That table is
-- (municipality, postcode), and municipality_population is a MUNICIPALITY attribute
-- repeated on each of its postcodes: summing directly would count Valencia 12 times. It is
-- reduced to the distinct municipality BEFORE summing. It is the same class of error as the
-- 'Total' aggregate — a plausible and wrong number.
populacao as (

    select wh, sum(municipality_population) as population_served
    from (
        select distinct wh, province_code, municipality_code, municipality_population
        from {{ source('stage', 'STG_GEOGRAPHY') }}
    )
    group by 1

)

select
    w.wh,
    w.province_code,
    w.province_name,
    w.municipio_code                                        as home_municipality_code,
    w.municipio_name                                        as home_municipality_name,
    a.municipalities_served,
    a.home_municipalities,
    c.postal_codes_served,
    p.population_served
from {{ source('stage', 'STG_WAREHOUSE') }} w
left join area a       on a.wh = w.wh
left join cobertura c  on c.wh = w.wh
left join populacao p  on p.wh = w.wh
