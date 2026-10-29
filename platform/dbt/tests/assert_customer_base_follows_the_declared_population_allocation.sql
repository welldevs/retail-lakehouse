-- EACH WAREHOUSE'S BASE IS ITS ADULT POPULATION TIMES THE RATE — recomputed from scratch.
--
-- THE FAILURE MODE THIS CATCHES, and that no other test catches. Until Phase 5 the base was
-- 5,000 customers per warehouse: the same number for AUFs whose populations differ by 4.6x
-- (mad1 has 7.10 million inhabitants, svq1 has 1.59). Nothing failed — the totals closed,
-- the addresses were real, the grain was unique, and the manifest matched the count. The only
-- thing wrong was that density did not exist, and density does not show up in any total.
-- This test redoes the calculation from INE and compares.
--
-- THE CALCULATION, and where each piece comes from:
--
--   municipal population (29005, sex_label='Total', current year)  summed over the service area
--     x the PROVINCE's adult share (31304, ages >= min_customer_age)       <- provincial proxy
--     x penetration rate (demand_profile.channel_reference_pct)            <- MAPA 2025 sec.3
--     = expected customers for that warehouse
--
-- THE ADULT SHARE IS MEASURED OVER THE WHOLE PYRAMID, here as in the export: it is the fraction of
-- adults in the population, and computing it over an already-truncated pyramid would return 100% for
-- every province — a bug that would go unnoticed because 100% is a plausible number.
--
-- THE RATE IS NOT A LITERAL IN THIS FILE. `customer_premises.customer_penetration_source` says
-- WHERE it lives, and this test follows the pointer. If someone points it elsewhere, the last
-- half below fails saying so — better than reading a number nobody declared.
--
-- A TOLERANCE OF 1 CUSTOMER, and why it weakens nothing. The export rounds in Python and
-- this test in SQL; the two sums of 101 proportions can differ in the last bit and flip the
-- rounding. The defects this test exists to catch — uniform allocation,
-- wrong denominator, wrong rate — are off by thousands, never by one.
{{ config(severity = 'error') }}

with premissa as (

    select
        max(case when premise_key = 'min_customer_age' then cast(value as integer) end)
            as min_customer_age,
        max(case when premise_key = 'customer_penetration_source' then value end)
            as penetration_source
    from {{ ref('customer_premises') }}

),

taxa as (

    select cast(value as double) as penetration_pct
    from {{ ref('demand_profile_seed') }}
    where param_key = 'channel_reference_pct'

),

-- Current municipal population, within the scope of each warehouse's service area.
municipal as (

    select
        a.wh,
        sum(p.population_value) as population_total
    from {{ ref('silver_ine_population_by_municipality') }} p
    join {{ ref('warehouse_service_area') }} a
      on p.province_code = a.province_code
     and p.municipality_code = a.municipality_code
    where p.is_latest_ingestion
      and p.sex_label = 'Total'
      and p.year = (
            select max(year) from {{ ref('silver_ine_population_by_municipality') }}
            where is_latest_ingestion
          )
    group by 1

),

-- The WHOLE provincial pyramid. The two aggregate labels are excluded for the same reason as the
-- export: 'Total' and '85 y mas anos' overlap the 101 single ages and summing all 103
-- naively gives 2.03x the right value.
piramide as (

    select
        s.province_name,
        cast(regexp_extract(s.age_label, '^(\d+)', 1) as integer) as age,
        s.population_value
    from {{ ref('silver_ine_population_series') }} s
    where cast(s.table_id as varchar) = '31304'
      and s.is_latest_ingestion
      and s.sex_label = 'Ambos sexos'
      and s.age_label not in ('Total', '85 y más años')
      and s.population_value is not null
      and s.year = (
            select max(year) from {{ ref('silver_ine_population_series') }}
            where cast(table_id as varchar) = '31304' and is_latest_ingestion
          )
      and s.fk_periodo = (
            select max(fk_periodo) from {{ ref('silver_ine_population_series') }}
            where cast(table_id as varchar) = '31304' and is_latest_ingestion
              and year = (
                    select max(year) from {{ ref('silver_ine_population_series') }}
                    where cast(table_id as varchar) = '31304' and is_latest_ingestion
                  )
          )

),

share_adulto as (

    select
        pi.province_name,
        sum(case when pi.age >= pr.min_customer_age then pi.population_value else 0 end)
            / sum(pi.population_value) as adult_share
    from piramide pi
    cross join premissa pr
    group by 1

),

esperado as (

    select
        m.wh,
        m.population_total,
        sa.adult_share,
        m.population_total * sa.adult_share as adult_population,
        -- floor(x + 0.5) and not round(): DuckDB's round() and Python's do not round .5
        -- the same way, and the export uses exactly this form so that both give the
        -- same integer without depending on a tie-breaking convention.
        cast(floor(m.population_total * sa.adult_share * t.penetration_pct / 100.0 + 0.5)
             as bigint) as clientes_esperados
    from municipal m
    join {{ ref('warehouse_province_map') }} wp on wp.wh = m.wh
    join share_adulto sa on sa.province_name = wp.province_name
    cross join taxa t

),

real as (

    select wh, count(*) as clientes_reais
    from {{ ref('silver_customer') }}
    where is_latest_ingestion
    group by 1

)

select
    e.wh                                              as wh,
    'ALOCACAO'                                        as verificacao,
    cast(e.population_total as bigint)                as population_total,
    round(e.adult_share * 100, 3)                     as adult_share_pct,
    e.clientes_esperados                              as clientes_esperados,
    r.clientes_reais                                  as clientes_reais,
    r.clientes_reais - e.clientes_esperados           as diferenca
from esperado e
full outer join real r on r.wh = e.wh
where r.clientes_reais is null
   or e.clientes_esperados is null
   or abs(r.clientes_reais - e.clientes_esperados) > 1

union all

-- THE RATE POINTER. Fails if `customer_penetration_source` stops pointing to
-- demand_profile.channel_reference_pct: from then on the calculation above would be using a number
-- the premise no longer declares, and passing silently would be worse than failing.
select
    'PREMISSA'                                        as wh,
    'PONTEIRO_DA_TAXA'                                as verificacao,
    null                                              as population_total,
    null                                              as adult_share_pct,
    null                                              as clientes_esperados,
    null                                              as clientes_reais,
    null                                              as diferenca
from premissa
where penetration_source is distinct from 'demand_profile.channel_reference_pct'
