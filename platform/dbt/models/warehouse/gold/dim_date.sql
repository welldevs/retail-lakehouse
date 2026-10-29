-- COMPLETE CALENDAR, and the completeness is the point.
--
-- GRAIN: one day. KEY: date_key (YYYYMMDD).
-- TYPE: synthetic — generated, not observed. It is the only dimension in this model that
-- comes from no source, and there is nothing wrong with that: a calendar is not a fact.
--
-- WHY COMPLETE, AND NOT JUST THE OBSERVED DAYS. Days 2026-08-17 to 08-23 do not exist in
-- the catalog and CANNOT be recovered — the Mercadona API only serves today's price.
-- A calendar with only the observed days would make the gap DISAPPEAR: a chart would
-- connect 08-16 to 08-24 as if they were consecutive. With the whole calendar, the gap is
-- visible, and FACT_INGESTION_RUN says it is an absence of observation, not of sales.
{{ config(materialized = 'table') }}

-- CALENDAR GENERATION, PER ENGINE. There is no portable SQL for "4,018 synthetic rows"
-- that serves both adapters: Snowflake generates with `table(generator(...))` + `seq4()`,
-- Postgres with `generate_series()`. Unlike DIM_CUSTOMER's `WINDOW` — where both engines
-- accept the SAME syntax and one simply avoids the one missing in the other —, here the two
-- row-generation syntaxes have no common denominator, so the branch is by `target.type`
-- and not by avoiding-what-is-missing. The result (full_date, 2020-01-01 to 2030-12-31) is
-- identical on both sides.
with dias as (

    {% if target.type == 'postgres' %}
    select generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day')::date
                                                            as full_date
    {% else %}
    -- 4,018 days. A fixed and generous range on purpose: a calendar derived from the data's
    -- min and max would fall short the day a future order arrived, and recomputing the
    -- dimension because of that is the kind of surprise a date dimension exists to avoid.
    select dateadd(day, seq4(), '2020-01-01'::date) as full_date
    from table(generator(rowcount => 4018))
    {% endif %}

)

select
    {% if target.type == 'postgres' %}
    cast(to_char(full_date, 'YYYYMMDD') as integer)        as date_key,
    full_date,
    extract(year from full_date)::int                      as year,
    extract(quarter from full_date)::int                   as quarter,
    extract(month from full_date)::int                     as month,
    trim(to_char(full_date, 'Month'))                      as month_name,
    extract(day from full_date)::int                       as day_of_month,
    extract(dow from full_date)::int                       as day_of_week,
    trim(to_char(full_date, 'Day'))                        as day_name,
    extract(week from full_date)::int                      as week_of_year,

    -- Spain: the week starts on Monday. EXTRACT(dow ...) returns 0=Sunday, same convention
    -- as Snowflake's dayofweek() below.
    (extract(dow from full_date) in (0, 6))                as is_weekend,
    {% else %}
    cast(to_char(full_date, 'YYYYMMDD') as number(38, 0))  as date_key,
    full_date,
    year(full_date)                                        as year,
    quarter(full_date)                                     as quarter,
    month(full_date)                                       as month,
    monthname(full_date)                                   as month_name,
    day(full_date)                                         as day_of_month,
    dayofweek(full_date)                                   as day_of_week,
    dayname(full_date)                                     as day_name,
    weekofyear(full_date)                                  as week_of_year,

    -- Spain: the week starts on Monday. dayofweek returns 0=Sunday.
    (dayofweek(full_date) in (0, 6))                       as is_weekend,
    {% endif %}

    cast(to_char(full_date, 'YYYY-MM') as varchar)         as year_month
from dias
