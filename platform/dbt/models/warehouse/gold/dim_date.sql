-- CALENDARIO COMPLETO, e a completude e o ponto.
--
-- GRAO: um dia. CHAVE: date_key (YYYYMMDD).
-- TIPO: synthetic — gerado, nao observado. E a unica dimensao deste modelo que nao vem de
-- nenhuma fonte, e nao ha nada de errado nisso: um calendario nao e um fato.
--
-- POR QUE COMPLETO, E NAO SO OS DIAS OBSERVADOS. Os dias 2026-08-17 a 08-23 nao existem
-- no catalogo e NAO podem ser recuperados — a API da Mercadona so serve o preco de hoje.
-- Um calendario que so tivesse os dias observados faria a lacuna DESAPARECER: um grafico
-- ligaria 08-16 a 08-24 como se fossem consecutivos. Com o calendario inteiro, a lacuna
-- e visivel, e FACT_INGESTION_RUN diz que ela e ausencia de observacao, nao de venda.
{{ config(materialized = 'table') }}

-- GERACAO DE CALENDARIO, POR MOTOR. Nao ha SQL portavel para "4.018 linhas sinteticas" que
-- sirva aos dois adaptadores: o Snowflake gera com `table(generator(...))` + `seq4()`, o
-- Postgres com `generate_series()`. Ao contrario do `WINDOW` de DIM_CUSTOMER — onde os dois
-- motores aceitam a MESMA sintaxe e um so evita a que falta num deles —, aqui as duas
-- sintaxes de geracao de linha nao tem denominador comum, entao o branch e por
-- `target.type` e nao por evitar-o-que-falta. O resultado (full_date, 2020-01-01 a
-- 2030-12-31) e identico dos dois lados.
with dias as (

    {% if target.type == 'postgres' %}
    select generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day')::date
                                                            as full_date
    {% else %}
    -- 4.018 dias. Faixa fixa e generosa de proposito: um calendario derivado do minimo e
    -- maximo do dado ficaria curto no dia em que chegasse um pedido futuro, e recalcular a
    -- dimensao por causa disso e o tipo de surpresa que uma dimensao de data existe para
    -- nao dar.
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

    -- Espanha: semana comeca na segunda. EXTRACT(dow ...) devolve 0=domingo, mesma
    -- convencao do dayofweek() do Snowflake abaixo.
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

    -- Espanha: semana comeca na segunda. dayofweek devolve 0=domingo.
    (dayofweek(full_date) in (0, 6))                       as is_weekend,
    {% endif %}

    cast(to_char(full_date, 'YYYY-MM') as varchar)         as year_month
from dias
