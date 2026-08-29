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

with dias as (

    -- 4.018 dias, de 2020-01-01 a 2030-12-31. Faixa fixa e generosa de proposito: um
    -- calendario derivado do minimo e maximo do dado ficaria curto no dia em que
    -- chegasse um pedido futuro, e recalcular a dimensao por causa disso e o tipo de
    -- surpresa que uma dimensao de data existe para nao dar.
    select dateadd(day, seq4(), '2020-01-01'::date) as full_date
    from table(generator(rowcount => 4018))

)

select
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

    cast(to_char(full_date, 'YYYY-MM') as varchar)         as year_month
from dias
