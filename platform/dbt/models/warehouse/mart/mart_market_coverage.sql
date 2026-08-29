-- GRAO: (wh, province_code, municipality_code) — um municipio da AUF por linha, TODOS
--        eles, inclusive os que ainda nao receberam nenhum cliente.
--
-- PERGUNTA QUE RESPONDE: onde a simulacao tem densidade e onde nao tem.
--
-- E O MART MAIS UTIL DA FASE, e o motivo e o LEFT JOIN. Partir dos 370 municipios da AUF e
-- pendurar os clientes neles — em vez de agrupar os clientes — faz o municipio com ZERO
-- clientes aparecer como uma linha com zero, e nao desaparecer da tabela. Um mart que
-- comecasse pelos clientes mostraria 100% de cobertura por construcao, sempre, qualquer
-- que fosse o tamanho da base. Medido hoje: com 5.000 clientes por armazem, mad1 cobre 125
-- dos 128 municipios; com os 200 originais cobria 46. Esta tabela e onde isso se ve.
--
-- customers_per_10k_inhabitants cruza o SINTETICO com o OBSERVADO, e por isso e a coluna
-- que precisa ser lida com mais cuidado: o numerador e inventado, o denominador e do INE.
-- Serve para comparar a densidade da simulacao ENTRE municipios, nunca como estimativa de
-- penetracao de mercado real.
{{ config(materialized = 'table') }}

with clientes as (

    select
        wh,
        province_code,
        municipality_code,
        count(*)                                            as customers,
        count(distinct postal_code)                         as postal_codes_used,
        count_if(sex_label = 'Mujeres')                     as customers_female,
        count_if(address_is_street_level)                   as customers_without_house_number,
        round(avg(age_at_ingestion), 2)                     as avg_age
    from {{ ref('dim_customer') }}
    where is_current
    group by 1, 2, 3

),

-- Reduz ao municipio ANTES de somar: STG_GEOGRAPHY e (municipio, CEP), e
-- municipality_population e atributo do municipio repetido em cada CEP dele.
geografia as (

    select
        wh,
        province_code,
        municipality_code,
        max(municipality_name)                              as municipality_name,
        max(municipality_population)                        as municipality_population,
        count(distinct postal_code)                         as postal_codes_available,
        sum(tramo_count)                                    as tramos,
        max(is_home_municipality)                           as is_home_municipality
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

    -- Denominador do INE, numerador sintetico. Comparar densidade ENTRE municipios, nunca
    -- ler como penetracao de mercado.
    case
        when g.municipality_population is null or g.municipality_population = 0 then null
        else round(10000.0 * coalesce(c.customers, 0) / g.municipality_population, 3)
    end                                                     as customers_per_10k_inhabitants,

    (c.customers is null)                                   as has_no_customers
from geografia g
join {{ ref('dim_warehouse') }} w on w.wh = g.wh
-- LEFT: o municipio sem cliente TEM de aparecer. E a unica forma de a tabela mostrar o
-- que falta em vez de so o que existe.
left join clientes c
       on  c.wh                = g.wh
      and  c.province_code     = g.province_code
      and  c.municipality_code = g.municipality_code
