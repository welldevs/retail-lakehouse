-- A BASE DE CADA ARMAZEM E A POPULACAO ADULTA DELE VEZES A TAXA — recalculada do zero.
--
-- O MODO DE FALHA QUE ISTO PEGA, e que nenhum outro teste pega. Ate a Fase 5 a base era
-- 5.000 clientes por armazem: um numero igual para AUFs que diferem por 4,6x em populacao
-- (mad1 tem 7,10 milhoes de habitantes, svq1 tem 1,59). Nada reprovava — os totais fechavam,
-- os enderecos eram reais, o grao era unico, e o manifesto batia com a contagem. A unica
-- coisa errada era que a densidade nao existia, e densidade nao aparece em nenhum total.
-- Este teste refaz a conta a partir do INE e compara.
--
-- A CONTA, e de onde vem cada pedaco:
--
--   populacao municipal (29005, sex_label='Total', ano corrente)  somada pela area de servico
--     x share adulto da PROVINCIA (31304, idades >= min_customer_age)      <- proxy provincial
--     x taxa de penetracao (demand_profile.channel_reference_pct)          <- MAPA 2025 sec.3
--     = clientes esperados daquele armazem
--
-- O SHARE ADULTO E MEDIDO SOBRE A PIRAMIDE INTEIRA, aqui como no export: e a fracao de
-- adultos na populacao, e calcula-la sobre uma piramide ja truncada devolveria 100% em toda
-- provincia — um bug que passaria despercebido porque 100% e um numero plausivel.
--
-- A TAXA NAO E LITERAL NESTE ARQUIVO. `customer_premises.customer_penetration_source` diz
-- ONDE ela mora, e este teste segue o ponteiro. Se alguem apontar para outro lugar, a ultima
-- metade abaixo reprova dizendo isso — melhor do que ler um numero que ninguem declarou.
--
-- TOLERANCIA DE 1 CLIENTE, e por que ela nao enfraquece nada. O export arredonda em Python e
-- este teste em SQL; as duas somas de 101 proporcoes podem diferir no ultimo bit e virar o
-- arredondamento. Os defeitos que este teste existe para pegar — alocacao uniforme,
-- denominador errado, taxa errada — erram por milhares, nunca por um.
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

-- Populacao municipal corrente, no escopo da area de servico de cada armazem.
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

-- Piramide provincial INTEIRA. Os dois rotulos agregados sao excluidos pela mesma razao do
-- export: 'Total' e '85 y mas anos' se sobrepoem as 101 idades simples e somar os 103
-- ingenuamente da 2,03x o valor certo.
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
        -- floor(x + 0.5) e nao round(): round() do DuckDB e o do Python nao arredondam .5
        -- para o mesmo lado, e o export usa exatamente esta forma para que os dois deem o
        -- mesmo inteiro sem depender de convencao de desempate.
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

-- O PONTEIRO DA TAXA. Reprova se `customer_penetration_source` deixar de apontar para
-- demand_profile.channel_reference_pct: a partir dai a conta acima estaria usando um numero
-- que a premissa nao declara mais, e passar em silencio seria pior do que reprovar.
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
