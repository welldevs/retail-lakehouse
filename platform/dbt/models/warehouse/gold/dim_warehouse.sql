-- OS QUATRO ARMAZENS e sua area de atuacao.
--
-- GRAO: um armazem. CHAVE: wh.
-- TIPO: a chave e OBSERVED (`wh` e parametro real da API da Mercadona, nao um rotulo que
-- este projeto inventou); o vinculo com provincia e DERIVED — foi construido a partir das
-- Areas Urbanas Funcionais do INE em scripts/derive_warehouse_province_map.py.
--
-- Os quatro nao foram escolhidos por tamanho de mercado, e sim por DIVERGENCIA DE
-- SORTIMENTO medida contra a fonte: armazens da mesma cidade tem catalogos identicos e
-- divergem so em preco, entao um segundo Madrid pagaria 152 requisicoes para agregar um
-- eixo. Ver ARCHITECTURE.md secao 2.1.
--
-- ARMADILHA DA FONTE, registrada aqui porque e onde alguem viria procurar: um `wh`
-- invalido NAO falha na origem. A API devolve 200 caindo em vlc1, entao um erro de
-- digitacao produz uma particao rotulada com o codigo errado contendo dados de Valencia —
-- errada e internamente consistente. Por isso o accepted_values em schema.yml nao e
-- burocracia.
{{ config(materialized = 'table') }}

with area as (

    select
        wh,
        count(*)                                            as municipalities_served,
        count_if(is_home_municipality)                      as home_municipalities
    from {{ source('stage', 'STG_SERVICE_AREA') }}
    group by 1

),

cobertura as (

    select wh, count(distinct postal_code) as postal_codes_served
    from {{ source('stage', 'STG_GEOGRAPHY') }}
    group by 1

),

-- POPULACAO NAO PODE SER SOMADA SOBRE O GRAO DE STG_GEOGRAPHY. Aquela tabela e
-- (municipio, CEP), e municipality_population e um atributo do MUNICIPIO repetido em cada
-- CEP dele: somar direto contaria Valencia 12 vezes. Reduz-se ao municipio distinto ANTES
-- de somar. E o mesmo erro de classe do agregado 'Total' — um numero plausivel e errado.
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
