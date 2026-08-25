-- O UNICO modelo que cruza datas.
--
-- Existe porque price_decreased e FALSO em 100% das linhas nas tres particoes, enquanto
-- 152 precos mudaram entre 08-16 e 08-24. O campo que a fonte oferece para sinalizar
-- variacao nao sinaliza nada: a unica forma de detectar mudanca e o diff de snapshots.
--
-- GRAO: (warehouse, ingestion_date, source_product_id), comparando cada particao com a
-- ANTERIOR EXISTENTE, nao com "ontem". Isso importa: ha um vao de 8 dias entre 08-16 e
-- 08-24, e os dias 08-17..08-23 nao existem nem podem ser recuperados (a API so serve o
-- preco de hoje). Usar lag() sobre a sequencia de particoes, em vez de aritmetica de
-- data, e o que mantem o modelo correto na presenca de vaos.
--
-- MEDE a variacao ligada a CHAVE DA FONTE, nao a de um item comercial. Ver a coluna
-- name_seen_before e CONTRACT.md 4.5.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_price_change',
    options = {'partition_by': 'ingestion_date, warehouse', 'overwrite_or_ignore': 1}
) }}

with priced as (

    -- Um preco por (id, armazem, data). Medido: o preco NAO divergre entre as aparicoes
    -- repetidas do mesmo id — apenas o array categories[] difere (198 e 199 casos). Por
    -- isso any_value nao esconde conflito aqui; e uma reducao segura, nao uma escolha
    -- arbitraria. O teste de unicidade em schema.yml protege essa premissa.
    select
        ingestion_date,
        warehouse,
        source_product_id,
        any_value(display_name)         as display_name,
        any_value(unit_price)           as unit_price,
        count(*)                        as catalog_appearances
    from {{ ref('silver_product_price') }}
    group by 1, 2, 3

),

-- Sequencia de particoes existentes, por armazem.
partition_pair as (

    select
        warehouse,
        ingestion_date                  as curr_date,
        lag(ingestion_date) over (
            partition by warehouse order by ingestion_date
        )                               as prev_date
    from (select distinct warehouse, ingestion_date from priced)

),

curr as (

    select pp.warehouse, pp.curr_date, pp.prev_date,
           p.source_product_id, p.display_name, p.unit_price, p.catalog_appearances
    from partition_pair pp
    join priced p
      on p.warehouse = pp.warehouse
     and p.ingestion_date = pp.curr_date
    where pp.prev_date is not null      -- a primeira particao nao tem anterior

),

prev as (

    select pp.warehouse, pp.curr_date, pp.prev_date,
           p.source_product_id, p.display_name, p.unit_price
    from partition_pair pp
    join priced p
      on p.warehouse = pp.warehouse
     and p.ingestion_date = pp.prev_date
    where pp.prev_date is not null

),

-- Nomes vistos em cada particao, para reconhecer id novo com nome antigo.
name_in_partition as (

    select distinct warehouse, ingestion_date, display_name from priced

)

select
    coalesce(c.warehouse, v.warehouse)                  as warehouse,
    coalesce(c.curr_date, v.curr_date)                  as ingestion_date,
    coalesce(c.prev_date, v.prev_date)                  as previous_ingestion_date,
    coalesce(c.source_product_id, v.source_product_id)  as source_product_id,
    coalesce(c.display_name, v.display_name)            as display_name,

    v.unit_price                                        as previous_unit_price,
    c.unit_price                                        as unit_price,
    c.unit_price - v.unit_price                          as price_delta,
    c.catalog_appearances,

    case
        when v.source_product_id is null then 'entrou'
        when c.source_product_id is null then 'saiu'
        when c.unit_price <> v.unit_price then 'preco_alterado'
        else 'estavel'
    end                                                 as change_type,

    -- ARMADILHA DE IDENTIDADE DE NEGOCIO. Id novo cujo display_name ja existia na
    -- particao anterior. NAO e lido como "produto novo" em silencio: vira fila de
    -- revisao, porque a fonte nao diz se e o mesmo item rechaveado ou um item retirado
    -- e outro lancado. Medido: 1 caso em 08-16, 5 casos em 08-24.
    -- Resolver identidade de negocio e do consumidor, com regra declarada como premissa
    -- (CONTRACT.md 4.5). Este modelo aponta os casos; nao decide por eles.
    coalesce(v.source_product_id is null and np.display_name is not null, false)
                                                        as name_seen_before

from curr c
full outer join prev v
  on  c.warehouse         = v.warehouse
 and  c.curr_date         = v.curr_date
 and  c.source_product_id = v.source_product_id
left join name_in_partition np
  on  np.warehouse      = coalesce(c.warehouse, v.warehouse)
 and  np.ingestion_date = coalesce(c.prev_date, v.prev_date)
 and  np.display_name   = coalesce(c.display_name, v.display_name)
