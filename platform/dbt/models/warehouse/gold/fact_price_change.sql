-- TRANSICAO DE PRECO entre snapshots EXISTENTES.
--
-- GRAO: (snapshot_date, previous_snapshot_date, wh, source_product_id). 43.204 linhas.
-- TIPO: observed (derivado por diff, mas de dado observado).
--
-- POR QUE EXISTE, e nao e por conveniencia. O campo `price_decreased` da fonte e FALSO em
-- 100% das linhas enquanto 152 precos mudaram entre duas particoes: o campo que a fonte
-- oferece para sinalizar variacao nao sinaliza nada. A unica forma de detectar mudanca e
-- o diff de snapshots, e essa correcao ja foi feita e testada no Silver.
--
-- POR QUE NAO E REDERIVADO AQUI a partir de FACT_PRICE_SNAPSHOT com uma window function:
-- isso criaria uma SEGUNDA implementacao do mesmo lag() ciente-de-lacuna, em outro motor,
-- e as duas divergiriam no primeiro ajuste. Duplicar 43 mil linhas custa menos que
-- duplicar logica. Alem disso o grao e genuinamente outro — transicao, nao snapshot.
--
-- previous_snapshot_date E A PARTICAO ANTERIOR EXISTENTE, nao "ontem". Ha um vao de 8 dias
-- entre 08-16 e 08-24, e days_between_snapshots torna isso visivel: uma variacao de preco
-- ao longo de 8 dias nao e comparavel a uma de 1 dia, e um mart que ignorasse a diferenca
-- estaria comparando coisas distintas.
--
-- price_delta/price_delta_pct SAO CRUS (sobre unit_price), preservados por fidelidade —
-- mesmo criterio de silver_product_price. purchasable_price_delta/_pct, ao lado, sao o
-- preco que um consumidor realmente paga (ver silver_price_change), e change_type ja
-- classifica "mudou" contra ELE, nao contra o cru. Quem le variacao de preco para decidir
-- algo (oferta, tendencia) quer as colunas purchasable_*, nao as cruas.
{{ config(materialized = 'table') }}

select
    {{ date_key('c.ingestion_date') }} as date_key,
    c.ingestion_date                                        as snapshot_date,
    c.previous_ingestion_date                               as previous_snapshot_date,
    {{ datediff("'day'", "c.previous_ingestion_date", "c.ingestion_date") }} as days_between_snapshots,

    c.warehouse                                             as wh,
    c.source_product_id,
    d.product_sk,

    c.previous_unit_price,
    c.unit_price,
    c.price_delta,
    case
        when c.previous_unit_price is null or c.previous_unit_price = 0 then null
        else round(100 * c.price_delta / c.previous_unit_price, 4)
    end                                                     as price_delta_pct,

    c.previous_purchasable_unit_price,
    c.purchasable_unit_price,
    c.purchasable_price_delta,
    case
        when c.previous_purchasable_unit_price is null or c.previous_purchasable_unit_price = 0
            then null
        else round(100 * c.purchasable_price_delta / c.previous_purchasable_unit_price, 4)
    end                                                     as purchasable_price_delta_pct,

    c.change_type,
    c.catalog_appearances,

    -- FILA DE REVISAO DE IDENTIDADE, nao um detalhe. Id novo cujo display_name ja existia
    -- na particao anterior: a fonte nao diz se e o mesmo item rechaveado ou um item
    -- retirado e outro lancado. Este fato APONTA os casos; nao decide por eles.
    c.name_seen_before
from {{ source('stage', 'STG_PRICE_CHANGE') }} c
left join {{ ref('dim_product') }} d
  on  d.source_product_id = c.source_product_id
 and  c.ingestion_date   >= d.valid_from
 and  (d.valid_to is null or c.ingestion_date < d.valid_to)
