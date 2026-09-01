-- GRAO: (stock_date, wh, category_id) — uma linha por categoria de nivel 2, armazem e dia.
--
-- PERGUNTA QUE RESPONDE: quanto de cada categoria cada armazem tinha, quanto faltou, quantos
-- dias de cobertura restam, e quantas ordens de compra a politica disparou.
--
-- ESTE MART TIRA QUATRO ITENS DE "FORA DE ALCANCE" — ruptura, giro, cobertura e reposicao —
-- e o gatilho declarado la era exatamente este: "nao existe fato de estoque nesta
-- plataforma". Passou a existir, e ele e CALCULADO, nao observado. O rotulo `synthetic`
-- viaja junto para que ninguem leia ruptura como medicao de campo.
--
-- POR QUE POR CATEGORIA E NAO POR PRODUTO. Por produto seriam ~17 mil linhas por dia por
-- armazem, que nao e painel, e um mart existe para ser lido. A categoria de nivel 2 e o
-- mesmo eixo de MART_ASSORTMENT_DAILY e MART_BASKET_DAILY, entao as tres tabelas se
-- comparam sem conversao.
--
-- ================================================================================
-- ADITIVIDADE: A ARMADILHA DESTE MART, e ela e maior aqui que nos outros.
-- ================================================================================
--
-- SALDO NAO SOMA ENTRE DIAS. `closing_units` de segunda mais o de terca nao e o estoque da
-- semana — e o mesmo estoque contado duas vezes. Somar entre PRODUTOS dentro do dia esta
-- certo; somar entre DIAS nunca esta. Todo BI reconstruindo isto vai tentar `sum(closing)`
-- num eixo de tempo e obter um numero grande e sem significado.
--
-- COBERTURA E RAZAO, E RAZAO NAO SE MEDIA. `days_of_cover` por categoria e calculado como
-- saldo total / demanda media total — a razao das somas. Tirar a media das coberturas por
-- produto responde outra pergunta ("quantos dias o produto TIPICO cobre") e da outro numero,
-- porque produto de giro baixo tem cobertura enorme e domina a media. As duas colunas estao
-- publicadas lado a lado justamente porque a diferenca entre elas confunde quem le.
--
-- RUPTURA E CONTADA EM DUAS UNIDADES, e nenhuma substitui a outra. `units_short` diz quanto
-- faltou; `series_with_shortfall` diz em quantos (produto, dia) faltou alguma coisa. Um
-- unico produto popular faltando 500 unidades e 500 produtos faltando 1 sao problemas
-- operacionais diferentes com o mesmo `units_short`.
--
-- LIMITE DECLARADO, e ele importa para ler qualquer numero desta tabela: a ruptura aqui e
-- INDEPENDENTE do `removal_rate` do gerador de pedidos, que produz linhas "unavailable" a
-- uma taxa fixa sorteada, sem olhar saldo nenhum. Uma nao causa a outra. Faze-las coerentes
-- exigiria o gerador LER o ledger, invertendo a dependencia do projeto (hoje pedido gera
-- estoque). Gatilho para mudar: um gerador de segunda passada.
{{ config(materialized = 'table') }}

with ledger as (

    select
        l.stock_date,
        l.wh,
        d.primary_category_id                       as category_id,
        c.category_name,
        l.opening_balance,
        l.closing_balance,
        l.units_demanded,
        l.units_fulfilled,
        l.units_short,
        l.reorder_units,
        l.mean_daily_demand,
        l.days_of_cover
    from {{ ref('fact_stock_ledger') }} l
    -- CATEGORIA PELO PRODUTO, em dois saltos, como MART_ASSORTMENT_DAILY faz: DIM_PRODUCT
    -- guarda `primary_category_id` e DIM_CATEGORY o nome. Produto sem dimensao resolvida
    -- fica com categoria NULA e continua na tabela — engolir a linha esconderia estoque de
    -- um produto que existe, que e pior que uma categoria vazia visivel.
    left join {{ ref('dim_product') }}  d on d.product_sk = l.product_sk
    left join {{ ref('dim_category') }} c on c.category_id = d.primary_category_id

)

select
    stock_date,
    wh,
    category_id,
    any_value(category_name)                        as category_name,

    -- ---- tamanho do problema ------------------------------------------------------
    -- Quantos pares (produto, dia) a linha agrega. E o denominador de qualquer leitura
    -- por produto, e o que impede ler `series_with_shortfall` sem saber sobre quantos.
    count(*)                                        as product_days,

    -- ---- movimento (ADITIVO em qualquer eixo) --------------------------------------
    sum(units_demanded)                             as units_demanded,
    sum(units_fulfilled)                            as units_fulfilled,
    sum(units_short)                                as units_short,
    sum(reorder_units)                              as reorder_units,
    count_if(reorder_units > 0)                     as replenishment_orders,

    -- ---- ruptura, nas duas unidades que nao se substituem --------------------------
    count_if(units_short > 0)                       as series_with_shortfall,
    -- FILL RATE sobre unidades: quanto da demanda a prateleira atendeu. Denominador zero
    -- (categoria sem demanda no dia) devolve nulo, e nao 1 — "nada foi pedido" nao e
    -- "tudo foi atendido", e a diferenca some se o zero virar 100%.
    case when sum(units_demanded) > 0
         then round(sum(units_fulfilled) / sum(units_demanded), 4)
    end                                             as fill_rate,

    -- ---- saldo (SEMI-aditivo: NAO some entre dias) ---------------------------------
    sum(opening_balance)                            as opening_units,
    sum(closing_balance)                            as closing_units,

    -- ---- cobertura, as duas leituras -----------------------------------------------
    -- A RAZAO DAS SOMAS: quantos dias o estoque da categoria cobre a demanda dela.
    case when sum(mean_daily_demand) > 0
         then round(sum(closing_balance) / sum(mean_daily_demand), 2)
    end                                             as days_of_cover,
    -- A MEDIA DAS RAZOES: quantos dias o produto TIPICO da categoria cobre. Numero
    -- diferente, pergunta diferente — produto de giro baixo tem cobertura enorme e domina.
    round(avg(days_of_cover), 2)                    as days_of_cover_typical_product,

    -- ---- giro -----------------------------------------------------------------------
    -- Saidas sobre saldo medio do dia. Nao e giro anualizado: a janela tem poucos dias e
    -- anualizar multiplicaria por um numero que ninguem observou.
    case when (sum(opening_balance) + sum(closing_balance)) > 0
         then round(sum(units_fulfilled) / ((sum(opening_balance) + sum(closing_balance)) / 2.0), 4)
    end                                             as turnover_daily,

    -- O ROTULO VIAJA COM O NUMERO. Estoque nao e observado por fonte nenhuma deste projeto.
    'synthetic'                                     as stock_label

from ledger
group by stock_date, wh, category_id
