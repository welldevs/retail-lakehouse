-- ESTOQUE — FATO PERIODIC SNAPSHOT, e este e o terceiro padrao de fato do warehouse.
--
-- GRAO: (stock_date, wh, source_product_id). Uma linha por serie por dia da janela,
--       inclusive nos dias sem venda.
-- TIPO: synthetic (a politica de cobertura) sobre observed (o consumo, que veio dos
--       pedidos, que vieram do catalogo real).
--
-- POR QUE PERIODIC SNAPSHOT E NAO TRANSACIONAL. FACT_ORDER_EVENT e transacional (uma linha
-- por evento); FACT_ORDER e accumulating (uma linha que se preenche). Este e o terceiro
-- caso: o SALDO nao e um evento nem um ciclo de vida — e o estado de uma prateleira num
-- instante, e a pergunta natural sobre ele ("quanto tinha no dia X") exige que o dia sem
-- movimento TAMBEM tenha linha. Um fato transacional de movimentos deixaria o leitor
-- reconstruir o saldo com window function, que e exatamente o calculo que este projeto
-- mediu NAO ser suficiente.
--
-- AS LINHAS DOS DIAS PARADOS NAO SAO ENCHIMENTO. Sao onde a reposicao chega: um produto que
-- nao vende ha tres dias tem saldo nesses tres dias, e uma chegada num deles muda o saldo
-- seguinte. Sem elas, `days_of_cover` nao teria como significar nada.
--
-- ADITIVIDADE, e o nome de cada coluna diz qual. As colunas de MOVIMENTO
-- (units_received/demanded/fulfilled/short, reorder_units) sao aditivas em qualquer eixo.
-- As de SALDO (opening_balance, closing_balance) sao semi-aditivas: somam entre produtos e
-- armazens, NAO somam entre dias — somar saldo de segunda com saldo de terca conta o mesmo
-- estoque duas vezes. `days_of_cover` nao e aditiva em eixo nenhum: e uma razao, e a media
-- dela sobre produtos e uma media de razoes, nao a razao das somas.
--
-- A JANELA DESTE FATO NAO E A DE FACT_ORDER, e a diferenca e real: o consumo e datado pela
-- SEPARACAO, e um pedido colocado tarde no ultimo dia e separado no dia seguinte. O ledger
-- enxerga um dia a mais que os pedidos. Igualar as duas janelas exigiria descartar consumo
-- observado para fazer duas tabelas parecerem simetricas.
{{ config(materialized = 'table') }}

with produto as (

    select product_sk, source_product_id, valid_from, valid_to
    from {{ ref('dim_product') }}

),

ledger as (

    select * from {{ source('stage', 'STG_STOCK_LEDGER') }}

)

select
    -- ---- eixos ------------------------------------------------------------------
    cast(to_char(l.stock_date, 'YYYYMMDD') as number(38, 0)) as date_key,
    l.stock_date,
    -- `wh` E A CHAVE DE DIM_WAREHOUSE, sem surrogate: o armazem nao versiona e a chave
    -- natural e parametro real da API da Mercadona. Inventar um warehouse_sk aqui criaria
    -- um vocabulario que o resto do Gold nao usa.
    l.wh,
    l.source_product_id,
    p.product_sk,

    -- ---- movimento (aditivo) ------------------------------------------------------
    l.units_received,
    l.units_demanded,
    l.units_fulfilled,
    -- RUPTURA: o que a demanda pediu e a prateleira nao tinha. Nunca virou saldo negativo,
    -- e a invariante que garante isso e verificada no Silver.
    l.units_short,
    l.reorder_units,
    l.reorder_eta,

    -- ---- saldo (SEMI-aditivo: nao some entre dias) --------------------------------
    l.opening_balance,
    l.closing_balance,

    -- ---- razoes (NAO aditivas) -----------------------------------------------------
    l.mean_daily_demand,
    l.days_of_cover,

    -- PROVENIENCIA ate o Gold. E como uma consulta ao warehouse descobre que esta coluna
    -- veio de um motor fora do Python — a propriedade que justificou o Iceberg.
    l.written_by

from ledger l
-- PRODUTO PELA VERSAO VIGENTE NA DATA, como FACT_ORDER_ITEM faz. O produto muda de nome e
-- de rotulo ao longo da janela; apontar para a versao corrente faria o saldo de 24/08
-- carregar o atributo de 01/09.
left join produto p
       on  p.source_product_id = l.source_product_id
      and  l.stock_date       >= p.valid_from
      and  (p.valid_to is null or l.stock_date < p.valid_to)
