-- GRAO: (order_date, wh, category_id) — uma linha por categoria de nivel 2, armazem e dia.
--
-- PERGUNTA QUE RESPONDE: o que entra na cesta, em que categoria, a que valor — e quanto
-- disso nao sobrevive a separacao.
--
-- CONVERSA COM MART_ASSORTMENT_DAILY PELO MESMO EIXO. La e o lado da oferta (quantos
-- produtos cada armazem lista por categoria, a que preco); aqui e o lado da demanda
-- (quantos foram pedidos e quantos foram entregues). Os dois tem grao
-- (data, wh, category_id) exatamente para poderem ser postos lado a lado sem reagregacao.
--
-- ATENCAO AO QUE E ADITIVO E AO QUE NAO E — e o nome de cada coluna diz qual.
-- `orders_touching_category` NAO SOMA entre categorias: um pedido com leite e pao conta
-- uma vez em cada, e somar as categorias de um dia daria muito mais que os pedidos
-- daquele dia. Por isso o nome nao e `orders`. Linhas, unidades e valor SAO
-- aditivos: cada linha pertence a exatamente uma categoria. Quem quiser contagem de
-- pedidos por dia usa MART_ORDER_FUNNEL, que tem o grao certo para isso.
--
-- REVENUE E O QUE FOI ENTREGUE, NAO O QUE FOI PEDIDO. `line_amount` e zero para linha
-- removida e para linha de pedido que nunca chegou a separacao; `line_amount_placed`
-- guarda o valor pedido ao lado. As duas colunas convivem porque a diferenca entre elas e
-- o unico numero que mede o quanto a separacao muda a cesta, e ela e real: 2.321 linhas
-- removidas e 5.508 de pedidos que morreram antes de alguem tocar neles.
--
-- A CATEGORIA E A DO MOMENTO DO PEDIDO, gravada no proprio evento order_placed, e nao a
-- que o produto tem hoje. Medido: as 120.693 linhas casam com a arvore de nivel 2 (151
-- categorias), zero orfas. DIM_CATEGORY entra por left join mesmo assim — se um dia uma
-- categoria sair da arvore, a receita nao pode desaparecer junto com o nome dela.
{{ config(materialized = 'table') }}

select
    i.order_date,
    i.date_key,
    d.year_month,
    i.wh,

    i.category_id,
    c.category_name,
    c.parent_category_id,
    c.parent_category_name,

    -- NAO ADITIVO entre categorias. Ver o cabecalho.
    count(distinct i.order_id)                              as orders_touching_category,

    -- Aditivos: cada linha pertence a exatamente uma categoria.
    count(*)                                                as lines_placed,
    count_if(i.line_status in ('fulfilled', 'substituted'))  as lines_fulfilled,
    count_if(i.line_status = 'substituted')                 as lines_substituted,
    count_if(i.line_status = 'removed')                     as lines_removed,
    count_if(i.line_status = 'not_picked')                  as lines_never_picked,

    sum(i.quantity)                                         as units_placed,
    sum(case when i.line_status in ('fulfilled', 'substituted')
             then i.quantity else 0 end)                    as units_fulfilled,

    sum(i.line_amount_placed)                               as revenue_placed,
    sum(i.line_amount)                                      as revenue_fulfilled,
    sum(i.line_amount_delta)                                as revenue_lost,
    '{{ var("currency") }}'                                 as currency,

    round(div0(sum(i.line_amount_placed), count(distinct i.order_id)), 4)
                                                            as avg_placed_per_order,
    round(div0(sum(i.line_amount), count(*)), 4)            as avg_fulfilled_per_line,

    round(div0(count_if(i.line_status = 'substituted'), count(*)), 4) as substitution_rate,
    round(div0(count_if(i.line_status = 'removed'),     count(*)), 4) as removal_rate,

    count(distinct i.source_product_id)                     as distinct_products_ordered
from {{ ref('fact_order_item') }} i
join {{ ref('dim_date') }} d          on d.date_key = i.date_key
left join {{ ref('dim_category') }} c on c.category_id = i.category_id
group by 1, 2, 3, 4, 5, 6, 7, 8
