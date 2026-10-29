-- THE ORDER AMOUNT CLOSES WITH THE SUM OF ITS LINES — IN BOTH DIRECTIONS.
--
-- WHAT IT CATCHES: fanout. FACT_ORDER_ITEM goes into TWO joins with DIM_PRODUCT (the ordered
-- product and the fulfilled one), each by validity interval. An interval with a boundary
-- closed on both sides, or a duplicated version in the dimension, multiplies rows — and
-- no sum fails on its own, it just gets bigger. Comparing against a total computed along
-- another path is the only cheap way to see it.
--
-- WHY THE OBVIOUS VARIANT WOULD NOT CATCH IT. Comparing only `net_amount` would let through the case
-- where fanout hits only removed lines (which contribute zero) or only orders never
-- picked (null net_amount). That is why BOTH sums are compared: the PLACED value
-- (which every line has, including removed ones) and the PICKED value.
--
-- AND WHY A NULL `net_amount` IS NOT AN ERROR. 298 orders died before picking —
-- cancellation or payment refused. For them the sum of the lines is exactly zero, because
-- `line_amount` measures what the line delivered and they delivered nothing. The test requires that
-- combination (null on one side, zero on the other) instead of ignoring it: filling net_amount with
-- the placed value was a real defect, found in Milestone 4 by comparing two independent
-- folds, and this test is what keeps it from coming back.
--
-- Fails with one row per order that does not close.
with por_pedido as (

    select
        order_id,
        count(*)                                            as linhas,
        sum(line_amount_placed)                             as soma_colocada,
        sum(line_amount)                                    as soma_entregue
    from {{ ref('fact_order_item') }}
    group by 1

)

select
    f.order_id,
    f.order_status,
    f.line_count_placed,
    i.linhas                                                as linhas_no_fato_de_item,
    f.gross_amount_placed,
    i.soma_colocada,
    f.net_amount,
    i.soma_entregue,
    case
        when i.order_id is null                             then 'pedido sem linhas'
        when f.line_count_placed <> i.linhas                then 'contagem de linhas diverge'
        when f.gross_amount_placed <> i.soma_colocada       then 'valor colocado diverge'
        when f.net_amount is null and i.soma_entregue <> 0  then 'nunca separado mas com valor'
        when f.net_amount is not null and f.net_amount <> i.soma_entregue
                                                            then 'valor separado diverge'
    end                                                     as motivo
from {{ ref('fact_order') }} f
left join por_pedido i on i.order_id = f.order_id
where i.order_id is null
   or f.line_count_placed <> i.linhas
   or f.gross_amount_placed <> i.soma_colocada
   or (f.net_amount is null     and i.soma_entregue <> 0)
   or (f.net_amount is not null and f.net_amount <> i.soma_entregue)
