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
with per_order as (

    select
        order_id,
        count(*)                                            as line_rows,
        sum(line_amount_placed)                             as placed_sum,
        sum(line_amount)                                    as fulfilled_sum
    from {{ ref('fact_order_item') }}
    group by 1

)

select
    f.order_id,
    f.order_status,
    f.line_count_placed,
    i.line_rows                                                as lines_in_item_fact,
    f.gross_amount_placed,
    i.placed_sum,
    f.net_amount,
    i.fulfilled_sum,
    case
        when i.order_id is null                             then 'order without lines'
        when f.line_count_placed <> i.line_rows                then 'line count differs'
        when f.gross_amount_placed <> i.placed_sum       then 'placed amount differs'
        when f.net_amount is null and i.fulfilled_sum <> 0  then 'never picked but has an amount'
        when f.net_amount is not null and f.net_amount <> i.fulfilled_sum
                                                            then 'picked amount differs'
    end                                                     as reason
from {{ ref('fact_order') }} f
left join per_order i on i.order_id = f.order_id
where i.order_id is null
   or f.line_count_placed <> i.line_rows
   or f.gross_amount_placed <> i.placed_sum
   or (f.net_amount is null     and i.fulfilled_sum <> 0)
   or (f.net_amount is not null and f.net_amount <> i.fulfilled_sum)
