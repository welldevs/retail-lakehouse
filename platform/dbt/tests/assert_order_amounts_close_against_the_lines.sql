-- The fold arithmetic closes, in both directions.
--
--   gross_amount_placed = sum of the PLACED lines
--   net_amount          = sum of the EFFECTIVE lines (a removed line contributes zero)
--
-- WHY THIS IS NOT REDUNDANT WITH THE MANIFEST. The reconciliation test compares Silver's fold
-- with the Source's fold. This one compares the ORDER fold with the LINES fold, two
-- Silver models built along different paths: silver_order sums what the
-- `order_picked` events declared, silver_order_line rebuilds line by line applying
-- substitution and removal. If the two agree, the substitution was applied to the right value.
--
-- `net_amount` is null for an order that never reached picking. Those stay out of the
-- second test on purpose: null there means "there was no picking", not "zero".
with das_linhas as (

    select
        order_id,
        cast(sum(line_amount_placed) as decimal(14, 2)) as soma_colocada,
        cast(sum(line_amount) as decimal(14, 2))        as soma_efetiva,
        count(*)                                        as linhas,
        -- Counting the COMPLEMENT of 'removed' was equivalent while only three
        -- values existed. With `not_picked` in the vocabulary it no longer is: a line of a
        -- cancelled order was not removed and was not fulfilled either.
        sum(case when line_status in ('fulfilled', 'substituted') then 1 else 0 end)
                                                        as linhas_cumpridas
    from {{ ref('silver_order_line') }}
    group by order_id

)

select
    o.order_id,
    o.gross_amount_placed,
    l.soma_colocada,
    o.net_amount,
    l.soma_efetiva,
    o.line_count_placed,
    l.linhas,
    o.line_count_picked,
    l.linhas_cumpridas
from {{ ref('silver_order') }} o
join das_linhas l on o.order_id = l.order_id
where o.gross_amount_placed is distinct from l.soma_colocada
   or o.line_count_placed   is distinct from l.linhas
   or (o.net_amount is not null and o.net_amount is distinct from l.soma_efetiva)
   or (o.line_count_picked is not null and o.line_count_picked is distinct from l.linhas_cumpridas)
