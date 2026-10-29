-- Closes manifest -> RAW -> parquet, and closes the FOLD along two independent paths.
--
-- WHAT MAKES THIS TEST DIFFERENT from its customer sibling. There the manifest declares a row count
-- of a file, and Silver recounts the same rows. Here the manifest declares
-- `order_rows`, `net_amount_picked` and `substituted_lines` — numbers that only exist after
-- FOLDING the log. The Source computed them in Python, walking the events; Silver recomputes them in
-- SQL, with window functions. They are two implementations of the same fold, and a divergence
-- means one of them is wrong.
--
-- WHY MONETARY VALUE COMES IN. Counting rows does not catch a PARTIAL loss of payload: an
-- `order_picked` event whose `picked_amount` got lost in transit would keep the count and
-- zero out the revenue. Summing money catches it. Same reason the customer test checks
-- `house_number_null` in addition to `customer_rows`.
with observado as (

    select
        e.ingestion_date,
        e.wh,
        count(*)                                                       as event_rows,
        count(distinct e.order_id)                                     as order_rows,
        sum(case when e.event_type = 'order_line_substituted' then 1 else 0 end)
                                                                       as substituted_lines,
        sum(case when e.event_type = 'order_line_removed' then 1 else 0 end)
                                                                       as removed_lines
    from {{ ref('silver_order_event') }} e
    group by e.ingestion_date, e.wh

),

do_fold as (

    select
        ingestion_date,
        wh,
        sum(line_count_placed)                                         as line_rows,
        count(distinct customer_id)                                    as customers_used,
        sum(case when price_source <> 'observed' then 1 else 0 end)    as carried_forward_orders,
        cast(sum(gross_amount_placed) as decimal(14, 2))               as gross_amount_placed,
        cast(sum(coalesce(net_amount, 0)) as decimal(14, 2))           as net_amount_picked
    from {{ ref('silver_order') }}
    group by ingestion_date, wh

)

select
    m.ingestion_date,
    m.wh,
    m.declared_event_rows,        o.event_rows,
    m.declared_order_rows,        o.order_rows,
    m.declared_line_rows,         f.line_rows,
    m.declared_substituted_lines, o.substituted_lines,
    m.declared_removed_lines,     o.removed_lines,
    m.declared_gross_amount_placed, f.gross_amount_placed,
    m.declared_net_amount_picked,   f.net_amount_picked
from {{ ref('silver_orders_manifest') }} m
join observado o on m.ingestion_date = o.ingestion_date and m.wh = o.wh
join do_fold   f on m.ingestion_date = f.ingestion_date and m.wh = f.wh
where m.declared_event_rows           <> o.event_rows
   or m.declared_order_rows           <> o.order_rows
   or m.declared_line_rows            <> f.line_rows
   or m.declared_customers_used       <> f.customers_used
   or m.declared_substituted_lines    <> o.substituted_lines
   or m.declared_removed_lines        <> o.removed_lines
   or m.declared_carried_forward_orders <> f.carried_forward_orders
   or m.declared_gross_amount_placed  <> f.gross_amount_placed
   or m.declared_net_amount_picked    <> f.net_amount_picked
