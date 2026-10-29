-- GRAIN: (order_date, wh, category_id) — one row per level-2 category, warehouse and day.
--
-- QUESTION IT ANSWERS: what goes into the basket, in which category, at what value — and how
-- much of it does not survive picking.
--
-- IT TALKS TO MART_ASSORTMENT_DAILY ALONG THE SAME AXIS. That one is the supply side (how many
-- products each warehouse lists per category, at what price); this one is the demand side
-- (how many were ordered and how many were delivered). Both have the grain
-- (date, wh, category_id) precisely so they can be put side by side without re-aggregation.
--
-- MIND WHAT IS ADDITIVE AND WHAT IS NOT — each column's name says which.
-- `orders_touching_category` does NOT SUM across categories: an order with milk and bread counts
-- once in each, and summing a day's categories would give far more than that day's
-- orders. That is why the name is not `orders`. Lines, units and value ARE
-- additive: each line belongs to exactly one category. Whoever wants an order count
-- per day uses MART_ORDER_FUNNEL, which has the right grain for that.
--
-- REVENUE IS WHAT WAS DELIVERED, NOT WHAT WAS ORDERED. `line_amount` is zero for a removed
-- line and for a line of an order that never reached picking; `line_amount_placed`
-- keeps the ordered value alongside. The two columns coexist because the difference between them
-- is the only number that measures how much picking changes the basket, and it is real: 2,321
-- removed lines and 5,508 from orders that died before anyone touched them.
--
-- THE CATEGORY IS THE ONE AT ORDER TIME, stamped on the order_placed event itself, not the
-- one the product has today. Measured: all 120,693 lines match the level-2 tree (151
-- categories), zero orphans. DIM_CATEGORY comes in by left join anyway — if a category ever
-- leaves the tree, the revenue must not disappear along with its name.
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

    -- NOT ADDITIVE across categories. See the header.
    count(distinct i.order_id)                              as orders_touching_category,

    -- Additive: each line belongs to exactly one category.
    count(*)                                                as lines_placed,
    {{ count_if("i.line_status in ('fulfilled', 'substituted')") }} as lines_fulfilled,
    {{ count_if("i.line_status = 'substituted'") }}           as lines_substituted,
    {{ count_if("i.line_status = 'removed'") }}                as lines_removed,
    {{ count_if("i.line_status = 'not_picked'") }}             as lines_never_picked,

    sum(i.quantity)                                         as units_placed,
    sum(case when i.line_status in ('fulfilled', 'substituted')
             then i.quantity else 0 end)                    as units_fulfilled,

    sum(i.line_amount_placed)                               as revenue_placed,
    sum(i.line_amount)                                      as revenue_fulfilled,
    sum(i.line_amount_delta)                                as revenue_lost,
    '{{ var("currency") }}'                                 as currency,

    round({{ div0('sum(i.line_amount_placed)', 'count(distinct i.order_id)') }}, 4)
                                                            as avg_placed_per_order,
    round({{ div0('sum(i.line_amount)', 'count(*)') }}, 4)   as avg_fulfilled_per_line,

    round({{ div0(count_if("i.line_status = 'substituted'"), 'count(*)') }}, 4) as substitution_rate,
    round({{ div0(count_if("i.line_status = 'removed'"),     'count(*)') }}, 4) as removal_rate,

    count(distinct i.source_product_id)                     as distinct_products_ordered
from {{ ref('fact_order_item') }} i
join {{ ref('dim_date') }} d          on d.date_key = i.date_key
left join {{ ref('dim_category') }} c on c.category_id = i.category_id
group by 1, 2, 3, 4, 5, 6, 7, 8
