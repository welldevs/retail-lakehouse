-- GRAIN: (order_date, wh, buyer_age_band, demand_group) — what each buyer cohort
-- takes from each demand group, per warehouse and day.
--
-- QUESTION IT ANSWERS: did Phase 5 change anything? The aggregate does NOT move on purpose
-- — that is the IPF acceptance criterion — so no total in this mart differs from what
-- MART_BASKET_DAILY already showed. What changes is in the CONDITIONAL, and this is the only
-- place in the warehouse where it can be queried.
--
-- HOW TO READ `share_within_band`. It is the slice that group takes WITHIN the cohort, not in
-- the total. Comparing the same row across two age bands is the useful reading: if wine weighs
-- 4x more in GE65 than in LT35, it is because the report measures 48.9% of household wine volume
-- in homes whose head is 65 or older, against 1.6% in those under 35.
--
-- WHY THE BAND COMES FROM THE ORDER AND NOT FROM THE CUSTOMER. `buyer_age_band` was stamped on the
-- `order_placed` event by the Source, and it is what chose that basket's weight vector. Deriving
-- the band here, from `dim_customer.birth_year`, would give TODAY's band and would reimplement the
-- band boundaries in a second language — which would diverge from the first at the first
-- adjustment, and the divergence would produce a plausible mix.
--
-- NOT ADDITIVE ACROSS BANDS IN THE ORDERS COLUMN. `orders_in_cohort` counts distinct
-- orders of that cohort that touched the group; summing a group's four bands does NOT give
-- the day's order total, for the same reason `orders_touching_category` does not sum
-- across categories in MART_BASKET_DAILY. Lines, units and value are additive.
{{ config(materialized = 'table') }}

with linhas as (

    select
        i.order_date,
        i.date_key,
        i.wh,
        o.buyer_age_band,
        i.demand_group,
        i.order_id,
        i.line_status,
        i.quantity,
        i.line_amount,
        i.line_amount_placed
    from {{ ref('fact_order_item') }} i
    join {{ ref('fact_order') }} o on o.order_id = i.order_id

),

por_coorte as (

    select
        order_date,
        wh,
        buyer_age_band,
        count(*) as lines_in_band
    from linhas
    group by 1, 2, 3

)

select
    l.order_date,
    l.date_key,
    d.year_month,
    l.wh,
    l.buyer_age_band,
    l.demand_group,

    count(distinct l.order_id)                              as orders_in_cohort,

    count(*)                                                as lines_placed,
    {{ count_if("l.line_status in ('fulfilled', 'substituted')") }} as lines_fulfilled,
    sum(l.quantity)                                         as units_placed,

    sum(l.line_amount_placed)                               as revenue_placed,
    sum(l.line_amount)                                      as revenue_fulfilled,
    '{{ var("currency") }}'                                 as currency,

    -- THE COLUMN THIS PHASE EXISTS TO PRODUCE. The denominator is the cohort itself in that
    -- (day, warehouse), not the total: that is how the comparison across bands isolates
    -- propensity from cohort size.
    round({{ div0('count(*)', 'max(b.lines_in_band)') }}, 6) as share_within_band
from linhas l
join {{ ref('dim_date') }} d on d.date_key = l.date_key
join por_coorte b
  on b.order_date = l.order_date
 and b.wh = l.wh
 and b.buyer_age_band = l.buyer_age_band
group by 1, 2, 3, 4, 5, 6
