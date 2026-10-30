-- GRAIN: (stock_date, wh, category_id) — one row per level-2 category, warehouse and day.
--
-- QUESTION IT ANSWERS: how much of each category each warehouse had, how much was short, how many
-- days of cover remain, and how many purchase orders the policy triggered.
--
-- THIS MART TAKES FOUR ITEMS OFF "OUT OF REACH" — stockout, turnover, cover and replenishment —
-- and the trigger declared there was exactly this: "there is no stock fact in this
-- platform". Now there is one, and it is COMPUTED, not observed. The `synthetic` label
-- travels along so that nobody reads stockout as a field measurement.
--
-- WHY PER CATEGORY AND NOT PER PRODUCT. Per product it would be ~17 thousand rows per day per
-- warehouse, which is not a panel, and a mart exists to be read. The level-2 category is the
-- same axis as MART_ASSORTMENT_DAILY and MART_BASKET_DAILY, so the three tables
-- compare without conversion.
--
-- ================================================================================
-- ADDITIVITY: THIS MART'S TRAP, and it is bigger here than in the others.
-- ================================================================================
--
-- BALANCE DOES NOT SUM ACROSS DAYS. Monday's `closing_units` plus Tuesday's is not the week's
-- stock — it is the same stock counted twice. Summing across PRODUCTS within the day is
-- right; summing across DAYS never is. Every BI rebuilding this will try `sum(closing)`
-- on a time axis and get a big, meaningless number.
--
-- COVER IS A RATIO, AND A RATIO IS NOT AVERAGED. `days_of_cover` per category is computed as
-- total balance / total average demand — the ratio of the sums. Taking the average of per-product
-- covers answers another question ("how many days the TYPICAL product covers") and gives another
-- number, because low-turnover products have huge cover and dominate the average. Both columns are
-- published side by side precisely because the difference between them confuses the reader.
--
-- STOCKOUT IS COUNTED IN TWO UNITS, and neither replaces the other. `units_short` says how much
-- was missing; `series_with_shortfall` says in how many (product, day) something was missing. A
-- single popular product short by 500 units and 500 products short by 1 are different operational
-- problems with the same `units_short`.
--
-- DECLARED LIMIT, and it matters for reading any number in this table: the stockout here is
-- INDEPENDENT of the order generator's `removal_rate_*`, which produces "unavailable" lines at
-- a fixed drawn rate, without looking at any balance. Neither causes the other. Making them coherent
-- would require the generator to READ the ledger, inverting the project's dependency (today orders
-- generate stock). Trigger to change it: a second-pass generator.
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
    -- CATEGORY THROUGH THE PRODUCT, in two hops, as MART_ASSORTMENT_DAILY does: DIM_PRODUCT
    -- holds `primary_category_id` and DIM_CATEGORY the name. A product with no resolved dimension
    -- gets a NULL category and stays in the table — swallowing the row would hide stock of
    -- a product that exists, which is worse than a visible empty category.
    left join {{ ref('dim_product') }}  d on d.product_sk = l.product_sk
    left join {{ ref('dim_category') }} c on c.category_id = d.primary_category_id

)

select
    stock_date,
    wh,
    category_id,
    any_value(category_name)                        as category_name,

    -- ---- size of the problem ------------------------------------------------------
    -- How many (product, day) pairs the row aggregates. It is the denominator of any per-product
    -- reading, and what keeps `series_with_shortfall` from being read without knowing over how many.
    count(*)                                        as product_days,

    -- ---- movement (ADDITIVE on any axis) ---------------------------------------------
    sum(units_demanded)                             as units_demanded,
    sum(units_fulfilled)                            as units_fulfilled,
    sum(units_short)                                as units_short,
    sum(reorder_units)                              as reorder_units,
    {{ count_if('reorder_units > 0') }}               as replenishment_orders,

    -- ---- stockout, in the two units that do not replace each other -------------------
    {{ count_if('units_short > 0') }}                 as series_with_shortfall,
    -- FILL RATE over units: how much of the demand the shelf served. A zero denominator
    -- (category with no demand that day) returns null, not 1 — "nothing was ordered" is not
    -- "everything was served", and the difference disappears if zero becomes 100%.
    case when sum(units_demanded) > 0
         then round(sum(units_fulfilled) / sum(units_demanded), 4)
    end                                             as fill_rate,

    -- ---- balance (SEMI-additive: do NOT sum across days) -----------------------------
    sum(opening_balance)                            as opening_units,
    sum(closing_balance)                            as closing_units,

    -- ---- cover, both readings ---------------------------------------------------------
    -- THE RATIO OF THE SUMS: how many days the category's stock covers its demand.
    case when sum(mean_daily_demand) > 0
         then round(sum(closing_balance) / sum(mean_daily_demand), 2)
    end                                             as days_of_cover,
    -- THE AVERAGE OF THE RATIOS: how many days the category's TYPICAL product covers. A different
    -- number, a different question — low-turnover products have huge cover and dominate.
    round(avg(days_of_cover), 2)                    as days_of_cover_typical_product,

    -- ---- turnover -------------------------------------------------------------------
    -- Outflows over the day's average balance. It is not annualized turnover: the window has few days and
    -- annualizing would multiply by a number nobody observed.
    case when (sum(opening_balance) + sum(closing_balance)) > 0
         then round(sum(units_fulfilled) / ((sum(opening_balance) + sum(closing_balance)) / 2.0), 4)
    end                                             as turnover_daily,

    -- THE LABEL TRAVELS WITH THE NUMBER. Stock is not observed by any source in this project.
    'synthetic'                                     as stock_label

from ledger
group by stock_date, wh, category_id
