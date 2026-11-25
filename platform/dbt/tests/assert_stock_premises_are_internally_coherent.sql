-- THE STOCK PREMISES MUST NOT CONTRADICT EACH OTHER.
--
-- A DIRECT SIBLING OF assert_order_premises_are_internally_coherent, and written BEFORE the
-- first balance existed — on purpose. There the defect lived for three phases because nobody
-- crossed two separately declared premises; here all four are born crossed.
--
-- THE THREE POSSIBLE CONTRADICTIONS, and what each would do to the measurement:
--
--   reorder point <= supplier lead time
--       The order arrives AFTER the shelf empties, always. Any stockout that appeared
--       would measure the seed's arithmetic, not demand variation — exactly what
--       `slot_adherence_rate` measured before Phase 7.
--
--   target level <= reorder point
--       The purchase order is born with zero or negative quantity: replenishing up to a level below
--       the trigger is an instruction that cannot be obeyed.
--
--   opening stock <= reorder point
--       Every series starts already needing replenishment. The first day of the window would become an
--       artificial spike of purchase orders that says nothing about the operation.
--
-- THERE IS NO ASSERTION HERE ABOUT STOCKOUT, TURNOVER OR COVER. Same rule as the sibling
-- test: the coherence of the INPUTS is checked, never the result. A test that looked at the stockout
-- rate would bless whoever tuned `opening_days_min/max` until it looked nice.
with p as (

    select premise_key, cast(value as double) as v
    from {{ ref('stock_premises') }}

),

premise as (

    select
        max(case when premise_key = 'opening_days_min'       then v end) as opening_min,
        max(case when premise_key = 'opening_days_max'       then v end) as opening_max,
        max(case when premise_key = 'reorder_point_days'     then v end) as reorder_point,
        max(case when premise_key = 'reorder_target_days'    then v end) as reorder_target,
        max(case when premise_key = 'supplier_lead_days'     then v end) as lead_days
    from p

)

select
    'reorder_point_days' as premise,
    reorder_point                as declared,
    lead_days                as compared,
    'reorder point <= supplier lead time: the order arrives after the stockout, always' as reason
from premise
where reorder_point <= lead_days

union all

select
    'reorder_target_days',
    reorder_target,
    reorder_point,
    'target level <= reorder point: the purchase order is born with quantity <= 0'
from premise
where reorder_target <= reorder_point

union all

select
    'opening_days_min',
    opening_min,
    reorder_point,
    'opening stock <= reorder point: some series starts already needing replenishment'
from premise
where opening_min <= reorder_point

union all

select
    'opening_days_max',
    opening_max,
    reorder_target,
    'maximum opening cover above the target level, or below the minimum'
from premise
where opening_max > reorder_target or opening_max < opening_min
