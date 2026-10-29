-- EVERY MILESTONE OF AN ORDER FALLS WITHIN THE ORDER'S OWN WINDOW.
--
-- WHY THIS TEST EXISTS, and it exists because of a real defect, not out of caution. On the
-- first Milestone 7 load EVERY timestamp crossed the boundary in the YEAR 56,648,666: DuckDB
-- annotates the timestamp unit only in parquet's modern LogicalType and leaves the
-- legacy ConvertedType as NONE, and the Snowflake reader fell back to the legacy one and assumed
-- milliseconds where there were microseconds.
--
-- WHAT DID NOT CATCH THE DEFECT, and this list is what justifies the test:
--
--   * the loader's re-check — it compares ROW COUNTS, and those were right;
--   * the 166 dbt nodes, all green — durations became big numbers, not nulls
--     or errors, and no type changed;
--   * assert_gold_grains_are_unique — the grain stayed unique;
--   * assert_order_funnel_totals_match_fact_order — a funnel is made of
--     `count_if(marco is not null)`, and "not null" stays exact when the instant is
--     shifted 56 million years;
--   * assert_fact_order_amount_equals_sum_of_items — money does not go through a timestamp.
--
-- What pointed it out was a human reading 80,000,060 minutes of picking time in a mart. This test is
-- what replaces "someone looking" with "the build fails".
--
-- WHAT IT ASSERTS. `placed_at` falls on the order day (the partition axis IS the
-- placement date, by contract); no milestone precedes placement; and no milestone goes beyond 7
-- days after it. The 7-day ceiling comes from the declared premises, not from taste: the largest
-- possible interval is minutes_to_return_max = 2,880 minutes (2 days) added to the previous
-- milestones, and 7 days leaves slack while still failing a UNIT error, which shifts
-- by orders of magnitude and never by hours.
--
-- WHY THE OBVIOUS VARIANT DOES NOT DO. `placed_at is not null` is already tested by not_null and
-- says nothing about the value. And comparing milestones with each other (picked_at >= picking_started_at)
-- would happily pass the real defect: both were shifted by the SAME factor, so
-- their relative order stayed right. Only anchoring against a date that came along
-- ANOTHER path — `order_date`, which travels as date32 and has no unit ambiguity —
-- makes the shift visible.
--
-- Fails with one row per implausible order.
{% set marcos = [
    'confirmed_at', 'payment_failed_at', 'cancelled_at', 'picking_started_at',
    'picked_at', 'dispatched_at', 'delivered_at', 'delivery_failed_at', 'returned_at'
] %}

select
    order_id,
    order_date,
    placed_at,
    last_event_at,
    case
        when cast(placed_at as date) <> order_date then 'colocacao fora do dia do pedido'
        {% for marco in marcos %}
        when {{ marco }} < placed_at then '{{ marco }} antes da colocacao'
        when {{ datediff("'day'", "order_date", marco) }} > 7 then '{{ marco }} mais de 7 dias depois'
        {% endfor %}
    end                                                     as motivo
from {{ ref('fact_order') }}
where cast(placed_at as date) <> order_date
   {% for marco in marcos %}
   or {{ marco }} < placed_at
   or {{ datediff("'day'", "order_date", marco) }} > 7
   {% endfor %}
