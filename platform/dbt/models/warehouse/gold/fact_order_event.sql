-- THE LOG INSIDE THE WAREHOUSE — a transactional fact, one event per row.
--
-- GRAIN: event_id. 44,456 rows. TYPE: synthetic (the event) — but the event_id is
--       DETERMINISTIC, sha256(order_id|sequence_no), never uuid4: the Source has no
--       clock or entropy, and re-running the same day produces the same ids.
--
-- WHY IT EXISTS IF FACT_ORDER ALREADY HAS THE MILESTONES. FACT_ORDER keeps UP TO 11
-- instants per order, one per expected milestone. This fact keeps the events that DO NOT
-- fit in a milestone column because they occur N times: 4,670 substitutions and 2,321 line
-- removals, which are precisely the ones that make the basket change after placement.
-- Without this table, "how many lines were swapped in this order" is a count; "when each
-- one was swapped, and in what order" is impossible.
--
-- TWO DATES, AND THE DIFFERENCE IS MEASURED. `date_key` is the ORDER day — the partition
-- axis, immutable, which keeps the whole fold in one place. `event_date_key` is the day
-- the event OCCURRED. They diverge in 4,567 of the 44,456 events, and there are 1,156
-- events that occurred on 08-28 and 08-29, outside the order window (08-24 to 08-27). A
-- fact with a single date would have to choose between "where the order lives" and "when
-- the thing happened", and either choice would lie to half of the questions. DIM_DATE
-- covers 2020-2030 on purpose, so both keys resolve.
--
-- THE PAYLOAD ARRIVES AS VARIANT, AND THE PARSE HAPPENS HERE. STAGE carries text because
-- transport should type nothing; GOLD types because it is where there is an engine that
-- understands VARIANT. `try_parse_json` and not `parse_json`: a malformed row should become
-- a visible null in a column, not bring down the 44-thousand-row build — and a test counts
-- how many nulls there are, so that "visible" does not depend on someone looking.
--
-- minutes_since_previous_event ORDERS BY sequence_no, NOT BY occurred_at. The contract
-- guarantees a contiguous sequence_no per order (1..N, no gaps, tested in Silver); two
-- events may share the same instant, and then ordering by time would produce an arbitrary
-- order that changes between runs.
{{ config(materialized = 'table') }}

with eventos as (

    select * from {{ source('stage', 'STG_ORDER_EVENT') }}

)

select
    {{ date_key('e.ingestion_date') }} as date_key,
    {{ date_key('e.event_date') }}     as event_date_key,
    e.ingestion_date                                        as order_date,
    e.event_date,
    e.crosses_order_date,

    e.event_id,
    e.order_id,
    e.wh,
    e.sequence_no,
    e.event_type,
    e.event_version,
    e.producer,
    e.occurred_at,

    -- Repeated instead of named via `window w as (...)`: Snowflake does not support the
    -- WINDOW clause. DuckDB does, and the repo has already paid once for that difference.
    {{ datediff("'minute'",
        "lag(e.occurred_at) over (partition by e.order_id order by e.sequence_no)",
        "e.occurred_at") }}                                  as minutes_since_previous_event,
    (e.sequence_no = 1)                                     as is_first_event,
    (e.sequence_no = max(e.sequence_no) over (partition by e.order_id))
                                                            as is_last_event,

    -- The event body, typed. Whoever wants to check the fold against the origin does
    -- payload:picked_amount here, without going back to RAW.
    try_parse_json(e.payload_json)                          as payload
from eventos e
