-- FINAL state of each order line, after substitutions and removals.
--
-- GRAIN: (order_id, line_no). `line_no` is the position in the PLACED basket and is never
-- reused: a substituted line keeps the same line_no and swaps the product, a removed line
-- keeps the line_no and drops out of the total. That is what makes it possible to rebuild
-- what was ordered and what was delivered without losing the correspondence between them.
--
-- WHY THE PLACED LINE AND THE FINAL LINE LIVE IN THE SAME TABLE. `source_product_id` is
-- what the customer ordered; `fulfilled_source_product_id` is what they received. Keeping
-- only the second would erase the substitution — which is exactly the fact the event model
-- exists to record. Keeping only the first would make revenue not close.
--
-- `line_status`: placed | fulfilled | substituted | removed | not_picked.
--
-- `not_picked` EXISTS BECAUSE THE OLTP DEMANDED IT. Until Milestone 4, this model labeled
-- `fulfilled` every line that had been neither substituted nor removed — INCLUDING those
-- of cancelled orders or with declined payment, which never reached picking. Measured: 298
-- orders, 5,508 lines claiming to have been fulfilled by an order that died before anyone
-- touched it.
--
-- The log never said that. `order_picked` is the only event that declares picking, and it
-- does not occur in those orders; `else 'fulfilled'` was a claim of the model, not of the
-- source. The defect only showed up when replicating the same log in an OLTP, where the
-- line is born `placed` and only becomes something else when an event changes it — two
-- independent folds disagreeing is the only cheap way to find this.
--
-- `placed` covers the order still in flight (neither picked nor terminal). It does not
-- occur in the current window — all 6,400 orders are terminal — and exists so that the
-- vocabulary is complete rather than complete by luck.
--
-- THE PRICE IS THE OBSERVED ONE, and the Source has already proven it: `validate`
-- re-checks, line by line, that the unit_price matches the catalog of that (warehouse,
-- price_as_of). Here it is only typed. The dbt test that redoes the check against
-- silver_product_price exists because this is the first time the number crosses RAW and
-- parquet — the same logic as assert_customer_lives_in_its_warehouse_service_area being
-- Phase 1's third check.
--
-- price_as_of AND price_source TRAVEL WITH THE NUMBER. Without them, comparing revenue
-- across days would compare prices of different vintages without warning — the same
-- discipline as the `currency` var, which Gold materializes "so that the premise travels
-- with the number".
{{ config(
    location = 's3://retail-lakehouse/silver/silver_order_line.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

with colocadas as (

    select
        ingestion_date,
        wh,
        order_id,
        occurred_at as placed_at,
        json_extract_string(payload, '$.customer_id')               as customer_id,
        cast(json_extract_string(payload, '$.price_as_of') as date) as price_as_of,
        json_extract_string(payload, '$.price_source')              as price_source,
        unnest(
            from_json(
                payload -> '$.lines',
                '[{"line_no":"INTEGER","source_product_id":"VARCHAR","category_id":"INTEGER",
                   "subgroup_id":"INTEGER","demand_group":"VARCHAR","quantity":"INTEGER",
                   "unit_price":"VARCHAR"}]'
            )
        ) as linha
    from {{ ref('silver_order_event') }}
    where event_type = 'order_placed'

),

substituicoes as (

    select
        order_id,
        cast(json_extract_string(payload, '$.line_no') as integer)  as line_no,
        json_extract_string(payload, '$.substitute_source_product_id')
                                                                    as substitute_source_product_id,
        cast(json_extract_string(payload, '$.substitute_unit_price') as decimal(10, 2))
                                                                    as substitute_unit_price,
        occurred_at                                                 as substituted_at
    from {{ ref('silver_order_event') }}
    where event_type = 'order_line_substituted'

),

remocoes as (

    select
        order_id,
        cast(json_extract_string(payload, '$.line_no') as integer)  as line_no,
        json_extract_string(payload, '$.reason')                    as removal_reason,
        occurred_at                                                 as removed_at
    from {{ ref('silver_order_event') }}
    where event_type = 'order_line_removed'

),

-- WHAT HAPPENED TO THE ORDER, to decide what happened to the line. Only two facts
-- matter, and both come from a declared event: there was picking, and there was an end
-- before it.
marcos_do_pedido as (

    select
        order_id,
        max(case when event_type = 'order_picked' then 1 else 0 end)  as foi_separado,
        max(case when event_type in ('order_cancelled', 'order_payment_failed')
                 then 1 else 0 end)                                   as morreu_antes
    from {{ ref('silver_order_event') }}
    group by order_id

)

select
    c.ingestion_date,
    c.wh,
    c.order_id,
    c.linha.line_no                                        as line_no,
    c.customer_id,
    c.placed_at,

    c.price_as_of,
    c.price_source,

    c.linha.source_product_id                              as source_product_id,
    c.linha.category_id                                    as category_id,
    c.linha.subgroup_id                                    as subgroup_id,
    -- Stamped on the event by the Source, resolved by the platform against the versioned
    -- mapping. It comes from the event and not from a join with the catalog here, for the
    -- same reason as `unit_price`: what matters is the group that applied AT THE TIME OF THE
    -- ORDER, not what the mapping would say today.
    c.linha.demand_group                                   as demand_group,
    c.linha.quantity                                       as quantity,
    cast(c.linha.unit_price as decimal(10, 2))             as unit_price,

    case
        when r.line_no is not null       then 'removed'
        when s.line_no is not null       then 'substituted'
        when m.foi_separado = 1          then 'fulfilled'
        when m.morreu_antes = 1          then 'not_picked'
        else 'placed'
    end                                                    as line_status,

    -- What the customer received. NULL when the line was removed: null here means "does not
    -- exist", not "unknown" — same discipline as house_number in Phase 1.
    case
        when r.line_no is not null then null
        when s.line_no is not null then s.substitute_source_product_id
        when m.foi_separado = 1    then c.linha.source_product_id
        else null
    end                                                    as fulfilled_source_product_id,
    case
        when r.line_no is not null then null
        when s.line_no is not null then s.substitute_unit_price
        when m.foi_separado = 1    then cast(c.linha.unit_price as decimal(10, 2))
        else null
    end                                                    as fulfilled_unit_price,

    -- Value the line actually contributed. Zero for a removed line, so that the per-order
    -- sum closes with net_amount without any consumer-side filter.
    case
        when r.line_no is not null then cast(0 as decimal(12, 2))
        when s.line_no is not null then cast(s.substitute_unit_price * c.linha.quantity as decimal(12, 2))
        when m.foi_separado = 1    then cast(cast(c.linha.unit_price as decimal(10, 2)) * c.linha.quantity as decimal(12, 2))
        -- A line of an order that was never picked contributes ZERO, for the same reason a
        -- removed line contributes zero: `line_amount` is what the line delivered, and it
        -- delivered nothing. The ordered value remains in `line_amount_placed`, next to it.
        else cast(0 as decimal(12, 2))
    end                                                    as line_amount,

    cast(cast(c.linha.unit_price as decimal(10, 2)) * c.linha.quantity as decimal(12, 2))
                                                           as line_amount_placed,

    s.substituted_at,
    r.removed_at,
    r.removal_reason

from colocadas c
left join substituicoes s on c.order_id = s.order_id and c.linha.line_no = s.line_no
left join remocoes      r on c.order_id = r.order_id and c.linha.line_no = r.line_no
join marcos_do_pedido   m on c.order_id = m.order_id
