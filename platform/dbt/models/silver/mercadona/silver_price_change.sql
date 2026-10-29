-- The ONLY model that crosses dates.
--
-- It exists because price_decreased is FALSE in 100% of rows across the three partitions,
-- while 152 prices changed between 08-16 and 08-24. The field the source offers to flag a
-- change flags nothing: the only way to detect a change is to diff snapshots.
--
-- GRAIN: (warehouse, ingestion_date, source_product_id), comparing each partition with the
-- PREVIOUS EXISTING one, not with "yesterday". That matters: there is an 8-day gap between
-- 08-16 and 08-24, and days 08-17..08-23 do not exist and cannot be recovered (the API only
-- serves today's price). Using lag() over the sequence of partitions, instead of date
-- arithmetic, is what keeps the model correct in the presence of gaps.
--
-- It MEASURES the change tied to the SOURCE KEY, not that of a commercial item. See the
-- name_seen_before column and CONTRACT.md 4.5.
--
-- CHANGE_TYPE AND THE DELTAS COMPARE purchasable_unit_price, NOT RAW unit_price — fixed
-- after Phase 4 (see DECISIONS.md, "Demand calibration against MAPA 2025"), which found
-- that `unit_price` is the CEILING of the weight selector (`reference_price * 99`) in 10
-- product x warehouse combinations, not a price anyone pays. That phase fixed demand; this
-- fix closes the SAME gap here, which had survived intact: silver_price_change was never
-- touched when purchasable_unit_price entered silver_product_price, so a frozen item at
-- 37 EUR/kg remained capable of showing up as a "price change" of thousands of euros if the
-- ceiling moved — measured that it did NOT move in the existing partitions, but the
-- mechanism was there, waiting. RAW `unit_price`/`previous_unit_price`/`price_delta` remain
-- below, unchanged — source fidelity is an invariant of this model, exactly as in
-- silver_product_price.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_price_change',
    options = {'partition_by': 'ingestion_date, warehouse', 'overwrite_or_ignore': 1}
) }}

with priced as (

    -- One price per (id, warehouse, date). Measured: the price does NOT diverge between
    -- repeated appearances of the same id — only the categories[] array differs (198 and 199
    -- cases). So any_value hides no conflict here; it is a safe reduction, not an arbitrary
    -- choice. The uniqueness test in schema.yml protects that premise.
    select
        ingestion_date,
        warehouse,
        source_product_id,
        any_value(display_name)              as display_name,
        any_value(unit_price)                as unit_price,
        any_value(purchasable_unit_price)     as purchasable_unit_price,
        count(*)                             as catalog_appearances
    from {{ ref('silver_product_price') }}
    group by 1, 2, 3

),

-- Sequence of existing partitions, per warehouse.
partition_pair as (

    select
        warehouse,
        ingestion_date                  as curr_date,
        lag(ingestion_date) over (
            partition by warehouse order by ingestion_date
        )                               as prev_date
    from (select distinct warehouse, ingestion_date from priced)

),

curr as (

    select pp.warehouse, pp.curr_date, pp.prev_date,
           p.source_product_id, p.display_name, p.unit_price, p.purchasable_unit_price,
           p.catalog_appearances
    from partition_pair pp
    join priced p
      on p.warehouse = pp.warehouse
     and p.ingestion_date = pp.curr_date
    where pp.prev_date is not null      -- the first partition has no previous one

),

prev as (

    select pp.warehouse, pp.curr_date, pp.prev_date,
           p.source_product_id, p.display_name, p.unit_price, p.purchasable_unit_price
    from partition_pair pp
    join priced p
      on p.warehouse = pp.warehouse
     and p.ingestion_date = pp.prev_date
    where pp.prev_date is not null

),

-- Names seen in each partition, to recognize a new id with an old name.
name_in_partition as (

    select distinct warehouse, ingestion_date, display_name from priced

)

select
    coalesce(c.warehouse, v.warehouse)                  as warehouse,
    coalesce(c.curr_date, v.curr_date)                  as ingestion_date,
    coalesce(c.prev_date, v.prev_date)                  as previous_ingestion_date,
    coalesce(c.source_product_id, v.source_product_id)  as source_product_id,
    coalesce(c.display_name, v.display_name)            as display_name,

    v.unit_price                                        as previous_unit_price,
    c.unit_price                                        as unit_price,
    c.unit_price - v.unit_price                          as price_delta,

    -- THE PRICE THAT MATTERS FOR DETECTING CHANGE. Equal to unit_price/previous_unit_price in
    -- everything, except the `bunch` rows (10 product x warehouse combinations, see header) —
    -- there unit_price is the weight selector's ceiling, never a consumer's price.
    v.purchasable_unit_price                            as previous_purchasable_unit_price,
    c.purchasable_unit_price                            as purchasable_unit_price,
    c.purchasable_unit_price - v.purchasable_unit_price  as purchasable_price_delta,
    c.catalog_appearances,

    -- COMPARES purchasable_unit_price, NOT raw unit_price: it is the price comparable across
    -- partitions, and "the price changed" must mean that, not "the selector ceiling changed".
    case
        when v.source_product_id is null then 'entrou'
        when c.source_product_id is null then 'saiu'
        when c.purchasable_unit_price <> v.purchasable_unit_price then 'preco_alterado'
        else 'estavel'
    end                                                 as change_type,

    -- BUSINESS IDENTITY TRAP. A new id whose display_name already existed in the previous
    -- partition. It is NOT silently read as a "new product": it goes to a review queue,
    -- because the source does not say whether it is the same item re-keyed or an item
    -- withdrawn and another launched. Measured: 1 case on 08-16, 5 cases on 08-24.
    -- Resolving business identity is the consumer's job, with a rule declared as a premise
    -- (CONTRACT.md 4.5). This model points out the cases; it does not decide for them.
    coalesce(v.source_product_id is null and np.display_name is not null, false)
                                                        as name_seen_before

from curr c
full outer join prev v
  on  c.warehouse         = v.warehouse
 and  c.curr_date         = v.curr_date
 and  c.source_product_id = v.source_product_id
left join name_in_partition np
  on  np.warehouse      = coalesce(c.warehouse, v.warehouse)
 and  np.ingestion_date = coalesce(c.prev_date, v.prev_date)
 and  np.display_name   = coalesce(c.display_name, v.display_name)
