-- FAITHFUL, TYPED PROJECTION of each snapshot. No cross-date reading happens here:
-- temporal interpretation belongs to silver_price_change.
--
-- GRAIN: (ingestion_date, warehouse, category_id, subgroup_id, source_product_id).
-- It is NOT source_product_id. CONTRACT.md 4.8: a product appears in more than one category
-- and in more than one subgroup — ~270 repeated rows per partition. It is source
-- semantics, preserved on purpose.
--
-- CONTRACT.md 4.4: unit_price is a string in 100% of records; converting to float would
-- lose decimal precision in currency. DECIMAL comes out of here, never DOUBLE.
--
-- TRIM on every price: previous_unit_price comes from the source with leading whitespace
-- ("       18.75") in 100% of non-nulls. DuckDB tolerates that in the CAST — measured — but
-- the TRIM stays for PORTABILITY: the dbt argument is that this same SQL later runs on
-- Snowflake or Spark, and not every engine is tolerant. Normalizing at the origin costs
-- nothing.
--
-- CONTRACT.md 4.6: the source does NOT declare a currency. There is no currency column
-- here, and the project var is a premise for Gold, not source data.
--
-- CONTRACT.md 4.9 — `unit_price` IS NOT ALWAYS THE PRICE OF A PURCHASABLE UNIT.
-- When `selling_method = 1` (weight selector) and `unit_size` is null, the source returns
-- `unit_price = reference_price * 99`: the price of the selector's CEILING, not of anything
-- a household buys. Measured on 2026-08-31: the factor is exactly 99.000 in 10 rows
-- (max 3,663.00 EUR for a frozen item at 37.00 EUR/kg), and 0.200 in the other 21 of the
-- same selling_method — which are the `unit_size` itself and are correct.
--
-- `unit_price` STAYS INTACT, because a faithful projection of the source is an invariant of
-- this model. What comes in are the DERIVED columns next to it, and they invent nothing:
-- `min_bunch_amount` and `increment_bunch_amount` were always in the RAW and this model
-- used to discard them.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_product_price',
    options = {'partition_by': 'ingestion_date, warehouse', 'overwrite': 1}
) }}

with catalog_file as (

    select
        ingestion_date,
        wh                              as warehouse,
        id                              as category_id,
        name                            as category_name,
        categories                      as subgroups
    from read_json(
        '{{ var("raw_prefix") }}/ingestion_date=*/wh=*/catalog/category_id=*.json',
        hive_partitioning = 1,
        union_by_name = true
    )

),

subgroup as (

    select
        ingestion_date,
        warehouse,
        category_id,
        category_name,
        unnest(subgroups)               as sg
    from catalog_file

),

product as (

    select
        ingestion_date,
        warehouse,
        category_id,
        category_name,
        sg.id                           as subgroup_id,
        sg.name                         as subgroup_name,
        unnest(sg.products)             as p
    from subgroup

),

typed as (

select
    ingestion_date,
    warehouse,
    category_id,
    category_name,
    subgroup_id,
    subgroup_name,

    -- SOURCE KEY, not business identity (CONTRACT.md 4.5). Consistent within the snapshot;
    -- no guarantee of stability across snapshots.
    p.id                                                    as source_product_id,
    p.display_name,

    -- Level-1 lineage coming from the product itself. Redundant with silver_category, and the
    -- redundancy is the test: measured, it agrees with the tree in 100% of rows.
    p.categories[1].id                                      as product_level1_category_id,
    p.categories[1].name                                    as product_level1_category_name,

    -- Prices. Strings in the source; DECIMAL here.
    trim(p.price_instructions.unit_price)::decimal(10, 2)    as unit_price,
    trim(p.price_instructions.bulk_price)::decimal(10, 2)    as bulk_price,
    trim(p.price_instructions.reference_price)::decimal(12, 3) as reference_price,
    p.price_instructions.reference_format                    as reference_format,
    trim(p.price_instructions.previous_unit_price)::decimal(10, 2) as previous_unit_price,

    -- Measured: FALSE in 100% of rows across the three partitions, while 152 prices changed
    -- between 08-16 and 08-24. Preserved for fidelity, but it does NOT serve to detect a
    -- change — which is why silver_price_change exists.
    p.price_instructions.price_decreased                     as price_decreased,

    -- tax_percentage is the populated field; `iva` is null in 100% of rows.
    trim(p.price_instructions.tax_percentage)::decimal(6, 3) as tax_percentage,

    p.price_instructions.is_pack                             as is_pack,
    p.price_instructions.pack_size                           as pack_size,
    p.price_instructions.unit_size                           as unit_size,
    p.price_instructions.size_format                         as size_format,
    p.price_instructions.unit_name                           as unit_name,
    p.price_instructions.total_units                         as total_units,
    p.price_instructions.selling_method                      as selling_method,
    p.price_instructions.approx_size                         as approx_size,

    -- THE THREE FIELDS THIS MODEL USED TO DISCARD. They have been in the RAW since the first
    -- partition; no re-ingestion was needed to bring them in. `min_bunch_amount` is the
    -- minimum purchasable portion in `reference_format` (0.15 kg for the loose frozen item),
    -- and it is the only path to a purchasable-unit price when `unit_price` is the selector's
    -- ceiling.
    p.price_instructions.bunch_selector                      as bunch_selector,
    p.price_instructions.min_bunch_amount                    as min_bunch_amount,
    p.price_instructions.increment_bunch_amount              as increment_bunch_amount,

    p.packaging,
    p.thumbnail,
    p.share_url,
    p.slug,
    p.published,
    p.is_new_arrival

from product

)

select
    *,

    -- HOW THIS ROW'S PRICE IS FORMED. A label, not a business rule: each value describes a
    -- source convention that was MEASURED, and the distinction exists because the three have
    -- different consequences for whoever builds a basket.
    --
    --   bunch  selling_method = 1 and unit_size null -> unit_price = reference_price * 99.
    --          It is the ONLY case where unit_price is unusable. 10 rows measured.
    --   piece  whole piece (ham of 8 to 9.5 kg): unit_price = reference_price *
    --          unit_size, a LEGITIMATE PRICE. What is not plausible there is the purchase
    --          incidence, and that is a demand-model problem, not this model's.
    --   unit   consumer packaging. The normal case.
    case
        when selling_method = 1 and unit_size is null then 'bunch'
        when packaging = 'Pieza'                      then 'piece'
        else 'unit'
    end                                                     as price_basis,

    -- THE PURCHASABLE PORTION, in `reference_format`. For `bunch` it is the selector's minimum
    -- portion; for the rest it is the source's own `unit_size` (null when the source does not
    -- declare it).
    case
        when selling_method = 1 and unit_size is null then min_bunch_amount
        else unit_size
    end                                                     as purchasable_unit_size,

    -- THE PRICE OF THE PURCHASABLE PORTION. Differs from `unit_price` ONLY in the `bunch` case,
    -- and there it is reference_price * min_bunch_amount — two observed fields, no estimate.
    case
        when selling_method = 1 and unit_size is null
            then (reference_price * min_bunch_amount)::decimal(10, 2)
        else unit_price
    end                                                     as purchasable_unit_price,

    -- CONTENT IN KG OR LITRE of the purchasable portion, derived from price / reference price.
    -- Null on purpose when `reference_format` is neither mass nor volume (`ud`, `dz`, `lv`,
    -- `dc`, `m`): converting eggs to kg would require a weight per egg that NO source in this
    -- repo measures. Coverage measured on 2026-08-31: 70.61% directly via kg/L, 11.88% via
    -- 100 g / 100 ml, 16.70% in `ud` and therefore null here.
    case
        when reference_format in ('kg', 'L') and reference_price > 0
            then ((case
                    when selling_method = 1 and unit_size is null
                        then (reference_price * min_bunch_amount)::decimal(10, 2)
                    else unit_price
                  end) / reference_price)::decimal(12, 4)
        when reference_format in ('100 g', '100 ml') and reference_price > 0
            then ((case
                    when selling_method = 1 and unit_size is null
                        then (reference_price * min_bunch_amount)::decimal(10, 2)
                    else unit_price
                  end) / reference_price * 0.1)::decimal(12, 4)
    end                                                     as net_content_kg_l

from typed
