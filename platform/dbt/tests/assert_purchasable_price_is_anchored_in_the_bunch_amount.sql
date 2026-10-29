-- THE MOST EXPENSIVE DEFECT EVER MEASURED IN THIS REPO, and the test that keeps it from coming back.
--
-- When `selling_method = 1` and `unit_size` is null, the source returns
-- `unit_price = reference_price * 99` — the price of the weight selector's CEILING, not of anything
-- a household buys. Measured on 2026-08-31: 10 rows, a factor of exactly 99.000, with
-- `unit_price` reaching 3,663.00 EUR for a frozen item at 37.00 EUR/kg. Used by a
-- price-indifferent basket generator, those 10 rows produced 16.6% of ALL the
-- simulated revenue, and none of the 947 existing tests failed.
--
-- WHY THE OBVIOUS VARIANT WOULD NOT CATCH IT
-- ------------------------------------------
-- An absolute ceiling (`purchasable_unit_price < 600`) would pass silently the day the
-- source brings a loose item at 3.00 EUR/kg: 3.00 * 99 = 297, comfortably below the ceiling,
-- and the defect would come back worth 297 EUR per unit. The threshold must be RELATIONAL —
-- anchored on the two observed fields that define the portion — not a chosen number.
--
-- THE TWO HALVES, and why both are needed
-- ---------------------------------------
--   1. every `bunch` row must equal `reference_price * min_bunch_amount`. It catches both the
--      return of the 99 ceiling and any "simplification" that makes
--      purchasable_unit_price = unit_price.
--   2. no row that is NOT `bunch` may have a price different from `unit_price`. It bounds
--      the derivation's blast radius: it fixes 10 rows, and must not start rewriting
--      the other 18,550 without anyone noticing. Without this half, a sign error in the CASE
--      would get past the first.
with bunch_desancorado as (

    select
        warehouse,
        ingestion_date,
        source_product_id,
        price_basis,
        unit_price,
        reference_price,
        min_bunch_amount,
        purchasable_unit_price,
        (reference_price * min_bunch_amount)::decimal(10, 2) as esperado,
        'bunch fora da ancora' as motivo
    from {{ ref('silver_product_price') }}
    where price_basis = 'bunch'
      and (
            purchasable_unit_price is distinct from (reference_price * min_bunch_amount)::decimal(10, 2)
         or min_bunch_amount is null
         or min_bunch_amount <= 0
      )

),

nao_bunch_reescrito as (

    select
        warehouse,
        ingestion_date,
        source_product_id,
        price_basis,
        unit_price,
        reference_price,
        min_bunch_amount,
        purchasable_unit_price,
        unit_price as esperado,
        'nao-bunch com preco reescrito' as motivo
    from {{ ref('silver_product_price') }}
    where price_basis <> 'bunch'
      and purchasable_unit_price is distinct from unit_price

)

select * from bunch_desancorado
union all
select * from nao_bunch_reescrito
