-- PASS-THROUGH of the seed to object storage — same reason/mechanism as order_premises.sql
-- (query.py only scans parquet under silver/, never a native table of the local .duckdb).
--
-- WHY THESE PREMISES BELONG TO ANOTHER DOMAIN, and are not more rows in order_premises. The
-- order ones govern what a customer DOES; these govern who EXISTS. The separation is not
-- cosmetic: Phase 5 went wrong precisely for lacking it — it treated "a newborn cannot buy"
-- as an order rule (min_buyer_age) when the missing rule was a registration one, and the
-- base kept 18.01% minors. Two seeds make visible which domain decides what, and an order
-- can no longer patch a registration defect from the outside.
--
-- min_buyer_age was NOT removed from order_premises. After this phase it discards zero
-- customers, and that is what it now proves: the day it discards someone again, one of the
-- two premises moved without the other. A floor that never fires is still a floor.
--
-- The `label` travels with each row and is always 'synthetic'. All four rows are a decision
-- of THIS platform. The only observed thing at stake — e-commerce's 2.2% of food volume —
-- is not here: `customer_penetration_source` POINTS to demand_profile.channel_reference_pct,
-- where it was measured and where it lives. Copying the number here would create a second
-- place to change it.
{{ config(
    location = 's3://retail-lakehouse/silver/customer_premises.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('customer_premises_seed') }}
