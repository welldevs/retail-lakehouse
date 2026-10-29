-- PASS-THROUGH of the seed to object storage — same reason/mechanism as
-- warehouse_service_area.sql (query.py only scans parquet under silver/, never a native
-- table of the local .duckdb).
--
-- WHY THE PREMISES MUST TRAVEL ALL THE WAY TO THE WAREHOUSE. `sla_minutes_picking` is the
-- threshold against which MART_FULFILLMENT_SLA counts breaches, and it is the SAME premise
-- the generator consumed to produce the durations. If the warehouse kept that threshold in
-- a dbt var, there would be two places where the premise lives — and the day someone edits
-- the seed and not the var, the mart starts measuring against a threshold no order ever
-- knew, without failing anything. That stopped being hypothetical in Phase 7: the threshold dropped from 90 to 60. The `currency` var is the opposite case and that is why it stays a var:
-- Mercadona does not declare a currency in any field, so the premise has no other home.
--
-- IT DOES NOT BELONG IN simulated_orders/. That folder holds what is derived from the
-- Source's RAW; this is an input declaration, and it does not depend on any partition
-- existing — the same criterion that keeps warehouse_service_area directly under silver/.
-- Practical consequence: `make silver` builds this model even with no orders generated.
--
-- The `label` travels with each row, and it is always 'synthetic'. It is the only way for
-- someone querying the warehouse to find out, without opening the CONTRACT, that these
-- numbers are a declared premise and not a measurement — the same discipline that makes
-- Silver never invent a currency.
{{ config(
    location = 's3://retail-lakehouse/silver/order_premises.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('order_premises_seed') }}
