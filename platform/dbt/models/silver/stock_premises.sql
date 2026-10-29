-- PASS-THROUGH of the seed to object storage — same reason/mechanism as order_premises.sql.
--
-- WHY THESE PREMISES ARE SEPARATE FROM THE ORDER ONES. Those in `order_premises` govern
-- what a customer DOES; these govern what the WAREHOUSE holds. They are different domains
-- and change for different reasons: the basket changes when something is learned about
-- buying behavior, stock coverage changes when something is learned about operations.
-- Putting them in a single file would make every order regeneration look like a stock
-- policy change, and vice versa.
--
-- WHAT IS SYNTHETIC HERE, AND WHAT IS NOT. The four rows are POLICY — how many days to
-- cover, when to replenish, up to where to replenish, how long the supplier takes. No
-- source in this repository measures stock, and the `label` says `synthetic` on all of them.
--
-- What is NOT synthetic is the number they multiply: the average daily demand per series
-- (warehouse x product) comes from the consumption MEASURED in `silver_order_line`. So each
-- product's stock level is observed; only the number of coverage days is declared. It is
-- the same shape as the customer base, sized by INE population: the structure comes from
-- the data, the policy comes from here, and the two remain distinguishable.
{{ config(
    location = 's3://retail-lakehouse/silver/stock_premises.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('stock_premises_seed') }}
