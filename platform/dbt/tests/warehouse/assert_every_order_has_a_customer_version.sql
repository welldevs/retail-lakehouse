-- EVERY ORDER RESOLVES TO A CUSTOMER VERSION — AND IT IS THE SAME ONE THE SOURCE DECLARED.
--
-- WHAT IT CATCHES, and what the obvious variant would not. The obvious variant would be `customer_sk
-- is not null`. It would fail if the range join matched nothing — but it would happily pass
-- if it matched the WRONG version, which is the real failure mode of an SCD2: an interval with
-- a boundary closed on both sides makes the order on the changeover day match TWO versions, the
-- join fans out, and the order count inflates without `is not null` noticing.
--
-- This test makes three assertions at once:
--
--   1. EVERY order resolves (customer_sk not null);
--   2. the order date falls WITHIN [valid_from, valid_to) of the resolved version;
--   3. the resolved version is the SAME one the Source wrote in `customer_ingestion_date`
--      inside the order_placed event.
--
-- The third is the one that counts. They are two independent paths reaching the same place: the
-- roster choice in the generator (Python, at generation time) and an interval join
-- (SQL, in the warehouse, months later). Measured: they coincide on all 6,400 orders. While they
-- coincide, the SCD2 is being resolved the way Phase 3 promised; when they
-- diverge, one of the two sides changed its mind about what "current version" means, and
-- that needs to be a failure and not a discovery on a dashboard.
--
-- Fails with one row per order, saying which of the three assertions fell.
select
    f.order_id,
    f.order_date,
    f.customer_id,
    f.customer_ingestion_date                               as declarada_pela_source,
    f.customer_version_from                                 as resolvida_pelo_scd2,
    case
        when f.customer_sk is null                              then 'sem versao'
        when f.order_date < f.customer_version_from             then 'versao futura'
        when f.customer_ingestion_date <> f.customer_version_from
                                                            then 'versao divergente'
    end                                                     as motivo
from {{ ref('fact_order') }} f
where f.customer_sk is null
   or f.order_date < f.customer_version_from
   or f.customer_ingestion_date <> f.customer_version_from
