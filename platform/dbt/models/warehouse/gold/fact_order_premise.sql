-- 30 ROWS THAT KEEP A MART FROM MEASURING AGAINST A THRESHOLD NOBODY USED.
--
-- GRAIN: premise_key. TYPE: synthetic — and the `label` column states that on every row.
--
-- DIRECT SIBLING OF FACT_INGESTION_RUN, and for the same reason: metadata promoted to a
-- fact on purpose, because without it the downstream mart has no way to be honest. There
-- it distinguishes "there was no price" from "there was no observation"; here, "no order
-- breached the SLA" from "the SLA was measured against the wrong number".
--
-- MART_FULFILLMENT_SLA counts breaches of `sla_minutes_picking`. That threshold has an
-- owner: the seed the generator read, whose sha256 is in the manifest of every RAW
-- partition. And its value CHANGES — in Phase 7 it dropped from 90 to 60, because 90 was
-- above the arithmetic ceiling of picking and the alert could never fire. Hard-coding the
-- number here would have turned that fix into a silent defect. Rewriting it
-- as a dbt var would create the second copy that diverges on the first edit — and nothing
-- would fail, because counting zero breaches against the wrong threshold looks exactly the
-- same as counting zero against the right one.
--
-- IT IS NOT A DIMENSION. No fact points to it, nothing joins it by key. It is a table of
-- DECLARED PARAMETERS read as scalars. It is called FACT_ following the precedent of
-- FACT_INGESTION_RUN, which is not a business measure either; inventing a third prefix for
-- two cases would be new vocabulary with no gain.
--
-- `label` IS ALWAYS 'synthetic', NEVER 'proxy'. A proxy is measured elsewhere and used
-- here; these numbers are not measured anywhere. No source in this repo measures basket,
-- purchase cadence or availability — and the Source refuses to export the reference if the
-- seed carries any other label.
{{ config(materialized = 'table') }}

select
    premise_key,
    value                                                   as premise_value,
    unit                                                    as premise_unit,
    label                                                   as premise_label,
    rationale
from {{ source('stage', 'STG_ORDER_PREMISE') }}
