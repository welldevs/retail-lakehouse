-- A WARNING, not an error: a change in schema_fingerprint means the shape of the source's response
-- changed. It is not a pipeline defect, but it needs a human eye before trusting Silver.
-- CONTRACT.md 4.7 tells the consumer to check; here the check is automatic.
{{ config(severity = 'warn') }}

select
    warehouse,
    count(distinct schema_fingerprint) as distinct_fingerprints,
    min(ingestion_date)                as first_partition,
    max(ingestion_date)                as last_partition
from {{ ref('raw_manifest') }}
group by 1
having count(distinct schema_fingerprint) > 1
