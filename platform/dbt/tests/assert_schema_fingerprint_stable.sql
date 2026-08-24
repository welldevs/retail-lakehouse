-- AVISO, nao erro: o schema_fingerprint mudar significa que a forma da resposta da fonte
-- mudou. Nao e defeito do pipeline, mas exige olho humano antes de confiar no Silver.
-- CONTRACT.md 4.7 manda o consumidor conferir; aqui a conferencia e automatica.
{{ config(severity = 'warn') }}

select
    warehouse,
    count(distinct schema_fingerprint) as distinct_fingerprints,
    min(ingestion_date)                as first_partition,
    max(ingestion_date)                as last_partition
from {{ ref('raw_manifest') }}
group by 1
having count(distinct schema_fingerprint) > 1
