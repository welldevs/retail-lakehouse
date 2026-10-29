-- Uniqueness of the COMPOSITE GRAIN. fk_periodo=28 is the only value measured in this table, but
-- it stays in the grain for symmetry with silver_ine_population_series (which really
-- needs it) and as a safeguard in case INE publishes more than one estimate/year here.
select
    ingestion_date,
    series_code,
    year,
    fk_periodo,
    count(*) as n
from {{ ref('silver_ine_population_by_municipality') }}
group by 1, 2, 3, 4
having count(*) > 1
