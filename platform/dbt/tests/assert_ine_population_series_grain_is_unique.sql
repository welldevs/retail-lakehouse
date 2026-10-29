-- Uniqueness of the COMPOSITE GRAIN. "year" alone is not enough: each series has 2 points per
-- year (fk_periodo 26 and 27, reference dates 31/12 and 30/06) — see the model comment.
select
    ingestion_date,
    table_id,
    series_code,
    year,
    fk_periodo,
    count(*) as n
from {{ ref('silver_ine_population_series') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
