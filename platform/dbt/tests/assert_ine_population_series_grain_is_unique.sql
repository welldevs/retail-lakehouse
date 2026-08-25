-- Unicidade do GRAO COMPOSTO. "year" sozinho nao basta: cada serie tem 2 pontos por
-- ano (fk_periodo 26 e 27, referencia 31/12 e 30/06) — ver comentario do modelo.
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
