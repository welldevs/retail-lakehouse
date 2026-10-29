-- A municipality has ONE population per sex and year. It is not redundant with the grain test:
-- that one uses series_code, and the defect this one catches had a DIFFERENT series_code.
--
-- The RAW side of table 29005 is national (~8,200 municipalities) and carries homonyms from other
-- provinces with identical "Nombre"; the join by name matched both and the municipality ended up
-- with two divergent rows (e.g. Torrent with 90,928 and with 182, the latter from Girona).
-- Resolved by ine_ambiguous_series_seed, with INE's official code. This test is what
-- guarantees it stays resolved.
select
    ingestion_date,
    province_code,
    municipality_code,
    sex_label,
    year,
    fk_periodo,
    count(*) as n
from {{ ref('silver_ine_population_by_municipality') }}
group by 1, 2, 3, 4, 5, 6
having count(*) > 1
