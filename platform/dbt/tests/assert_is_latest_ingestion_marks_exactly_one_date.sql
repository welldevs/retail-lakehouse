-- `is_latest_ingestion` must mark ONE and only one ingestion_date per model.
--
-- The column exists because these models stack every extraction on purpose, and without
-- an explicit marker whoever queried without filtering would double-count (it already happened:
-- silver_ine_population_series ended up with 1,547,496 rows in each of two dates).
-- The correct expression is `max(ingestion_date) over ()`; swapping it absent-mindedly for an `over (partition
-- by ...)` would mark several dates as "the most recent" and the filter would stop
-- filtering anything. This test is what keeps that regression from passing silently.
{% set modelos = [
    'silver_callejero_sections',
    'silver_callejero_population_units',
    'silver_callejero_streets',
    'silver_callejero_pseudo_addresses',
    'silver_callejero_tramos',
    'silver_ine_population_series',
    'silver_ine_population_by_municipality',
    'silver_customer',
] %}

{% for modelo in modelos %}
select
    '{{ modelo }}'                                             as modelo,
    count(distinct ingestion_date)                             as datas_marcadas,
    count(*) filter (where is_latest_ingestion is null)        as marcas_nulas
from {{ ref(modelo) }}
where is_latest_ingestion
group by 1
having count(distinct ingestion_date) <> 1
    or count(*) filter (where is_latest_ingestion is null) > 0
{% if not loop.last %}union all{% endif %}
{% endfor %}
