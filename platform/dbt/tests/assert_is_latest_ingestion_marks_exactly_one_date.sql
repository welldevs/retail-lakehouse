-- `is_latest_ingestion` tem de marcar UMA e apenas uma ingestion_date por modelo.
--
-- A coluna existe porque estes modelos empilham todas as extracoes de proposito, e sem
-- uma marca explicita quem consultasse sem filtrar contaria em dobro (ja aconteceu:
-- silver_ine_population_series ficou com 1.547.496 linhas em cada uma de duas datas).
-- A expressao correta e `max(ingestion_date) over ()`; trocar por um `over (partition
-- by ...)` distraidamente marcaria varias datas como "a mais recente" e o filtro passaria
-- a nao filtrar nada. Este teste e o que impede essa regressao passar em silencio.
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
