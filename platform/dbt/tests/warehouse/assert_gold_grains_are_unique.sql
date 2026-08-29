-- CADA GRAO DECLARADO NO CABECALHO DO MODELO E, DE FATO, UNICO.
--
-- Um grao escrito num comentario e uma promessa; este teste e o que a torna verificavel.
-- Sao os graos que nenhum teste `unique` de coluna unica alcanca, porque sao compostos —
-- e e exatamente onde um join a mais se esconde: o fanout nao falha, so multiplica linhas
-- e infla toda soma a jusante em silencio.
--
-- Falha com uma linha por chave repetida, dizendo em qual modelo.
{% set graos = [
    ('fact_price_snapshot',      ['snapshot_date', 'wh', 'source_product_id']),
    ('fact_price_change',        ['snapshot_date', 'previous_snapshot_date', 'wh', 'source_product_id']),
    ('fact_ingestion_run',       ['source_name', 'ingestion_date', 'wh']),
    ('fact_population_municipality', ['province_code', 'municipality_code', 'sex_label', 'year']),
    ('dim_geography',            ['province_code', 'municipality_code', 'postal_code']),
    ('dim_product',              ['source_product_id', 'valid_from']),
    ('dim_customer',             ['customer_id', 'valid_from']),
    ('mart_price_evolution',     ['snapshot_date', 'wh', 'source_product_id']),
    ('mart_assortment_daily',    ['snapshot_date', 'wh', 'category_id']),
    ('mart_customer_base',       ['customer_id']),
    ('mart_market_coverage',     ['wh', 'province_code', 'municipality_code']),
    ('fact_order',               ['order_id']),
    ('fact_order_item',          ['order_id', 'line_no']),
    ('fact_order_event',         ['event_id']),
    ('fact_order_premise',       ['premise_key']),
    ('mart_order_funnel',        ['order_date', 'wh']),
    ('mart_fulfillment_sla',     ['order_date', 'wh']),
    ('mart_basket_daily',        ['order_date', 'wh', 'category_id']),
] %}

{% for modelo, colunas in graos %}
select
    '{{ modelo }}'                          as modelo,
    '{{ colunas | join(", ") }}'            as grao,
    count(*)                                as linhas_na_chave
from {{ ref(modelo) }}
group by {{ colunas | join(', ') }}
having count(*) > 1
{% if not loop.last %}union all{% endif %}
{% endfor %}
