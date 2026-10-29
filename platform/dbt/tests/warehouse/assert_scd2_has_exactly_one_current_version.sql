-- Every natural key of the two SCD2s has EXACTLY one current version, and the intervals
-- neither overlap nor leave gaps.
--
-- These are two classic SCD2 defects, and neither fails on its own:
--   * TWO current versions: a join with the dimension starts duplicating every fact linked to
--     that key, and the total silently doubles;
--   * ZERO current versions: the same join simply loses the rows, and the total
--     shrinks with no error at all.
--
-- Both are a typical consequence of a `lead()` with the wrong partition, which keeps
-- compiling and producing a normal-looking table.
--
-- Fails with one row per natural key with a count different from 1.
{% set dimensoes = [
    ('dim_product',  'source_product_id'),
    ('dim_customer', 'customer_id'),
] %}

{% for modelo, chave in dimensoes %}
select
    '{{ modelo }}'                                          as modelo,
    cast({{ chave }} as varchar)                            as chave_natural,
    {{ count_if('is_current') }}                                    as versoes_correntes,
    count(*)                                                as versoes_totais
from {{ ref(modelo) }}
group by {{ chave }}
having {{ count_if('is_current') }} <> 1
{% if not loop.last %}union all{% endif %}
{% endfor %}
