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
{% set dimensions = [
    ('dim_product',  'source_product_id'),
    ('dim_customer', 'customer_id'),
] %}

{% for model, natural_key_column in dimensions %}
select
    '{{ model }}'                                          as model,
    cast({{ natural_key_column }} as varchar)                            as natural_key,
    {{ count_if('is_current') }}                                    as versoes_correntes,
    count(*)                                                as versoes_totais
from {{ ref(model) }}
group by {{ natural_key_column }}
having {{ count_if('is_current') }} <> 1
{% if not loop.last %}union all{% endif %}
{% endfor %}
