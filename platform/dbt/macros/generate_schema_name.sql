{#
    ABSOLUTE SCHEMA NAME, not concatenated.

    dbt's DEFAULT behavior is to prefix: with `schema: GOLD` in the profile and
    `+schema: MART` on the model, the result is `GOLD_MART`. That exists so that several
    developers can share a database without colliding — each with their own prefix.

    Here that default is wrong, and it was measured: the first run created RETAIL.GOLD_GOLD
    and RETAIL.GOLD_MART next to the GOLD and MART schemas that `ensure_schemas` (in
    snowflake_load.py) had just created with a comment and as grant targets. There were
    four schemas for two layers, and the tests could not find the tables.

    GOLD, MART and STAGE are not developer schemas: they are the model's LAYERS, with a
    fixed name, created and commented by the platform and targeted by per-role grants. A
    session prefix has no business on them. When it makes sense to isolate runs (CI, more
    than one person), the right isolation is per DATABASE — switch SNOWFLAKE_DATABASE — and
    not per schema name, because only then do the three layers keep the same names in any
    environment.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
