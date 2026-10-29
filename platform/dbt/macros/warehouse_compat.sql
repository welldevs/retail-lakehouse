{#
    FOUR SNOWFLAKE FUNCTIONS WITH NO SYNTAX EQUIVALENT IN POSTGRES, used DOZENS of times
    in Gold and Mart: date_key (to_char + NUMBER), count_if, div0, datediff.

    Unlike the WINDOW in dim_customer.sql — where both engines accept the SAME syntax and
    one simply avoids the one missing in the other —, here there is no common syntactic
    denominator. The alternative to these four macros would be a
    `{% if target.type == 'postgres' %}` at each of the ~40 call sites, repeating the same
    translation 40 times instead of once. The dbt SQL remains the portable asset
    (ARCHITECTURE.md:44) — only the TRANSLATION of Snowflake-only syntax lives in one place.

    payload:VARIANT (`try_parse_json`) is NOT here: Postgres has no native "safe parse"
    function (a direct cast to jsonb RAISES on invalid JSON, it never returns null).
    Instead of a Jinja macro, `try_parse_json` becomes a real SQL FUNCTION, created by an
    `on-run-start` hook (see dbt_project.yml) — the text `try_parse_json(x)` in
    fact_order_event.sql stays IDENTICAL on both engines, calling either Snowflake's
    native function or the equivalent function the hook creates in Postgres.
#}

{% macro date_key(column) %}
    {%- if target.type == 'postgres' -%}
        cast(to_char({{ column }}, 'YYYYMMDD') as bigint)
    {%- else -%}
        cast(to_char({{ column }}, 'YYYYMMDD') as number(38, 0))
    {%- endif -%}
{% endmacro %}

{#
    CAST AS ::numeric ON POSTGRES, and not left as bigint: mart_order_funnel.sql (and
    others) do `count_if(x) / count(*)` without going through div0 — Snowflake's NUMBER
    divides as decimal by default, but bigint/bigint in Postgres truncates to an integer
    (0 or 1) and the whole rate would silently come out wrong. numeric/bigint already
    promotes to real division.
#}
{% macro count_if(condition) %}
    {%- if target.type == 'postgres' -%}
        (count(*) filter (where {{ condition }}))::numeric
    {%- else -%}
        count_if({{ condition }})
    {%- endif -%}
{% endmacro %}

{% macro div0(numerator, denominator) %}
    {%- if target.type == 'postgres' -%}
        (case when ({{ denominator }}) = 0 then 0
              else ({{ numerator }})::numeric / ({{ denominator }}) end)
    {%- else -%}
        div0({{ numerator }}, {{ denominator }})
    {%- endif -%}
{% endmacro %}

{#
    Only the two grains this project actually uses: 'minute' (the durations of FACT_ORDER
    and FACT_ORDER_EVENT) and 'day' (FACT_PRICE_CHANGE and the milestone plausibility
    test). A third grain here must FAIL loudly, for the same reason as the cut's
    `_TYPE_MAP` — guessing the conversion would be worse than not supporting it.
#}
{% macro datediff(unit, start_date, end_date) %}
    {%- if target.type == 'postgres' -%}
        {%- if unit in ('minute', "'minute'") -%}
            (extract(epoch from ({{ end_date }}) - ({{ start_date }})) / 60)::bigint
        {%- elif unit in ('day', "'day'") -%}
            (({{ end_date }})::date - ({{ start_date }})::date)
        {%- else -%}
            {{ exceptions.raise_compiler_error("datediff: grain with no declared Postgres equivalent: " ~ unit) }}
        {%- endif -%}
    {%- else -%}
        datediff({{ unit }}, {{ start_date }}, {{ end_date }})
    {%- endif -%}
{% endmacro %}

{% macro median(column) %}
    {%- if target.type == 'postgres' -%}
        percentile_cont(0.5) within group (order by {{ column }})
    {%- else -%}
        median({{ column }})
    {%- endif -%}
{% endmacro %}

{#
    Postgres does not accept MAX(boolean) ("function max(boolean) does not exist"); the
    native equivalent is the boolean aggregate `bool_or` (true if any row is true) — the
    same semantics MAX already has over boolean in Snowflake, where true > false.
#}
{% macro max_bool(column) %}
    {%- if target.type == 'postgres' -%}
        bool_or({{ column }})
    {%- else -%}
        max({{ column }})
    {%- endif -%}
{% endmacro %}

{% macro listagg(expression, delimiter) %}
    {%- if target.type == 'postgres' -%}
        string_agg({{ expression }}, {{ delimiter }})
    {%- else -%}
        listagg({{ expression }}, {{ delimiter }})
    {%- endif -%}
{% endmacro %}

{#
    Registered once per run (on-run-start, Postgres target only — see dbt_project.yml).
    `exception when others` is what turns a malformed row into NULL instead of bringing
    down the whole 44-thousand-row materialization — the same guarantee Snowflake's
    `try_parse_json` already gives, and which the `not_null` on `payload` in
    fact_order_event.sql depends on being able to see (see schema.yml).
#}
{% macro create_try_parse_json_function() %}
create or replace function try_parse_json(texto text)
returns jsonb
language plpgsql
immutable
as $$
begin
    return texto::jsonb;
exception when others then
    return null;
end;
$$
{% endmacro %}
