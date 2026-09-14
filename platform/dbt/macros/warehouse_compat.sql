{#
    QUATRO FUNCOES SNOWFLAKE SEM EQUIVALENTE DE SINTAXE NO POSTGRES, usadas DEZENAS de
    vezes no Gold e no Mart: date_key (to_char + NUMBER), count_if, div0, datediff.

    Ao contrario do WINDOW de dim_customer.sql — onde os dois motores aceitam a MESMA
    sintaxe e um so evita a que falta no outro —, aqui nao ha denominador sintatico comum.
    A alternativa a estas quatro macros seria um `{% if target.type == 'postgres' %}` em
    cada um dos ~40 pontos de chamada, repetindo a mesma traducao 40 vezes em vez de uma.
    O SQL do dbt continua sendo o ativo portavel (ARCHITECTURE.md:44) — so a TRADUCAO da
    sintaxe Snowflake-only mora num lugar so.

    payload:VARIANT (`try_parse_json`) NAO esta aqui: o Postgres nao tem uma funcao nativa
    de "parse seguro" (um cast direto para jsonb LANCA excecao em JSON invalido, nunca
    devolve nulo). Em vez de uma macro Jinja, `try_parse_json` vira uma FUNCAO SQL real,
    criada por um hook `on-run-start` (ver dbt_project.yml) — o texto `try_parse_json(x)`
    em fact_order_event.sql fica IDENTICO nos dois motores, chamando ou a funcao nativa do
    Snowflake ou a funcao equivalente que o hook cria no Postgres.
#}

{% macro date_key(column) %}
    {%- if target.type == 'postgres' -%}
        cast(to_char({{ column }}, 'YYYYMMDD') as bigint)
    {%- else -%}
        cast(to_char({{ column }}, 'YYYYMMDD') as number(38, 0))
    {%- endif -%}
{% endmacro %}

{#
    CITADO COMO ::numeric NO POSTGRES, e nao deixado bigint: mart_order_funnel.sql (e
    outros) fazem `count_if(x) / count(*)` sem passar por div0 — Snowflake's NUMBER divide
    como decimal por padrao, mas bigint/bigint no Postgres trunca para inteiro (0 ou 1) e
    a taxa inteira sairia errada em silencio. numeric/bigint ja promove para divisao real.
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
    So os dois graos que este projeto usa de fato: 'minute' (as duracoes de FACT_ORDER e
    FACT_ORDER_EVENT) e 'day' (FACT_PRICE_CHANGE e o teste de plausibilidade dos marcos).
    Um terceiro grao aqui deve FALHAR alto, pelo mesmo motivo do `_TYPE_MAP` do recorte —
    adivinhar a conversao seria pior que nao suportar.
#}
{% macro datediff(unit, start_date, end_date) %}
    {%- if target.type == 'postgres' -%}
        {%- if unit in ('minute', "'minute'") -%}
            (extract(epoch from ({{ end_date }}) - ({{ start_date }})) / 60)::bigint
        {%- elif unit in ('day', "'day'") -%}
            (({{ end_date }})::date - ({{ start_date }})::date)
        {%- else -%}
            {{ exceptions.raise_compiler_error("datediff: grao sem equivalente Postgres declarado: " ~ unit) }}
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
    Postgres nao aceita MAX(boolean) ("function max(boolean) does not exist"); o
    equivalente nativo e o agregado booleano `bool_or` (verdadeiro se qualquer linha for
    verdadeira) — a mesma semantica que MAX ja tem sobre boolean no Snowflake, onde
    verdadeiro > falso.
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
    Registrada uma vez por execucao (on-run-start, so no target Postgres — ver
    dbt_project.yml). `exception when others` e o que faz uma linha malformada virar NULO
    em vez de derrubar a materializacao inteira de 44 mil linhas — a mesma garantia que
    `try_parse_json` do Snowflake ja da, e que o `not_null` sobre `payload` em
    fact_order_event.sql depende de poder enxergar (ver schema.yml).
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
