{#
    NOME DE SCHEMA ABSOLUTO, nao concatenado.

    O comportamento PADRAO do dbt e prefixar: com `schema: GOLD` no profile e
    `+schema: MART` no modelo, o resultado e `GOLD_MART`. Isso existe para que varios
    desenvolvedores compartilhem um banco sem colidir — cada um com seu prefixo.

    Aqui esse padrao esta errado, e foi medido: a primeira execucao criou RETAIL.GOLD_GOLD
    e RETAIL.GOLD_MART ao lado dos schemas GOLD e MART que `ensure_schemas` (em
    snowflake_load.py) tinha acabado de criar com comentario e destino de grant. Ficaram
    quatro schemas para duas camadas, e os testes nao achavam as tabelas.

    GOLD, MART e STAGE nao sao schemas de desenvolvedor: sao as CAMADAS do modelo, com
    nome fixo, criadas e comentadas pela plataforma e alvo dos grants por role. Um prefixo
    de sessao nao tem o que fazer neles. Quando fizer sentido isolar execucoes (CI, mais de
    uma pessoa), o isolamento certo e por DATABASE — trocar SNOWFLAKE_DATABASE — e nao por
    nome de schema, porque so assim as tres camadas continuam se chamando do mesmo jeito
    em qualquer ambiente.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
