-- PASSAGEM do seed para o object storage — mesmo motivo/mecanismo de order_premises.sql
-- (query.py so escaneia parquet sob silver/, nunca tabela nativa do .duckdb local).
--
-- POR QUE ESTAS PREMISSAS SAO DE OUTRO DOMINIO, e nao mais linhas em order_premises. As de
-- pedido governam o que um cliente FAZ; estas governam quem EXISTE. A separacao nao e
-- cosmetica: a Fase 5 errou exatamente por nao te-la — tratou "recem-nascido nao pode
-- comprar" como regra de pedido (min_buyer_age) quando a regra que faltava era de cadastro,
-- e a base seguiu com 18,01% de menores. Dois seeds tornam visivel qual dominio decide o
-- que, e um pedido nao pode mais consertar um defeito de cadastro por fora.
--
-- min_buyer_age NAO foi removido de order_premises. Depois desta fase ele descarta zero
-- clientes, e e isso que ele passa a provar: o dia em que voltar a descartar alguem, uma das
-- duas premissas se moveu sem a outra. Um piso que nunca dispara ainda e um piso.
--
-- O `label` viaja junto de cada linha e e sempre 'synthetic'. As quatro linhas sao decisao
-- DESTA plataforma. A unica coisa observada em jogo — os 2,2% do e-commerce no volume de
-- alimentacao — nao esta aqui: `customer_penetration_source` APONTA para
-- demand_profile.channel_reference_pct, onde ela foi medida e onde ela mora. Copiar o numero
-- para ca criaria um segundo lugar para muda-lo.
{{ config(
    location = 's3://retail-lakehouse/silver/customer_premises.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('customer_premises_seed') }}
