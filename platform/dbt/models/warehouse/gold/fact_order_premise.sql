-- 30 LINHAS QUE IMPEDEM UM MART MEDIR CONTRA UM LIMIAR QUE NINGUEM USOU.
--
-- GRAO: premise_key. TIPO: synthetic — e a coluna `label` afirma isso em toda linha.
--
-- IRMA DIRETA DE FACT_INGESTION_RUN, e pelo mesmo motivo: metadado promovido a fato de
-- proposito, porque sem ele o mart a jusante nao tem como ser honesto. La distinguem-se
-- "nao houve preco" de "nao houve observacao"; aqui, "nenhum pedido violou o SLA" de
-- "o SLA foi medido contra o numero errado".
--
-- MART_FULFILLMENT_SLA conta violacoes de `sla_minutes_picking`. Esse limiar tem dono: o
-- seed que o gerador leu, cujo sha256 esta no manifesto de cada particao do RAW. E o valor
-- dele MUDA — na Fase 7 caiu de 90 para 60, porque 90 estava acima do teto aritmetico da
-- separacao e o alerta era inexercivel. Cravar o numero aqui teria transformado essa
-- correcao num defeito silencioso. Reescreve-lo
-- como var do dbt criaria a segunda copia que diverge na primeira edicao — e nada
-- reprovaria, porque contar zero violacao contra o limiar errado tem exatamente a mesma
-- aparencia de contar zero contra o certo.
--
-- NAO E DIMENSAO. Nenhum fato aponta para ela, nada se junta a ela por chave. E uma tabela
-- de PARAMETROS DECLARADOS lida por escalar. Chama-se FACT_ pelo precedente de
-- FACT_INGESTION_RUN, que tambem nao e medida de negocio nenhuma; inventar um terceiro
-- prefixo para dois casos seria vocabulario novo sem ganho.
--
-- `label` E SEMPRE 'synthetic', NUNCA 'proxy'. Um proxy e medido em outro lugar e usado
-- aqui; estes numeros nao sao medidos em lugar nenhum. Nenhuma fonte deste repo mede
-- cesta, cadencia de compra ou disponibilidade — e a Source recusa exportar a referencia
-- se o seed trouxer qualquer outro rotulo.
{{ config(materialized = 'table') }}

select
    premise_key,
    value                                                   as premise_value,
    unit                                                    as premise_unit,
    label                                                   as premise_label,
    rationale
from {{ source('stage', 'STG_ORDER_PREMISE') }}
