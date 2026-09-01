-- NENHUM TITULAR DE CONTA E MENOR DE IDADE.
--
-- O ACHADO QUE ISTO CONGELA, e por que ele precisou de DOIS testes. Medido em 2026-08-31:
-- 18,01% dos clientes tinham menos de 18 anos (3.602 de 20.000), com `age_at_ingestion` de
-- 0 a 100 — havia titular de conta recem-nascido. A Fase 5 tratou isso no dominio errado:
-- criou `min_buyer_age` e filtrou na hora do PEDIDO, deixando o cadastro intacto sob o
-- argumento de que `silver_customer` era projecao fiel da populacao residente. O argumento
-- estava certo sobre o que a Source entrega e errado sobre o que um cadastro e — uma base
-- de clientes nao e um censo. `assert_no_order_comes_from_a_minor` continua existindo e
-- continua certo; ele so nao era suficiente, porque provava que ninguem COMPRAVA sendo
-- menor, nunca que ninguem EXISTIA sendo menor.
--
-- TODAS AS ingestion_date, e nao so a corrente. Um menor numa geracao antiga continua sendo
-- um menor na base: `silver_customer` empilha o historico de proposito, os pedidos fixam
-- `customer_ingestion_date` e o export de pedidos resolve a versao MAIS RECENTE de cada
-- cliente — filtrar por `is_latest_ingestion` aqui deixaria a porta dos fundos aberta.
--
-- A IDADE MINIMA NAO E CONSTANTE NESTE ARQUIVO: vem de `customer_premises`, que projeta o
-- proprio seed. Repeti-la aqui criaria um segundo lugar para muda-la, e os dois divergiriam.
--
-- E A IDADE E `age_at_ingestion`, nao a idade hoje. E o atributo que o gerador realmente
-- amostrou (`birth_year = ano(ingestion_date) - idade`), entao a subtracao devolve o valor
-- exato sorteado. Comparar contra o relogio faria este teste mudar de resposta sozinho com
-- o passar do tempo, o que e o oposto do que um teste faz.
with limite as (

    select cast(value as integer) as min_customer_age
    from {{ ref('customer_premises') }}
    where premise_key = 'min_customer_age'

)

select
    c.ingestion_date,
    c.wh,
    c.customer_id,
    c.birth_year,
    c.age_at_ingestion,
    l.min_customer_age
from {{ ref('silver_customer') }} c
cross join limite l
where c.age_at_ingestion < l.min_customer_age

union all

-- O PISO DA PROPRIA PREMISSA. Mesmo mecanismo, mesma razao e mesmo numero de
-- `assert_no_order_comes_from_a_minor`: ler a premissa prova que o DADO obedece o que foi
-- declarado, mas sozinho nao impede a premissa de ser revertida — com o limiar em zero nao
-- existe menor e o teste passaria com uma base de recem-nascidos. O seed pode SUBIR a idade
-- minima quando quiser; abaixo de 18 ele nao pode ir em silencio.
select
    current_date  as ingestion_date,
    'PREMISSA'    as wh,
    '-'           as customer_id,
    null          as birth_year,
    null          as age_at_ingestion,
    l.min_customer_age
from limite l
where l.min_customer_age < 18
