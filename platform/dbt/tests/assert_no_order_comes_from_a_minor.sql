-- NENHUM PEDIDO NASCE DE UM MENOR DE IDADE.
--
-- O ACHADO QUE ISTO CONGELA. Medido em 2026-08-31: 18,01% dos clientes tinham menos de 18
-- anos — 3.602 de 20.000 — com `age_at_ingestion` indo de 0 a 100. Havia titular de conta
-- recem-nascido. Isso nao era defeito da Source de OLTP, que declara no contrato que a
-- idade vem da distribuicao POPULACIONAL provincial do INE e entrega exatamente isso; o que
-- nunca fora declarado era a diferenca entre RESIDENTE e QUEM COLOCA UM PEDIDO.
--
-- POR QUE ISTO VIRA TESTE, e nao so uma premissa. Enquanto a idade nao fazia nada, um
-- comprador de tres anos era inofensivo. A partir do momento em que ela governa a demanda,
-- 18% da base entraria na faixa '-35 anos' do MAPA sendo crianca — e a calibracao ficaria
-- errada por construcao, sem nenhum total quebrar. Um teste aqui e o que impede a premissa
-- de ser silenciosamente revertida por quem nao souber por que ela existe.
--
-- A IDADE MINIMA NAO E CONSTANTE NESTE ARQUIVO: vem de `order_premises`, que projeta o
-- proprio seed. Repeti-la aqui criaria um segundo lugar para muda-la, e os dois divergiriam.
--
-- MEDIDO AO ESCREVER ESTE TESTE: baixar `min_buyer_age` para 0 no seed faz este teste
-- passar, porque com o limiar em zero nao existe menor. Ler a premissa e o comportamento
-- certo — o teste verifica que o DADO obedece o que foi declarado — mas sozinho ele nao
-- protege a premissa de ser revertida. Por isso a segunda metade abaixo: um PISO, e nao uma
-- copia do valor. O seed pode subir a idade minima quando quiser; abaixo de 18 ele nao pode
-- ir em silencio, porque foi exatamente ai que a base tinha titular de conta de tres anos.
with limite as (

    select cast(value as integer) as min_buyer_age
    from {{ ref('order_premises') }}
    where premise_key = 'min_buyer_age'

)

select
    o.order_id,
    o.wh,
    o.ingestion_date,
    o.customer_id,
    c.birth_year,
    year(o.ingestion_date) - c.birth_year as idade_no_pedido,
    l.min_buyer_age
from {{ ref('silver_order') }} o
join {{ ref('silver_customer') }} c
  on c.customer_id = o.customer_id
 and c.ingestion_date = o.customer_ingestion_date
cross join limite l
where year(o.ingestion_date) - c.birth_year < l.min_buyer_age

union all

-- O PISO DA PROPRIA PREMISSA. Devolve uma linha — e portanto reprova — se alguem baixar
-- min_buyer_age abaixo de 18. As colunas casam com as da consulta acima porque `union all`
-- exige; o que importa e a ultima.
select
    'PREMISSA'                                          as order_id,
    '-'                                                 as wh,
    current_date                                        as ingestion_date,
    '-'                                                 as customer_id,
    null                                                as birth_year,
    null                                                as idade_no_pedido,
    l.min_buyer_age
from limite l
where l.min_buyer_age < 18
