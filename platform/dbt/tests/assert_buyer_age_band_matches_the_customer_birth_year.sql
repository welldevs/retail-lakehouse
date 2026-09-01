-- A FAIXA CARIMBADA E A QUE A IDADE DO CLIENTE PRODUZ, e este teste a recalcula do zero.
--
-- `buyer_age_band` chega ao Silver de dentro do payload de `order_placed`, e nao de um join
-- com `silver_customer`. Isso e deliberado e esta certo — o que importa e a faixa que valia
-- NO MOMENTO DO PEDIDO — mas tem o mesmo preco de `demand_group`: o carimbo nao se corrige
-- sozinho, e um carimbo errado nao quebra nada visivel.
--
-- O MODO DE FALHA QUE ISTO PEGA. A faixa e o que ESCOLHE o vetor de pesos que sorteia a
-- cesta. Carimbar `35_49` num cliente de 70 anos faria aquele pedido sair com o mix de uma
-- coorte que nao e a dele: sem erro, sem nulo, com todos os totais fechando, e com um
-- `MART_DEMAND_COHORT` que atribui a fatia errada a faixa errada — exatamente a leitura que
-- o mart existe para permitir.
--
-- POR QUE A VARIANTE OBVIA NAO PEGARIA. `buyer_age_band is not null` passaria com folga, e
-- `accepted_values` tambem: um carimbo trocado continua sendo uma das quatro faixas
-- validas. So recalcular contra `birth_year` distingue as duas coisas.
--
-- A IDADE E A DIFERENCA DE ANOS, e nao a data completa. E a mesma convencao com que
-- `birth_year` foi construido na Source de OLTP (`reference_year - idade`) e a mesma que o
-- gerador usa. Comparar com data completa aqui produziria uma segunda definicao de idade e
-- este teste reprovaria por convencao, nao por defeito.
with pedido as (

    select
        o.order_id,
        o.wh,
        o.ingestion_date,
        o.customer_id,
        o.customer_ingestion_date,
        o.buyer_age_band,
        year(o.ingestion_date) - c.birth_year as idade
    from {{ ref('silver_order') }} o
    join {{ ref('silver_customer') }} c
      on c.customer_id = o.customer_id
     and c.ingestion_date = o.customer_ingestion_date

)

select
    order_id,
    wh,
    ingestion_date,
    customer_id,
    idade,
    buyer_age_band,
    case
        when idade <= 34 then 'LT35'
        when idade <= 49 then '35_49'
        when idade <= 64 then '50_64'
        else 'GE65'
    end as faixa_esperada
from pedido
where buyer_age_band is distinct from case
        when idade <= 34 then 'LT35'
        when idade <= 49 then '35_49'
        when idade <= 64 then '50_64'
        else 'GE65'
    end
