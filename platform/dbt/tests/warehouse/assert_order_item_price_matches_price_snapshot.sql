-- O FECHO CRUZADO ENTRE AS DUAS SOURCES: o preco pago num pedido e o preco que a Mercadona
-- publicou naquele armazem naquele dia.
--
-- E O TESTE MAIS CARO DE ENGANAR DO WAREHOUSE, porque liga dois fatos produzidos por
-- caminhos que nunca se tocam: FACT_PRICE_SNAPSHOT vem da API real (observed);
-- FACT_ORDER_ITEM vem de um log de eventos sintetico gerado meses depois, que COPIOU o
-- preco da referencia exportada. Se o gerador tivesse inventado preco, arredondado, ou
-- escolhido produto de outro armazem, e aqui que aparece.
--
-- A JUNCAO E PELO PRODUTO CUMPRIDO, NAO PELO PEDIDO — e essa e a parte que a variante
-- obvia erra. Numa linha substituida o cliente pagou o preco do SUBSTITUTO, que e outro
-- source_product_id; juntar por `source_product_id` faria o teste comparar o preco do
-- produto A com o valor pago pelo produto B e reprovar 4.670 linhas corretas. Juntar pelo
-- cumprido e o que torna a substituicao verificavel em vez de invisivel.
--
-- (wh, price_as_of) ENTRAM NA CHAVE, e nao so o produto: 187 produtos custam diferente
-- entre os 4 armazens no mesmo dia. Um teste que ignorasse o eixo de armazem passaria
-- comparando contra o preco de outro lugar.
--
-- LINHAS SEM PRODUTO CUMPRIDO FICAM DE FORA — removidas, e as de pedido que nunca chegou a
-- separacao. Nao ha preco pago para conferir, e exigir um seria exigir que o modelo
-- inventasse o numero que este teste existe para proibir.
--
-- A COMPARACAO E CONTRA `purchasable_unit_price`, e nao contra o `unit_price` cru. Nas 10
-- combinacoes produto x armazem vendidas a granel sem `unit_size`, a API devolve
-- `reference_price * 99` — o teto do seletor de peso, nao um preco de consumo. Casar contra
-- o valor cru faria este teste EXIGIR que o pedido tivesse cobrado 1.084,05 EUR por 150 g de
-- langostino, ou seja, exigir de volta o defeito. O valor cru viaja no relatorio de falha ao
-- lado, para que a diferenca seja visivel quando houver.
--
-- Falha com uma linha por divergencia.
select
    i.order_id,
    i.line_no,
    i.wh,
    i.price_as_of,
    i.fulfilled_source_product_id,
    i.unit_price_paid,
    p.purchasable_unit_price                                as preco_no_catalogo,
    p.unit_price                                            as preco_cru_da_fonte,
    p.price_basis,
    case
        when p.source_product_id is null then 'produto cumprido ausente do catalogo daquele armazem e dia'
        else 'preco pago diverge do catalogo'
    end                                                     as motivo
from {{ ref('fact_order_item') }} i
left join {{ ref('fact_price_snapshot') }} p
       on  p.snapshot_date      = i.price_as_of
      and  p.wh                 = i.wh
      and  p.source_product_id  = i.fulfilled_source_product_id
where i.fulfilled_source_product_id is not null
  and (p.source_product_id is null or p.purchasable_unit_price <> i.unit_price_paid)
