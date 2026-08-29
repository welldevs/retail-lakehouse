-- Um cliente de W so pede de W. Restricao 1 das cinco que o FAQ ja tinha escrito antes de
-- Orders existir, e a mais facil de quebrar em silencio.
--
-- Medido na Fase 1: 0 CEPs cruzam armazem — a area de servico de cada um e disjunta. Logo um
-- pedido cujo cliente pertence a outro armazem nao e "um caso raro", e defeito.
--
-- ESTE E O TERCEIRO CHECK DA MESMA REGRA, e os tres sao independentes de proposito: o gerador
-- so escolhe entre clientes daquele armazem, `orders-validate` reconfere contra a referencia,
-- e este olha o dado DEPOIS de atravessar RAW e parquet. Mesma escada de
-- assert_customer_lives_in_its_warehouse_service_area.
--
-- O JOIN E POR ID, e a comparacao e de `wh`. Nao ha nome envolvido: a licao dos 370 de 370
-- municipios grafados diferente pelas duas fontes do INE vale aqui tambem.
select
    o.order_id,
    o.wh          as warehouse_do_pedido,
    c.wh          as warehouse_do_cliente,
    o.customer_id
from {{ ref('silver_order') }} o
left join (
    select distinct customer_id, wh from {{ ref('silver_customer') }}
) c on o.customer_id = c.customer_id
where c.customer_id is null
   or c.wh is distinct from o.wh
