-- O GRUPO DE DEMANDA E CARIMBADO NO EVENTO, e por isso ele pode ficar velho.
--
-- `demand_group` chega ao Silver de dentro do payload de `order_placed`, e nao de um join
-- com o catalogo. Isso e deliberado e esta certo: o que importa e o grupo que valia NO
-- MOMENTO DO PEDIDO, e nao o que o de-para diria hoje. Mas tem um preco — o carimbo nao se
-- corrige sozinho.
--
-- O MODO DE FALHA QUE ISTO PEGA: uma janela regerada pela metade. Alguem edita
-- `demand_category_mapping_seed.csv`, reexporta a referencia e roda `orders-refresh` em
-- dois dos quatro armazens. Os outros dois ficam com o carimbo antigo. O resultado e uma
-- janela em que o MESMO produto pertence a dois grupos, e qualquer agregacao por
-- `demand_group` mistura duas calibracoes diferentes — sem erro, sem nulo, sem total
-- quebrado. Exatamente a classe de defeito mais cara deste repo.
--
-- POR QUE A VARIANTE OBVIA NAO PEGARIA. `demand_group is not null` passaria com folga: as
-- duas metades TEM grupo, so que grupos diferentes. E comparar contra o de-para exigiria
-- reimplementar a resolucao de coringas em SQL — uma segunda implementacao da mesma regra,
-- que divergiria da primeira no primeiro ajuste. A conferencia contra o de-para e do
-- `validate` da Source, que le o mesmo arquivo que o gerador leu; aqui o que se verifica e
-- a CONSISTENCIA INTERNA da janela, que e a pergunta que so esta camada consegue fazer.
--
-- O grao e (produto, categoria, subgrupo) porque e essa a trinca que o de-para resolve: o
-- mesmo produto em duas categorias pode legitimamente cair em grupos diferentes.
select
    source_product_id,
    category_id,
    subgroup_id,
    count(distinct demand_group)                                  as grupos_distintos,
    string_agg(distinct demand_group, ' | ' order by demand_group) as grupos,
    min(ingestion_date)                                           as primeiro_dia,
    max(ingestion_date)                                           as ultimo_dia
from {{ ref('silver_order_line') }}
where demand_group is not null
group by 1, 2, 3
having count(distinct demand_group) > 1
