-- PRECO OBSERVADO — e a PRESENCA DA LINHA E O SORTIMENTO.
--
-- GRAO: (snapshot_date, wh, source_product_id). 60.384 linhas.
-- TIPO: observed.
--
-- CHAMA-SE SNAPSHOT, NAO DAILY, e a diferenca nao e estilistica. Ha lacunas medidas: os
-- dias 2026-08-17 a 08-23 nao existem e NAO podem ser recuperados — a API da Mercadona so
-- serve o preco de hoje, e backfill e impossivel nesta fonte. "Daily" prometeria uma
-- continuidade que a fonte nao tem, e um mart que a assumisse interpolaria a lacuna em
-- silencio. Quem precisar saber se um dia foi observado consulta FACT_INGESTION_RUN.
--
-- O EIXO `wh` FICA NO GRAO, e isso e medido: 187 produtos tem unit_price diferente entre
-- os 4 armazens no mesmo dia, e 0 divergem dentro do mesmo armazem. Um fato de preco sem
-- `wh` mediria uma media que nao existe em armazem nenhum.
--
-- NAO EXISTE FACT_ASSORTMENT, de proposito: a presenca de uma linha aqui JA e a afirmacao
-- "este produto estava no catalogo deste armazem neste dia". Medido que os catalogos
-- divergem (4.283 a 4.335 produtos por armazem, so 3.859 nos quatro), entao a informacao
-- e real — mas uma tabela separada seria a mesma informacao numa segunda copia.
--
-- CUIDADO AO LER AUSENCIA: uma linha faltando pode significar "produto fora do catalogo"
-- OU "dia nao observado". So o cruzamento com FACT_INGESTION_RUN distingue os dois.
{{ config(materialized = 'table') }}

select
    cast(to_char(p.ingestion_date, 'YYYYMMDD') as number(38, 0)) as date_key,
    p.ingestion_date                                        as snapshot_date,
    p.warehouse                                             as wh,
    p.source_product_id,

    -- Aponta a VERSAO da dimensao vigente no dia do snapshot, nao a versao corrente:
    -- e o que faz um preco antigo se ligar ao nome que o produto tinha na epoca.
    d.product_sk,
    p.primary_category_id,

    p.unit_price,

    -- O PRECO DE UNIDADE COMPRAVEL, ao lado do valor cru. Sao iguais em 99,9% das linhas; nas
    -- 10 combinacoes produto x armazem vendidas a granel sem `unit_size`, a API devolve
    -- `reference_price * 99` — o teto do seletor de peso, e nao um preco de consumo. E contra
    -- ESTA coluna que FACT_ORDER_ITEM fecha: o pedido cobrou a porcao, nao o teto.
    p.purchasable_unit_price,
    p.price_basis,
    p.net_content_kg_l,
    p.min_bunch_amount,

    p.bulk_price,
    p.reference_price,
    p.reference_format,
    p.previous_unit_price,
    p.tax_percentage,

    -- PREMISSA DO CONSUMIDOR, NAO DADO DA FONTE. A Mercadona nao declara moeda em nenhum
    -- campo (CONTRACT.md 4.6), e o Silver nao inventa. A var existe no dbt_project desde o
    -- primeiro dia justamente para o Gold; materializa-la aqui faz a premissa viajar junto
    -- do numero, em vez de ficar so num arquivo de configuracao que ninguem le ao olhar um
    -- dashboard. Trocar de moeda e editar a var, nao cacar 'EUR' espalhado por modelo.
    '{{ var("currency") }}'                                 as currency,

    -- Preco sem imposto, derivado do preco COMPRAVEL. tax_percentage e da fonte; a divisao
    -- e deste modelo, e por isso a coluna tem nome proprio em vez de sobrescrever
    -- unit_price. Derivar do valor cru daria 3.663,00 sem imposto para 150 g de langostino.
    case
        when p.tax_percentage is null or p.tax_percentage = 0 then null
        else round(p.purchasable_unit_price / (1 + p.tax_percentage / 100), 4)
    end                                                     as unit_price_ex_tax,

    p.is_pack,
    p.pack_size,
    p.unit_size,
    p.unit_name,
    p.selling_method,
    p.published,
    p.is_new_arrival
from {{ source('stage', 'STG_PRODUCT_PRICE') }} p
left join {{ ref('dim_product') }} d
  on  d.source_product_id = p.source_product_id
 and  p.ingestion_date   >= d.valid_from
 and  (d.valid_to is null or p.ingestion_date < d.valid_to)
