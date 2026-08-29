-- A ARVORE DE CATEGORIAS, achatada em dois niveis.
--
-- GRAO: uma categoria de nivel 2 (151). CHAVE: category_id.
-- TIPO: observed.
--
-- GLOBAL, sem eixo de armazem nem de data: medido que a arvore tem exatamente 151
-- categorias de nivel 2 em CADA um dos 4 armazens, identicas. Dar um eixo `wh` a esta
-- dimensao criaria 604 linhas para representar 151 coisas.
--
-- DOIS NIVEIS E FINAL, nao simplificacao: verificado nos snapshots que nenhum no de nivel
-- 2 carrega filhos. E o nivel 1 (26 categorias) existe APENAS nesta arvore — a API
-- responde 404/410 para ids de nivel 1 (CONTRACT.md secao 6), entao ele nao e uma
-- entidade consultavel, so um agrupamento. Por isso vira atributo achatado e nao uma
-- dimensao propria com chave estrangeira.
{{ config(materialized = 'table') }}

select
    md5(cast(category_id as varchar))       as category_sk,

    category_id,
    category_name,
    category_order,

    parent_category_id,
    parent_category_name,
    parent_category_order
from {{ source('stage', 'STG_CATEGORY') }}
