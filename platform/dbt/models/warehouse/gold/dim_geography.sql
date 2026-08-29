-- A GEOGRAFIA ANALITICA: onde o cliente mora e para onde se entrega.
--
-- GRAO: (province_code, municipality_code, postal_code) — 699 linhas, destiladas de
-- 304.952 tramos do Callejero.
-- CHAVE: geography_sk (surrogate). Chave natural: as tres colunas acima.
-- TIPO: observed (INE Callejero + INE 29005).
--
-- POR QUE O GRAO E ESSE, medido:
--   * postal_code SOZINHO NAO SERVE — 29 CEPs cruzam fronteira de municipio. Chavear so
--     pelo CEP fundiria dois municipios numa linha e a soma por municipio ficaria errada.
--   * municipality sozinho perde a granularidade de entrega, que e onde a roteirizacao
--     acontece.
--   * (municipio, CEP) e o menor grao que responde as duas perguntas. E `wh` e FUNCAO
--     desse par, nao mais uma dimensao: medido que 0 CEPs cruzam armazem, entao a
--     atribuicao e inequivoca.
--
-- OS DOIS NOMES DE MUNICIPIO viajam juntos com rotulos distintos porque os dois produtos
-- do INE grafam o MESMO municipio de forma diferente em 370 de 370 casos: o Callejero usa
-- caixa alta com artigo entre parenteses ("BRUC (EL)"), a tabela 29005 usa caixa mista com
-- artigo posposto ("Bruc, El"). Isso foi descoberto quando a validacao da Fase 1 reprovou
-- 200/200 clientes. Deixar so um nome esconderia a divergencia; dar o mesmo rotulo aos
-- dois seria armadilha. O que se compara entre fontes e sempre o CODIGO.
--
-- O QUE ESTA DIMENSAO DELIBERADAMENTE NAO TEM: section_code, street_id, faixa de
-- numeracao. Sao 517 mil linhas de resolucao de endereco que servem ao GERADOR de
-- clientes, nao ao analista, e ficam no S3. Gatilho para um DIM_CENSUS_SECTION: uma
-- pergunta real por seccao censitaria (seriam 10.030 linhas no escopo).
--
-- municipality_population e o valor do ANO CORRENTE, atributo de TAMANHO. A serie
-- historica (1996-2025) e fato e vive em FACT_POPULATION_MUNICIPALITY. Somar esta coluna
-- sobre varias linhas do mesmo municipio conta a mesma populacao varias vezes — reduza ao
-- municipio distinto antes.
{{ config(materialized = 'table') }}

select
    md5(concat_ws('|', province_code, municipality_code, postal_code)) as geography_sk,

    province_code,
    municipality_code,
    postal_code,

    municipality_name_ine                                   as municipality_name,
    municipality_name_callejero,
    wh,
    is_home_municipality,

    municipality_population,
    tramo_count,
    section_count,
    street_count
from {{ source('stage', 'STG_GEOGRAPHY') }}
