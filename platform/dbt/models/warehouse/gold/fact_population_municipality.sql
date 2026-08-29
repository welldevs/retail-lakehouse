-- POPULACAO OBSERVADA por municipio, sexo e ano. O DENOMINADOR do modelo.
--
-- GRAO: (province_code, municipality_code, sex_label, year). 21.410 linhas, 1996-2025.
-- TIPO: observed (INE, tabela 29005, cifras oficiales).
--
-- POR QUE FATO E NAO DIMENSAO. Tem grao temporal (29 anos medidos), e aditiva por
-- municipio e por sexo, e e o denominador de toda razao per-capita ou de penetracao. Isso
-- e a definicao de fato. O que vai para DIM_GEOGRAPHY e apenas o valor do ANO CORRENTE,
-- como atributo de tamanho — conveniencia de slicing, nao a serie.
--
-- O AGREGADO 'Total' JA FOI EXCLUIDO NO RECORTE, e vale dizer por que aqui tambem: a
-- fonte mistura o agregado com o detalhe na MESMA coluna sex_label. Somar os tres rotulos
-- da exatamente o dobro da populacao. E a mesma armadilha que na Fase 1 inflou a piramide
-- etaria em 2,03x com age_label, e ela nao falha — produz um numero plausivel. Um teste em
-- schema.yml reconfere Hombres + Mujeres contra o 'Total' da propria fonte.
--
-- LIMITACAO DECLARADA: este fato e ANUAL e municipal. Nao existe faixa etaria por
-- municipio nas tabelas que esta plataforma ingere — a 29005 nao tem coluna de idade e a
-- 31304 so tem provincia. A piramide etaria dos clientes sinteticos usa a distribuicao
-- PROVINCIAL como proxy, e essa aproximacao esta declarada no contrato da Source, nao
-- escondida aqui.
{{ config(materialized = 'table') }}

select
    md5(concat_ws('|', province_code, municipality_code))   as municipality_key,
    province_code,
    municipality_code,
    municipality_name,
    wh,

    sex_label,
    year,
    reference_date,

    population_value,
    -- is_secret vem da fonte: o INE suprime valores de municipios muito pequenos por
    -- sigilo estatistico. Null aqui NAO e dado faltando por erro nosso, e a coluna
    -- companheira e o que distingue os dois casos.
    is_secret,

    -- year e reference_date PODEM discordar, e nao e erro de calculo: e a convencao do
    -- INE (year=2025 registrado com Fecha em 2024-12-31). Nenhum dos dois e "corrigido"
    -- para bater com o outro; quem precisar da data exata usa reference_date.
    (year = (select max(year) from {{ source('stage', 'STG_POPULATION_MUNICIPALITY') }}))
                                                            as is_latest_year
from {{ source('stage', 'STG_POPULATION_MUNICIPALITY') }}
