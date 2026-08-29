-- GRAO: (customer_id) — a base VIGENTE, uma linha por cliente. As versoes anteriores
--        ficam em DIM_CUSTOMER; aqui e o retrato atual.
--
-- PERGUNTA QUE RESPONDE: quem sao os clientes, onde moram e em que contexto demografico.
--
-- SINTETICO E OBSERVADO NA MESMA LINHA, com os rotulos separados de proposito:
--   * a PESSOA (nome, sexo, idade) e sintetica — amostrada, nao observada;
--   * o ENDERECO (municipio, via, CEP, faixa de numeracao) e real, do Callejero;
--   * a POPULACAO do municipio e observada, do INE.
-- Quem consulta este mart precisa saber qual metade e qual. E por isso que
-- municipality_population esta aqui: sem contexto, "3 clientes em Ajalvir" nao diz nada;
-- com ele, diz que 3 clientes cobrem um municipio de 4.500 habitantes.
{{ config(materialized = 'table') }}

select
    c.customer_id,
    c.customer_sk,
    c.valid_from                                            as generated_at,

    c.wh,
    w.province_name,

    c.province_code,
    c.municipality_code,
    c.municipality_name,
    c.postal_code,
    g.is_home_municipality,
    g.municipality_population,

    -- Endereco: atribuicao sintetica sobre um tramo real. address_is_street_level diz que
    -- o Callejero declara que a via nao tem numeracao — nao que o numero se perdeu.
    c.street_name,
    c.house_number,
    c.numbering_type,
    c.address_is_street_level,

    c.full_name,
    c.sex_label,
    c.birth_year,
    c.age_at_ingestion,
    c.age_band
from {{ ref('dim_customer') }} c
join {{ ref('dim_warehouse') }} w  on w.wh = c.wh
left join {{ ref('dim_geography') }} g on g.geography_sk = c.geography_sk
where c.is_current
