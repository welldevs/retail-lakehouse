-- CLIENTE, SCD2 — e o SCD2 aqui existe por um motivo diferente do de DIM_PRODUCT.
--
-- GRAO: uma versao de cliente, (customer_id, valid_from).
-- CHAVE: customer_sk. Chave natural: customer_id.
-- TIPO: a PESSOA e synthetic; o ENDERECO e observed. Os dois convivem na mesma linha e a
-- distincao importa: municipio, via, CEP e faixa de numeracao vem sempre de uma linha real
-- do Callejero. O cliente e inventado; o lugar onde ele mora nao.
--
-- POR QUE SCD2 E NAO UMA TABELA FIXA. A base de clientes e RECARREGAVEL de proposito — vai
-- crescer para dar densidade real. Cada regeracao cria uma ingestion_date nova, e o
-- comportamento do gerador tem duas consequencias que a dimensao precisa representar:
--
--   * AUMENTAR A BASE E ADITIVO. O gerador consome uma unica random.Random(seed) em ordem
--     fixa e nada antes do laco depende de `count`, entao os primeiros N clientes de uma
--     geracao maior sao byte a byte os mesmos de antes. Verificado ponta a ponta: 200 ->
--     5.000 preservou os 200 (test_aumentar_count_e_aditivo_nao_reembaralha, e os sha256
--     conferidos no dado real). Crescer NAO invalida quem ja existe.
--   * TROCAR A DATA OU A SEED NAO E ADITIVO. Outra ingestion_date preserva a IDADE e
--     desloca birth_year — a mesma pessoa, mais velha. Outra seed troca as pessoas por
--     tras dos mesmos ids. O manifesto registra as duas coisas em `history`.
--
-- Uma tabela fixa nao teria como representar isso; o SCD2 tem. valid_from = a
-- ingestion_date da geracao. Fatos de pedido devem apontar para customer_sk (a versao
-- vigente na data do pedido) e carregar customer_id como chave de negocio.
--
-- O ENDERECO E ATRIBUICAO, NAO IDENTIDADE. Dois clientes podem receber o mesmo tramo E o
-- mesmo numero — endereco identico — e isso nao e colisao nem bug: e a consequencia de o
-- endereco ser sorteado entre os candidatos reais do municipio. Por isso o endereco e
-- ATRIBUTO desta dimensao e nao um DIM_ADDRESS: nao ha entidade "endereco" com vida
-- propria no modelo. Gatilho para criar uma: um pedido poder ser entregue num endereco
-- diferente do cadastro.
--
-- age_at_ingestion, NAO idade calculada contra hoje. A idade e o atributo que o gerador
-- realmente sorteou; calcula-la contra a data corrente faria a dimensao mudar sozinha a
-- cada aniversario sem nenhuma nova observacao.
{{ config(materialized = 'table') }}

with versoes as (

    select
        customer_id,
        ingestion_date                                      as valid_from,
        wh,
        province_code,
        province_name,
        municipality_code,
        municipality_name,
        postal_code,
        street_name,
        numbering_type,
        house_number,
        candidate_index,
        first_name,
        last_name,
        sex_label,
        birth_year,
        age_at_ingestion,
        is_latest_ingestion
    from {{ source('stage', 'STG_CUSTOMER') }}

)

select
    md5(concat_ws('|', customer_id, cast(valid_from as varchar))) as customer_sk,

    customer_id,
    valid_from,
    -- Fechado-aberto [valid_from, valid_to), pelo mesmo motivo de DIM_PRODUCT: as
    -- geracoes nao sao diarias e subtrair um dia inventaria uma data sem observacao.
    --
    -- A janela e repetida em vez de nomeada por `window w as (...)`: o Snowflake nao
    -- suporta a clausula WINDOW. O DuckDB suporta, e foi assim que isto passou no parse
    -- local e so falhou no motor de verdade.
    lead(valid_from) over (partition by customer_id order by valid_from) as valid_to,
    lead(valid_from) over (partition by customer_id order by valid_from) is null
                                                            as is_current,

    wh,

    -- Geografia. Junta-se a DIM_GEOGRAPHY pelo mesmo hash natural, e nao por nome: os dois
    -- produtos do INE grafam municipio de forma diferente em 370 de 370 casos.
    md5(concat_ws('|', province_code, municipality_code, postal_code)) as geography_sk,
    province_code,
    province_name,
    municipality_code,
    municipality_name,
    postal_code,

    -- Endereco: atribuicao sintetica derivada de um tramo real.
    street_name,
    numbering_type,
    house_number,
    (house_number is null)                                  as address_is_street_level,
    -- Auditoria: aponta a linha exata do address_candidates.json usado na geracao. So tem
    -- significado contra a referencia identificada no manifesto daquela particao.
    candidate_index,

    first_name,
    last_name,
    first_name || ' ' || last_name                          as full_name,
    sex_label,
    birth_year,
    age_at_ingestion,

    -- Faixas etarias para slicing. Rotulos declarados aqui e nao herdados de nenhuma
    -- fonte: sao decisao deste modelo, nao vocabulario do INE.
    case
        when age_at_ingestion < 18 then '00-17'
        when age_at_ingestion < 30 then '18-29'
        when age_at_ingestion < 45 then '30-44'
        when age_at_ingestion < 65 then '45-64'
        else '65+'
    end                                                     as age_band,

    is_latest_ingestion                                     as is_current_generation
from versoes
