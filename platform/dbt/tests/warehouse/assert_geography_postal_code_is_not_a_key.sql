-- TESTE INVERTIDO: prova que postal_code NAO e chave, e que a modelagem sabe disso.
--
-- Medido: 29 CEPs cruzam fronteira de municipio no escopo das 4 AUFs. Se alguem um dia
-- "simplificar" DIM_GEOGRAPHY para o grao de CEP, esses 29 casos colapsariam e a soma por
-- municipio ficaria errada — silenciosamente, porque a tabela continuaria parecendo certa.
--
-- Este teste falha se a divergencia DESAPARECER, que e o oposto do usual. Nao e capricho:
-- um teste que so verifica o esperado nao protege a razao pela qual o grao e composto. Se
-- um dia o INE republicar o Callejero sem nenhum CEP compartilhado, este teste falha e
-- alguem revisita a decisao com o dado na mao, em vez de manter uma chave composta por
-- inercia.
--
-- Falha quando NENHUM CEP cruza municipio (ou seja, quando o grao composto perdeu o motivo
-- de existir e o teste precisa ser reavaliado).
with compartilhados as (

    select postal_code, count(distinct province_code || municipality_code) as municipios
    from {{ ref('dim_geography') }}
    group by 1
    having count(distinct province_code || municipality_code) > 1

)

select
    'nenhum CEP cruza municipio: o grao composto de dim_geography perdeu a justificativa'
        as motivo,
    (select count(*) from compartilhados)  as ceps_compartilhados
where (select count(*) from compartilhados) = 0
