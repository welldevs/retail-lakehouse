-- AS PREMISSAS DE ESTOQUE NAO PODEM SE CONTRADIZER ENTRE SI.
--
-- IRMA DIRETA DE assert_order_premises_are_internally_coherent, e escrita ANTES de o
-- primeiro saldo existir — de proposito. La o defeito viveu tres fases porque ninguem
-- cruzou duas premissas declaradas separadamente; aqui as quatro nascem cruzadas.
--
-- AS TRES CONTRADICOES POSSIVEIS, e o que cada uma faria com a medicao:
--
--   ponto de reposicao <= prazo do fornecedor
--       A ordem chega DEPOIS de a prateleira esvaziar, sempre. A ruptura que aparecesse
--       mediria aritmetica do seed, e nao variacao de demanda — exatamente o que
--       `slot_adherence_rate` media antes da Fase 7.
--
--   nivel alvo <= ponto de reposicao
--       A ordem de compra nasce com quantidade zero ou negativa: repor ate um nivel abaixo
--       do gatilho e uma instrucao que nao se pode obedecer.
--
--   estoque inicial <= ponto de reposicao
--       Toda serie comeca ja precisando repor. O primeiro dia da janela viraria um pico
--       artificial de ordens de compra que nao diz nada sobre a operacao.
--
-- NAO HA AQUI NENHUMA ASSERCAO SOBRE RUPTURA, GIRO OU COBERTURA. Mesma regra do teste
-- irmao: afere-se a coerencia das ENTRADAS, nunca o resultado. Um teste que olhasse a taxa
-- de ruptura abencoaria quem ajustasse `opening_days_of_demand` ate ela ficar bonita.
with p as (

    select premise_key, cast(value as double) as v
    from {{ ref('stock_premises') }}

),

premissa as (

    select
        max(case when premise_key = 'opening_days_of_demand' then v end) as inicial,
        max(case when premise_key = 'reorder_point_days'     then v end) as ponto,
        max(case when premise_key = 'reorder_target_days'    then v end) as alvo,
        max(case when premise_key = 'supplier_lead_days'     then v end) as prazo
    from p

)

select
    'reorder_point_days' as premissa,
    ponto                as declarado,
    prazo                as comparado,
    'ponto de reposicao <= prazo do fornecedor: a ordem chega depois da ruptura, sempre' as regra
from premissa
where ponto <= prazo

union all

select
    'reorder_target_days',
    alvo,
    ponto,
    'nivel alvo <= ponto de reposicao: a ordem de compra nasce com quantidade <= 0'
from premissa
where alvo <= ponto

union all

select
    'opening_days_of_demand',
    inicial,
    ponto,
    'estoque inicial <= ponto de reposicao: toda serie comeca precisando repor'
from premissa
where inicial <= ponto
