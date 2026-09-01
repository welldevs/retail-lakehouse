-- AS PREMISSAS DO PEDIDO NAO PODEM SE CONTRADIZER ENTRE SI.
--
-- O ACHADO QUE ISTO CONGELA. Medido em 2026-09-01, sobre 86.803 pedidos entregues:
-- 73.124 deles — 84% — foram ENTREGUES ANTES DE A JANELA PROMETIDA ABRIR. E, no mesmo
-- seed, `sla_minutes_picking` valia 90 minutos enquanto a separacao mais longa que o
-- modelo consegue produzir custa 80 (`basket_lines_max` x `minutes_per_line_picked`):
-- um alerta acima do TETO ARITMETICO, que nunca dispara.
--
-- POR QUE ISSO E CORRECAO DE MODELO, E NAO AJUSTE DE RESULTADO. A distincao importa mais
-- que o numero: mexer numa premissa ate a saida agradar e o que este projeto recusa; tornar
-- duas premissas mutuamente coerentes e outra coisa. Uma janela que ABRE 24h depois de um
-- pedido que o proprio modelo entrega em no maximo 8,5h nao e um resultado indesejado — e
-- um modelo internamente contraditorio, a mesma familia do "argumento verdadeiro sobre a
-- propriedade errada" da Fase 6. `slot_adherence_rate` marcava 8,5% fixos e
-- `orders_breaching_sla` era estruturalmente zero: os dois indicadores existiam sem medir
-- nada.
--
-- O QUE ESTE TESTE VERIFICA, E O QUE ELE DE PROPOSITO NAO VERIFICA.
--
-- Ele afere a DERIVACAO, nunca o resultado. Nao existe aqui nenhuma assercao sobre quanto
-- deu a adesao — e isso e deliberado. Depois desta correcao fica trivial continuar mexendo
-- em `slot_lead_hours_*` ate a adesao parecer bonita, e um teste que olhasse a adesao
-- ABENCOARIA esse ajuste em vez de pega-lo. Aferindo a derivacao, quem no futuro mexer
-- nestes numeros para consertar um KPI derruba um teste. A adesao que sair e REPORTADA,
-- nao alvo — mesma regra da taxa ancorada da Fase 6.
--
-- A CADEIA DECLARADA, que este arquivo NAO repete: as cinco etapas entre `order_placed` e
-- `order_delivered` ja moram no seed, uma linha cada, e o gerador as percorre nessa ordem.
-- O ciclo e a soma delas. Cravar 52 e 512 aqui criaria um segundo lugar onde o ciclo vive,
-- e os dois divergiriam no primeiro ajuste de `minutes_to_picking_max`.
--
-- MEDIDO AO ESCREVER ESTE TESTE, com o seed anterior: FAIL 3. `slot_lead_hours_min` = 2
-- contra 1 derivado, `slot_lead_hours_max` = 24 contra 8, e `sla_minutes_picking` = 90
-- contra um teto de 80. As quatro clausulas guardam falhas DIFERENTES, e nenhuma implica
-- outra: janela cedo demais, janela tarde demais, limiar inexercivel e limiar constante.
with p as (

    select premise_key, cast(value as double) as v
    from {{ ref('order_premises') }}

),

premissa as (

    select
        max(case when premise_key = 'minutes_to_payment_min'   then v end) as pagamento_min,
        max(case when premise_key = 'minutes_to_payment_max'   then v end) as pagamento_max,
        max(case when premise_key = 'minutes_to_picking_min'   then v end) as separacao_inicio_min,
        max(case when premise_key = 'minutes_to_picking_max'   then v end) as separacao_inicio_max,
        max(case when premise_key = 'minutes_per_line_picked'  then v end) as por_linha,
        max(case when premise_key = 'basket_lines_min'         then v end) as linhas_min,
        max(case when premise_key = 'basket_lines_mode'        then v end) as linhas_moda,
        max(case when premise_key = 'basket_lines_max'         then v end) as linhas_max,
        max(case when premise_key = 'minutes_to_dispatch_min'  then v end) as despacho_min,
        max(case when premise_key = 'minutes_to_dispatch_max'  then v end) as despacho_max,
        max(case when premise_key = 'minutes_to_delivered_min' then v end) as entrega_min,
        max(case when premise_key = 'minutes_to_delivered_max' then v end) as entrega_max,
        max(case when premise_key = 'slot_lead_hours_min'      then v end) as janela_min,
        max(case when premise_key = 'slot_lead_hours_max'      then v end) as janela_max,
        max(case when premise_key = 'sla_minutes_picking'      then v end) as sla_separacao,
        max(case when premise_key = 'sla_picking_percentile'   then v end) as sla_percentil
    from p

),

ciclo as (

    select
        *,
        pagamento_min + separacao_inicio_min + (por_linha * linhas_min)
                      + despacho_min + entrega_min                        as ciclo_min,
        pagamento_max + separacao_inicio_max + (por_linha * linhas_max)
                      + despacho_max + entrega_max                        as ciclo_max,
        por_linha * linhas_max                                            as separacao_teto,
        por_linha * linhas_moda                                           as separacao_moda,
        -- O p-esimo percentil da triangular(a, b, m) no ramo acima da moda. `b` e
        -- `basket_lines_max + 1` porque e assim que o gerador sorteia
        -- (`rng.triangular(low, high + 1, mode)`), e `floor` porque ele trunca com
        -- `int()`. Copiar a formula sem o +1 daria 29 linhas em vez de 30 — um erro de
        -- uma linha que ninguem notaria e que faria o limiar sair 2 minutos mais baixo.
        floor((linhas_max + 1)
              - sqrt((1 - sla_percentil) * ((linhas_max + 1) - linhas_min)
                                         * ((linhas_max + 1) - linhas_moda)))
              * por_linha                                                 as sla_derivado
    from premissa

)

-- A JANELA ABRE CEDO O BASTANTE PARA CABER A ENTREGA MAIS RAPIDA QUE O MODELO PRODUZ.
select
    'slot_lead_hours_min' as premissa,
    janela_min            as declarado,
    ceil(ciclo_min / 60)  as derivado,
    'a menor antecedencia e o ciclo mais curto arredondado para cima: '
      || ciclo_min || ' min' as regra
from ciclo
where janela_min <> ceil(ciclo_min / 60)

union all

-- E FECHA CEDO O BASTANTE PARA QUE A ENTREGA MAIS LENTA AINDA POSSA ALCANCA-LA. Este e o
-- limite que estava violado: uma janela abrindo em 24h contra um ciclo de no maximo 8,5h
-- torna a promessa impossivel de cumprir por CONSTRUCAO, e a adesao vira um numero sobre a
-- aritmetica do seed em vez de sobre a operacao.
select
    'slot_lead_hours_max',
    janela_max,
    floor(ciclo_max / 60),
    'a maior antecedencia e o ciclo mais longo arredondado para baixo: '
      || ciclo_max || ' min'
from ciclo
where janela_max <> floor(ciclo_max / 60)

union all

-- O LIMIAR DE ALERTA MORA ABAIXO DO TETO ARITMETICO. Acima dele o alerta e inexercivel:
-- nenhuma cesta possivel demora tanto.
select
    'sla_minutes_picking',
    sla_separacao,
    separacao_teto,
    'limiar >= teto aritmetico (basket_lines_max x minutes_per_line_picked): inexercivel'
from ciclo
where sla_separacao >= separacao_teto

union all

-- E E EXATAMENTE O QUE A POLITICA DECLARADA PRODUZ. Sem esta clausula as duas anteriores
-- so definem uma FAIXA — entre 29 e 79 minutos — e escolher 79 para quase zerar os alertas
-- passaria nos dois. A politica (`sla_picking_percentile`) e o que pode ser discutido; o
-- limiar e consequencia dela. Quem quiser alertar menos muda a politica, onde a mudanca
-- fica visivel como o que ela e.
select
    'sla_minutes_picking',
    sla_separacao,
    sla_derivado,
    'derivado de sla_picking_percentile sobre a triangular declarada da cesta'
from ciclo
where sla_separacao <> sla_derivado

union all

-- E ACIMA DA SEPARACAO MODAL. Abaixo dela, a cesta TIPICA ja violaria o SLA, e o alerta
-- deixaria de distinguir o pedido problematico do pedido comum — que e a outra forma de um
-- limiar nao medir nada.
select
    'sla_minutes_picking',
    sla_separacao,
    separacao_moda,
    'limiar <= separacao modal (basket_lines_mode x minutes_per_line_picked): alerta constante'
from ciclo
where sla_separacao <= separacao_moda
