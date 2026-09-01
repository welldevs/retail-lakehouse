-- GRAO: (order_date, wh) — uma linha por armazem por dia da janela.
--
-- PERGUNTA QUE RESPONDE: quanto tempo cada etapa leva, e quantos pedidos estouram o
-- limiar DECLARADO de separacao.
--
-- O LIMIAR NAO E CONSTANTE DESTE MODELO. Ele vem de FACT_ORDER_PREMISE, que veio do seed
-- que o GERADOR leu, cujo sha256 esta no manifesto de cada particao do RAW. Cravar 90 aqui
-- (ou numa var do dbt) criaria a segunda copia do numero, e no dia em que alguem editar o
-- seed sem editar a copia este mart passa a contar violacoes contra um limiar que nenhum
-- pedido conheceu — sem reprovar nada, porque zero violacao contra o limiar errado tem
-- exatamente a aparencia de zero contra o certo.
--
-- DOIS ACHADOS QUE FICARAM REGISTRADOS AQUI POR TRES FASES, E FORAM CORRIGIDOS NA FASE 7.
-- O texto abaixo conta os dois porque o motivo de eles terem demorado tanto vale mais que
-- a correcao.
--
-- PRIMEIRO. `sla_minutes_picking` valia 90 e era INALCANCAVEL por construcao: a separacao
-- leva `minutes_per_line_picked` (2) x numero de linhas, e `basket_lines_max` e 40 — teto
-- de 80 minutos. Medido em 2026-06 na janela de entao: p50 = 36, p90 = 62, MAXIMO = 80,00,
-- ZERO violacoes. E nao porque a operacao fosse boa: porque as tres premissas nao se
-- cruzavam.
--
-- SEGUNDO. A janela de entrega prometida quase nunca era a janela em que a entrega
-- acontecia — e o desvio era para CEDO, nao para tarde. Medido em 2026-09-01 sobre 86.803
-- entregas: 73.124 (84%) chegavam ANTES de a janela abrir. A causa era aritmetica e estava
-- nas premissas: `slot_lead_hours` sorteava o inicio da janela entre 2h e 24h depois da
-- colocacao, enquanto a soma dos marcos entregava em no maximo 8,5h.
--
-- POR QUE ELES FICARAM REGISTRADOS TANTO TEMPO, E O QUE MUDOU. A regra que este projeto
-- segue e recusar ajuste de premissa ate a saida agradar, e ela esta certa. O que faltava
-- era a distincao: mexer numa premissa para melhorar um numero e uma coisa; tornar duas
-- premissas MUTUAMENTE COERENTES e outra. Uma janela que abre 24h depois de um pedido que o
-- proprio modelo entrega em no maximo 8,5h nao e um resultado indesejado — e um modelo
-- internamente contraditorio. O mesmo vale para um alerta acima do teto aritmetico. Sob a
-- regra antiga, os dois eram intocaveis; sob a distincao, os dois eram defeito de modelo.
--
-- COMO A CORRECAO SE PROTEGE DE VIRAR O QUE ELA RECUSA. As tres premissas passaram a ser
-- DERIVADAS das outras linhas do mesmo seed, e `assert_order_premises_are_internally_coherent`
-- afere a DERIVACAO — nunca o resultado. Nao ha nele nenhuma assercao sobre quanto deu a
-- adesao. Quem no futuro ajustar `slot_lead_hours_*` ou `sla_minutes_picking` para consertar
-- um KPI derruba um teste, inclusive com um valor que caiba na faixa plausivel: medido, trocar
-- o limiar de 60 por 75 reprova.
--
-- O QUE O MART CONTINUA FAZENDO IGUAL: carrega `sla_minutes`, `max_picking_minutes` e
-- `orders_breaching_sla` LADO A LADO. Antes isso servia para entender um zero estrutural;
-- agora serve para o leitor ver contra que limiar a contagem foi feita. A razao nao mudou —
-- um numero de violacoes sem o limiar ao lado nao e conferivel.
--
-- O QUE MUDA AQUI E O QUE SE PUBLICA. `orders_delivered_within_slot` sozinho diz 8% e deixa
-- quem le concluir "a operacao atrasa", que e o oposto do que acontece. Chegar cedo e
-- chegar tarde sao problemas operacionais OPOSTOS, e colapsar os dois em "fora da janela"
-- apaga qual dos dois e. Por isso as tres contagens viajam separadas.
--
-- PERCENTIL SOBRE PEDIDOS QUE ALCANCARAM O MARCO, e nao sobre todos. `percentile_cont`
-- ignora nulo, e nulo aqui significa "a etapa nao aconteceu". Um p90 de tempo de entrega
-- que contasse os cancelados como zero mediria a operacao de outra empresa. As contagens
-- ao lado dizem quantos pedidos entraram em cada percentil, para que nenhum p90 seja lido
-- sem saber sobre quantos ele foi calculado.
{{ config(materialized = 'table') }}

with sla as (

    -- `max(case when ...)` e nao `where premise_key = ...`: um filtro que nao casa devolve
    -- ZERO linhas, e o cross join abaixo esvaziaria o mart inteiro — o mart vira 0 linhas e
    -- nenhum teste de not_null reprova sobre tabela vazia. Com o max(), a CTE devolve
    -- SEMPRE uma linha; se a premissa sumir do seed, a coluna vira nula e o teste
    -- not_null em sla_minutes reprova, que e o comportamento que se quer.
    select
        max(case when premise_key = 'sla_minutes_picking'
                 then premise_value end)                    as sla_minutes_picking
    from {{ ref('fact_order_premise') }}

)

select
    f.order_date,
    f.date_key,
    d.year_month,
    f.wh,

    count(*)                                                as orders_placed,

    -- ---- quantos pedidos alcancaram cada marco (o denominador de cada percentil) ----
    count(f.minutes_to_confirm)                             as orders_with_confirm,
    count(f.minutes_to_pick)                                as orders_with_pick,
    count(f.minutes_to_deliver)                             as orders_with_deliver,

    -- ---- p50 / p90 por etapa -------------------------------------------------------
    percentile_cont(0.5) within group (order by f.minutes_to_confirm) as p50_minutes_to_confirm,
    percentile_cont(0.9) within group (order by f.minutes_to_confirm) as p90_minutes_to_confirm,
    percentile_cont(0.5) within group (order by f.minutes_to_pick)    as p50_minutes_to_pick,
    percentile_cont(0.9) within group (order by f.minutes_to_pick)    as p90_minutes_to_pick,
    percentile_cont(0.5) within group (order by f.minutes_to_dispatch) as p50_minutes_to_dispatch,
    percentile_cont(0.9) within group (order by f.minutes_to_dispatch) as p90_minutes_to_dispatch,
    percentile_cont(0.5) within group (order by f.minutes_to_deliver) as p50_minutes_to_deliver,
    percentile_cont(0.9) within group (order by f.minutes_to_deliver) as p90_minutes_to_deliver,
    percentile_cont(0.5) within group (order by f.minutes_placed_to_delivered)
                                                            as p50_minutes_placed_to_delivered,
    percentile_cont(0.9) within group (order by f.minutes_placed_to_delivered)
                                                            as p90_minutes_placed_to_delivered,

    -- ---- o limiar declarado, e a distancia ate ele ---------------------------------
    max(s.sla_minutes_picking)                              as sla_minutes,
    max(f.minutes_to_pick)                                  as max_picking_minutes,
    count_if(f.minutes_to_pick > s.sla_minutes_picking)     as orders_breaching_sla,

    -- ---- a promessa comercial, que e o SLA que a operacao de fato tem ---------------
    -- Diferente do limiar de separacao: a janela de entrega e escolhida por pedido, esta
    -- no log, e o evento order_delivered declara se foi cumprida. Aqui ha variacao real.
    count(f.delivered_within_slot)                          as orders_with_slot_outcome,
    count_if(f.delivered_within_slot)                       as orders_delivered_within_slot,
    -- Cedo e tarde separados de proposito: sao problemas operacionais opostos, e
    -- "fora da janela" nao diz qual deles esta acontecendo. O desequilibrio entre os dois
    -- e o que denunciou a incoerencia das premissas em 2026-09-01: 73.124 cedo contra 84
    -- por cento do total. Estas duas colunas sao o instrumento que tornou isso visivel.
    count_if(f.delivered_at is not null
             and f.delivered_at < f.delivery_slot_start)    as orders_delivered_before_slot,
    count_if(f.delivered_at is not null
             and f.delivered_at > f.delivery_slot_end)      as orders_delivered_after_slot,
    round(
        div0(count_if(f.delivered_within_slot), count(f.delivered_within_slot)), 4
    )                                                       as slot_adherence_rate
from {{ ref('fact_order') }} f
join {{ ref('dim_date') }} d on d.date_key = f.date_key
cross join sla s
group by 1, 2, 3, 4
