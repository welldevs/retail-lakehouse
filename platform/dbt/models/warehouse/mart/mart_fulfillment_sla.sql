-- GRAO: (order_date, wh) — 16 linhas.
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
-- ACHADO QUE FICA REGISTRADO EM VEZ DE CORRIGIDO: `sla_minutes_picking` = 90 e INALCANCAVEL
-- por construcao. A separacao leva `minutes_per_line_picked` (2) x numero de linhas, e
-- `basket_lines_max` e 40 — teto de 80 minutos. Medido na janela: p50 = 36, p90 = 62,
-- MAXIMO = 80,00. Zero violacoes, e nao porque a operacao seja boa: porque as tres
-- premissas nao se cruzam. Baixar o limiar ate o alerta acender seria adaptar a premissa ao
-- resultado desejado, que e o oposto de verificar. Por isso o mart carrega `sla_minutes`,
-- `max_picking_minutes` e `orders_breaching_sla` LADO A LADO — quem le ve 90, ve 80 e ve 0,
-- e entende o zero em vez de comemora-lo. Gatilho para mexer: `basket_lines_max` x
-- `minutes_per_line_picked` passar de `sla_minutes_picking`.
--
-- SEGUNDO ACHADO, DA MESMA FAMILIA E TAMBEM REGISTRADO EM VEZ DE CORRIGIDO: a janela de
-- entrega prometida quase nunca e a janela em que a entrega acontece — e o desvio e para
-- CEDO, nao para tarde. Medido nas 6.046 entregas: 5.166 chegam ANTES de a janela abrir,
-- 471 dentro, 409 depois. Mediana de 4,6h entre colocacao e entrega contra 12,8h ate o
-- inicio da janela.
--
-- A causa e aritmetica e esta nas premissas: `slot_lead_hours` sorteia o inicio da janela
-- entre 2h e 24h depois da colocacao, enquanto a soma dos marcos entrega em ~4,6h. As duas
-- premissas foram declaradas separadamente e nunca foram conciliadas. Nao e defeito do
-- warehouse — o numero atravessou fielmente — e mexer no seed para a taxa "melhorar" seria
-- ajustar a entrada ate a saida agradar.
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
    -- ZERO linhas, e o cross join abaixo esvaziaria o mart inteiro — 16 linhas viram 0 e
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
    -- "fora da janela" nao diz qual deles esta acontecendo. Medido: 5.166 cedo, 409 tarde.
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
