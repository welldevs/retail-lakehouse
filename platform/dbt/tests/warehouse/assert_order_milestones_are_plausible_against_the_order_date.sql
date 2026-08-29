-- TODO MARCO DE UM PEDIDO CAI DENTRO DA JANELA DO PROPRIO PEDIDO.
--
-- POR QUE ESTE TESTE EXISTE, e ele existe por um defeito real e nao por precaucao. Na
-- primeira carga do Marco 7 TODO timestamp atravessou a fronteira no ANO 56.648.666: o
-- DuckDB anota a unidade do timestamp so no LogicalType moderno do parquet e deixa o
-- ConvertedType legado em NONE, e o leitor do Snowflake caia no legado e assumia
-- milissegundos onde havia microssegundos.
--
-- O QUE NAO PEGOU O DEFEITO, e e essa lista que justifica o teste:
--
--   * a reconferencia do carregador — compara CONTAGEM de linhas, e ela estava certa;
--   * os 166 nos do dbt, todos verdes — as duracoes viraram numeros grandes, nao nulos
--     nem erros, e nenhum tipo mudou;
--   * assert_gold_grains_are_unique — o grao continuou unico;
--   * assert_order_funnel_totals_match_fact_order — um funil e feito de
--     `count_if(marco is not null)`, e "nao nulo" continua exato quando o instante esta
--     56 milhoes de anos deslocado;
--   * assert_fact_order_amount_equals_sum_of_items — dinheiro nao passa por timestamp.
--
-- Quem apontou foi um humano lendo 80.000.060 minutos de separacao num mart. Este teste e
-- o que substitui "alguem olhar" por "a construcao reprova".
--
-- O QUE ELE AFIRMA. `placed_at` cai no dia do pedido (o eixo de particao E a data da
-- colocacao, por contrato); nenhum marco antecede a colocacao; e nenhum marco passa de 7
-- dias depois dela. O teto de 7 dias vem das premissas declaradas, nao de gosto: o maior
-- intervalo possivel e minutes_to_return_max = 2.880 minutos (2 dias) somado aos marcos
-- anteriores, e 7 dias deixa folga sem deixar de reprovar um erro de UNIDADE, que desloca
-- em ordens de grandeza e nunca em horas.
--
-- POR QUE A VARIANTE OBVIA NAO SERVE. `placed_at is not null` ja e testado por not_null e
-- nao diz nada sobre o valor. E comparar marcos entre si (picked_at >= picking_started_at)
-- passaria alegremente no defeito real: os dois estavam deslocados pelo MESMO fator, entao
-- a ordem relativa entre eles continuava certa. So ancorar contra uma data que veio por
-- OUTRO caminho — `order_date`, que viaja como date32 e nao tem ambiguidade de unidade —
-- torna o deslocamento visivel.
--
-- Falha com uma linha por pedido implausivel.
{% set marcos = [
    'confirmed_at', 'payment_failed_at', 'cancelled_at', 'picking_started_at',
    'picked_at', 'dispatched_at', 'delivered_at', 'delivery_failed_at', 'returned_at'
] %}

select
    order_id,
    order_date,
    placed_at,
    last_event_at,
    case
        when cast(placed_at as date) <> order_date then 'colocacao fora do dia do pedido'
        {% for marco in marcos %}
        when {{ marco }} < placed_at then '{{ marco }} antes da colocacao'
        when datediff('day', order_date, {{ marco }}) > 7 then '{{ marco }} mais de 7 dias depois'
        {% endfor %}
    end                                                     as motivo
from {{ ref('fact_order') }}
where cast(placed_at as date) <> order_date
   {% for marco in marcos %}
   or {{ marco }} < placed_at
   or datediff('day', order_date, {{ marco }}) > 7
   {% endfor %}
