-- Fecha manifesto -> RAW -> parquet, e fecha o FOLD por dois caminhos independentes.
--
-- O QUE TORNA ESTE TESTE DIFERENTE do irmao de clientes. La o manifesto declara uma contagem
-- de linhas de um arquivo, e o Silver reconta as mesmas linhas. Aqui o manifesto declara
-- `order_rows`, `net_amount_picked` e `substituted_lines` — numeros que so existem depois de
-- DOBRAR o log. A Source os calculou em Python, percorrendo eventos; o Silver os recalcula em
-- SQL, com window functions. Sao duas implementacoes do mesmo fold, e uma divergencia
-- significa que uma delas esta errada.
--
-- POR QUE VALOR MONETARIO ENTRA. Contar linhas nao pega uma perda PARCIAL de payload: um
-- evento `order_picked` cujo `picked_amount` se perdeu na travessia manteria a contagem e
-- zeraria a receita. Somar dinheiro pega. Mesmo motivo pelo qual o teste de clientes confere
-- `house_number_null` alem de `customer_rows`.
with observado as (

    select
        e.ingestion_date,
        e.wh,
        count(*)                                                       as event_rows,
        count(distinct e.order_id)                                     as order_rows,
        sum(case when e.event_type = 'order_line_substituted' then 1 else 0 end)
                                                                       as substituted_lines,
        sum(case when e.event_type = 'order_line_removed' then 1 else 0 end)
                                                                       as removed_lines
    from {{ ref('silver_order_event') }} e
    group by e.ingestion_date, e.wh

),

do_fold as (

    select
        ingestion_date,
        wh,
        sum(line_count_placed)                                         as line_rows,
        count(distinct customer_id)                                    as customers_used,
        sum(case when price_source <> 'observed' then 1 else 0 end)    as carried_forward_orders,
        cast(sum(gross_amount_placed) as decimal(14, 2))               as gross_amount_placed,
        cast(sum(coalesce(net_amount, 0)) as decimal(14, 2))           as net_amount_picked
    from {{ ref('silver_order') }}
    group by ingestion_date, wh

)

select
    m.ingestion_date,
    m.wh,
    m.declared_event_rows,        o.event_rows,
    m.declared_order_rows,        o.order_rows,
    m.declared_line_rows,         f.line_rows,
    m.declared_substituted_lines, o.substituted_lines,
    m.declared_removed_lines,     o.removed_lines,
    m.declared_gross_amount_placed, f.gross_amount_placed,
    m.declared_net_amount_picked,   f.net_amount_picked
from {{ ref('silver_orders_manifest') }} m
join observado o on m.ingestion_date = o.ingestion_date and m.wh = o.wh
join do_fold   f on m.ingestion_date = f.ingestion_date and m.wh = f.wh
where m.declared_event_rows           <> o.event_rows
   or m.declared_order_rows           <> o.order_rows
   or m.declared_line_rows            <> f.line_rows
   or m.declared_customers_used       <> f.customers_used
   or m.declared_substituted_lines    <> o.substituted_lines
   or m.declared_removed_lines        <> o.removed_lines
   or m.declared_carried_forward_orders <> f.carried_forward_orders
   or m.declared_gross_amount_placed  <> f.gross_amount_placed
   or m.declared_net_amount_picked    <> f.net_amount_picked
