-- TODA CHEGADA TEM UMA ORDEM QUE A EXPLICA, EMITIDA `supplier_lead_days` DIAS ANTES.
--
-- E O TESTE DA REALIMENTACAO, e por isso ele existe separado da conservacao do saldo. A
-- conservacao verifica a aritmetica DENTRO de um dia; esta verifica o laco ENTRE dias, que
-- e a unica razao de este job nao ser SQL. Um `sum() over (...)` passaria na conservacao —
-- ele so nunca produziria uma chegada.
--
-- AS TRES COISAS QUE ELE AMARRA:
--
--   1. Nenhuma chegada sem ordem. `units_received > 0` no dia D exige uma linha da MESMA
--      serie no dia D - prazo com `reorder_units` igual.
--   2. Nenhuma ordem chega antes da hora. `reorder_eta` tem de ser exatamente
--      `stock_date + supplier_lead_days`.
--   3. Nenhuma ordem se perde DENTRO da janela. Uma ordem cuja chegada caberia na janela
--      tem de ter chegado. As que chegam DEPOIS do ultimo dia sao legitimas e ficam de
--      fora: pedido em transito no fim do periodo e propriedade real de qualquer ledger, e
--      exigir que sumisse seria exigir que o modelo mentisse. (Foi exatamente esta a
--      assercao errada que reprovou a pergunta S7 do spike na primeira execucao.)
--
-- O PRAZO NAO E CONSTANTE DESTE ARQUIVO: vem de `stock_premises`, que projeta o seed que o
-- job leu. Cravar 2 aqui criaria a segunda copia, e no dia em que o seed mudasse este teste
-- passaria a conferir um laco que nenhuma linha percorreu.
with prazo as (

    select cast(value as integer) as supplier_lead_days
    from {{ ref('stock_premises') }}
    where premise_key = 'supplier_lead_days'

),

janela as (

    select max(stock_date) as ultimo_dia from {{ ref('silver_stock_ledger') }}

),

ledger as (

    select * from {{ ref('silver_stock_ledger') }}

),

-- 1. chegada orfa: recebeu sem que nada tenha sido pedido no dia certo
chegada_sem_ordem as (

    select
        c.wh, c.source_product_id, c.stock_date,
        'chegada sem ordem que a explique ' || p.supplier_lead_days || ' dia(s) antes' as violacao
    from ledger c
    cross join prazo p
    left join ledger o
           on o.wh = c.wh
          and o.source_product_id = c.source_product_id
          and o.stock_date = c.stock_date - p.supplier_lead_days
          and o.reorder_units = c.units_received
    where c.units_received > 0
      and o.wh is null

),

-- 2. eta fora do prazo declarado
eta_errada as (

    select
        l.wh, l.source_product_id, l.stock_date,
        'reorder_eta <> stock_date + supplier_lead_days' as violacao
    from ledger l
    cross join prazo p
    where l.reorder_units > 0
      and l.reorder_eta is distinct from l.stock_date + p.supplier_lead_days

),

-- 3. ordem que caberia na janela e nunca chegou
ordem_perdida as (

    select
        o.wh, o.source_product_id, o.stock_date,
        'ordem com chegada dentro da janela que nunca chegou' as violacao
    from ledger o
    cross join prazo p
    cross join janela j
    left join ledger c
           on c.wh = o.wh
          and c.source_product_id = o.source_product_id
          and c.stock_date = o.reorder_eta
          and c.units_received = o.reorder_units
    where o.reorder_units > 0
      and o.reorder_eta <= j.ultimo_dia
      and c.wh is null

)

select * from chegada_sem_ordem
union all
select * from eta_errada
union all
select * from ordem_perdida
