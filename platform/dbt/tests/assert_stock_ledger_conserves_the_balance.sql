-- O SALDO FECHA, TODO DIA, EM TODA SERIE.
--
-- TRES INVARIANTES QUE NAO SE IMPLICAM, e por isso viajam juntas num teste so:
--
--   1. CONSERVACAO       fechamento = abertura + recebido - atendido
--   2. NAO-NEGATIVIDADE  fechamento >= 0
--   3. RUPTURA HONESTA   se faltou alguma coisa, a prateleira tem de estar vazia
--
-- POR QUE A TERCEIRA E A QUE PEGA ERRO DE VERDADE. As duas primeiras um `sum()` errado ainda
-- passa: um job que simplesmente ignorasse a demanda que nao cabe conservaria o saldo e
-- nunca ficaria negativo — e reportaria ruptura zero para sempre. A terceira amarra as duas
-- pontas: `units_short > 0` so e legitimo com `closing_balance = 0`. Ruptura com saldo
-- sobrando significa que o job deixou de atender o que tinha como atender.
--
-- E A QUARTA, sobre a demanda: atendido + faltou = pedido. Sem ela, perder uma unidade no
-- caminho nao quebraria nenhuma das outras tres.
--
-- SOMA NENHUMA DENUNCIA ESTA CLASSE DE DEFEITO — e a mesma licao da densidade da base de
-- clientes na Fase 6. O total de unidades consumidas continuaria batendo com o Silver com
-- qualquer uma destas invariantes quebrada; o que quebra e a REPARTICAO no tempo.
select
    wh,
    source_product_id,
    stock_date,
    opening_balance,
    units_received,
    units_demanded,
    units_fulfilled,
    units_short,
    closing_balance,
    case
        when closing_balance <> opening_balance + units_received - units_fulfilled
            then 'conservacao: fechamento <> abertura + recebido - atendido'
        when closing_balance < 0
            then 'saldo negativo: a prateleira nao deve o que nao tem'
        when units_short > 0 and closing_balance > 0
            then 'ruptura com saldo sobrando: o job deixou de atender o que podia'
        when units_fulfilled + units_short <> units_demanded
            then 'demanda perdida: atendido + faltou <> pedido'
    end as violacao
from {{ ref('silver_stock_ledger') }}
where closing_balance <> opening_balance + units_received - units_fulfilled
   or closing_balance < 0
   or (units_short > 0 and closing_balance > 0)
   or units_fulfilled + units_short <> units_demanded
