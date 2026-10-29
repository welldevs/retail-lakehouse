-- THE BALANCE CLOSES, EVERY DAY, IN EVERY SERIES.
--
-- THREE INVARIANTS THAT DO NOT IMPLY EACH OTHER, which is why they travel together in one test:
--
--   1. CONSERVATION      closing = opening + received - served
--   2. NON-NEGATIVITY    closing >= 0
--   3. HONEST STOCKOUT   if something was short, the shelf must be empty
--
-- WHY THE THIRD IS THE ONE THAT CATCHES REAL ERRORS. A wrong `sum()` still passes the first
-- two: a job that simply ignored the demand that does not fit would conserve the balance and
-- never go negative — and would report zero stockout forever. The third ties both
-- ends together: `units_short > 0` is only legitimate with `closing_balance = 0`. A stockout with balance
-- left over means the job failed to serve what it was able to serve.
--
-- AND THE FOURTH, about demand: served + short = ordered. Without it, losing a unit along
-- the way would break none of the other three.
--
-- NO SUM REVEALS THIS CLASS OF DEFECT — it is the same lesson as the customer base density
-- in Phase 6. The total units consumed would still match Silver with
-- any of these invariants broken; what breaks is the ALLOCATION over time.
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
