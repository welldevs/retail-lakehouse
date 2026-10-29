-- THE STOCK PREMISES MUST NOT CONTRADICT EACH OTHER.
--
-- A DIRECT SIBLING OF assert_order_premises_are_internally_coherent, and written BEFORE the
-- first balance existed — on purpose. There the defect lived for three phases because nobody
-- crossed two separately declared premises; here all four are born crossed.
--
-- THE THREE POSSIBLE CONTRADICTIONS, and what each would do to the measurement:
--
--   reorder point <= supplier lead time
--       The order arrives AFTER the shelf empties, always. Any stockout that appeared
--       would measure the seed's arithmetic, not demand variation — exactly what
--       `slot_adherence_rate` measured before Phase 7.
--
--   target level <= reorder point
--       The purchase order is born with zero or negative quantity: replenishing up to a level below
--       the trigger is an instruction that cannot be obeyed.
--
--   opening stock <= reorder point
--       Every series starts already needing replenishment. The first day of the window would become an
--       artificial spike of purchase orders that says nothing about the operation.
--
-- THERE IS NO ASSERTION HERE ABOUT STOCKOUT, TURNOVER OR COVER. Same rule as the sibling
-- test: the coherence of the INPUTS is checked, never the result. A test that looked at the stockout
-- rate would bless whoever tuned `opening_days_of_demand` until it looked nice.
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
