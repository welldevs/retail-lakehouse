-- THE STAMPED BAND IS THE ONE THE CUSTOMER'S AGE PRODUCES, and this test recomputes it from scratch.
--
-- `buyer_age_band` reaches Silver from inside the `order_placed` payload, not from a join
-- with `silver_customer`. That is deliberate and right — what matters is the band that held
-- AT THE TIME OF THE ORDER — but it carries the same price as `demand_group`: the stamp does not
-- correct itself, and a wrong stamp breaks nothing visible.
--
-- THE FAILURE MODE THIS CATCHES. The band is what CHOOSES the weight vector that draws the
-- basket. Stamping `35_49` on a 70-year-old customer would make that order come out with the mix
-- of a cohort that is not theirs: no error, no null, all totals closing, and a
-- `MART_DEMAND_COHORT` that assigns the wrong slice to the wrong band — exactly the reading
-- the mart exists to allow.
--
-- WHY THE OBVIOUS VARIANT WOULD NOT CATCH IT. `buyer_age_band is not null` would pass easily, and
-- so would `accepted_values`: a swapped stamp is still one of the four valid
-- bands. Only recomputing against `birth_year` tells the two apart.
--
-- AGE IS THE DIFFERENCE IN YEARS, not the full date. It is the same convention with which
-- `birth_year` was built in the OLTP Source (`reference_year - age`) and the same the
-- generator uses. Comparing with the full date here would produce a second definition of age and
-- this test would fail by convention, not by defect.
with pedido as (

    select
        o.order_id,
        o.wh,
        o.ingestion_date,
        o.customer_id,
        o.customer_ingestion_date,
        o.buyer_age_band,
        year(o.ingestion_date) - c.birth_year as idade
    from {{ ref('silver_order') }} o
    join {{ ref('silver_customer') }} c
      on c.customer_id = o.customer_id
     and c.ingestion_date = o.customer_ingestion_date

)

select
    order_id,
    wh,
    ingestion_date,
    customer_id,
    idade,
    buyer_age_band,
    case
        when idade <= 34 then 'LT35'
        when idade <= 49 then '35_49'
        when idade <= 64 then '50_64'
        else 'GE65'
    end as faixa_esperada
from pedido
where buyer_age_band is distinct from case
        when idade <= 34 then 'LT35'
        when idade <= 49 then '35_49'
        when idade <= 64 then '50_64'
        else 'GE65'
    end
