-- THE SAME PERSON CANNOT BE IN TWO BANDS IN THE SAME WINDOW — unless they have a birthday
-- within it, and in that case the two bands must be NEIGHBORS and in the right order.
--
-- THE FAILURE MODE THIS CATCHES: a half-regenerated window. Someone edits the band
-- boundaries, re-exports the reference and runs `orders-refresh` on two of the four warehouses.
-- The result is a window in which the same customer appears as `35_49` one day and `GE65` the
-- next, and any per-cohort aggregation mixes two definitions — no error, no null, no
-- broken total. It is the sibling of `assert_demand_group_is_stable_across_the_window`, on the
-- buyer axis instead of the product one.
--
-- WHY THE OBVIOUS VARIANT WOULD NOT CATCH IT. `assert_buyer_age_band_matches_the_customer_birth_year`
-- checks each order against the customer record and would pass: both halves are internally
-- correct, each under its own band definition. What only THIS layer can ask is
-- whether the window is coherent WITH ITSELF.
--
-- A BIRTHDAY IS LEGITIMATE, which is why the condition is not "a single band". The current window has
-- four days, but the model does not depend on that: what is required is that the transition be to the
-- next band and forward in time. Two non-adjacent bands, or a customer who
-- GETS YOUNGER over the window, are defects.
with per_customer as (

    select
        customer_id,
        wh,
        buyer_age_band,
        min(ingestion_date) as first_day,
        max(ingestion_date) as last_day
    from {{ ref('silver_order') }}
    where buyer_age_band is not null
    group by 1, 2, 3

),

banded as (

    select
        p.*,
        case buyer_age_band
            when 'LT35'  then 1
            when '35_49' then 2
            when '50_64' then 3
            when 'GE65'  then 4
        end as band_rank
    from per_customer p

),

extremes as (

    select
        customer_id,
        wh,
        count(*)                                            as bands,
        min(band_rank)                                        as first_rank,
        max(band_rank)                                        as last_rank,
        string_agg(buyer_age_band, ' | ' order by band_rank)  as bands_seen
    from banded
    group by 1, 2

)

select
    e.customer_id,
    e.wh,
    e.bands,
    e.bands_seen,
    younger.last_day   as end_of_younger_band,
    older.first_day   as start_of_older_band
from extremes e
join banded younger
  on younger.customer_id = e.customer_id and younger.wh = e.wh
 and younger.band_rank = e.first_rank
join banded older
  on older.customer_id = e.customer_id and older.wh = e.wh
 and older.band_rank = e.last_rank
where e.bands > 1
  and (
      -- a jump of more than one band: no birthday explains it
      e.last_rank - e.first_rank > 1
      -- or the older band starts BEFORE the younger one ends: the customer got younger
      or older.first_day < younger.last_day
  )
