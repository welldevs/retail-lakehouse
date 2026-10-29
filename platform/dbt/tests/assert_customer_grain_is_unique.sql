-- GRAIN of silver_customer: (ingestion_date, customer_id).
--
-- It is not customer_id alone — the same ids reappear in each regeneration of the base, and that
-- is deliberate (CONTRACT.md section 3: uniqueness holds within the ingestion_date, not across
-- them). Nor is it (ingestion_date, wh, customer_id): the wh is already embedded in the
-- id itself, so cust_mad1_000042 and cust_vlc1_000042 never collide, and adding wh to the
-- key would hide a duplicated id within the same warehouse.
--
-- Fails with one row per repeated key.
select
    ingestion_date,
    customer_id,
    count(*) as linhas
from {{ ref('silver_customer') }}
group by 1, 2
having count(*) > 1
