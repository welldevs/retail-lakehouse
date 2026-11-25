-- Each warehouse must have exactly 1 municipality flagged as home
-- (is_home_municipality = true) — nem 0 nem 2+.
select
    wh,
    count(*) as n_home
from {{ ref('warehouse_service_area') }}
where is_home_municipality
group by 1
having count(*) != 1
