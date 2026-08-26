-- Cada warehouse precisa ter exatamente 1 municipio marcado como sede
-- (is_home_municipality = true) — nem 0 nem 2+.
select
    wh,
    count(*) as n_home
from {{ ref('warehouse_service_area') }}
where is_home_municipality
group by 1
having count(*) != 1
