-- Unicidade do GRAO composto (wh, province_code, municipality_code).
select
    wh,
    province_code,
    municipality_code,
    count(*) as n
from {{ ref('warehouse_service_area') }}
group by 1, 2, 3
having count(*) > 1
