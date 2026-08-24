-- PREMISSA QUE O silver_price_change DEPENDE.
--
-- Ele reduz as aparicoes repetidas de um id com any_value(unit_price). Isso so e seguro
-- porque o preco NAO divergre entre as aparicoes do mesmo id na mesma particao (medido:
-- 0 divergencias; apenas o array categories[] difere, em 198 e 199 casos). Se a fonte
-- passar a devolver precos diferentes por categoria, any_value viraria escolha
-- arbitraria e silenciosa — este teste falha antes disso acontecer.
select
    ingestion_date,
    warehouse,
    source_product_id,
    count(distinct unit_price) as distinct_prices
from {{ ref('silver_product_price') }}
group by 1, 2, 3
having count(distinct unit_price) > 1
