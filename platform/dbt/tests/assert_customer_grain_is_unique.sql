-- GRAO de silver_customer: (ingestion_date, customer_id).
--
-- Nao e customer_id sozinho — os mesmos ids reaparecem em cada regeracao da base, e isso
-- e deliberado (CONTRACT.md secao 3: unicidade vale dentro da ingestion_date, nao entre
-- elas). Nao e (ingestion_date, wh, customer_id) tampouco: o wh ja esta embutido no
-- proprio id, entao cust_mad1_000042 e cust_vlc1_000042 nunca colidem e acrescentar wh a
-- chave esconderia um id duplicado dentro do mesmo armazem.
--
-- Falha com uma linha por chave repetida.
select
    ingestion_date,
    customer_id,
    count(*) as linhas
from {{ ref('silver_customer') }}
group by 1, 2
having count(*) > 1
