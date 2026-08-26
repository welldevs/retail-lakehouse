-- Unicidade do GRAO COMPOSTO. fk_periodo=28 e o unico valor medido nesta tabela, mas
-- fica no grao por simetria com silver_ine_population_series (que precisa dele de
-- verdade) e como salvaguarda caso o INE publique mais de uma estimativa/ano aqui.
select
    ingestion_date,
    series_code,
    year,
    fk_periodo,
    count(*) as n
from {{ ref('silver_ine_population_by_municipality') }}
group by 1, 2, 3, 4
having count(*) > 1
