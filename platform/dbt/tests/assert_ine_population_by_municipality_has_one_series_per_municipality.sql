-- Um municipio tem UMA populacao por sexo e ano. Nao e redundante com o teste de grao:
-- aquele usa series_code, e o defeito que este pega tinha series_code DIFERENTE.
--
-- O lado RAW da tabela 29005 e nacional (~8.200 municipios) e traz homonimos de outras
-- provincias com "Nombre" identico; o join por nome casava os dois e o municipio ficava
-- com duas linhas divergentes (ex.: Torrent com 90.928 e com 182, esta ultima de Girona).
-- Resolvido por ine_ambiguous_series_seed, com o codigo oficial do INE. Este teste e o
-- que garante que continua resolvido.
select
    ingestion_date,
    province_code,
    municipality_code,
    sex_label,
    year,
    fk_periodo,
    count(*) as n
from {{ ref('silver_ine_population_by_municipality') }}
group by 1, 2, 3, 4, 5, 6
having count(*) > 1
