-- Every day with prices has a corresponding ingestion run, and vice versa.
--
-- It is what keeps the reading of gaps honest. FACT_INGESTION_RUN is the only table that
-- distinguishes "there was no price" from "there was no observation" — but it only serves that purpose if it
-- really covers every observed day. Two directions, two different errors:
--
--   * price without a run: data showed up with no trace of how it arrived — broken
--     provenance, and the gap is no longer explainable;
--   * complete run without prices: the Source declared it landed the partition and the fact
--     is empty — loss between RAW and the dimensional model.
--
-- Only the catalog source comes in: the OLTP one produces no prices, and demanding a price fact
-- from it would be comparing different things.
--
-- The name is 'mercadona_catalog_api', with the suffix — it is the value the Source writes to
-- source.name in the manifest, MEASURED, not the short name one imagines when writing the test.
-- Getting this wrong does not make the test fail by mistake: it makes the whole join not match, and the test
-- fails ALL 14 partitions at once, as if the model were broken.
--
-- Fails with one row per day/warehouse that exists on only one side.
with precos as (

    select distinct snapshot_date as ingestion_date, wh
    from {{ ref('fact_price_snapshot') }}

),

execucoes as (

    select distinct ingestion_date, wh
    from {{ ref('fact_ingestion_run') }}
    where source_name = 'mercadona_catalog_api' and is_valid_observation

)

select
    coalesce(p.ingestion_date, e.ingestion_date)             as ingestion_date,
    coalesce(p.wh, e.wh)                                     as wh,
    case
        when e.ingestion_date is null then 'preco sem execucao de ingestao'
        else 'execucao valida sem nenhum preco'
    end                                                      as problema
from precos p
full outer join execucoes e
  on  e.ingestion_date = p.ingestion_date
 and  e.wh             = p.wh
where p.ingestion_date is null or e.ingestion_date is null
