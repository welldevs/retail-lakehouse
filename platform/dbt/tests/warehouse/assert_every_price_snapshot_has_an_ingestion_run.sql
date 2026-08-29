-- Todo dia com preco tem uma execucao de ingestao correspondente, e vice-versa.
--
-- E o que mantem honesta a leitura das lacunas. FACT_INGESTION_RUN e a unica tabela que
-- distingue "nao houve preco" de "nao houve observacao" — mas ela so serve para isso se
-- realmente cobrir todos os dias observados. Duas direcoes, dois erros diferentes:
--
--   * preco sem execucao: dado apareceu sem rastro de como chegou — proveniencia
--     quebrada, e a lacuna deixa de ser explicavel;
--   * execucao completa sem preco: a Source declarou ter aterrissado a particao e o fato
--     esta vazio — perda entre o RAW e o modelo dimensional.
--
-- So a source do catalogo entra: a de OLTP nao produz preco, e cobrar dela um fato de
-- preco seria comparar coisas diferentes.
--
-- O nome e 'mercadona_catalog_api', com o sufixo — e o valor que a Source grava em
-- source.name do manifesto, MEDIDO, nao o nome curto que se imagina ao escrever o teste.
-- Errar isso nao faz o teste falhar por engano: faz o join inteiro nao casar, e o teste
-- reprova TODAS as 14 particoes de uma vez, como se o modelo estivesse quebrado.
--
-- Falha com uma linha por dia/armazem que exista de um lado so.
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
