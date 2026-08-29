-- O EIXO DA PARTICAO TEM DE CONCORDAR COM O REGISTRO, nos dois modelos da source.
--
-- Medido: em customers.json o campo `wh` do registro e o segmento hive `wh=` do caminho
-- tem o mesmo nome, e o read_json do DuckDB deduplica os dois numa coluna so — o valor do
-- registro vence e o do caminho some. Uma particao pousada sob o diretorio errado ficaria
-- invisivel: a coluna leria "vlc1" (do registro) enquanto o objeto moraria em wh=svq1/.
-- silver_customer.partition_wh existe exatamente para dar ao caminho um nome proprio e
-- tornar essa comparacao possivel.
--
-- No manifesto o problema nao existe (source.wh e o hive wh tem nomes distintos), mas a
-- checagem e a mesma e vale pelo mesmo motivo — e o padrao que raw_manifest.sql ja usa
-- com warehouse/source_warehouse.
--
-- Falha com uma linha por divergencia.
select 'silver_customer' as modelo, ingestion_date, wh, partition_wh as caminho, count(*) as linhas
from {{ ref('silver_customer') }}
where partition_wh is distinct from wh
group by 1, 2, 3, 4

union all

select 'silver_oltp_manifest', ingestion_date, source_warehouse, wh, count(*)
from {{ ref('silver_oltp_manifest') }}
where source_warehouse is distinct from wh
group by 1, 2, 3, 4
