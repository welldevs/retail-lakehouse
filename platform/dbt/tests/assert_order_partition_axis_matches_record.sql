-- O eixo da particao tem de bater com o que o registro diz, nos dois modelos que expoem os
-- dois lados.
--
-- POR QUE ESTA COLUNA EXTRA EXISTE. O campo `wh` do evento e o segmento hive `wh=` tem o
-- MESMO nome, e o DuckDB os deduplica numa coluna so — o valor do registro vence e o do
-- caminho desaparece. Sem `partition_wh`, uma particao pousada sob o diretorio errado seria
-- internamente consistente e portanto invisivel para todo teste a jusante. Mesmo defeito que
-- o `wh` invalido da API da Mercadona produz, e que ja esta registrado no ARCHITECTURE.md.
select 'silver_order_event' as modelo, order_id as chave, wh, partition_wh
from {{ ref('silver_order_event') }}
where partition_wh is distinct from wh

union all

select 'silver_orders_manifest', run_id, wh, source_warehouse
from {{ ref('silver_orders_manifest') }}
where source_warehouse is distinct from wh
   or partition_wh is distinct from wh
