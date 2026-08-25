-- PASSAGEM do seed para o object storage. O seed (platform/dbt/seeds/) e a fonte de
-- verdade VERSIONADA — mas `retail_platform query`/`connect_lakehouse()` nunca le
-- tabela nativa do .duckdb local, so escaneia parquet sob s3://.../silver/ (ver
-- query.py). Sem este modelo, o seed carrega certinho no `dbt build` mas fica invisivel
-- pra qualquer consulta feita do jeito que o resto da plataforma consulta o Silver —
-- medido: `make query SQL="select * from warehouse_province_map"` dava "table does not
-- exist" mesmo com o seed carregado, porque a tabela so existia LOCALMENTE, nunca no
-- bucket. Este modelo e so um "select *" do seed, materializado igual aos outros 5
-- modelos do Silver, pra ficar visivel do mesmo jeito que eles.
--
-- Nao pertence a mercadona/ nem a ine/: e uma tabela de referencia (dimensao),
-- ortogonal as duas sources, por isso fica direto sob silver/ como raw_manifest.sql
-- ficava antes de virar especifico da Mercadona.
{{ config(
    location = 's3://retail-lakehouse/silver/warehouse_province_map.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('warehouse_province_map_seed') }}
