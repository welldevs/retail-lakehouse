-- PASSAGEM do selo da captura para o object storage — mesmo mecanismo de order_premises.
--
-- ELE PODE ESTAR VAZIO, E VAZIO SIGNIFICA ALGO. Zero linhas quer dizer "a captura ainda nao
-- foi selada", que e o estado legitimo de um repositorio antes de `make freeze` e de um
-- clone novo. `assert_raw_matches_the_frozen_capture` trata esse caso como "nada a guardar"
-- e passa; a partir do primeiro selo, ele passa a cobrar.
--
-- E UMA MAQUINA DE DOIS ESTADOS, declarada: nao-selado -> selado. O que NAO existe e um
-- terceiro estado onde o selo existe e nao vale.
{{ config(
    location = 's3://retail-lakehouse/silver/frozen_capture.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('frozen_capture_seed') }}
