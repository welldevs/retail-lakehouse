-- Unicidade do GRAO COMPOSTO. Nao ha uma coluna unica de "id do tramo" nos bytes do
-- TRAM (ver comentario do modelo) — verificado sem excecao contra 304.952 linhas reais
-- das 4 provincias antes de assumir esta chave.
select
    ingestion_date,
    section_code,
    entity_suffix,
    street_id,
    pseudo_address_id,
    postal_code,
    numbering_type,
    number_from,
    number_from_qualifier,
    number_to,
    number_to_qualifier,
    count(*) as n
from {{ ref('silver_callejero_tramos') }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
having count(*) > 1
