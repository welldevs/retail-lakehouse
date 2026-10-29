-- INVERTED TEST: proves that postal_code is NOT a key, and that the model knows it.
--
-- Measured: 29 postal codes cross a municipality boundary within the scope of the 4 AUFs. If someone ever
-- "simplifies" DIM_GEOGRAPHY to the postal-code grain, those 29 cases would collapse and the per-municipality
-- sum would be wrong — silently, because the table would still look right.
--
-- This test fails if the divergence DISAPPEARS, which is the opposite of the usual. It is not whim:
-- a test that only checks what is expected does not protect the reason the grain is composite. If
-- INE ever republishes the Callejero without any shared postal code, this test fails and
-- someone revisits the decision with the data in hand, instead of keeping a composite key out of
-- inertia.
--
-- Fails when NO postal code crosses municipalities (that is, when the composite grain has lost its reason
-- to exist and the test needs to be reassessed).
with compartilhados as (

    select postal_code, count(distinct province_code || municipality_code) as municipios
    from {{ ref('dim_geography') }}
    group by 1
    having count(distinct province_code || municipality_code) > 1

)

select
    'nenhum CEP cruza municipio: o grao composto de dim_geography perdeu a justificativa'
        as motivo,
    (select count(*) from compartilhados)  as ceps_compartilhados
where (select count(*) from compartilhados) = 0
