-- EVERY ORDER PARTITION WAS BORN FROM THE SAME PREMISES. ONE WINDOW, ONE CONTRACT.
--
-- THE GAP THIS CLOSES, declared in Milestone B and measured on the spot. Each partition's
-- manifest keeps `premises_sha256` — the hash of the reference package the generator read — and the
-- Source validates that hash AT GENERATION. But nothing compared the partitions WITH EACH OTHER after
-- they landed.
--
-- THE DANGEROUS STATE IT CATCHES is concrete and has already happened in this repository: in Milestone B the
-- seed changed (`sla_minutes_picking` 90 -> 60, `slot_lead_hours` 2-24h -> 1-8h) and RAW
-- stayed the old one. `make silver` passed completely. If the next regeneration had been
-- INTERRUPTED midway, half the window would have the new premises and half the old — and
-- nothing would fail, because each partition stays internally consistent and all totals
-- keep closing. The warehouse would measure window adherence against two policies at the same
-- time, publishing a number that describes neither.
--
-- IT IS THE SAME CLASS AS THE CUSTOMER BASE DENSITY IN PHASE 6: an ALLOCATION defect, which
-- no sum reveals.
--
-- `generator_version` travels along for the same reason: two generator versions in the same window
-- is the same disease under another name.
--
-- WHAT IT STILL DOES NOT CATCH, and it stays declared: if ALL partitions are uniformly
-- old, they agree with each other and this test passes. Comparing RAW against the repository's CURRENT
-- seed requires recomputing the hash of the reference package, which is work outside
-- dbt — and it is what `make freeze` seals.
with particoes as (

    select distinct
        premises_sha256,
        generator_version
    from {{ ref('silver_orders_manifest') }}

),

contagem as (

    select
        count(distinct premises_sha256)  as conjuntos_de_premissa,
        count(distinct generator_version) as versoes_do_gerador
    from particoes

)

select
    c.conjuntos_de_premissa,
    c.versoes_do_gerador,
    (select string_agg(distinct premises_sha256, ' | ') from particoes) as premissas,
    (select string_agg(distinct generator_version, ' | ') from particoes) as geradores,
    'a janela mistura mais de um contrato: metade dela responde a outra politica' as violacao
from contagem c
where c.conjuntos_de_premissa <> 1
   or c.versoes_do_gerador <> 1
