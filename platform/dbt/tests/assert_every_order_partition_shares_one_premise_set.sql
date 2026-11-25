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
with partitions as (

    select distinct
        premises_sha256,
        generator_version
    from {{ ref('silver_orders_manifest') }}

),

counts as (

    select
        count(distinct premises_sha256)  as premise_sets,
        count(distinct generator_version) as generator_versions
    from partitions

)

select
    c.premise_sets,
    c.generator_versions,
    (select string_agg(distinct premises_sha256, ' | ') from partitions) as premises,
    (select string_agg(distinct generator_version, ' | ') from partitions) as generators,
    'the window mixes more than one contract: half of it answers to another policy' as reason
from counts c
where c.premise_sets <> 1
   or c.generator_versions <> 1
