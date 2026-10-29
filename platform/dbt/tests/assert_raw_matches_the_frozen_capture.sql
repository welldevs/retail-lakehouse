-- THE SEALED RAW MUST NOT HAVE CHANGED — the part dbt can see.
--
-- DIVISION OF LABOR, declared because the missing half matters. `make freeze-check`
-- re-reads the object storage and compares sha256 by sha256: that is the COMPLETE check, and it
-- requires MinIO to be running. This test does the part that runs in `make silver`, with no extra network and
-- without anyone needing to remember: it checks whether the partitions Silver SEES are the same
-- ones that were sealed, with the same declared counts.
--
-- WHAT THIS CATCHES, and it is this phase's real risk: data landed AFTER closing. A
-- new partition breaks no sum — it just makes every number already published
-- about the window wrong, silently. It is the same class as the Phase 6 allocation defect.
--
-- WHAT IT DOES NOT CATCH: bytes altered inside a partition whose manifest still
-- declares the same counts. For that the object must be re-read — `make freeze-check`.
--
-- AN EMPTY SEAL IS A LEGITIMATE STATE. A repository before `make freeze`, or a fresh clone,
-- has zero rows in `frozen_capture` and this test passes. There is no third state.
--
-- ONLY THE ORDERS DOMAIN, on purpose: it is the only one this project REGENERATES. The other four
-- sources come from extraction against an external source and are never rewritten in place; what
-- covers them is `freeze-check`. Widening this test to them would require a manifest model
-- for each, which does not exist today — and creating four models for a risk that does not
-- materialize would be building against a hypothesis.
with selo as (

    -- EXPLICIT CAST, not trust in inference. The seal is born EMPTY, and a parquet with no
    -- rows does not carry a reliable type between write and read: the first run
    -- of this test broke with `replace(INTEGER, ...)`, an error about TYPE three steps away
    -- from the cause, which was the ABSENCE of data. Declaring the type in the seed solves the load; the cast
    -- here solves the read, and together they make the unsealed state behave like
    -- the sealed one, only without rows.
    select
        replace(cast(partition_key as varchar), 'ingestion_date=', '')  as chave,
        cast(records as bigint)                                         as records
    from {{ ref('frozen_capture') }}
    where cast(source as varchar) = 'simulated_orders'

),

vivo as (

    -- `declared_event_rows`, AND NOT THE SUM OF THE THREE COUNTS. The first version of this test
    -- summed orders + lines + events and failed on all 36 partitions — not from a defect, from
    -- comparing different quantities. The seal aggregates `records` of the files declared in the
    -- manifest, and the orders partition has ONE file: `order_events.jsonl`. So the seal's number
    -- is the EVENT count, and only that. Measured: all 36 match exactly.
    --
    -- If the Source ever starts landing more than one file per partition, this mapping
    -- stops holding — and failing is the right behavior, because the seal would be measuring something
    -- else.
    select
        strftime(ingestion_date, '%Y-%m-%d') || '/wh=' || partition_wh  as chave,
        declared_event_rows                                             as records
    from {{ ref('silver_orders_manifest') }}

)

-- Empty seal: nothing to guard yet.
select
    coalesce(s.chave, v.chave)                    as chave,
    s.records                                     as registros_selados,
    v.records                                     as registros_no_silver,
    case
        when s.chave is null then 'particao NOVA, aterrissada depois do selo'
        when v.chave is null then 'particao SELADA que sumiu do Silver'
        else 'contagem declarada MUDOU dentro de uma particao selada'
    end                                           as violacao
from selo s
full outer join vivo v on v.chave = s.chave
where (select count(*) from selo) > 0
  and (s.chave is null or v.chave is null or s.records <> v.records)
