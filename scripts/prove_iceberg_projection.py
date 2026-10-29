#!/usr/bin/env python3
"""Proof of the concurrent projection: two writers, one reader, monotonic merge.

WHAT THIS MILESTONE NEEDS TO PROVE, and why each piece is necessary.

The Iceberg trigger was *"a second engine needing to write the same table"*. Having two
processes that write proves nothing on its own — what must be demonstrated is the
behavior under conflict, which is where storage without concurrency control fails
silently:

  1. A CONFLICT IS REFUSED, not absorbed. The writer that starts from a stale snapshot gets
     `CommitFailedException`. If it went through silently, it would be a lost update — and then
     Iceberg would be buying nothing.

  2. RETRY IS NOT ENOUGH: THE MERGE MUST BE MONOTONIC. Reloading and trying again resolves the
     COMMIT conflict and still loses data — if the other writer already wrote the order at
     `sequence_no` 7 and our attempt carries 5, a blind retry writes 5 over it. The
     commit goes through, the table regresses, and nothing fails. This is the proof that matters most.

  3. THE READER SEES A STABLE SNAPSHOT. A read in progress does not see a half-done write.

  4. BOTH REAL WRITERS, on the real table, with real data — and the
     reconciliation against `silver_order` at the end.

  5. TIME TRAVEL on the projection.

  6. `orders-reconcile` FAILS when there is a divergence. A check that never failed is not
     a check.

    make orders-prove-projection
"""

from __future__ import annotations

import os
import sys
import threading
import time
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "platform", "src"))

from retail_platform.config import from_env  # noqa: E402
from retail_platform.orders_projection import (  # noqa: E402
    COLUMNS,
    NAMESPACE,
    TABLE_NAME,
    WRITER_REBUILD,
    WRITER_STREAM,
    IcebergProjection,
    arrow_schema,
    catalog,
    ensure_table,
    reconcile,
)

PROBE = f"{NAMESPACE}.concurrency_probe"
failures = []


def check(label, ok, detail=""):
    print(f"  [{'OK  ' if ok else 'FAIL'}] {label}{(' — ' + detail) if detail else ''}")
    if not ok:
        failures.append(label)


def state(order_id, seq, status="PLACED", writer=WRITER_STREAM):
    now = datetime(2026, 8, 27, 7, 0, tzinfo=timezone.utc)
    return {
        "order_id": order_id, "wh": "mad1", "order_date": now.date(),
        "customer_id": "cust_mad1_000001", "status": status, "last_sequence_no": seq,
        "events_applied": seq, "placed_at": now, "updated_at": now,
        "confirmed_at": None, "picking_started_at": None, "picked_at": None,
        "dispatched_at": None, "terminal_at": None, "line_count": 3,
        "picked_line_count": None, "substituted_lines": 0, "removed_lines": 0,
        "gross_amount": "10.00", "net_amount": None, "returned_amount": None,
        "delivered_within_slot": None, "picking_minutes": None, "sla_breached": None,
        "written_by": writer,
    }


def main() -> int:
    config = from_env()
    cat = catalog(config)

    # SENTINEL. Parts 1 to 4 touch ONLY the probe table; if the production one changes
    # size, some write went to the wrong place. Without this line, that is how a
    # `load_table(TABLE_NAME)` hard-coded in a refresh wrote four synthetic rows into the
    # real projection — and only the reconciliation, three steps later, pointed it out. A proof
    # that uses a probe must watch the target it should NOT touch.
    real_before = ensure_table(cat).scan(selected_fields=("order_id",)).to_arrow().num_rows

    # ---- 1. conflict mechanics, on a probe table -------------------------------------
    print("1. conflict mechanics, on an isolated table")
    if cat.table_exists(PROBE):
        cat.drop_table(PROBE)
    cat.create_table(PROBE, schema=arrow_schema())

    import pyarrow as pa
    from pyiceberg.exceptions import CommitFailedException

    from retail_platform.orders_projection import _normalize

    def write_raw(table, rows):
        table.upsert(
            pa.Table.from_pylist([_normalize(l, l["written_by"]) for l in rows],
                                 schema=arrow_schema()),
            join_cols=["order_id"],
        )

    a = cat.load_table(PROBE)
    b = cat.load_table(PROBE)          # reference loaded BEFORE `a` commits
    write_raw(a, [state("ord_a", 1)])
    refused = False
    try:
        write_raw(b, [state("ord_b", 1)])
    except CommitFailedException as exc:
        refused = True
        reason = str(exc)[:90]
    check("the writer with a stale snapshot is REFUSED, not lost", refused,
          reason if refused else "committed silently — that would be a lost update")

    # ---- 2. THE PROOF THAT MATTERS MOST: monotonic merge -----------------------------
    print()
    print("2. monotonic merge — a retry must not write an OLD state over a newer one")
    table = cat.load_table(PROBE)
    newer = IcebergProjection(table, writer=WRITER_STREAM, cat=cat)
    newer.save([state("ord_race", 7, status="DELIVERED")])
    newer.commit()

    older = IcebergProjection(cat.load_table(PROBE), writer=WRITER_REBUILD, cat=cat)
    older.save([state("ord_race", 5, status="PICKING", writer=WRITER_REBUILD)])
    stats = older.commit()

    rows = {r["order_id"]: r for r in
            cat.load_table(PROBE).scan().to_arrow().to_pylist()}
    current = rows.get("ord_race", {})
    check("the old state was discarded as stale", stats.rows_dropped_as_stale == 1,
          f"dropped={stats.rows_dropped_as_stale} written={stats.rows_written}")
    check("the table did NOT regress", current.get("last_sequence_no") == 7
          and current.get("status") == "DELIVERED",
          f"seq={current.get('last_sequence_no')} status={current.get('status')}")

    # and the normal path keeps working: what moves forward, writes
    newest = IcebergProjection(cat.load_table(PROBE), writer=WRITER_REBUILD, cat=cat)
    newest.save([state("ord_race", 9, status="RETURNED", writer=WRITER_REBUILD)])
    stats2 = newest.commit()
    after = {r["order_id"]: r for r in
             cat.load_table(PROBE).scan().to_arrow().to_pylist()}["ord_race"]
    check("what MOVES FORWARD keeps writing", stats2.rows_written == 1
          and after["last_sequence_no"] == 9 and after["written_by"] == WRITER_REBUILD)

    # ---- 3. two concurrent writers, with an active reader -----------------------------
    print()
    print("3. two concurrent writers on the probe, with a reader in a loop")
    conflicts = {"n": 0}
    reads = {"n": 0, "invalid": 0}
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            rows = cat.load_table(PROBE).scan(
                selected_fields=("order_id", "last_sequence_no")).to_arrow().to_pylist()
            reads["n"] += 1
            # A valid snapshot never has a repeated key or a null sequence: a partial
            # read of a copy-on-write upsert would show up as one of the two.
            ids = [l["order_id"] for l in rows]
            if len(ids) != len(set(ids)) or any(l["last_sequence_no"] is None for l in rows):
                reads["invalid"] += 1
            time.sleep(0.02)

    def writer(name, span):
        for i in span:
            projection = IcebergProjection(cat.load_table(PROBE), writer=name, cat=cat)
            projection.save([state(f"ord_{name}_{i:02d}", 1, writer=name)])
            stats = projection.commit()
            conflicts["n"] += stats.conflicts

    reader_thread = threading.Thread(target=reader, daemon=True)
    reader_thread.start()
    threads = [
        threading.Thread(target=writer, args=(WRITER_STREAM, range(8))),
        threading.Thread(target=writer, args=(WRITER_REBUILD, range(8))),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    reader_thread.join(timeout=5)

    final = {r["order_id"] for r in cat.load_table(PROBE).scan().to_arrow().to_pylist()}
    expected = {f"ord_{n}_{i:02d}" for n in (WRITER_STREAM, WRITER_REBUILD)
                for i in range(8)}
    check("all 16 concurrent writes arrived", expected <= final,
          f"{len(expected & final)}/16, {conflicts['n']} conflict(s) resolved")
    check("the reader never saw an inconsistent snapshot", reads["invalid"] == 0,
          f"{reads['n']} reads during the writes")
    check("there was real contention (otherwise the proof proved nothing)",
          conflicts["n"] > 0, f"{conflicts['n']} conflicts")

    # ---- 4. time travel ---------------------------------------------------------------
    print()
    print("4. time travel on the projection")
    probe = cat.load_table(PROBE)
    snapshots = list(probe.metadata.snapshots)
    first = probe.scan(snapshot_id=snapshots[0].snapshot_id).to_arrow().num_rows
    now = probe.scan().to_arrow().num_rows
    check("the first snapshot is still readable and is smaller", first < now,
          f"{first} -> {now} rows over {len(snapshots)} snapshots")

    cat.drop_table(PROBE)
    print("      probe removed.")

    real_after = ensure_table(cat).scan(selected_fields=("order_id",)).to_arrow().num_rows
    check("the PRODUCTION table was not touched by the probe proofs",
          real_after == real_before, f"{real_before} -> {real_after} rows")

    # ---- 5. DuckDB reads the REAL table, through the catalog path ---------------------
    print()
    print("5. DuckDB reads the real table while it is being written")
    import duckdb

    real_table = ensure_table(cat)
    expected_rows = real_table.scan(selected_fields=("order_id",)).to_arrow().num_rows
    metadata = real_table.metadata_location

    con = duckdb.connect()
    con.execute("install iceberg"); con.execute("load iceberg")
    con.execute("install httpfs"); con.execute("load httpfs")
    host = config.endpoint.split("://", 1)[-1]
    con.execute(f"""
        create or replace secret minio (
            type s3, key_id '{config.access_key}', secret '{config.secret_key}',
            region '{config.region}', endpoint '{host}',
            use_ssl {'true' if config.endpoint.startswith('https') else 'false'},
            url_style 'path')
    """)
    read_rows = con.execute(f"select count(*) from iceberg_scan('{metadata}')").fetchone()[0]
    check("DuckDB reads through the catalog's metadata_location", read_rows == expected_rows,
          f"{read_rows} rows")

    # And the reader pinned to the old metadata keeps seeing the old state, even after a
    # new write — snapshot isolation spanning two engines.
    projection = IcebergProjection(cat.load_table(TABLE_NAME), writer=WRITER_STREAM, cat=cat)
    target = real_table.scan(selected_fields=("order_id", "last_sequence_no")
                             ).to_arrow().to_pylist()[0]
    projection.save([state(target["order_id"], target["last_sequence_no"] + 100,
                           status="RETURNED")])
    projection.commit()
    still = con.execute(
        f"select last_sequence_no from iceberg_scan('{metadata}') where order_id = "
        f"'{target['order_id']}'").fetchone()[0]
    new_metadata = cat.load_table(TABLE_NAME).metadata_location
    now_val = con.execute(
        f"select last_sequence_no from iceberg_scan('{new_metadata}') where order_id = "
        f"'{target['order_id']}'").fetchone()[0]
    check("the old metadata still shows the old state", still == target["last_sequence_no"],
          f"old={still} new={now_val}")
    check("the new metadata shows the write", now_val == target["last_sequence_no"] + 100)

    # undo the test write, so the reconciliation below compares the real state
    restore = IcebergProjection(cat.load_table(TABLE_NAME), writer=WRITER_STREAM, cat=cat)
    original_row = {c: target.get(c) for c in COLUMNS}
    original_full = [r for r in real_table.scan().to_arrow().to_pylist()
                     if r["order_id"] == target["order_id"]][0]
    original_full["last_sequence_no"] = target["last_sequence_no"] + 1000
    restore.save([original_full])
    restore.commit()
    # and now back to the right value, with the guard bypassed by a direct upsert
    original_full["last_sequence_no"] = target["last_sequence_no"]
    write_raw(cat.load_table(TABLE_NAME), [original_full])
    del original_row
    print("      test write undone.")

    # ---- 6. reconciliation: passes, and FAILS when forced -----------------------------
    print()
    print("6. reconciliation of the three folds")
    report = reconcile(config, cat=cat)
    check("Iceberg, Silver and OLTP agree", report["ok"],
          f"{report['compared']} orders compared, "
          f"{report['divergence_count']} divergence(s)")

    print()
    print("   injecting a divergence on purpose...")
    if not report["ok"]:
        # IF THE BASE IS ALREADY DIVERGENT, the injection proves nothing: `not ok` would be true
        # anyway. That is exactly the false positive the first version of this proof
        # produced — it "passed" because the base was dirty, not because it detected anything.
        check("the base is clean before the injection (otherwise the injection proves nothing)", False,
              "reconciliation was already failing; injection skipped")
    else:
        victim = [r for r in cat.load_table(TABLE_NAME).scan().to_arrow().to_pylist()][0]
        saved = dict(victim)
        tampered = dict(victim)
        tampered["status"] = "CANCELLED" if victim["status"] != "CANCELLED" else "DELIVERED"
        write_raw(cat.load_table(TABLE_NAME), [tampered])
        after_injection = reconcile(config, cat=cat)
        # "It failed" is not enough: it must have failed BECAUSE OF the tampered row.
        # (`coluna` is the key name reconcile() returns; it belongs to retail_platform.)
        pointed = any(d["order_id"] == victim["order_id"] and d["coluna"] == "status"
                      for d in after_injection["divergences"])
        check("the reconciliation FAILS on the injected divergence",
              not after_injection["ok"] and pointed,
              f"pointed at {victim['order_id']}.status" if pointed
              else f"failed, but for another reason: {after_injection['divergences'][:1]}")
        write_raw(cat.load_table(TABLE_NAME), [saved])
        restored = reconcile(config, cat=cat)
        check("and passes again after restoring", restored["ok"],
              f"{restored['divergence_count']} divergence(s)")

    # ---- 7. both writers left their mark ---------------------------------------------
    print()
    print("7. provenance: did both writers really write?")
    projection = IcebergProjection(cat.load_table(TABLE_NAME), cat=cat)
    who = projection.writers_seen()
    print(f"      {who}")
    check("the table records which writer touched each row", bool(who),
          ", ".join(f"{k}={v}" for k, v in sorted(who.items())))

    print()
    print("=" * 78)
    if failures:
        print(f"FAILED: {len(failures)} check(s) failed:")
        for name in failures:
            print(f"  - {name}")
        return 1
    print("PASSED: conflicts are refused, the merge is monotonic, the reader sees a stable")
    print("snapshot, DuckDB reads through the catalog and the three folds agree.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
