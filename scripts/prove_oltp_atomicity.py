#!/usr/bin/env python3
"""Outbox atomicity proof, against a real Postgres.

WHY THIS FILE EXISTS, AND WHY IT IS NOT A `make test` TEST.

`make test` is offline — a repository invariant. The double in `platform/tests/fake_pg.py`
proves, offline, that `apply_event` emits the outbox row and the state changes between the
same begin and the same commit. That catches the defect people actually make (one extra
`commit()` in the middle), but it does NOT prove that `rollback` undoes anything: a double
that undoes only demonstrates that the double undoes.

Atomicity is a property of the ENGINE. It can only be proven against the engine. And the
right way to prove it is the same the repository already uses for `verify-landing` and for
every dbt test: MAKE IT FAIL ON PURPOSE and check that the damage did not survive.

The injection does not touch the platform code — it touches the DATABASE. A trigger that
raises an exception on the outbox insert is a failure the applier can neither foresee nor
handle, which is exactly the kind of failure transactions exist for.

    make orders-prove-atomicity
"""

from __future__ import annotations

import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "platform", "src"))

from retail_platform.orders_oltp import (  # noqa: E402
    OrdersOltpError,
    apply_partition,
    connect,
    create_schema,
    drop_schema,
    read_log,
    verify_outbox,
)

BOOM = """
create or replace function retail_injected_boom() returns trigger
language plpgsql as $$ begin raise exception 'injected failure on %', TG_TABLE_NAME; end $$;
create trigger retail_injected_boom before insert on {table}
for each row execute function retail_injected_boom();
"""

DROP_BOOM = "drop trigger if exists retail_injected_boom on {table};"

failures = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "OK  " if condition else "FAIL"
    print(f"  [{mark}] {label}{(' — ' + detail) if detail else ''}")
    if not condition:
        failures.append(label)


def counts(connection) -> tuple[int, int, int]:
    with connection.cursor() as cur:
        cur.execute("select count(*) from orders")
        orders = cur.fetchone()[0]
        cur.execute("select count(*) from order_line")
        lines = cur.fetchone()[0]
        cur.execute("select count(*) from outbox")
        outbox = cur.fetchone()[0]
    return orders, lines, outbox


def inject(connection, table: str) -> None:
    with connection.cursor() as cur:
        cur.execute(BOOM.format(table=table))
    connection.commit()


def remove(connection, table: str) -> None:
    with connection.cursor() as cur:
        cur.execute(DROP_BOOM.format(table=table))
    connection.commit()


def main(partition: str) -> int:
    events = len(read_log(partition))
    print(f"partition ....... {partition}")
    print(f"events in log ... {events}")
    print()

    with connect() as connection:
        print("preparing an empty OLTP...")
        drop_schema(connection)
        create_schema(connection)
        check("the OLTP starts empty", counts(connection) == (0, 0, 0))
        print()

        # ---- 1. the failure is in the OUTBOX; the STATE must not survive -------------
        print("1. trigger that makes the OUTBOX insert blow up")
        inject(connection, "outbox")
        try:
            apply_partition(connection, partition)
            check("orders-apply failed", False, "applied without error, which is the defect")
        except Exception as exc:
            check("orders-apply failed", True, f"{type(exc).__name__}")
        remove(connection, "outbox")
        orders, lines, outbox = counts(connection)
        check("no order survived", orders == 0, f"orders={orders}")
        check("no line survived", lines == 0, f"order_line={lines}")
        check("no outbox row", outbox == 0, f"outbox={outbox}")
        print()

        # ---- 2. the failure is in the STATE; the OUTBOX must not survive -------------
        # The reverse direction matters as much as the first, and it is the one people forget:
        # an outbox that survived a state that did not change WOULD PUBLISH AN EVENT THAT NEVER
        # HAPPENED. Proving only one side would prove half of the pattern.
        print("2. trigger that makes the ORDERS insert blow up")
        inject(connection, "orders")
        try:
            apply_partition(connection, partition)
            check("orders-apply failed", False, "applied without error, which is the defect")
        except Exception as exc:
            check("orders-apply failed", True, f"{type(exc).__name__}")
        remove(connection, "orders")
        orders, lines, outbox = counts(connection)
        check("no order survived", orders == 0, f"orders={orders}")
        check("NO outbox row survived", outbox == 0, f"outbox={outbox}")
        print()

        # ---- 3. no injection: the whole partition goes in ----------------------------
        print("3. no injection — the whole partition")
        result = apply_partition(connection, partition)
        orders, lines, outbox = counts(connection)
        check("every log event became an outbox row", outbox == events,
              f"outbox={outbox} log={events}")
        check("exactly 1 outbox row per event", result.applied == events,
              f"applied={result.applied}")
        check("nothing was skipped on this first apply", result.skipped == 0)
        check("the orders exist", orders == result.orders, f"orders={orders}")
        proof = verify_outbox(connection, partition)
        check("the outbox rebuilds the log byte for byte", proof["matches"],
              f"sha256 {proof['outbox_sha256'][:16]}... vs {proof['declared_sha256'][:16]}...")
        print()

        # ---- 4. replay: idempotency through unique event_id ---------------------------
        print("4. replay of the SAME partition")
        again = apply_partition(connection, partition)
        orders2, lines2, outbox2 = counts(connection)
        check("nothing was applied again", again.applied == 0, f"applied={again.applied}")
        check("everything was recognized as already applied", again.skipped == events)
        check("the outbox did not grow", outbox2 == outbox, f"{outbox} -> {outbox2}")
        check("the state did not change", (orders2, lines2) == (orders, lines))
        check("the rebuilt log is still identical",
              verify_outbox(connection, partition)["matches"])
        print()

        # ---- 5. the order guard: the OLTP REFUSES out-of-order events -----------------
        # This property is what gives meaning to `key = order_id` in the Milestone 5 Kafka. If the
        # OLTP accepted out-of-order events, preserving per-order ordering in the broker would be
        # decoration; it is because it REFUSES them that the partition key becomes a requirement.
        print("5. order guard — out-of-sequence event on a new order")
        log = read_log(partition)
        future = next(
            ((e, line) for e, line in log if e["sequence_no"] == 2), None
        )
        if future is None:
            check("there is a seq=2 event in the log to inject", False)
        else:
            source_event, _source_line = future
            forged = dict(source_event)
            forged["order_id"] = "ord_inexistente_00000000_999999"
            forged["event_id"] = "f" * 32
            import json as _json
            forged_line = _json.dumps(forged, ensure_ascii=False, sort_keys=True)
            from retail_platform.orders_oltp import apply_event
            try:
                apply_event(connection, forged, forged_line)
                check("the OLTP refused the orphan event", False, "accepted it, which is the defect")
            except OrdersOltpError as exc:
                # "fora de ordem" stays in Portuguese: it is the literal message raised by
                # retail_platform.orders_oltp (pinned by platform/tests/test_orders_oltp.py).
                check("the OLTP refused the orphan event", "fora de ordem" in str(exc),
                      str(exc)[:80])
            orders3, _, outbox3 = counts(connection)
            check("the refusal left no outbox row behind", outbox3 == outbox,
                  f"{outbox} -> {outbox3}")
        print()

    print("=" * 78)
    if failures:
        print(f"FAILED: {len(failures)} check(s) failed:")
        for name in failures:
            print(f"  - {name}")
        return 1
    print("PASSED: state and event are atomic in both directions, the outbox rebuilds")
    print("the log byte for byte, replay is idempotent and out-of-order events are refused.")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: prove_oltp_atomicity.py <orders-partition>")
        raise SystemExit(2)
    try:
        raise SystemExit(main(sys.argv[1]))
    except OrdersOltpError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(2)
