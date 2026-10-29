"""Postgres connection double that RECORDS THE TRANSACTION BOUNDARY.

WHAT IT PROVES, AND WHAT IT DOES NOT — the distinction matters more than the code.

  PROVES: that `apply_event` emits the outbox row and the state changes between the same
  begin and the same `commit`, with no intermediate commit, and that any failure in the
  middle ends in `rollback` and never in `commit`. That is the defect people really commit
  when writing an outbox — one extra `commit()` in the middle, and the pattern becomes a
  dual-write without anything changing in appearance. A double that records the order of
  calls catches that, and catches it offline.

  DOES NOT PROVE: that `rollback` undoes anything. That is a property of the ENGINE, and no
  double can demonstrate it — a double that "undoes" only demonstrates that the double
  undoes. The matching proof is `make orders-prove-atomicity`, against a real Postgres, with
  a trigger that makes the outbox insert blow up.

Both proofs are necessary and neither replaces the other. It is the same pair as
`fake_s3.py` (validates the checksum as the server would) and `verify-landing` (reads the
object back from the real MinIO).
"""

from __future__ import annotations

import re


class FakePgError(Exception):
    """What the double raises in place of psycopg.Error."""


# ANCHORED AT THE START, not at any position. A `select ... for update skip locked` —
# which is exactly how the publisher drains the outbox — contains the word "update" in the
# middle, and an unanchored regex would tag it as a write. The test would then assert on the
# wrong tag and pass for the wrong reason; that is what happened in the first version of
# this double.
_INSERT = re.compile(r"^insert\s+into\s+(\w+)", re.I)
_UPDATE = re.compile(r"^update\s+(\w+)", re.I)
_SELECT = re.compile(r"^(select|with)\b", re.I)
_DELETE = re.compile(r"^delete\s+from\s+(\w+)", re.I)


def tag(sql: str) -> str:
    """Readable tag of the statement, so the test assertion speaks the language of the domain."""
    text = " ".join(sql.split())
    for pattern, prefix in ((_INSERT, "insert"), (_UPDATE, "update"), (_DELETE, "delete")):
        found = pattern.match(text)
        if found:
            return f"{prefix}:{found.group(1).lower()}"
    if _SELECT.match(text):
        return "select"
    return text.split(" ")[0].lower()


class FakeCursor:
    def __init__(self, connection):
        self._connection = connection
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        label = tag(sql)
        self._connection.journal.append(("execute", label, params))
        if self._connection.fail_on is not None and self._connection.fail_on(label, params):
            raise FakePgError(f"injected failure on {label}")
        self.rowcount = self._connection.rowcount_for(label, params)
        self._connection._pending = self._connection.result_for(label, params)
        return self

    def fetchone(self):
        rows = self._connection._pending
        return rows[0] if rows else None

    def fetchall(self):
        return list(self._connection._pending)


class FakeConnection:
    """Fake connection with a journal. `journal` is the artifact the tests read.

    `rowcounts` and `results` are maps of tag -> value (or callable(params)), so a test
    writes the scenario instead of describing SQL.
    """

    def __init__(self, *, rowcounts=None, results=None, fail_on=None, applied_event_ids=()):
        self.journal: list[tuple] = []
        self.rowcounts = rowcounts or {}
        self.results = results or {}
        self.fail_on = fail_on
        # Events the outbox already has. That is what makes `on conflict do nothing` return 0
        # rows, which is how a replay finds out there is nothing to do.
        self.applied_event_ids = set(applied_event_ids)
        self._pending: list = []
        self._queues: dict = {}
        self.closed = False

    # ---- interface consumed by orders_oltp.py ------------------------------
    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.journal.append(("commit",))

    def rollback(self):
        self.journal.append(("rollback",))

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # ---- script -------------------------------------------------------------
    def rowcount_for(self, label, params):
        if label == "insert:outbox":
            # params[0] is event_id. Already applied -> 0 rows, exactly as Postgres answers
            # an `on conflict (event_id) do nothing`.
            return 0 if params and params[0] in self.applied_event_ids else 1
        value = self.rowcounts.get(label, 1)
        return value(params) if callable(value) else value

    def result_for(self, label, params):
        value = self.results.get(label, [])
        if callable(value):
            return value(params)
        # Queued script: a list OF lists is consumed in order, one per call. That is what
        # allows writing a scenario where three consecutive SELECTs return different things
        # — the `order_picked` case, which checks three distinct invariants.
        if value and isinstance(value[0], list):
            queue = self._queues.setdefault(label, list(value))
            return queue.pop(0) if queue else []
        return value

    # ---- journal reading ----------------------------------------------------
    @property
    def tags(self) -> list[str]:
        return [row[1] if row[0] == "execute" else row[0] for row in self.journal]

    @property
    def commits(self) -> int:
        return sum(1 for row in self.journal if row[0] == "commit")

    @property
    def rollbacks(self) -> int:
        return sum(1 for row in self.journal if row[0] == "rollback")

    def params_of(self, label) -> list:
        return [row[2] for row in self.journal if row[0] == "execute" and row[1] == label]
