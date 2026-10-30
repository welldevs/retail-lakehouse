"""Iceberg table double: in-memory state, conflict on demand.

WHAT IT PROVES. The monotonic merge and the retry loop are the part of `IcebergProjection`
that decides whether two writers trample each other — and it is pure logic over
`last_sequence_no`, not a property of the format. Exercising it offline is fast and
deterministic, and allows forcing the conflict exactly where you want it.

WHAT IT DOES NOT PROVE, and that is why `make orders-prove-projection` exists: that the real
Iceberg REJECTS a commit coming from a stale snapshot. A double that raises
`CommitFailedException` only demonstrates that the double raises. Same pair as fake_pg.py
with `make orders-prove-atomicity` and fake_kafka.py with `make orders-prove-stream`.
"""

from __future__ import annotations


class FakeSnapshot:
    def __init__(self, snapshot_id):
        self.snapshot_id = snapshot_id


class FakeMetadata:
    def __init__(self, snapshots):
        self.snapshots = snapshots


class _FakeArrow:
    def __init__(self, rows):
        self._rows = rows

    def to_pylist(self):
        return [dict(row) for row in self._rows]

    @property
    def num_rows(self):
        return len(self._rows)


class _FakeScan:
    def __init__(self, rows, fields):
        self._rows = rows
        self._fields = fields

    def to_arrow(self):
        if self._fields is None:
            return _FakeArrow(self._rows)
        return _FakeArrow([{c: row.get(c) for c in self._fields}
                           for row in self._rows])


class FakeIcebergTable:
    """Table with `scan`/`upsert`/`refresh` and a conflict script.

    `conflicts_before_success` makes the first N `upsert` calls raise
    `CommitFailedException`, which is how pyiceberg signals "the branch changed since you
    loaded this reference".

    `on_conflict_state` lets another writer "show up" between the rejection and the retry —
    it is the scenario that separates a blind retry from a monotonic merge.
    """

    def __init__(self, rows=(), *, conflicts_before_success=0, on_conflict_state=None):
        self.rows = {row["order_id"]: dict(row) for row in rows}
        self.conflicts_before_success = conflicts_before_success
        self.on_conflict_state = on_conflict_state
        self.upserts = 0
        self.refreshes = 0
        self.journal: list[tuple] = []
        self._snapshots = [FakeSnapshot(1)]

    # ---- interface consumed by IcebergProjection ---------------------------
    def scan(self, selected_fields=None, snapshot_id=None):
        self.journal.append(("scan", tuple(selected_fields or ())))
        return _FakeScan(list(self.rows.values()), selected_fields)

    def upsert(self, arrow_table, join_cols=("order_id",)):
        from pyiceberg.exceptions import CommitFailedException

        self.upserts += 1
        if self.conflicts_before_success > 0:
            self.conflicts_before_success -= 1
            self.journal.append(("conflict",))
            if self.on_conflict_state:
                # Another writer committed first. This is exactly where a blind retry would
                # write an old state over it.
                for row in self.on_conflict_state:
                    self.rows[row["order_id"]] = dict(row)
                self.on_conflict_state = None
            raise CommitFailedException("branch main has changed")
        rows = arrow_table.to_pylist()
        self.journal.append(("upsert", tuple(sorted(r["order_id"] for r in rows))))
        for row in rows:
            self.rows[row["order_id"]] = dict(row)
        self._snapshots.append(FakeSnapshot(len(self._snapshots) + 1))
        return None

    def append(self, arrow_table):
        rows = arrow_table.to_pylist()
        self.appends = getattr(self, "appends", 0) + 1
        self.journal.append(("append", len(rows)))
        for row in rows:
            self.rows[row["order_id"]] = dict(row)
        self._snapshots.append(FakeSnapshot(len(self._snapshots) + 1))

    def refresh(self):
        self.refreshes += 1
        self.journal.append(("refresh",))

    @property
    def metadata(self):
        return FakeMetadata(list(self._snapshots))

    def name(self):
        return ("projection", "live_order_state")


class FakeCatalog:
    """Catalog that always returns the SAME table — the double's refresh is `refresh()`."""

    def __init__(self, table):
        self._table = table
        self.loads = 0

    def load_table(self, _name):
        self.loads += 1
        self._table.journal.append(("load_table",))
        return self._table

    # ---- what `reset_table` / `ensure_table` call ------------------------------
    def create_namespace_if_not_exists(self, _namespace):
        return None

    def table_exists(self, _name):
        return True

    def drop_table(self, _name):
        self._table.rows.clear()
        self._table.journal.append(("drop_table",))

    def create_table(self, _name, schema=None):
        return self._table
