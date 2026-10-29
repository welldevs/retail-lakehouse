"""The live projection: types, monotonic merge, retry and the seam between the two sinks.

THREE LAYERS, the same pattern as test_orders_oltp.py and test_orders_stream.py:

  1. `_normalize` and `_as_key` are PURE — types and cross-engine comparison, nothing running.
  2. The monotonic merge and the retry loop, against `fake_iceberg.FakeIcebergTable`. It is
     logic over `last_sequence_no`, not a property of the format, and it is the part that
     decides whether two writers trample each other.
  3. SEAM PARITY: both sinks expose the same small interface. That was the Milestone 5
     promise, and an interface promise only holds if it is asserted.

What is NOT here: that Iceberg rejects a commit coming from a stale snapshot. A double that
raises the exception only demonstrates that the double raises. That is
`make orders-prove-projection`.
"""

from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from retail_platform.orders_projection import (
    COLUMNS,
    WRITER_REBUILD,
    WRITER_STREAM,
    IcebergProjection,
    OrdersProjectionError,
    _as_key,
    _normalize,
    arrow_schema,
    partitions_of,
)

from .fake_iceberg import FakeCatalog, FakeIcebergTable


def state(order_id="ord_a", seq=1, status="PLACED", **extra):
    base = {
        "order_id": order_id, "wh": "mad1", "order_date": date(2026, 8, 27),
        "customer_id": "cust_mad1_000001", "status": status, "last_sequence_no": seq,
        "events_applied": seq,
        "placed_at": datetime(2026, 8, 27, 7, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 8, 27, 7, 0, tzinfo=timezone.utc),
        "confirmed_at": None, "picking_started_at": None, "picked_at": None,
        "dispatched_at": None, "terminal_at": None, "line_count": 3,
        "picked_line_count": None, "substituted_lines": 0, "removed_lines": 0,
        "gross_amount": "10.00", "net_amount": None, "returned_amount": None,
        "delivered_within_slot": None, "picking_minutes": None, "sla_breached": None,
    }
    base.update(extra)
    return base


class TestNormalize(unittest.TestCase):
    def test_money_becomes_decimal_never_float(self):
        """Precedent of `silver_product_price` and obligation 4.4 of the Mercadona contract.
        A float here would reintroduce rounding error into the order amount."""
        row = _normalize(state(gross_amount="10.00", net_amount="9.35"), WRITER_STREAM)
        self.assertEqual(row["gross_amount"], Decimal("10.00"))
        self.assertIsInstance(row["gross_amount"], Decimal)
        self.assertIsInstance(row["net_amount"], Decimal)

    def test_null_money_stays_null(self):
        """A null `net_amount` means "no picking happened", not zero — the same discipline as
        the OLTP and Silver, and the defect Milestone 4 found."""
        row = _normalize(state(net_amount=None), WRITER_STREAM)
        self.assertIsNone(row["net_amount"])

    def test_the_writer_is_stamped_by_the_projection_not_by_the_state(self):
        """Provenance belongs to the writer, not to the fold. If it came from the state, both
        writers could stamp the same thing and the column would say nothing."""
        row = _normalize(state(), WRITER_REBUILD)
        self.assertEqual(row["written_by"], WRITER_REBUILD)

    def test_every_declared_column_is_produced(self):
        row = _normalize(state(), WRITER_STREAM)
        self.assertEqual(sorted(row), sorted(COLUMNS))

    def test_the_key_column_is_the_only_one_that_cannot_be_null(self):
        """The nullability of the others has MEANING — see net_amount. Declaring everything
        non-null would force inventing a value where the correct answer is "does not exist"."""
        schema = arrow_schema()
        non_null = [c.name for c in schema if not c.nullable]
        self.assertEqual(non_null, ["order_id"])


class TestComparisonKey(unittest.TestCase):
    """The normalization that decides whether three engines agree. Getting it wrong is worse than not having it."""

    def test_the_same_number_in_three_types_compares_equal(self):
        self.assertEqual(_as_key(Decimal("10.00")), _as_key(10))
        self.assertEqual(_as_key(Decimal("10.00")), _as_key(10.0))

    def test_it_normalizes_scale_but_never_rounds(self):
        """The first version formatted with `:.2f`, and that made 10.001 and 10.00 compare
        EQUAL — a reconciliation that rounds away precisely the difference it came to check.
        Today all three sources are decimal(12,2) and the defect would not show; it would show
        the day one of them changed scale, which is the worst day to discover it."""
        self.assertNotEqual(_as_key(Decimal("10.001")), _as_key(Decimal("10.00")))
        self.assertNotEqual(_as_key(Decimal("10.01")), _as_key(Decimal("10.00")))

    def test_null_is_not_zero_and_true_is_not_one(self):
        self.assertIsNone(_as_key(None))
        self.assertNotEqual(_as_key(None), _as_key(0))
        self.assertNotEqual(_as_key(True), _as_key(1))


class TestMonotonicMerge(unittest.TestCase):
    """The property that separates "two writers" from "two writers that work"."""

    def _projection(self, table, writer=WRITER_STREAM):
        return IcebergProjection(table, writer=writer, cat=FakeCatalog(table))

    def test_a_state_that_advances_is_written(self):
        table = FakeIcebergTable([_normalize(state(seq=3), WRITER_STREAM)])
        projection = self._projection(table)
        projection.save([state(seq=4, status="PICKED")])
        stats = projection.commit()
        self.assertEqual(stats.rows_written, 1)
        self.assertEqual(table.rows["ord_a"]["last_sequence_no"], 4)

    def test_a_state_that_does_not_advance_is_dropped_as_stale(self):
        """RETRY IS NOT ENOUGH. If the other writer already stored the order at sequence_no 7
        and our attempt carries 5, a blind retry writes 5 over it: the commit passes, the table
        REGRESSES, and nothing fails. The guard is the same `last_sequence_no` that protects
        the OLTP and the consumer, now protecting the concurrent write."""
        table = FakeIcebergTable([_normalize(state(seq=7, status="DELIVERED"), WRITER_STREAM)])
        projection = self._projection(table, WRITER_REBUILD)
        projection.save([state(seq=5, status="PICKING")])
        stats = projection.commit()
        self.assertEqual(stats.rows_dropped_as_stale, 1)
        self.assertEqual(stats.rows_written, 0)
        self.assertEqual(table.rows["ord_a"]["last_sequence_no"], 7)
        self.assertEqual(table.rows["ord_a"]["status"], "DELIVERED")
        # And no upsert was even issued: not writing is the CORRECT result here.
        self.assertEqual(table.upserts, 0)

    def test_an_equal_sequence_is_also_dropped(self):
        """`>` and not `>=`: rewriting the same state would spend a whole snapshot for nothing,
        and in a copy-on-write format a snapshot costs rewriting the table."""
        table = FakeIcebergTable([_normalize(state(seq=5), WRITER_STREAM)])
        projection = self._projection(table)
        projection.save([state(seq=5)])
        self.assertEqual(projection.commit().rows_dropped_as_stale, 1)

    def test_an_unknown_order_is_always_written(self):
        projection = self._projection(FakeIcebergTable([]))
        projection.save([state(seq=1)])
        self.assertEqual(projection.commit().rows_written, 1)

    def test_a_mixed_batch_writes_only_what_advances(self):
        table = FakeIcebergTable([
            _normalize(state("ord_a", seq=9), WRITER_STREAM),
            _normalize(state("ord_b", seq=2), WRITER_STREAM),
        ])
        projection = self._projection(table, WRITER_REBUILD)
        projection.save([state("ord_a", seq=4), state("ord_b", seq=6),
                         state("ord_c", seq=1)])
        stats = projection.commit()
        self.assertEqual((stats.rows_written, stats.rows_dropped_as_stale), (2, 1))
        self.assertEqual(table.rows["ord_a"]["last_sequence_no"], 9)
        self.assertEqual(table.rows["ord_b"]["last_sequence_no"], 6)
        self.assertIn("ord_c", table.rows)


class TestRetryLoop(unittest.TestCase):
    def test_a_conflict_is_retried_after_reloading(self):
        table = FakeIcebergTable([], conflicts_before_success=2)
        projection = IcebergProjection(table, cat=FakeCatalog(table))
        projection.save([state(seq=1)])
        stats = projection.commit()
        self.assertEqual(stats.conflicts, 2)
        self.assertEqual(stats.attempts, 3)
        self.assertEqual(stats.rows_written, 1)

    def test_the_retry_re_reads_before_writing_again(self):
        """THIS is the test that separates a blind retry from a monotonic merge. Between the
        rejection and the retry, another writer committed a NEWER state. A retry that reused
        the previous read would write the old one over it and the commit would pass."""
        table = FakeIcebergTable(
            [_normalize(state(seq=1), WRITER_STREAM)],
            conflicts_before_success=1,
            on_conflict_state=[_normalize(state(seq=9, status="DELIVERED"), WRITER_STREAM)],
        )
        projection = IcebergProjection(table, writer=WRITER_REBUILD, cat=FakeCatalog(table))
        projection.save([state(seq=3, status="PICKING")])
        stats = projection.commit()
        self.assertEqual(stats.rows_written, 0)
        self.assertEqual(stats.rows_dropped_as_stale, 1)
        self.assertEqual(table.rows["ord_a"]["last_sequence_no"], 9)
        self.assertEqual(table.rows["ord_a"]["status"], "DELIVERED")

    def test_the_refresh_reloads_the_table_it_was_given(self):
        """An identifier hard-coded in a refresh function works until a second object exists,
        and then it writes to the wrong place without any error.

        The first version reloaded `TABLE_NAME`. In the first test that used a probe table, a
        conflict made the probe writer reload the PRODUCTION table and write to it — four
        synthetic rows landed in `live_order_state`, and only the reconciliation, three steps
        later, pointed it out."""
        class ProbeCatalog(FakeCatalog):
            def __init__(self, table):
                super().__init__(table)
                self.requests = []

            def load_table(self, name):
                self.requests.append(name)
                return super().load_table(name)

        table = FakeIcebergTable([], conflicts_before_success=1)
        cat = ProbeCatalog(table)
        projection = IcebergProjection(table, cat=cat)
        projection.save([state(seq=1)])
        projection.commit()
        self.assertTrue(cat.requests, "the refresh reloaded nothing")
        for request in cat.requests:
            self.assertEqual(request, table.name(),
                             "the refresh reloaded a name that is not the table's own")

    def test_it_gives_up_loudly_instead_of_looping_forever(self):
        table = FakeIcebergTable([], conflicts_before_success=999)
        projection = IcebergProjection(table, cat=FakeCatalog(table))
        projection.save([state(seq=1)])
        with self.assertRaises(OrdersProjectionError) as ctx:
            projection.commit()
        self.assertIn("contention", str(ctx.exception))

    def test_rollback_discards_the_buffer(self):
        table = FakeIcebergTable([])
        projection = IcebergProjection(table, cat=FakeCatalog(table))
        projection.save([state(seq=1)])
        projection.rollback()
        self.assertEqual(projection.commit().rows_written, 0)
        self.assertEqual(table.upserts, 0)


class TestSinkParity(unittest.TestCase):
    """The seam declared in Milestone 5, asserted instead of promised."""

    def test_both_sinks_expose_the_same_small_interface(self):
        """If `orders-project` needed one more method to write to Iceberg, the seam would have
        leaked — and swapping storage would no longer be swapping the sink."""
        from retail_platform.orders_stream import PostgresProjection

        required = ("load", "save", "commit", "rollback", "count", "digest")
        for cls in (PostgresProjection, IcebergProjection):
            for method in required:
                self.assertTrue(callable(getattr(cls, method, None)),
                                f"{cls.__name__} does not expose {method}")

    def test_the_digest_ignores_provenance(self):
        """`written_by` is provenance, not state. Including it would make the digest change when
        the SAME content was written by the other writer — and the property we want to measure
        is precisely that both produce the same state."""
        one = FakeIcebergTable([_normalize(state(seq=4), WRITER_STREAM)])
        other = FakeIcebergTable([_normalize(state(seq=4), WRITER_REBUILD)])
        self.assertEqual(
            IcebergProjection(one, cat=FakeCatalog(one)).digest(),
            IcebergProjection(other, cat=FakeCatalog(other)).digest(),
        )

    def test_the_digest_changes_when_state_changes(self):
        one = FakeIcebergTable([_normalize(state(seq=4), WRITER_STREAM)])
        other = FakeIcebergTable([_normalize(state(seq=5), WRITER_STREAM)])
        self.assertNotEqual(
            IcebergProjection(one, cat=FakeCatalog(one)).digest(),
            IcebergProjection(other, cat=FakeCatalog(other)).digest(),
        )


class TestPartitionSelection(unittest.TestCase):
    def test_through_limits_the_batch_layer_to_settled_history(self):
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as root:
            for day in ("2026-08-24", "2026-08-25", "2026-08-26"):
                path = os.path.join(root, f"ingestion_date={day}", "wh=mad1")
                os.makedirs(path)
                open(os.path.join(path, "_SUCCESS"), "w").close()
            self.assertEqual(len(partitions_of(root)), 3)
            self.assertEqual(len(partitions_of(root, through="2026-08-25")), 2)

    def test_a_partition_without_success_is_not_read(self):
        """`_SUCCESS` is the last object the Source writes. Reading a partition without it would
        be reading an interrupted extraction as if it were complete."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "ingestion_date=2026-08-24", "wh=mad1"))
            self.assertEqual(partitions_of(root), [])


if __name__ == "__main__":
    unittest.main()
