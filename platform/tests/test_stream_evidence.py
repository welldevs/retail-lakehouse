"""THE STREAM EVIDENCE, checked without a broker, without Postgres and without Iceberg.

The report exists because the streaming half is declared debt: `make test` runs without
network, so the engines only exist while `make stream-up` is up. That makes the report the
only record that their SEMANTICS were exercised — and the failure mode that matters is not
"it broke", it is "it wrote something plausible and wrong".

The tests below aim exactly at that, and the asymmetry with the warehouse evidence is
deliberate: there the capture either happens whole or not at all, because it is a single
connection; here there are THREE independent planes, and the normal case is one of them
being down. What must not happen is an absent plane turning into a zero — "the outbox has
0 events" is a very different claim from "the OLTP did not answer", and both fit in the
same table cell.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.stream_evidence import render, write  # noqa: E402


def _oltp():
    return {
        "orders": 6400, "order_lines": 120693, "outbox_rows": 44456,
        "unpublished": 0, "orders_in_outbox": 6400,
        "published_from": "2026-08-28 18:38:48+00:00",
        "published_to": "2026-08-28 18:45:17+00:00",
        "by_event_type": {"order_placed": 6400, "order_delivered": 6046},
        "orders_by_status": {"DELIVERED": 5985, "CANCELLED": 196},
        "lines_by_status": {"fulfilled": 108194, "removed": 2321},
    }


def _broker():
    return {
        "topic": "retail.orders.events.v1",
        "bootstrap": "localhost:9092",
        "watermarks": {0: {"low": 0, "high": 11306}, 1: {"low": 0, "high": 11164}},
        "lags": {
            "orders-projector": {
                0: {"committed": 11306, "high": 11306, "lag": 0},
                1: {"committed": 11164, "high": 11164, "lag": 0},
            },
            "orders-projector-iceberg": {
                0: {"committed": 11000, "high": 11306, "lag": 306},
                1: {"committed": 11164, "high": 11164, "lag": 0},
            },
        },
    }


def _projection():
    return {
        "table": "projection.live_order_state",
        "metadata_location": "s3://retail-lakehouse/iceberg/.../00052-abc.metadata.json",
        "rows": 6400, "snapshots": 105, "current_snapshot_id": 304920774543672206,
        "written_by": {"rebuild": 4800, "stream": 1600},
        "by_status": {"DELIVERED": 5985},
    }


def _reconciliation(ok=True):
    if ok:
        return {"orders": {"iceberg": 6400, "silver": 6400, "oltp": 6400},
                "compared": 6400, "missing": {}, "divergences": [],
                "divergence_count": 0, "ok": True}
    return {
        "orders": {"iceberg": 6400, "silver": 6400, "oltp": 6399},
        "compared": 6399, "missing": {"oltp": ["ord_mad1_20260827_000010"]},
        "divergences": [{"order_id": "ord_vlc1_20260824_000001", "coluna": "net_amount",
                         "iceberg": "10.00", "silver": "10.50", "oltp": "10.00"}],
        "divergence_count": 1, "ok": False,
    }


def _data(**overrides):
    base = {
        "capturado_em": "2026-08-29 13:32:28 UTC",
        "oltp": _oltp(),
        "broker": _broker(),
        "projecao": _projection(),
        "reconciliacao": _reconciliation(),
    }
    base.update(overrides)
    return base


class FullPlaneTest(unittest.TestCase):
    def setUp(self):
        self.text = render(_data())

    def test_capture_date_appears(self):
        """Without a date, the report is not evidence — it is a claim."""
        self.assertIn("2026-08-29 13:32:28 UTC", self.text)

    def test_all_three_planes_appear(self):
        for heading in ("Transactional plane", "Transport — Kafka", "Projection — Iceberg",
                          "The three folds"):
            self.assertIn(heading, self.text)

    def test_topic_total_is_summed_not_written(self):
        """11,306 + 11,164 = 22,470. A hand-written total goes stale on the first run."""
        self.assertIn("22,470 messages", self.text)

    def test_provenance_of_both_writers_is_published(self):
        """`written_by` is the only QUERYABLE proof that there are two writers. Without it,
        the Iceberg trigger ("a second engine needs to write the same table") becomes an
        anecdote."""
        self.assertIn("`rebuild`", self.text)
        self.assertIn("4,800", self.text)
        self.assertIn("`stream`", self.text)
        self.assertIn("1,600", self.text)

    def test_each_group_lag_is_separate(self):
        """A summed lag would hide precisely the decoupling point: the Iceberg sink can be
        306 messages behind without that affecting the Postgres sink."""
        self.assertIn("`orders-projector`", self.text)
        self.assertIn("`orders-projector-iceberg`", self.text)
        self.assertIn("Total lag: **306**", self.text)
        self.assertIn("Total lag: **0**", self.text)

    def test_current_metadata_is_published_with_full_path(self):
        """It is what lets DuckDB read the table without guessing — and what proves the
        path came from the CATALOG."""
        self.assertIn("00052-abc.metadata.json", self.text)

    def test_agreement_of_three_folds_is_declared_as_agreement(self):
        self.assertIn("all three agree", self.text)
        self.assertIn("6,400 orders", self.text)


class DivergenceTest(unittest.TestCase):
    def test_divergence_shows_row_and_column(self):
        """The most expensive failure mode of this report would be a divergence turning into a
        summary. The reader needs the order_id and the column to go and look."""
        text = render(_data(reconciliacao=_reconciliation(ok=False)))
        self.assertIn("**Divergences: 1.**", text)
        self.assertIn("ord_vlc1_20260824_000001", text)
        self.assertIn("net_amount", text)
        self.assertNotIn("all three agree", text)

    def test_order_missing_from_a_source_is_named(self):
        text = render(_data(reconciliacao=_reconciliation(ok=False)))
        self.assertIn("missing from `oltp`", text)
        self.assertIn("ord_mad1_20260827_000010", text)


class PlaneDownTest(unittest.TestCase):
    """The NORMAL case, not the exceptional one: the three planes come up on demand."""

    def test_absent_plane_shows_as_absence_never_as_zero(self):
        """"The outbox has 0 events" and "the OLTP did not answer" fit in the same table cell
        and mean opposite things. The first is a measurement; the second is the lack of
        one. A report that confuses them is worse than no report."""
        text = render(_data(oltp={"erro": "connection refused"}))
        self.assertIn("**OLTP not observed.**", text)
        self.assertIn("connection refused", text)
        self.assertIn("make stream-up", text)
        self.assertNotIn("| events in `outbox` | 0 |", text)

    def test_one_plane_down_does_not_bring_down_the_others(self):
        """Partial evidence is useful; evidence that does not exist is not."""
        text = render(_data(broker={"erro": "broker does not exist"}))
        self.assertIn("**Broker not observed.**", text)
        self.assertIn("| orders in `orders` | 6,400 |", text)
        self.assertIn("`rebuild`", text)

    def test_failed_group_does_not_pose_as_zero_lag(self):
        """Zero lag is the BEST possible news; an unreadable group must not pass for it. It is
        the same trap as the empty sample in the warehouse evidence."""
        broker = _broker()
        broker["lags"]["orders-projector-iceberg"] = {"erro": "unknown group"}
        text = render(_data(broker=broker))
        self.assertIn("not observed: `unknown group`", text)

    def test_all_planes_down_still_produces_a_readable_report(self):
        text = render(_data(oltp={"erro": "a"}, broker={"erro": "b"},
                              projecao={"erro": "c"}, reconciliacao={"erro": "d"}))
        self.assertEqual(text.count("not observed"), 4)
        self.assertIn("2026-08-29 13:32:28 UTC", text)


class WriteTest(unittest.TestCase):
    def test_writes_atomically_and_leaves_no_temp_file(self):
        """A report truncated by an interruption must not look complete — same discipline as
        the cut's parquet and the landing."""
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "sub", "README.md")
            written = write(_data(), target)
            self.assertTrue(os.path.exists(written))
            self.assertEqual(
                [f for f in os.listdir(os.path.dirname(written)) if f.endswith(".tmp")], []
            )
            self.assertIn("Stream plane evidence",
                          open(written, encoding="utf-8").read())


if __name__ == "__main__":
    unittest.main()
