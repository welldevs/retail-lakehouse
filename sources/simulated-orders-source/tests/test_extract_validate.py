"""extract and validate end to end, over a fixture reference."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout

from simulated_orders_source import extract as extract_module
from simulated_orders_source import validate as validate_module
from simulated_orders_source.partition import events_path, manifest_path, partition_path

from .support import ORDER_DATE, WH, Args, write_reference


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = write_reference(os.path.join(self.tmp.name, "ref"))
        self.root = os.path.join(self.tmp.name, "orders")
        self.partition = partition_path(self.root, ORDER_DATE, WH)

    def extract(self, **kwargs):
        args = Args(
            reference=self.reference,
            out=self.root,
            wh=WH,
            date=ORDER_DATE,
            seed=42,
            overwrite=False,
        )
        args.__dict__.update(kwargs)
        with redirect_stdout(io.StringIO()) as output:
            code = extract_module.run(args)
        return code, output.getvalue()

    def validate(self, strict=True):
        args = Args(partition=self.partition, reference=self.reference, strict=strict)
        with redirect_stdout(io.StringIO()) as output:
            code = validate_module.run(args)
        return code, output.getvalue()

    def manifest(self):
        with open(manifest_path(self.partition), encoding="utf-8") as handle:
            return json.load(handle)

    def rewrite_log(self, events):
        with open(events_path(self.partition), "w", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")

    def log(self):
        with open(events_path(self.partition), encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]


class ExtractTest(Base):
    def test_writes_log_manifest_and_success(self):
        code, _ = self.extract()
        self.assertEqual(code, 0)
        for name in ("order_events.jsonl", "_manifest.json", "_SUCCESS"):
            self.assertTrue(os.path.exists(os.path.join(self.partition, name)), name)

    def test_partition_has_no_folded_state_alongside(self):
        # There is no orders.json: the state is the fold, and the fold lives in Silver. Two
        # representations of the same truth drift apart.
        self.extract()
        files = sorted(os.listdir(self.partition))
        self.assertEqual(files, ["_SUCCESS", "_manifest.json", "order_events.jsonl"])

    def test_manifest_declares_the_three_reproducible_inputs(self):
        self.extract()
        config = self.manifest()["config"]
        self.assertEqual(config["seed"], 42)
        self.assertIn("premises_sha256", config)
        self.assertIn("reference", config)

    def test_manifest_declares_input_lineage(self):
        self.extract()
        reference = self.manifest()["reference"]
        self.assertEqual(reference["price_as_of"], ORDER_DATE)
        self.assertEqual(reference["price_source"], "observed")
        self.assertIn("customer_ingestion_dates", reference)

    def test_re_extracting_complete_partition_is_refused(self):
        self.extract()
        code, output = self.extract()
        self.assertEqual(code, 2)
        self.assertIn("immutable", output)

    def test_overwrite_rewrites_and_accumulates_history(self):
        self.extract()
        code, _ = self.extract(overwrite=True)
        self.assertEqual(code, 0)
        self.assertEqual(len(self.manifest()["history"]), 1)

    def test_missing_reference_is_fatal_without_writing_anything(self):
        code, output = self.extract(reference=os.path.join(self.tmp.name, "empty"))
        self.assertEqual(code, 2)
        self.assertIn("ERROR", output)
        self.assertFalse(os.path.exists(os.path.join(self.partition, "_SUCCESS")))

    def test_day_outside_window_is_fatal(self):
        code, _ = self.extract(date="2026-01-01")
        self.assertEqual(code, 2)


class ValidateTest(Base):
    def setUp(self):
        super().setUp()
        self.extract()

    def test_intact_partition_passes_strict(self):
        code, output = self.validate()
        self.assertEqual(code, 0, output)

    def test_tampered_checksum_fails(self):
        events = self.log()
        events[0]["wh"] = "bcn1"
        self.rewrite_log(events)
        self.assertEqual(self.validate()[0], 1)

    def test_orphan_file_fails(self):
        with open(os.path.join(self.partition, "leftover.json"), "w", encoding="utf-8") as handle:
            handle.write("{}")
        code, output = self.validate()
        self.assertEqual(code, 1)
        self.assertIn("outside the manifest", output)

    def test_removed_success_fails(self):
        os.unlink(os.path.join(self.partition, "_SUCCESS"))
        code, output = self.validate()
        self.assertEqual(code, 1)
        self.assertIn("_SUCCESS", output)

    def test_reference_with_other_premises_fails(self):
        # Swapping the premises table and regenerating would change the rates behind the same
        # order_id: the digest in the manifest is what makes that visible.
        other = write_reference(os.path.join(self.tmp.name, "ref2"))
        with open(os.path.join(other, "premises.json"), encoding="utf-8") as handle:
            payload = json.load(handle)
        payload["seed_sha256"] = "f" * 64
        with open(os.path.join(other, "premises.json"), "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        args = Args(partition=self.partition, reference=other, strict=False)
        with redirect_stdout(io.StringIO()) as output:
            code = validate_module.run(args)
        self.assertEqual(code, 1)
        self.assertIn("premises", output.getvalue())

    def test_missing_manifest_fails_without_traceback(self):
        os.unlink(manifest_path(self.partition))
        code, output = self.validate()
        self.assertEqual(code, 1)
        self.assertIn("manifest not found", output)

    def test_out_of_order_log_fails(self):
        events = self.log()
        events.reverse()
        self.rewrite_log(events)
        code, output = self.validate()
        self.assertEqual(code, 1)
        self.assertIn("out of order", output)

    def test_tampered_log_fails_with_code_1_and_never_with_exception(self):
        # Counting a broken log must produce a mismatch, never a traceback: otherwise
        # "invalid partition" would become "unhandled exception" and the orchestrator could not
        # tell bad data from a validator bug.
        events = [e for e in self.log() if e["event_type"] != "order_payment_authorized"]
        self.rewrite_log(events)
        code, _ = self.validate()
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
