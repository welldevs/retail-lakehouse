"""End-to-end extraction, no network: every closed gap has a test that fails without the fix."""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from ine_population_source import extract, partition as part
from ine_population_source.canonical import digest, read_json, write_json
from tests.support import extract_args, make_fetcher_factory, series, table_payload

TABLE_31304 = table_payload(series("A", "Total. Madrid. Ambos sexos.", (2025, 6779888)))
TABLE_9689 = table_payload(series("B", "Total. Barcelona. Ambos sexos.", (2025, 5716544)))
RESPONSES = {"/ES/DATOS_TABLA/31304": TABLE_31304, "/ES/DATOS_TABLA/9689": TABLE_9689}


def run_extract(out, responses=None, errors=None, **overrides):
    factory, created = make_fetcher_factory(
        dict(RESPONSES if responses is None else responses), errors
    )
    args = extract_args(out, **overrides)
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = extract.run(args, fetcher_factory=factory)
    return code, created[0] if created else None, buffer.getvalue()


class ExtractionHappyPathTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")

    def test_complete_extraction(self):
        code, fetcher, _ = run_extract(self.root, tables=["31304", "9689"])
        self.assertEqual(code, 0)
        self.assertEqual(fetcher.requests, 2)

        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["manifest_version"], 1)
        self.assertEqual(manifest["source"]["name"], "ine_population_api")
        self.assertEqual(manifest["partition"], {"ingestion_date": "2026-01-01"})
        self.assertEqual(manifest["totals"]["series_count"], 2)
        self.assertEqual(manifest["totals"]["table_files"], 2)
        self.assertEqual(manifest["failures"], [])
        self.assertEqual(manifest["history"], [])
        self.assertIn("sha256", manifest["schema_fingerprint"])

    def test_success_marker_created_when_complete(self):
        run_extract(self.root)
        self.assertTrue(os.path.exists(part.success_path(self.partition)))

    def test_totals_match_the_sum_of_the_files(self):
        run_extract(self.root, tables=["31304", "9689"])
        manifest = part.read_manifest(self.partition)
        self.assertEqual(
            manifest["totals"]["bytes"], sum(f["bytes"] for f in manifest["files"])
        )
        self.assertEqual(
            manifest["totals"]["table_files"],
            sum(1 for f in manifest["files"] if f["stage"] == "tables"),
        )


class ArgumentGuardTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def test_empty_tables_list_is_rejected(self):
        code, fetcher, _ = run_extract(self.root, tables=[])
        self.assertEqual(code, 2)
        self.assertIsNone(fetcher)

    def test_malicious_table_id_is_rejected(self):
        self.assertEqual(run_extract(self.root, tables=["../../etc"])[0], 2)

    def test_invalid_date_is_rejected(self):
        self.assertEqual(run_extract(self.root, date="15-08-2026")[0], 2)


class ResumeAndImmutabilityTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")

    def _partial(self):
        return run_extract(
            self.root, tables=["31304", "9689"], errors={"/ES/DATOS_TABLA/9689": "HTTP 500"}
        )

    def test_partial_failure_returns_1_and_does_not_mark_success(self):
        code, _, _ = self._partial()
        self.assertEqual(code, 1)
        manifest = part.read_manifest(self.partition)
        self.assertFalse(manifest["complete"])
        self.assertEqual(len(manifest["failures"]), 1)
        self.assertFalse(os.path.exists(part.success_path(self.partition)))

    def test_rerun_resumes_reusing_what_already_exists(self):
        self._partial()
        code, fetcher, _ = run_extract(self.root, tables=["31304", "9689"])
        self.assertEqual(code, 0)
        # only the missing table; the one already written was reused
        self.assertEqual(fetcher.requests, 1)
        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["totals"]["series_count"], 2)

    def test_history_preserves_the_previous_run(self):
        self._partial()
        run_extract(self.root, tables=["31304", "9689"])
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["history"]), 1)
        self.assertFalse(manifest["history"][0]["complete"])

    def test_complete_partition_is_immutable(self):
        run_extract(self.root)
        code, fetcher, output = run_extract(self.root)
        self.assertEqual(code, 2)
        self.assertIsNone(fetcher)
        self.assertIn("immutable", output)

    def test_overwrite_rewrites_complete_partition(self):
        run_extract(self.root)
        code, fetcher, _ = run_extract(self.root, overwrite=True)
        self.assertEqual(code, 0)
        self.assertEqual(fetcher.requests, 1)


class CorruptAndTamperedFilesTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        run_extract(self.root, tables=["31304", "9689"], errors={"/ES/DATOS_TABLA/9689": "HTTP 500"})
        self.target = part.table_path(self.partition, 31304)

    def test_truncated_file_is_downloaded_again_instead_of_crashing_the_run(self):
        with open(self.target, "wb") as handle:
            handle.write(b'[{"COD": "A", "Nomb')
        code, fetcher, output = run_extract(self.root, tables=["31304", "9689"])
        self.assertEqual(code, 0)
        self.assertIn("unreadable", output)
        self.assertIn("/ES/DATOS_TABLA/31304", fetcher.paths)
        self.assertEqual(part.read_manifest(self.partition)["totals"]["series_count"], 2)

    def test_empty_file_is_downloaded_again(self):
        open(self.target, "wb").close()
        self.assertEqual(run_extract(self.root, tables=["31304", "9689"])[0], 0)

    def test_valid_file_with_another_shape_is_downloaded_again(self):
        write_json(self.target, {"not": "a list"})
        code, fetcher, _ = run_extract(self.root, tables=["31304", "9689"])
        self.assertEqual(code, 0)
        self.assertIn("/ES/DATOS_TABLA/31304", fetcher.paths)

    def test_tampered_file_is_downloaded_again_and_recorded_as_anomaly(self):
        payload, _ = read_json(self.target)
        payload.append({"COD": "injected", "Nombre": "x", "Data": [{"Anyo": 2020, "Valor": 1}]})
        write_json(self.target, payload)
        code, fetcher, output = run_extract(self.root, tables=["31304", "9689"])
        self.assertEqual(code, 0)
        self.assertIn("differs from the previous manifest", output)
        self.assertIn("/ES/DATOS_TABLA/31304", fetcher.paths)
        restored, _ = read_json(self.target)
        codes = [s["COD"] for s in restored]
        self.assertNotIn("injected", codes)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["anomalies"]), 1)
        self.assertEqual(
            manifest["anomalies"][0]["kind"], "conteudo_divergente_do_manifesto_anterior"
        )

    def test_tampering_is_not_laundered_by_rerun(self):
        payload, _ = read_json(self.target)
        payload.append({"COD": "injected", "Nombre": "x", "Data": [{"Anyo": 2020, "Valor": 1}]})
        write_json(self.target, payload)
        run_extract(self.root, tables=["31304", "9689"])
        run_extract(self.root, tables=["31304", "9689"], overwrite=True)
        restored, _ = read_json(self.target)
        codes = [s["COD"] for s in restored]
        self.assertNotIn("injected", codes)


class PartitionWithoutManifestTest(unittest.TestCase):
    """Without a previous manifest, the contract locks cannot be switched off."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")

    def test_preliminary_manifest_exists_before_any_table(self):
        responses = dict(RESPONSES)
        responses["/ES/DATOS_TABLA/31304"] = KeyboardInterrupt()
        factory, _ = make_fetcher_factory(responses)
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                extract.run(extract_args(self.root), fetcher_factory=factory)
        manifest = part.read_manifest(self.partition)
        self.assertIsNotNone(manifest, "interruption left the partition without a manifest")
        self.assertFalse(manifest["complete"])
        self.assertFalse(os.path.exists(part.success_path(self.partition)))

    def test_file_not_in_canonical_form_is_downloaded_again(self):
        os.makedirs(os.path.join(self.partition, "tables"), exist_ok=True)
        target = part.table_path(self.partition, 31304)
        with open(target, "w", encoding="utf-8") as handle:
            json.dump([{"COD": "forged", "Nombre": "x", "Data": []}], handle, indent=4)
        code, fetcher, output = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertIn("canonical form", output)
        self.assertIn("/ES/DATOS_TABLA/31304", fetcher.paths)
        manifest = part.read_manifest(self.partition)
        self.assertTrue(
            any(a["kind"] == "arquivo_nao_canonico" for a in manifest["anomalies"])
        )
        entry = next(f for f in manifest["files"] if f["path"].endswith("table_id=31304.json"))
        with open(target, "rb") as handle:
            self.assertEqual(entry["sha256"], digest(handle.read()))

    def test_success_without_manifest_still_blocks_rewrite(self):
        run_extract(self.root)
        os.unlink(part.manifest_path(self.partition))
        code, _, output = run_extract(self.root)
        self.assertEqual(code, 2)
        self.assertIn("_SUCCESS", output)

    def test_success_without_manifest_yields_to_overwrite(self):
        run_extract(self.root)
        os.unlink(part.manifest_path(self.partition))
        self.assertEqual(run_extract(self.root, overwrite=True)[0], 0)


class CorruptPreviousManifestTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        run_extract(self.root, tables=["31304", "9689"], errors={"/ES/DATOS_TABLA/9689": "HTTP 500"})

    def _corrupt(self, content=b"{not json"):
        with open(part.manifest_path(self.partition), "wb") as handle:
            handle.write(content)

    def test_unreadable_manifest_fails_with_code_2_not_3(self):
        self._corrupt()
        code, _, output = run_extract(self.root)
        self.assertEqual(code, 2)
        self.assertIn("unreadable", output)

    def test_unreadable_manifest_is_rescued_by_overwrite(self):
        self._corrupt()
        self.assertEqual(run_extract(self.root, overwrite=True)[0], 0)

    def test_manifest_without_files_list_fails_with_code_2(self):
        write_json(part.manifest_path(self.partition), {"files": "not a list"})
        code, _, output = run_extract(self.root)
        self.assertEqual(code, 2)
        self.assertIn("files", output)


class InventorySweepTest(unittest.TestCase):
    """The manifest must describe the whole partition, not only what the run touched."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")

    def test_file_of_table_id_removed_from_config_stays_declared(self):
        run_extract(self.root, tables=["31304", "9689"])
        # the next run only asks for 31304; the 9689 file stays on disk
        code, _, output = run_extract(self.root, tables=["31304"], overwrite=True)
        self.assertEqual(code, 0)
        self.assertIn("[inventory]", output)
        manifest = part.read_manifest(self.partition)
        declared = {f["table_id"] for f in manifest["files"] if f["stage"] == "tables"}
        self.assertIn("9689", declared)
        self.assertEqual(manifest["totals"]["series_count"], 2)


class SourceShapeChangeTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")

    def test_payload_that_is_not_a_list_becomes_recorded_non_fatal_failure(self):
        """Unlike the Mercadona tree, a table with an unexpected shape is not fatal for the
        whole partition: it is just one more per-table_id failure, re-runnable."""
        code, _, _ = run_extract(self.root, responses={"/ES/DATOS_TABLA/31304": {"not": "list"}})
        self.assertEqual(code, 1)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["failures"]), 1)
        # The persisted SchemaError text is a contract literal (kept in Portuguese).
        self.assertIn("lista JSON", manifest["failures"][0]["error"])

    def test_unavailable_table_is_recorded_failure(self):
        code, _, _ = run_extract(self.root, errors={"/ES/DATOS_TABLA/31304": "HTTP 503"})
        self.assertEqual(code, 1)

    def test_series_without_points_records_zero_points_but_counts_as_series(self):
        responses = {"/ES/DATOS_TABLA/31304": [{"COD": "A", "Nombre": "x", "Data": []}]}
        code, _, _ = run_extract(self.root, responses=responses)
        self.assertEqual(code, 0)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(manifest["totals"]["series_count"], 1)
        self.assertEqual(manifest["totals"]["data_point_count"], 0)


if __name__ == "__main__":
    unittest.main()
