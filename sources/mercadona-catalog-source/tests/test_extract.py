"""End-to-end extraction, no network: every closed gap has a test that fails without the fix."""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from mercadona_catalog_source import extract, partition as part
from mercadona_catalog_source.canonical import digest, read_json, write_json
from tests.support import catalog, extract_args, make_fetcher_factory, tree

TREE = tree((1, "Parent", [(10, "Alfa"), (11, "Beta")]))
CATALOGS = {
    "/categories/10/": catalog(10, "Alfa", ("g1", [(1, "p1", "1.00"), (2, "p2", "2.00")])),
    "/categories/11/": catalog(11, "Beta", ("g1", [(3, "p3", "3.00")])),
}
RESPONSES = {"/categories/": TREE, **CATALOGS}


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
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")

    def test_full_extraction(self):
        code, fetcher, _ = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertEqual(fetcher.requests, 3)  # tree + 2 categories

        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["manifest_version"], 2)
        self.assertEqual(manifest["source"]["name"], "mercadona_catalog_api")
        self.assertEqual(manifest["partition"], {"ingestion_date": "2026-01-01", "warehouse": "mad1"})
        self.assertEqual(manifest["totals"]["product_rows"], 3)
        self.assertEqual(manifest["totals"]["unique_product_ids"], 3)
        self.assertEqual(manifest["totals"]["catalog_files"], 2)
        self.assertEqual(manifest["failures"], [])
        self.assertEqual(manifest["history"], [])
        self.assertIn("sha256", manifest["schema_fingerprint"])

    def test_success_marker_created_when_complete(self):
        run_extract(self.root)
        self.assertTrue(os.path.exists(part.success_path(self.partition)))

    def test_totals_match_the_sum_of_the_files(self):
        run_extract(self.root)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(
            manifest["totals"]["bytes"], sum(f["bytes"] for f in manifest["files"])
        )
        self.assertEqual(
            manifest["totals"]["product_rows"],
            sum(f["records"] for f in manifest["files"] if f["stage"] == "catalog"),
        )


class ArgumentGuardTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def test_limit_zero_is_refused_instead_of_processing_everything(self):
        code, fetcher, _ = run_extract(self.root, limit=0)
        self.assertEqual(code, 2)
        self.assertIsNone(fetcher)

    def test_negative_limit_is_refused(self):
        self.assertEqual(run_extract(self.root, limit=-3)[0], 2)

    def test_valid_limit_produces_partial_partition(self):
        code, _, _ = run_extract(self.root, limit=1)
        self.assertEqual(code, 0)
        manifest = part.read_manifest(
            os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")
        )
        self.assertFalse(manifest["complete"])
        self.assertEqual(manifest["totals"]["catalog_files"], 1)

    def test_malicious_warehouse_is_refused(self):
        self.assertEqual(run_extract(self.root, wh="../../etc")[0], 2)

    def test_invalid_date_is_refused(self):
        self.assertEqual(run_extract(self.root, date="15-08-2026")[0], 2)


class ResumeAndImmutabilityTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")

    def _partial(self):
        return run_extract(self.root, errors={"/categories/11/": "HTTP 500"})

    def test_partial_failure_returns_1_and_does_not_mark_success(self):
        code, _, _ = self._partial()
        self.assertEqual(code, 1)
        manifest = part.read_manifest(self.partition)
        self.assertFalse(manifest["complete"])
        self.assertEqual(len(manifest["failures"]), 1)
        self.assertFalse(os.path.exists(part.success_path(self.partition)))

    def test_rerun_resumes_reusing_what_already_exists(self):
        self._partial()
        code, fetcher, _ = run_extract(self.root)
        self.assertEqual(code, 0)
        # tree + only the missing category; the one already written was reused
        self.assertEqual(fetcher.requests, 2)
        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["totals"]["product_rows"], 3)

    def test_history_preserves_the_previous_run(self):
        self._partial()
        run_extract(self.root)
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
        self.assertEqual(fetcher.requests, 3)

    def test_mismatched_language_in_same_partition_is_refused(self):
        self._partial()
        code, _, output = run_extract(self.root, lang="en")
        self.assertEqual(code, 2)
        self.assertIn("lang", output)


class CorruptAndTamperedFilesTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")
        run_extract(self.root, errors={"/categories/11/": "HTTP 500"})
        self.target = part.catalog_path(self.partition, 10)

    def test_truncated_file_is_redownloaded_instead_of_crashing_the_run(self):
        with open(self.target, "wb") as handle:
            handle.write(b'{"id": 10, "categ')
        code, fetcher, output = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertIn("unreadable", output)
        self.assertIn("/categories/10/", fetcher.paths)
        self.assertEqual(part.read_manifest(self.partition)["totals"]["product_rows"], 3)

    def test_empty_file_is_redownloaded(self):
        open(self.target, "wb").close()
        self.assertEqual(run_extract(self.root)[0], 0)

    def test_valid_file_with_other_shape_is_redownloaded(self):
        write_json(self.target, ["not", "an", "object"])
        code, fetcher, _ = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertIn("/categories/10/", fetcher.paths)

    def test_tampered_file_is_redownloaded_and_recorded_as_anomaly(self):
        payload, _ = read_json(self.target)
        payload["categories"][0]["products"].append(
            {"id": "999", "display_name": "injected", "price_instructions": {"unit_price": "0.01"}}
        )
        write_json(self.target, payload)
        code, fetcher, output = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertIn("differs from the previous manifest", output)
        # re-downloaded from the source: the injected content does not survive
        self.assertIn("/categories/10/", fetcher.paths)
        restored, _ = read_json(self.target)
        names = [p["display_name"] for g in restored["categories"] for p in g["products"]]
        self.assertNotIn("injected", names)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["anomalies"]), 1)
        self.assertEqual(
            manifest["anomalies"][0]["kind"], "conteudo_divergente_do_manifesto_anterior"
        )
        self.assertEqual(
            manifest["totals"]["product_rows"],
            sum(f["records"] for f in manifest["files"] if f["stage"] == "catalog"),
        )

    def test_tampering_is_not_laundered_by_rerun(self):
        payload, _ = read_json(self.target)
        payload["categories"][0]["products"].append(
            {"id": "999", "display_name": "injected", "price_instructions": {"unit_price": "0.01"}}
        )
        write_json(self.target, payload)
        run_extract(self.root)
        # second re-run: the content was already restored, nothing injected survives
        run_extract(self.root, overwrite=True)
        restored, _ = read_json(self.target)
        names = [p["display_name"] for g in restored["categories"] for p in g["products"]]
        self.assertNotIn("injected", names)


class PartitionWithoutManifestTest(unittest.TestCase):
    """Without a previous manifest, the contract locks must not be switched off.

    Regression for the audit's most serious finding: an interrupted run left the
    partition without a manifest, and the resume accepted any content on disk, in
    any language, on top of any partition.
    """

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")

    def test_preliminary_manifest_exists_right_after_the_tree(self):
        responses = dict(RESPONSES)
        responses["/categories/11/"] = KeyboardInterrupt()
        factory, _ = make_fetcher_factory(responses)
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                extract.run(extract_args(self.root), fetcher_factory=factory)
        manifest = part.read_manifest(self.partition)
        self.assertIsNotNone(manifest, "interruption left the partition without a manifest")
        self.assertFalse(manifest["complete"])
        self.assertEqual(manifest["source"]["lang"], "es")
        self.assertFalse(os.path.exists(part.success_path(self.partition)))

    def test_resume_after_interruption_refuses_another_language(self):
        responses = dict(RESPONSES)
        responses["/categories/11/"] = KeyboardInterrupt()
        factory, _ = make_fetcher_factory(responses)
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                extract.run(extract_args(self.root), fetcher_factory=factory)
        code, _, output = run_extract(self.root, lang="en")
        self.assertEqual(code, 2)
        self.assertIn("lang", output)

    def test_file_not_in_canonical_form_is_redownloaded(self):
        os.makedirs(os.path.join(self.partition, "catalog"), exist_ok=True)
        target = part.catalog_path(self.partition, 10)
        with open(target, "w", encoding="utf-8") as handle:
            json.dump({"id": 10, "name": "forged", "categories": []}, handle, indent=4)
        code, fetcher, output = run_extract(self.root)
        self.assertEqual(code, 0)
        self.assertIn("canonical form", output)
        self.assertIn("/categories/10/", fetcher.paths)
        manifest = part.read_manifest(self.partition)
        self.assertTrue(
            any(a["kind"] == "arquivo_nao_canonico" for a in manifest["anomalies"])
        )
        # the recorded digest is the canonical one, not the forged file's
        entry = next(f for f in manifest["files"] if f["path"].endswith("category_id=10.json"))
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
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")
        run_extract(self.root, errors={"/categories/11/": "HTTP 500"})

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
    """The manifest must describe the whole partition, not just what the run touched."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")

    def test_file_not_visited_due_to_limit_enters_the_manifest(self):
        run_extract(self.root, errors={"/categories/11/": "HTTP 500"})  # writes 10
        code, _, output = run_extract(self.root, limit=1)
        self.assertEqual(code, 0)
        manifest = part.read_manifest(self.partition)
        declared = {
            f["category_id"] for f in manifest["files"] if f["stage"] == "catalog"
        }
        self.assertEqual(declared, {10})
        self.assertEqual(manifest["totals"]["catalog_files"], 1)

    def test_file_of_category_removed_from_tree_stays_declared(self):
        run_extract(self.root)
        # the source removes category 11 from the tree; the file stays on disk
        responses = dict(RESPONSES)
        responses["/categories/"] = tree((1, "Parent", [(10, "Alfa")]))
        code, _, output = run_extract(self.root, responses=responses, overwrite=True)
        self.assertEqual(code, 0)
        self.assertIn("[inventory]", output)
        manifest = part.read_manifest(self.partition)
        declared = {
            f["category_id"] for f in manifest["files"] if f["stage"] == "catalog"
        }
        self.assertIn(11, declared)
        self.assertEqual(manifest["totals"]["product_rows"], 3)


class SourceShapeChangeTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01", "wh=mad1")

    def test_tree_with_unexpected_format_is_fatal_but_keeps_the_download(self):
        code, _, output = run_extract(self.root, responses={"/categories/": {"items": []}})
        self.assertEqual(code, 2)
        self.assertIn("category tree format", output)
        # the successful download is not discarded because of the format
        payload, _ = read_json(part.categories_path(self.partition))
        self.assertEqual(payload, {"items": []})

    def test_tree_without_level_2_categories_is_fatal(self):
        code, _, _ = run_extract(self.root, responses={"/categories/": tree((1, "Parent", []))})
        self.assertEqual(code, 2)

    def test_unavailable_tree_is_fatal(self):
        code, _, _ = run_extract(self.root, errors={"/categories/": "HTTP 503"})
        self.assertEqual(code, 2)

    def test_catalog_that_is_not_an_object_becomes_recorded_failure(self):
        responses = dict(RESPONSES)
        responses["/categories/10/"] = ["unexpected", "list"]
        code, _, _ = run_extract(self.root, responses=responses)
        self.assertEqual(code, 1)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["failures"]), 1)
        # The persisted failure text is a contract literal (Portuguese, kept verbatim).
        self.assertIn("objeto JSON", manifest["failures"][0]["error"])

    def test_catalog_without_products_records_zero_rows(self):
        responses = dict(RESPONSES)
        responses["/categories/10/"] = {"id": 10, "name": "Alfa", "sections": []}
        code, _, _ = run_extract(self.root, responses=responses)
        self.assertEqual(code, 0)
        self.assertEqual(part.read_manifest(self.partition)["totals"]["product_rows"], 1)


if __name__ == "__main__":
    unittest.main()
