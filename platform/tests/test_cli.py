"""has-data: the Makefile decides, from the exit code, whether to exclude from `dbt build`
the Silver model of a source that has not landed anything yet. No network."""

from __future__ import annotations

import unittest
from unittest import mock

from retail_platform.cli import EXIT_FAILED, EXIT_OK, main

from .fake_s3 import FakeConfig, FakeS3Client


class TestHasData(unittest.TestCase):
    def setUp(self):
        self.client = FakeS3Client()
        self.config = FakeConfig(self.client)
        patcher = mock.patch("retail_platform.cli.from_env", return_value=self.config)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_exit_ok_when_at_least_one_object_exists_under_the_prefix(self):
        self.client.store[("retail-raw", "ine_population_api/ingestion_date=2026-08-25/x.json")] = b"{}"
        self.assertEqual(main(["has-data", "ine_population_api"]), EXIT_OK)

    def test_exit_failed_when_nothing_exists_under_the_prefix(self):
        self.assertEqual(main(["has-data", "ine_population_api"]), EXIT_FAILED)

    def test_does_not_match_a_different_source_prefix(self):
        """A prefix that is a substring of another must not be confused with it: without the
        trailing slash, "ine_population" would match "ine_population_api_v2" too."""
        self.client.store[("retail-raw", "ine_population_api_v2/ingestion_date=2026-08-25/x.json")] = b"{}"
        self.assertEqual(main(["has-data", "ine_population_api"]), EXIT_FAILED)

    def test_accepts_prefix_with_or_without_trailing_slash(self):
        self.client.store[("retail-raw", "mercadona_catalog_api/ingestion_date=2026-08-25/x.json")] = b"{}"
        self.assertEqual(main(["has-data", "mercadona_catalog_api/"]), EXIT_OK)
        self.assertEqual(main(["has-data", "mercadona_catalog_api"]), EXIT_OK)


if __name__ == "__main__":
    unittest.main()
