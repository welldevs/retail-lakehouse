"""Defensive payload navigation and schema fingerprint."""

from __future__ import annotations

import unittest

from ine_population_source import schema
from tests.support import series, table_payload


class SeriesOfTest(unittest.TestCase):
    def test_lists_valid_series(self):
        payload = table_payload(series("A", "Total. Madrid.", (2025, 1)), series("B", "Total. Sevilla.", (2025, 2)))
        self.assertEqual([s["COD"] for s in schema.series_of(payload)], ["A", "B"])

    def test_discards_item_that_is_not_a_mapping(self):
        payload = [series("A", "x", (2025, 1)), "junk", None, 42]
        self.assertEqual(len(schema.series_of(payload)), 1)

    def test_unexpected_shape_returns_empty_list(self):
        for payload in (None, "text", {"not": "list"}, 42):
            with self.subTest(payload=payload):
                self.assertEqual(schema.series_of(payload), [])


class DataPointsOfTest(unittest.TestCase):
    def test_lists_valid_data_points(self):
        s = series("A", "x", (2024, 1), (2025, 2))
        self.assertEqual(len(schema.data_points_of(s)), 2)

    def test_discards_point_that_is_not_a_mapping(self):
        s = series("A", "x", (2025, 1))
        s["Data"].append("junk")
        self.assertEqual(len(schema.data_points_of(s)), 1)

    def test_unexpected_shape_returns_empty_list(self):
        for value in (None, "text", {"Data": "not-list"}, {"no_data": True}):
            with self.subTest(value=value):
                self.assertEqual(schema.data_points_of(value), [])


class DefensiveCountingTest(unittest.TestCase):
    def test_counts_series_and_data_points(self):
        payload = table_payload(series("A", "x", (2024, 1), (2025, 2)), series("B", "y", (2025, 3)))
        self.assertEqual(schema.count_series(payload), 2)
        self.assertEqual(schema.count_data_points(payload), 3)

    def test_does_not_raise_on_degenerate_payload(self):
        for payload in (None, [], "x", {}, [None, 3]):
            with self.subTest(payload=payload):
                self.assertEqual(schema.count_series(payload), 0)
                self.assertEqual(schema.count_data_points(payload), 0)


class ValidatePayloadTest(unittest.TestCase):
    def test_accepts_list_of_series(self):
        payload = table_payload(series("A", "x", (2025, 1)))
        self.assertEqual(len(schema.validate_payload(payload)), 1)

    def test_root_that_is_not_a_list_raises_schema_error(self):
        for payload in ({"results": []}, "text", None, 42):
            with self.subTest(payload=payload):
                with self.assertRaises(schema.SchemaError):
                    schema.validate_payload(payload)

    def test_list_without_recognizable_series_raises_schema_error(self):
        with self.assertRaises(schema.SchemaError):
            schema.validate_payload(["junk", None])


class FingerprintTest(unittest.TestCase):
    def test_same_shape_produces_same_hash(self):
        a = table_payload(series("A", "x", (2025, 1)))
        b = table_payload(series("B", "y", (2024, 9)))
        self.assertEqual(schema.fingerprint([a])["sha256"], schema.fingerprint([b])["sha256"])

    def test_field_renamed_in_series_changes_the_hash(self):
        base = table_payload(series("A", "x", (2025, 1)))
        mutated = table_payload(series("A", "x", (2025, 1)))
        mutated[0]["Codigo"] = mutated[0].pop("COD")
        self.assertNotEqual(
            schema.fingerprint([base])["sha256"], schema.fingerprint([mutated])["sha256"]
        )

    def test_field_renamed_in_data_point_changes_the_hash(self):
        base = table_payload(series("A", "x", (2025, 1)))
        mutated = table_payload(series("A", "x", (2025, 1)))
        mutated[0]["Data"][0]["Ano"] = mutated[0]["Data"][0].pop("Anyo")
        self.assertNotEqual(
            schema.fingerprint([base])["sha256"], schema.fingerprint([mutated])["sha256"]
        )

    def test_records_the_observed_keys(self):
        fp = schema.fingerprint([table_payload(series("A", "x", (2025, 1)))])
        self.assertIn("COD", fp["series_keys"])
        self.assertIn("Valor", fp["data_point_keys"])
        self.assertNotIn("Data", fp["series_keys"])


if __name__ == "__main__":
    unittest.main()
