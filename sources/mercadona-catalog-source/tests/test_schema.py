"""Defensive payload navigation and schema fingerprint."""

from __future__ import annotations

import unittest

from mercadona_catalog_source import schema
from tests.support import catalog, tree


class FlattenTreeTest(unittest.TestCase):
    def test_flattens_to_level_2(self):
        nodes = schema.flatten_tree(tree((1, "Parent", [(10, "A"), (11, "B")])))
        self.assertEqual([n["id"] for n in nodes], [10, 11])
        self.assertEqual(nodes[0]["parent_name"], "Parent")

    def test_dedupes_ids_repeated_under_different_parents(self):
        nodes = schema.flatten_tree(
            tree((1, "P1", [(10, "A")]), (2, "P2", [(10, "A"), (11, "B")]))
        )
        self.assertEqual([n["id"] for n in nodes], [10, 11])

    def test_drops_child_without_id(self):
        payload = {"results": [{"id": 1, "name": "P", "categories": [{"name": "no id"}]}]}
        self.assertEqual(schema.flatten_tree(payload), [])

    def test_tolerates_child_without_name(self):
        payload = {"results": [{"id": 1, "name": "P", "categories": [{"id": 10}]}]}
        self.assertEqual(schema.flatten_tree(payload)[0]["name"], "")

    def test_unexpected_format_raises_schema_error(self):
        for payload in ([], "text", {"resultados": []}, {"results": "not-a-list"}, None):
            with self.subTest(payload=payload):
                with self.assertRaises(schema.SchemaError):
                    schema.flatten_tree(payload)


class DefensiveCountingTest(unittest.TestCase):
    def test_counts_products_across_multiple_subgroups(self):
        payload = catalog(1, "C", ("g1", [(1, "a", "1.00"), (2, "b", "2.00")]), ("g2", [(3, "c", "3.00")]))
        self.assertEqual(schema.count_products(payload), 3)
        self.assertEqual(schema.collect_product_ids(payload), {"1", "2", "3"})

    def test_does_not_raise_on_degenerate_payload(self):
        for payload in (None, [], "x", {}, {"categories": "x"}, {"categories": [None, 3]}):
            with self.subTest(payload=payload):
                self.assertEqual(schema.count_products(payload), 0)
                self.assertEqual(schema.collect_product_ids(payload), set())

    def test_product_without_id_counts_as_row_but_not_as_unique(self):
        payload = {"categories": [{"name": "g", "products": [{"display_name": "x"}]}]}
        self.assertEqual(schema.count_products(payload), 1)
        self.assertEqual(schema.collect_product_ids(payload), set())

    def test_counts_level_1_categories(self):
        self.assertEqual(schema.count_level1(tree((1, "A", []), (2, "B", []))), 2)
        self.assertEqual(schema.count_level1("garbage"), 0)


class FingerprintTest(unittest.TestCase):
    def test_same_shape_produces_same_hash(self):
        a = catalog(1, "C", ("g", [(1, "a", "1.00")]))
        b = catalog(2, "D", ("g", [(2, "b", "9.99")]))
        self.assertEqual(schema.fingerprint([a])["sha256"], schema.fingerprint([b])["sha256"])

    def test_renamed_field_changes_the_hash(self):
        base = catalog(1, "C", ("g", [(1, "a", "1.00")]))
        mutated = catalog(1, "C", ("g", [(1, "a", "1.00")]))
        product = mutated["categories"][0]["products"][0]
        product["renamed_display_name"] = product.pop("display_name")
        self.assertNotEqual(
            schema.fingerprint([base])["sha256"], schema.fingerprint([mutated])["sha256"]
        )

    def test_renamed_price_field_changes_the_hash(self):
        base = catalog(1, "C", ("g", [(1, "a", "1.00")]))
        mutated = catalog(1, "C", ("g", [(1, "a", "1.00")]))
        prices = mutated["categories"][0]["products"][0]["price_instructions"]
        prices["renamed_unit_price"] = prices.pop("unit_price")
        self.assertNotEqual(
            schema.fingerprint([base])["sha256"], schema.fingerprint([mutated])["sha256"]
        )

    def test_records_the_observed_keys(self):
        fp = schema.fingerprint([catalog(1, "C", ("g", [(1, "a", "1.00")]))])
        self.assertIn("display_name", fp["product_keys"])
        self.assertIn("unit_price", fp["price_instruction_keys"])


if __name__ == "__main__":
    unittest.main()
