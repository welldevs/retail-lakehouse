"""Schema fingerprint and required fields."""

from __future__ import annotations

import unittest

from simulated_oltp_source.schema import (
    CUSTOMER_FIELDS,
    count_customers,
    customers_of,
    fingerprint,
    missing_fields,
)


def customer(**overrides) -> dict:
    base = {field: "x" for field in CUSTOMER_FIELDS}
    base["house_number"] = 12
    base["birth_year"] = 1990
    base["candidate_index"] = 0
    base.update(overrides)
    return base


class ShapeTest(unittest.TestCase):
    def test_unexpected_shape_does_not_raise(self):
        for payload in (None, {}, 3, "text", [1, 2], [{"a": 1}, 5]):
            with self.subTest(payload=payload):
                self.assertIsInstance(customers_of(payload), list)

    def test_counts_only_objects(self):
        self.assertEqual(count_customers([customer(), 7, customer()]), 2)


class FingerprintTest(unittest.TestCase):
    def test_same_keys_give_the_same_hash(self):
        a = fingerprint([[customer(customer_id="a")]])
        b = fingerprint([[customer(customer_id="b")]])
        self.assertEqual(a["sha256"], b["sha256"])

    def test_extra_key_changes_the_hash(self):
        a = fingerprint([[customer()]])
        b = fingerprint([[customer(extra=1)]])
        self.assertNotEqual(a["sha256"], b["sha256"])

    def test_fingerprint_is_the_union_of_keys(self):
        """That is why house_number must always exist, even when null: if the key appeared
        only in some records, the partition hash would depend on the seed."""
        without = customer()
        without.pop("house_number")
        mixed = fingerprint([[without, customer()]])
        self.assertIn("house_number", mixed["customer_keys"])
        self.assertEqual(mixed["sha256"], fingerprint([[customer()]])["sha256"])

    def test_keys_come_out_sorted(self):
        keys = fingerprint([[customer()]])["customer_keys"]
        self.assertEqual(keys, sorted(keys))


class RequiredFieldsTest(unittest.TestCase):
    def test_complete_record_reports_nothing(self):
        self.assertEqual(missing_fields([customer()]), [])

    def test_missing_field_is_reported_once(self):
        incomplete = customer()
        incomplete.pop("postal_code")
        self.assertEqual(missing_fields([incomplete, customer()]), ["postal_code"])

    def test_null_house_number_counts_as_present(self):
        self.assertEqual(missing_fields([customer(house_number=None)]), [])


if __name__ == "__main__":
    unittest.main()
