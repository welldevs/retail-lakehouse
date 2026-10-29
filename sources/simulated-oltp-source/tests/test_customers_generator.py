"""Generation is deterministic, geographically coherent and never fabricates an address."""

from __future__ import annotations

import collections
import os
import random
import tempfile
import unittest

from simulated_oltp_source import customers_generator, reference_data
from simulated_oltp_source.customers_generator import GenerationError, generate, pick_house_number

from tests import support


class GeneratorTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = reference_data.load(
            support.write_reference(os.path.join(self.tmp.name, "ref"))
        )

    def gen(self, wh=support.WH_A, count=50, seed=7, date="2026-08-27"):
        return generate(self.reference, wh, count, seed, date)


class ReproducibilityTest(GeneratorTestCase):
    def test_same_seed_produces_exactly_the_same_output(self):
        self.assertEqual(self.gen(seed=99), self.gen(seed=99))

    def test_different_seeds_produce_different_outputs(self):
        # Without this, an ignored --seed would go unnoticed: the output would still be
        # "deterministic", just insensitive to the parameter.
        self.assertNotEqual(self.gen(seed=1), self.gen(seed=2))

    def test_does_not_depend_on_the_clock(self):
        """birth_year comes from the partition date, never from datetime.now()."""
        from_2026 = self.gen(count=20, date="2026-08-27")
        from_2030 = self.gen(count=20, date="2030-01-01")
        for before, after in zip(from_2026, from_2030):
            self.assertEqual(after["birth_year"] - before["birth_year"], 4)

    def test_increasing_count_is_additive_not_reshuffled(self):
        """GROWING THE BASE IS APPEND-ONLY: the first M customers of a generation of N are
        exactly the M customers of a generation of M.

        It holds because the loop is `for index in range(count)` over a SINGLE
        random.Random(seed) consumed in fixed order, and nothing before the loop depends on
        count. It is the property that allows reloading the base with more customers without
        invalidating the existing ones — without it, every --count increase would swap the
        people behind every customer_id.

        Conditions: same seed, same reference and same ingestion_date. Changing the date
        preserves the sampled AGE and shifts birth_year (see
        test_does_not_depend_on_the_clock); changing the reference remaps the draws to other
        candidates.
        """
        large = self.gen(count=200, seed=4242)
        for smaller in (1, 7, 50, 199):
            self.assertEqual(
                self.gen(count=smaller, seed=4242),
                large[:smaller],
                f"generation of {smaller} diverged from the prefix of 200",
            )

    def test_candidate_order_defines_the_candidate_index(self):
        """The index must point to the right reference row, not to another one."""
        for customer in self.gen():
            candidate = self.reference.candidate(customer["candidate_index"])
            self.assertEqual(customer["postal_code"], candidate["postal_code"])
            self.assertEqual(customer["street_name"], candidate["street_name"])


class GeographicCoherenceTest(GeneratorTestCase):
    def test_every_customer_belongs_to_the_requested_warehouse(self):
        for customer in self.gen(wh=support.WH_B, count=30):
            self.assertEqual(customer["wh"], support.WH_B)

    def test_no_customer_crosses_the_functional_urban_area(self):
        """The customer's municipality must be in their own warehouse's FUA."""
        for wh in (support.WH_A, support.WH_B):
            allowed = {
                (m["province_code"], m["municipality_code"])
                for m in self.reference.municipalities(wh)
            }
            for customer in self.gen(wh=wh, count=60):
                key = (customer["province_code"], customer["municipality_code"])
                self.assertIn(key, allowed, f"{customer['customer_id']} outside the FUA")

    def test_address_is_always_a_real_reference_row(self):
        for customer in self.gen(count=100):
            candidate = self.reference.candidate(customer["candidate_index"])
            self.assertEqual(customer["wh"], candidate["wh"])
            self.assertEqual(customer["province_code"], candidate["province_code"])
            self.assertEqual(customer["municipality_code"], candidate["municipality_code"])
            self.assertEqual(customer["numbering_type"], candidate["numbering_type"])

    def test_postal_code_always_exists_in_the_reference_for_that_municipality(self):
        valid = collections.defaultdict(set)
        for row in support.CANDIDATES:
            valid[(row["wh"], row["province_code"], row["municipality_code"])].add(
                row["postal_code"]
            )
        for customer in self.gen(count=100):
            key = (customer["wh"], customer["province_code"], customer["municipality_code"])
            self.assertIn(customer["postal_code"], valid[key])

    def test_drawn_candidate_belongs_to_the_drawn_municipality(self):
        for customer in self.gen(count=100):
            candidate = self.reference.candidate(customer["candidate_index"])
            self.assertEqual(
                (candidate["province_code"], candidate["municipality_code"]),
                (customer["province_code"], customer["municipality_code"]),
            )


class NumberingTest(GeneratorTestCase):
    def test_numbering_type_zero_never_produces_a_number(self):
        generated = 0
        for customer in self.gen(count=300):
            if customer["numbering_type"] == "0":
                generated += 1
                self.assertIsNone(
                    customer["house_number"],
                    f"{customer['customer_id']} got a number on a street without numbering",
                )
        self.assertGreater(generated, 0, "the fixture exercised no tramo without numbering")

    def test_house_number_is_never_zero(self):
        """Range 0000..0000 with declared numbering exists in the real data (14 tramos)."""
        for customer in self.gen(count=300):
            self.assertNotEqual(customer["house_number"], 0)

    def test_degenerate_zero_to_zero_range_produces_no_number(self):
        rng = random.Random(0)
        self.assertIsNone(pick_house_number(rng, "2", 0, 0))

    def test_range_without_number_of_declared_parity_produces_no_number(self):
        rng = random.Random(0)
        self.assertIsNone(pick_house_number(rng, "2", 3, 3))
        self.assertEqual(pick_house_number(rng, "1", 3, 3), 3)

    def test_parity_and_range_always_respected(self):
        for customer in self.gen(count=300):
            number = customer["house_number"]
            if number is None:
                continue
            candidate = self.reference.candidate(customer["candidate_index"])
            expected = 1 if candidate["numbering_type"] == "1" else 0
            self.assertEqual(number % 2, expected)
            self.assertGreaterEqual(number, min(candidate["number_from"], candidate["number_to"]))
            self.assertLessEqual(number, max(candidate["number_from"], candidate["number_to"]))

    def test_house_number_key_exists_in_every_record(self):
        """An optional key would make the partition schema_fingerprint depend on the seed."""
        for customer in self.gen(count=100):
            self.assertIn("house_number", customer)

    def test_each_tramo_produces_the_number_its_real_range_allows(self):
        """Tramo by tramo, no averaging: a degenerate range and a range without the parity
        give None; the range with a single odd number gives exactly that odd number."""
        expected = {3: None, 4: None, 5: 3}  # see tests/support.py
        seen = set()
        for customer in self.gen(count=400):
            index = customer["candidate_index"]
            if index in expected:
                seen.add(index)
                self.assertEqual(
                    customer["house_number"],
                    expected[index],
                    f"tramo {index} produced {customer['house_number']!r}",
                )
        self.assertEqual(seen, set(expected), "the fixture did not exercise the three tramos")


class DistributionTest(GeneratorTestCase):
    def test_municipality_population_weight_is_respected(self):
        customers = self.gen(count=3000)
        counts = collections.Counter(c["municipality_code"] for c in customers)
        fraction = counts["001"] / len(customers)
        # Real weight in the fixture: 0.9. Tolerance wide enough not to be flaky and narrow
        # enough to fail an ignored weight (which would give 0.5).
        self.assertGreater(fraction, 0.86)
        self.assertLess(fraction, 0.94)

    def test_tramo_choice_is_uniform_not_weighted(self):
        """Population is NOT street density: within the municipality every tramo has the same
        chance. Weighting here would invent a distribution no source measured."""
        customers = [c for c in self.gen(count=3000) if c["municipality_code"] == "001"]
        counts = collections.Counter(c["candidate_index"] for c in customers)
        self.assertEqual(set(counts), {0, 1, 2}, "some tramo of the municipality was never drawn")
        for index in (0, 1, 2):
            fraction = counts[index] / len(customers)
            self.assertGreater(fraction, 0.28, f"tramo {index} under-represented: {fraction:.3f}")
            self.assertLess(fraction, 0.39, f"tramo {index} over-represented: {fraction:.3f}")

    def test_municipality_sex_proportion_is_respected(self):
        # whb has sex_hombres_proportion = 1.0 in the fixture: no woman can come out.
        customers = self.gen(wh=support.WH_B, count=200)
        self.assertEqual(
            set(c["sex_label"] for c in customers), {customers_generator.SEX_MALE}
        )

    def test_age_comes_from_the_province_own_distribution(self):
        customers = self.gen(wh=support.WH_B, count=100, date="2026-08-27")
        # Province 02 only has ages 40 and 41 in the fixture.
        self.assertEqual(
            set(c["birth_year"] for c in customers), {2026 - 40, 2026 - 41}
        )


class IdentityTest(GeneratorTestCase):
    def test_customer_id_is_sequential_unique_and_carries_the_warehouse(self):
        customers = self.gen(count=50)
        ids = [c["customer_id"] for c in customers]
        self.assertEqual(len(set(ids)), len(ids))
        self.assertEqual(ids[0], f"cust_{support.WH_A}_000000")
        self.assertEqual(ids[-1], f"cust_{support.WH_A}_000049")

    def test_customer_id_does_not_derive_from_any_geographic_field(self):
        """Identity belongs to the customer; the address is an assignment. Changing the seed
        changes the address of cust_..._000000 and keeps the id — proof that one does not
        derive from the other."""
        a = self.gen(count=20, seed=1)[0]
        b = self.gen(count=20, seed=2)[0]
        self.assertEqual(a["customer_id"], b["customer_id"])
        self.assertNotEqual(
            (a["candidate_index"], a["postal_code"]),
            (b["candidate_index"], b["postal_code"]),
        )

    def test_ids_do_not_collide_across_warehouses_on_the_same_date(self):
        a = {c["customer_id"] for c in self.gen(wh=support.WH_A, count=30)}
        b = {c["customer_id"] for c in self.gen(wh=support.WH_B, count=30)}
        self.assertEqual(a & b, set())

    def test_two_customers_may_share_the_same_address(self):
        """An address is an assignment, not identity: repetition is expected, not a collision."""
        customers = self.gen(count=300)
        counts = collections.Counter(c["candidate_index"] for c in customers)
        self.assertTrue(any(n > 1 for n in counts.values()))


class ErrorTest(GeneratorTestCase):
    def test_invalid_count_is_rejected(self):
        with self.assertRaises(GenerationError):
            self.gen(count=0)

    def test_unknown_warehouse_is_rejected(self):
        with self.assertRaises(reference_data.ReferenceError):
            self.gen(wh="nonexistent")


if __name__ == "__main__":
    unittest.main()
