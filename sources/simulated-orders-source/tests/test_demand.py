"""The weighted draw of the demand group.

Each test here names the failure mode it catches AND why the obvious variant would not catch
it. The class of defect that haunts this module is always the same: a wrong CDF, a swapped
weight or an unstable order produce a PLAUSIBLE mix. Nothing blows up, no total comes out
wrong, and the error only shows up months later in a chart nobody can explain.
"""

from __future__ import annotations

import os
import random
import subprocess
import sys
import unittest

from simulated_orders_source.demand import DemandError, DemandModel

from .support import WH, demand_payload

PACKAGE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class LoadTest(unittest.TestCase):
    def test_valid_profile_loads(self):
        model = DemandModel(demand_payload())
        self.assertEqual(model.version, "fixture_v1")
        self.assertEqual(model.groups, ("GROUP_A", "GROUP_B", "GROUP_C"))
        self.assertAlmostEqual(sum(model.weights.values()), 1.0)

    def test_weights_not_summing_to_one_fail(self):
        # TRUNCATED PROFILE. This is the most likely defect of all: someone edits the seed,
        # removes a row and the rest still looks right. Without this check the draw would
        # simply ignore the group that vanished, and the mix would look plausible.
        with self.assertRaises(DemandError) as caught:
            DemandModel(demand_payload(weights={"GROUP_A": "0.6", "GROUP_B": "0.3"}))
        self.assertIn("sum to", str(caught.exception))

    def test_negative_weight_fails(self):
        with self.assertRaises(DemandError):
            DemandModel(demand_payload(weights={"GROUP_A": "1.3", "GROUP_B": "-0.3"}))

    def test_profile_without_version_fails(self):
        payload = demand_payload()
        del payload["demand_model_version"]
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("demand_model_version", str(caught.exception))

    def test_duplicate_group_fails(self):
        payload = demand_payload()
        payload["groups"].append({"demand_group": "GROUP_A", "line_weight": "0.0"})
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("duplicate", str(caught.exception))

    def test_group_without_weight_fails_on_lookup(self):
        # THERE IS NO DEFAULT WEIGHT. A `.get(group, 0)` here would be an undeclared demand
        # premise — the group would vanish from the basket without any warning.
        model = DemandModel(demand_payload())
        with self.assertRaises(DemandError) as caught:
            model.weight_of("NONEXISTENT_GROUP")
        self.assertIn("has no weight", str(caught.exception))


class DrawTest(unittest.TestCase):
    def setUp(self):
        self.model = DemandModel(demand_payload())
        self.available = self.model.groups
        self.cdf = self.model.cumulative(self.available)

    def test_cdf_ends_at_one_and_is_increasing(self):
        self.assertAlmostEqual(self.cdf[-1], 1.0)
        self.assertEqual(list(self.cdf), sorted(self.cdf))

    def test_converges_to_declared_weights(self):
        # THE CENTRAL TEST. An inverted CDF, an off-by-one index or a normalization by the
        # wrong total all produce a draw that WORKS — returns valid groups, never blows up —
        # with the wrong proportions. Comparing against the declared weights is the only way
        # to tell them apart.
        #
        # The obvious variant ("draws without error") would pass on all three wrong
        # implementations. 60,000 draws leave the sampling error around 0.2 points for the
        # 0.6 group; the 1-point tolerance is loose enough not to flicker and tight enough
        # to catch a weight swap between two groups.
        rng = random.Random(20260831)
        counts = {g: 0 for g in self.available}
        draws = 60000
        for _ in range(draws):
            counts[self.model.pick(rng, self.available, self.cdf)] += 1
        for group, expected in (("GROUP_A", 0.6), ("GROUP_B", 0.3), ("GROUP_C", 0.1)):
            observed = counts[group] / draws
            self.assertAlmostEqual(
                observed, expected, delta=0.01,
                msg=f"{group}: expected ~{expected}, observed {observed:.4f}",
            )

    def test_swapped_weights_are_detected(self):
        # Proves the test above actually FAILS, and does not pass by being loose: with A and C
        # swapped, A's share drops from 0.6 to 0.1.
        model = DemandModel(
            demand_payload(weights={"GROUP_A": "0.1", "GROUP_B": "0.3", "GROUP_C": "0.6"})
        )
        cdf = model.cumulative(model.groups)
        rng = random.Random(20260831)
        counts = {g: 0 for g in model.groups}
        for _ in range(20000):
            counts[model.pick(rng, model.groups, cdf)] += 1
        self.assertLess(counts["GROUP_A"] / 20000, 0.2)

    def test_subset_is_renormalized(self):
        # A group may have no product in a (warehouse, day). Its weight must be redistributed
        # in PROPORTION to those present, not split equally: splitting equally would erase the
        # calibration precisely in the warehouse with the smaller assortment.
        partial = ("GROUP_A", "GROUP_B")
        cdf = self.model.cumulative(partial)
        self.assertAlmostEqual(cdf[0], 0.6 / 0.9, places=6)
        self.assertAlmostEqual(cdf[-1], 1.0)

    def test_zero_weight_subset_fails(self):
        model = DemandModel(
            demand_payload(weights={"GROUP_A": "1.0", "GROUP_B": "0.0", "GROUP_C": "0.0"})
        )
        with self.assertRaises(DemandError) as caught:
            model.cumulative(("GROUP_B", "GROUP_C"))
        self.assertIn("zero weight", str(caught.exception))

    def test_empty_list_fails(self):
        with self.assertRaises(DemandError):
            self.model.cumulative(())

    def test_one_rng_draw_per_pick(self):
        # Per-seed reproducibility depends on HOW MANY times the rng is called, not only on
        # which seed it started with. If `pick` started consuming two draws, every order
        # generated after it would change — and the partition sha256 would change without
        # anything in the model having changed.
        class Counter:
            def __init__(self):
                self.calls = 0
                self._rng = random.Random(1)

            def random(self):
                self.calls += 1
                return self._rng.random()

        counter = Counter()
        for _ in range(50):
            self.model.pick(counter, self.available, self.cdf)
        self.assertEqual(counter.calls, 50)


class DeterminismTest(unittest.TestCase):
    def test_does_not_depend_on_pythonhashseed(self):
        # The CDF is built from a SORTED TUPLE, never from iterating a dict. CPython's str
        # hash is randomized per process: a CDF built in a dictionary's iteration order would
        # make the output depend on the environment variable, and the defect would show up as
        # divergence ACROSS MACHINES — the most expensive kind to diagnose.
        code = (
            "import sys, random, json; sys.path.insert(0, 'src'); sys.path.insert(0, '.');"
            "from simulated_orders_source.demand import DemandModel;"
            "from tests.support import demand_payload;"
            "m = DemandModel(demand_payload());"
            "cdf = m.cumulative(m.groups);"
            "r = random.Random(99);"
            "print(''.join(m.pick(r, m.groups, cdf)[-1] for _ in range(200)))"
        )
        outputs = {
            subprocess.run(
                [sys.executable, "-c", code],
                capture_output=True,
                text=True,
                cwd=PACKAGE_DIR,
                env={"PYTHONHASHSEED": str(seed), "PATH": "/usr/bin:/bin"},
            ).stdout.strip()
            for seed in (0, 13, 5150)
        }
        self.assertEqual(len(outputs), 1, f"draw varied across processes: {outputs}")
        self.assertTrue(outputs.pop(), "the subprocess produced no output")


class SeasonalityTest(unittest.TestCase):
    def test_neutral_profile_is_no_op(self):
        model = DemandModel(demand_payload())
        self.assertEqual(
            [model.seasonal_factor(m) for m in range(1, 13)], [1.0] * 12
        )

    def test_non_neutral_profile_changes_the_output(self):
        # THE PAIR WITH THE TEST ABOVE IS WHAT COUNTS. Alone, "neutral is a no-op" would also
        # pass on an implementation that IGNORES the seasonal profile — an inert slot,
        # delivered as if it worked. This test proves the mechanism exists; the other proves
        # the delivered profile is neutral on purpose.
        factors = {m: (1.4 if m == 12 else 1.0) for m in range(1, 13)}
        model = DemandModel(demand_payload(seasonality=factors))
        self.assertEqual(model.seasonal_factor(12), 1.4)
        self.assertEqual(model.seasonal_factor(8), 1.0)

    def test_incomplete_seasonal_profile_fails(self):
        payload = demand_payload()
        del payload["seasonality"]["7"]
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("12 months", str(caught.exception))

    def test_missing_month_on_lookup_fails(self):
        model = DemandModel(demand_payload())
        with self.assertRaises(DemandError):
            model.seasonal_factor(13)


class CohortTest(unittest.TestCase):
    """Per-cohort propensity: (age band x autonomous community) -> weights.

    The failure mode that haunts this section is the cheapest to commit and the most expensive
    to find: the code READS the cohort, ignores it, and keeps drawing from the aggregate
    vector. Everything sums to 1, no total breaks, and the dashboard shows a perfectly
    plausible mix in which old and young buy exactly the same thing.
    """

    def test_every_cohort_weights_sum_to_one(self):
        model = DemandModel(demand_payload())
        for cohort in model.cohorts:
            total = sum(model.cohort_weights[cohort].values())
            self.assertAlmostEqual(total, 1.0, places=9, msg=cohort)

    def test_cohort_with_truncated_vector_fails(self):
        # A vector that loses a group does NOT sum to 1, and drawing with it would give more
        # weight to all the others — plausible, and invisible in a per-warehouse total.
        payload = demand_payload()
        payload["cohorts"]["weights"][0]["groups"].pop()
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("sum to", str(caught.exception))

    def test_profile_without_cohort_section_fails(self):
        # Without this, a previous-version profile would load under the new version and the
        # draw would fall back to the aggregate vector. It would be a silent no-op — the worst
        # possible outcome for a layer whose success criterion is not moving the aggregate.
        payload = demand_payload()
        del payload["cohorts"]
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("cohorts", str(caught.exception))

    def test_age_band_by_age(self):
        model = DemandModel(demand_payload())
        self.assertEqual(model.band_of(18), "LT35")
        self.assertEqual(model.band_of(34), "LT35")
        self.assertEqual(model.band_of(35), "35_49")
        self.assertEqual(model.band_of(49), "35_49")
        self.assertEqual(model.band_of(50), "50_64")
        self.assertEqual(model.band_of(64), "50_64")
        self.assertEqual(model.band_of(65), "GE65")
        self.assertEqual(model.band_of(101), "GE65")

    def test_bands_without_open_top_fail(self):
        # Without exactly one band with a null ceiling, `band_of` would have nowhere to put
        # whoever is past the last bound — and returning the last band by accident of JSON
        # order would be worse.
        payload = demand_payload()
        for band in payload["cohorts"]["age_bands"]:
            if band["max_age"] is None:
                band["max_age"] = 99
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("open", str(caught.exception))

    def test_unknown_cohort_fails(self):
        model = DemandModel(demand_payload())
        with self.assertRaises(DemandError):
            model.cumulative(model.groups, "GE65|99")
        with self.assertRaises(DemandError) as caught:
            model.region_of("wh_nonexistent")
        self.assertIn("community", str(caught.exception))

    def test_flat_matrix_reproduces_the_aggregate_draw(self):
        # Proves the change is in the PROFILE, not hidden in the code: with every cohort
        # carrying the aggregate vector, the drawn sequence must be identical to what the
        # cohort-less draw would produce with the same seed.
        model = DemandModel(demand_payload())
        available = model.groups

        rng_a = random.Random(20260901)
        aggregate = model.cumulative(available)
        output_a = [model.pick(rng_a, available, aggregate) for _ in range(500)]

        rng_b = random.Random(20260901)
        per_cohort = model.cumulative(available, f"GE65|{model.region_of(WH)}")
        output_b = [model.pick(rng_b, available, per_cohort) for _ in range(500)]

        self.assertEqual(output_a, output_b)

    def test_two_cohorts_with_different_weights_draw_differently(self):
        # THE PAIR WITH THE TEST ABOVE IS WHAT COUNTS, exactly as with the seasonal profile.
        # Alone, "flat matrix reproduces the aggregate" would also pass on an implementation
        # that IGNORES the cohort. This one proves the mechanism exists.
        young = {"GROUP_A": "0.1", "GROUP_B": "0.2", "GROUP_C": "0.7"}
        old = {"GROUP_A": "0.8", "GROUP_B": "0.15", "GROUP_C": "0.05"}
        vectors = {}
        for ccaa in ("13", "09"):
            for band in ("LT35", "35_49", "50_64", "GE65"):
                vectors[f"{band}|{ccaa}"] = old if band == "GE65" else young
        model = DemandModel(demand_payload(cohort_weights=vectors))

        def mix(cohort):
            rng = random.Random(7)
            cdf = model.cumulative(model.groups, cohort)
            output = [model.pick(rng, model.groups, cdf) for _ in range(4000)]
            return output.count("GROUP_A") / len(output)

        self.assertGreater(mix("GE65|13"), 0.7)
        self.assertLess(mix("LT35|13"), 0.2)

    def test_per_cohort_cdf_does_not_depend_on_pythonhashseed(self):
        # Same trap as the aggregate draw, one level up. The target is iterating a `set`:
        # CPython's `str` hash is randomized per process, so a tuple of cohorts or bands
        # derived from a set comes out in a different order on each run. The output only
        # changes across PROCESSES, never within one — that is why the test runs three
        # subprocesses with distinct PYTHONHASHSEED instead of repeating the call here.
        script = (
            "import json,random;"
            "from simulated_orders_source.demand import DemandModel;"
            "import sys;"
            "m=DemandModel(json.load(sys.stdin));"
            "r=random.Random(11);"
            "c=m.cumulative(m.groups,'GE65|13');"
            "print('|'.join(m.cohorts));"
            "print('|'.join(b for b,_ in m.age_bands));"
            "print(','.join(m.pick(r,m.groups,c) for _ in range(60)))"
        )
        import json as _json
        payload = _json.dumps(demand_payload())
        outputs = set()
        for seed_value in ("0", "1", "424242"):
            environment = dict(os.environ, PYTHONHASHSEED=seed_value, PYTHONPATH="src")
            result = subprocess.run(
                [sys.executable, "-c", script],
                cwd=PACKAGE_DIR, env=environment, input=payload,
                capture_output=True, text=True, check=True,
            )
            outputs.add(result.stdout.strip())
        self.assertEqual(len(outputs), 1, f"diverging outputs: {outputs}")

    def test_frequency_index_is_per_warehouse(self):
        model = DemandModel(demand_payload(frequency={"13": "0.9", "09": "1.1"}))
        self.assertAlmostEqual(model.frequency_index("mad1"), 0.9)
        self.assertAlmostEqual(model.frequency_index("bcn1"), 1.1)


if __name__ == "__main__":
    unittest.main()
