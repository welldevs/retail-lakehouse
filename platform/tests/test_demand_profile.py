"""The demand profile: mapping, channel tilt and weights.

No network and no Lakehouse. What is tested here is the RULE — which triple goes to which
group, how the MAPA target becomes a line weight, and what happens when the evidence falls
short. The repo's REAL seeds are exercised alongside the fixtures: a hand-edited seed that
breaks an invariant must fail here, and not three layers later.
"""

from __future__ import annotations

import csv
import os
import tempfile
import unittest
from decimal import Decimal

from retail_platform import demand_profile as dp

# The suite runs with cwd=platform/, so the path is derived from this file instead of being
# relative to the repo — otherwise "platform/dbt/seeds" would become "platform/platform/dbt/seeds".
REAL_SEEDS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "dbt", "seeds")
)


# --------------------------------------------------------------------------------
# Fixtures: minimal seeds on disk
# --------------------------------------------------------------------------------

BENCHMARK = [
    dict(mapa_key="FRUTA", mapa_label="Fruta", scope="fresh", use_as_weight="true",
         volume_share_pct="10.00", value_share_pct="8.00", avg_price_eur_kg="2.00",
         volume_yoy_pct="", value_yoy_pct="", ecommerce_volume_pct="1.1",
         channel_basis="fine", informe_section="4.9", provenance="informe_table", note=""),
    dict(mapa_key="AGUA", mapa_label="Agua", scope="rest", use_as_weight="true",
         volume_share_pct="10.00", value_share_pct="1.00", avg_price_eur_kg="0.24",
         volume_yoy_pct="", value_yoy_pct="", ecommerce_volume_pct="",
         channel_basis="coarse", informe_section="4.4.3", provenance="informe_table", note=""),
    dict(mapa_key="HUEVOS", mapa_label="Huevos", scope="fresh", use_as_weight="true",
         volume_share_pct="5.00", value_share_pct="5.00", avg_price_eur_kg="3.85",
         volume_yoy_pct="", value_yoy_pct="", ecommerce_volume_pct="1.5",
         channel_basis="fine", informe_section="4.13", provenance="informe_table", note=""),
    dict(mapa_key="AGREGADO", mapa_label="Agregado", scope="rest", use_as_weight="false",
         volume_share_pct="25.00", value_share_pct="14.00", avg_price_eur_kg="",
         volume_yoy_pct="", value_yoy_pct="", ecommerce_volume_pct="",
         channel_basis="coarse", informe_section="4.0", provenance="informe_table",
         note="aggregate, not used as a weight"),
    dict(mapa_key="SIN_BENCHMARK", mapa_label="No benchmark", scope="rest",
         use_as_weight="false", volume_share_pct="", value_share_pct="", avg_price_eur_kg="",
         volume_yoy_pct="", value_yoy_pct="", ecommerce_volume_pct="", channel_basis="coarse",
         informe_section="", provenance="none", note="declared target"),
    dict(mapa_key="NO_FOOD", mapa_label="Non-food", scope="none", use_as_weight="false",
         volume_share_pct="", value_share_pct="", avg_price_eur_kg="", volume_yoy_pct="",
         value_yoy_pct="", ecommerce_volume_pct="", channel_basis="coarse",
         informe_section="", provenance="none", note="outside the universe"),
]

MAPPING = [
    dict(l1="Fruta y verdura", l2="Fruta", l3="*", mapa_key="FRUTA", rationale="x"),
    dict(l1="Fruta y verdura", l2="Fruta", l3="Patata", mapa_key="SIN_BENCHMARK", rationale="x"),
    dict(l1="Agua", l2="*", l3="*", mapa_key="AGUA", rationale="x"),
    dict(l1="Huevos", l2="*", l3="*", mapa_key="HUEVOS", rationale="x"),
    dict(l1="Limpieza", l2="*", l3="*", mapa_key="NO_FOOD", rationale="x"),
]

PARAMS = [
    dict(param_key="demand_model_version", value="test_v1", unit="version",
         label="synthetic", rationale="x"),
    dict(param_key="cohort_dimensions", value="age,region", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="cohort_independence", value="multiplicative", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="cohort_calibration", value="ipf", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="ipf_tolerance", value="0.000000001", unit="proportion",
         label="synthetic", rationale="x"),
    dict(param_key="ipf_max_iterations", value="200", unit="iterations",
         label="synthetic", rationale="x"),
    dict(param_key="region_frequency_basis", value="per_capita_volume", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="region_frequency_normalization", value="served_regions", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="food_line_share", value="0.8", unit="proportion", label="synthetic", rationale="x"),
    dict(param_key="benchmark_volume_coverage_pct", value="25.00", unit="percent",
         label="derived", rationale="x"),
    dict(param_key="unbenchmarked_allocation", value="assortment", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="within_group_selection", value="uniform", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="volume_coverage_min", value="0.5", unit="proportion",
         label="synthetic", rationale="x"),
    dict(param_key="channel_reference_pct", value="2.2", unit="percent", label="observed", rationale="x"),
    dict(param_key="channel_fresh_pct", value="1.1", unit="percent", label="observed", rationale="x"),
    dict(param_key="channel_rest_pct", value="2.8", unit="percent", label="observed", rationale="x"),
]

SEASONALITY = [dict(month=str(m), factor="1.0", label="synthetic", rationale="x")
               for m in range(1, 13)]

# Fixture cohorts. The VOLUME shares are uneven on purpose and differ across the three
# groups: with equal indices the per-cohort profile would be indistinguishable from the
# aggregate, and a test that passes on both implementations tests neither. The four bands add
# up to 100 in both columns, which is the same checksum the real seed must close.
COHORT_AGE = []
for _key, _vols in (
    ("FRUTA", ("5.00", "20.00", "33.00", "42.00")),
    ("AGUA", ("15.00", "35.00", "30.00", "20.00")),
    ("HUEVOS", ("10.00", "25.00", "32.00", "33.00")),
):
    for _band, _pop, _vol in zip(
        ("LT35", "35_49", "50_64", "GE65"),
        ("8.89", "30.33", "31.34", "29.44"),
        _vols,
    ):
        COHORT_AGE.append(dict(
            mapa_key=_key, age_band=_band, population_share_pct=_pop,
            volume_share_pct=_vol, informe_page="1", provenance="informe_chart", note="",
        ))

COHORT_REGION = []
for _key, _vols in (
    ("FRUTA", ("18.00", "12.00")),
    ("AGUA", ("14.00", "9.00")),
    ("HUEVOS", ("16.00", "11.00")),
):
    for _ccaa, _label, _pop, _vol in zip(
        ("13", "09"), ("Madrid", "Cataluna"), ("13.86", "16.26"), _vols
    ):
        COHORT_REGION.append(dict(
            mapa_key=_key, ccaa_code=_ccaa, ccaa_label=_label,
            population_share_pct=_pop, volume_share_pct=_vol,
            informe_page="1", provenance="informe_chart", note="",
        ))

REGIONS = [
    dict(ccaa_code="00", ccaa_label="Total Espana", per_capita_kg_l="577.32",
         per_capita_eur="1874.75", informe_section="3", provenance="informe_prose", note=""),
    dict(ccaa_code="13", ccaa_label="Madrid", per_capita_kg_l="505.86",
         per_capita_eur="1754.95", informe_section="3", provenance="informe_prose", note=""),
    dict(ccaa_code="09", ccaa_label="Cataluna", per_capita_kg_l="620.82",
         per_capita_eur="2130.97", informe_section="3", provenance="informe_prose", note=""),
]

CCAA_MAP = [
    dict(province_code="28", province_name="Madrid", ccaa_code="13", ccaa_label="Madrid"),
    dict(province_code="08", province_name="Barcelona", ccaa_code="09", ccaa_label="Cataluna"),
]

# Fixture customer base: the four bands in both communities, with uneven sizes — if every
# cohort had the same mass, IPF would never have anything to correct.
COHORT_COUNTS = {
    ("LT35", "13"): 120, ("35_49", "13"): 200, ("50_64", "13"): 180, ("GE65", "13"): 150,
    ("LT35", "09"): 90, ("35_49", "09"): 160, ("50_64", "09"): 140, ("GE65", "09"): 170,
}
WAREHOUSE_REGIONS = {"mad1": "13", "bcn1": "09"}


def write_seeds(
    directory,
    benchmark=None,
    mapping=None,
    params=None,
    seasonality=None,
    cohort_age=None,
    cohort_region=None,
    regions=None,
    ccaa_map=None,
):
    for name, rows in (
        (dp.BENCHMARK_SEED, benchmark if benchmark is not None else BENCHMARK),
        (dp.MAPPING_SEED, mapping if mapping is not None else MAPPING),
        (dp.PROFILE_SEED, params if params is not None else PARAMS),
        (dp.SEASONALITY_SEED, seasonality if seasonality is not None else SEASONALITY),
        (dp.COHORT_AGE_SEED, cohort_age if cohort_age is not None else COHORT_AGE),
        (dp.COHORT_REGION_SEED, cohort_region if cohort_region is not None else COHORT_REGION),
        (dp.REGION_SEED, regions if regions is not None else REGIONS),
        (dp.CCAA_MAP_SEED, ccaa_map if ccaa_map is not None else CCAA_MAP),
    ):
        path = os.path.join(directory, name)
        with open(path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    return directory


def catalog(groups):
    """Catalog rows already stamped: (demand_group, content in kg).

    Uniform popularity and one unit per line, so the kg per line is the plain mean of the
    contents — the product-choice weighting has its own tests in test_product_choice.py.
    """
    return [{"demand_group": g, "net_content_kg_l": kg, "popularity_weight": "1.000000",
             "quantity_max": 1} for g, kg in groups]


# --------------------------------------------------------------------------------


class ResolutionTest(unittest.TestCase):
    def test_most_specific_rule_wins(self):
        # `Patata` matches the l3 rule AND the l2-wildcard rule. Without ordering by
        # specificity, the result would depend on the file order — and the mapping would stop
        # being a declarative read and become a program.
        self.assertEqual(
            dp.resolve(MAPPING, "Fruta y verdura", "Fruta", "Patata"), "SIN_BENCHMARK"
        )
        self.assertEqual(
            dp.resolve(MAPPING, "Fruta y verdura", "Fruta", "Manzana"), "FRUTA"
        )

    def test_triple_without_rule_fails(self):
        # THERE IS NO DEFAULT TARGET. An `else SIN_BENCHMARK` here would make every new
        # Mercadona category silently fall into a bucket, and the mix would change unannounced.
        with self.assertRaises(dp.DemandProfileError) as caught:
            dp.resolve(MAPPING, "Categoria Nova", "Sub", "Item")
        self.assertIn("no mapping rule", str(caught.exception))

    def test_two_equally_specific_rules_fail(self):
        ambiguous = MAPPING + [
            dict(l1="Fruta y verdura", l2="Fruta", l3="*", mapa_key="AGUA", rationale="x")
        ]
        with self.assertRaises(dp.DemandProfileError) as caught:
            dp.resolve(ambiguous, "Fruta y verdura", "Fruta", "Manzana")
        self.assertIn("equally specific", str(caught.exception))

    def test_dead_rule_is_detected(self):
        triples = [("Agua", "Agua", "Agua sin gas")]
        dead = dp.unused_rules(MAPPING, triples)
        self.assertIn("FRUTA", {m["mapa_key"] for m in dead})

    def test_all_real_rules_are_alive_and_cover_the_repo(self):
        # Against the REAL seeds. If someone adds a rule that matches nothing, or removes one
        # that did match, this test says so — offline, without touching the Lakehouse.
        mapping = dp.load_mapping(REAL_SEEDS)
        self.assertGreater(len(mapping), 100)
        keys = {r["mapa_key"] for r in mapping}
        benchmark = dp.load_benchmark(REAL_SEEDS)
        missing = keys - set(benchmark)
        self.assertEqual(missing, set(), f"mapping points to nonexistent keys: {missing}")


class BenchmarkTest(unittest.TestCase):
    def test_disjoint_weights_do_not_exceed_one_hundred(self):
        # THE CROSS-CUTTING SLICES ARE THE TRAP. `SIN GLUTEN` (3.75%) and `ECOLOGICOS`
        # (2.31%) are not categories: they are slices of the others. Adding them to the rest
        # would exceed 100% and normalization would hide the error by spreading the
        # difference over everyone.
        benchmark = dp.load_benchmark(REAL_SEEDS)
        share_sum = sum(v["volume_share_pct"] for v in benchmark.values() if v["use_as_weight"])
        self.assertLess(share_sum, Decimal("100"), f"weightable keys add up to {share_sum}, which exceeds 100%")
        self.assertGreater(share_sum, Decimal("50"), f"weightable keys add up to only {share_sum}: coverage too low")

    def test_declared_coverage_matches_the_sum(self):
        # `benchmark_volume_coverage_pct` splits the food scope between calibrated and
        # uncalibrated. If it diverges from the real sum, the split becomes a chosen number
        # instead of a derived one — and nobody would notice.
        benchmark = dp.load_benchmark(REAL_SEEDS)
        params = dp.load_params(REAL_SEEDS)
        share_sum = sum(v["volume_share_pct"] for v in benchmark.values() if v["use_as_weight"])
        declared = Decimal(params["benchmark_volume_coverage_pct"])
        self.assertAlmostEqual(
            float(share_sum), float(declared), places=1,
            msg=f"declared {declared} against real sum {share_sum}",
        )

    def test_weightable_without_volume_share_fails(self):
        broken = [dict(r) for r in BENCHMARK]
        broken[0]["volume_share_pct"] = ""
        with tempfile.TemporaryDirectory() as tmp:
            write_seeds(tmp, benchmark=broken)
            with self.assertRaises(dp.DemandProfileError) as caught:
                dp.load_benchmark(tmp)
        self.assertIn("without volume_share_pct", str(caught.exception))

    def test_missing_scope_fails(self):
        no_scope = [r for r in BENCHMARK if r["mapa_key"] != "NO_FOOD"]
        with tempfile.TemporaryDirectory() as tmp:
            write_seeds(tmp, benchmark=no_scope)
            with self.assertRaises(dp.DemandProfileError) as caught:
                dp.load_benchmark(tmp)
        self.assertIn("NO_FOOD", str(caught.exception))

    def test_every_cited_section_looks_like_a_section(self):
        # The traceability of this table IS the section citation. `informe_section` must
        # survive as text: typed as a number, "4.3" becomes 4.3 and "3" becomes 3.0.
        benchmark = dp.load_benchmark(REAL_SEEDS)
        for key, row in benchmark.items():
            if row["provenance"] in ("informe_table", "informe_prose"):
                self.assertTrue(
                    row["informe_section"],
                    f"{key} came from the report and cites no section",
                )


class ChannelTest(unittest.TestCase):
    def test_fine_basis_uses_the_group_own_share(self):
        params = dp.load_params(REAL_SEEDS)
        row = {"channel_basis": "fine", "ecommerce_volume_pct": Decimal("4.4"), "scope": "rest"}
        tilt, base = dp.channel_tilt(row, params)
        self.assertEqual(base, "fine")
        self.assertAlmostEqual(float(tilt), 2.0, places=6)

    def test_coarse_basis_separates_fresh_from_rest(self):
        # 1.1% of fresh volume arrives via e-commerce against 2.8% of the rest. Using only
        # the 2.2 average for everyone would erase exactly the difference that makes fresh
        # fruit weigh less in an online basket — which is the report's channel finding.
        params = dp.load_params(REAL_SEEDS)
        fresh, base_f = dp.channel_tilt({"channel_basis": "coarse", "scope": "fresh"}, params)
        rest, base_r = dp.channel_tilt({"channel_basis": "coarse", "scope": "rest"}, params)
        self.assertEqual((base_f, base_r), ("coarse", "coarse"))
        self.assertLess(fresh, 1)
        self.assertGreater(rest, 1)

    def test_fine_basis_without_number_falls_back_to_coarse(self):
        # Declaring `fine` without the number must not become a division by None. It falls
        # back to coarse AND DECLARES it did, so the report does not claim precision it lacks.
        params = dp.load_params(REAL_SEEDS)
        _tilt, base = dp.channel_tilt(
            {"channel_basis": "fine", "ecommerce_volume_pct": None, "scope": "rest"}, params
        )
        self.assertEqual(base, "coarse")


class BuildTest(unittest.TestCase):
    def _profile(self, rows, counts=None, regions=None, **kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            write_seeds(tmp, **kwargs)
            return dp.build(
                rows,
                tmp,
                customers_by_cohort=COHORT_COUNTS if counts is None else counts,
                warehouse_regions=WAREHOUSE_REGIONS if regions is None else regions,
            )

    def test_weights_sum_to_one_and_the_three_blocks_match(self):
        profile = self._profile(catalog([
            ("FRUTA", "1.0"), ("FRUTA", "1.0"),
            ("AGUA", "1.5"), ("AGUA", "1.5"),
            ("SIN_BENCHMARK", "0.5"),
            ("NO_FOOD", None), ("NO_FOOD", None),
        ]))
        weights = {g["demand_group"]: Decimal(g["line_weight"]) for g in profile["groups"]}
        self.assertAlmostEqual(float(sum(weights.values())), 1.0, places=9)
        self.assertAlmostEqual(float(weights["NO_FOOD"]), 0.2, places=9)
        # calibrated block = food_line_share (0.8) x coverage (0.25)
        calibrated = weights["FRUTA"] + weights["AGUA"]
        self.assertAlmostEqual(float(calibrated), 0.8 * 0.25, places=9)

    def test_group_without_kg_coverage_falls_back_to_line_share(self):
        # THE GUARD THAT HELD ON REAL DATA: eggs are sold in 'ud'/'dz' and only 18% of the
        # assortment converts to kg. Without this guard the volume target would be divided by
        # the mean weight of two unrepresentative products, and the whole group would be
        # sized by them.
        profile = self._profile(catalog([
            ("FRUTA", "1.0"),
            ("AGUA", "1.0"),
            ("HUEVOS", None), ("HUEVOS", None), ("HUEVOS", None), ("HUEVOS", "0.65"),
            ("SIN_BENCHMARK", "0.5"), ("NO_FOOD", None),
        ]))
        by_group = {g["demand_group"]: g for g in profile["groups"]}
        self.assertEqual(by_group["HUEVOS"]["target_path"], "lines")
        self.assertIsNone(by_group["HUEVOS"]["mean_kg_per_line"])
        self.assertEqual(by_group["FRUTA"]["target_path"], "volume")

    def test_larger_package_gets_fewer_lines_for_the_same_target(self):
        # THE BRIDGE. FRUTA and AGUA have the SAME volume target (10% each) but their channel
        # scope would differ — so the test pins both to `rest` via its own benchmark,
        # isolating the only effect that matters here: package size.
        bench = [dict(r) for r in BENCHMARK]
        for r in bench:
            r["scope"] = "rest"
            r["channel_basis"] = "coarse"
            r["ecommerce_volume_pct"] = ""
        profile = self._profile(
            catalog([("FRUTA", "0.5"), ("AGUA", "5.0"), ("SIN_BENCHMARK", "1"), ("NO_FOOD", None)]),
            benchmark=bench,
        )
        weights = {g["demand_group"]: Decimal(g["line_weight"]) for g in profile["groups"]}
        # AGUA weighs 10x more per unit, so it needs 10x fewer lines.
        self.assertAlmostEqual(float(weights["FRUTA"] / weights["AGUA"]), 10.0, places=6)

    def test_row_without_stamped_group_fails(self):
        with self.assertRaises(dp.DemandProfileError) as caught:
            self._profile([{"net_content_kg_l": "1.0"}])
        self.assertIn("demand_group", str(caught.exception))

    def test_empty_catalog_fails(self):
        with self.assertRaises(dp.DemandProfileError):
            self._profile([])

    def test_no_weightable_group_fails(self):
        with self.assertRaises(dp.DemandProfileError) as caught:
            self._profile(catalog([("SIN_BENCHMARK", "1"), ("NO_FOOD", None)]))
        self.assertIn("no catalog group", str(caught.exception))

    def test_food_line_share_out_of_range_fails(self):
        params = [dict(r) for r in PARAMS]
        for r in params:
            if r["param_key"] == "food_line_share":
                r["value"] = "1.4"
        with self.assertRaises(dp.DemandProfileError) as caught:
            self._profile(catalog([("FRUTA", "1"), ("NO_FOOD", None)]), params=params)
        self.assertIn("food_line_share", str(caught.exception))

    def test_nan_in_content_does_not_poison_the_mean(self):
        # `Decimal('nan')` does NOT raise: it builds a NaN that survives arithmetic and
        # silently turns the whole mean into NaN. A driver that returns NaN for null — pandas
        # does — would come in through here.
        profile = self._profile(catalog([
            ("FRUTA", "1.0"), ("FRUTA", float("nan")),
            ("AGUA", "1.0"), ("SIN_BENCHMARK", "1"), ("NO_FOOD", None),
        ]))
        fruit = next(g for g in profile["groups"] if g["demand_group"] == "FRUTA")
        self.assertEqual(fruit["mean_kg_per_line"], "1.000000")

    def test_profile_carries_version_hashes_and_seasonality(self):
        profile = self._profile(catalog([("FRUTA", "1"), ("AGUA", "1"), ("NO_FOOD", None)]))
        self.assertEqual(profile["demand_model_version"], "test_v1")
        self.assertEqual(set(profile["seeds_sha256"]), set(dp.SEEDS))
        self.assertEqual(profile["seasonality_applies_to"], "daily_order_rate")
        self.assertEqual(len(profile["seasonality"]), 12)


class SeasonalityTest(unittest.TestCase):
    def test_real_profile_is_neutral(self):
        # The DELIVERED profile is neutral for lack of numeric evidence. This test guards
        # that decision: if someone invents a factor without bringing the number, it fails
        # and forces the conversation.
        factors = dp.load_seasonality(REAL_SEEDS)
        self.assertEqual(set(factors.values()), {Decimal("1.0")})

    def test_missing_month_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seeds(tmp, seasonality=SEASONALITY[:11])
            with self.assertRaises(dp.DemandProfileError) as caught:
                dp.load_seasonality(tmp)
        self.assertIn("12 months", str(caught.exception))

    def test_non_positive_factor_fails(self):
        broken = [dict(r) for r in SEASONALITY]
        broken[0]["factor"] = "0"
        with tempfile.TemporaryDirectory() as tmp:
            write_seeds(tmp, seasonality=broken)
            with self.assertRaises(dp.DemandProfileError):
                dp.load_seasonality(tmp)


class StabilityTest(unittest.TestCase):
    def test_output_is_byte_stable_across_runs(self):
        # The profile enters the manifest by sha256. If `build` varies between runs — by
        # iterating a set, for example — the hash changes with nothing having changed, and
        # the partition's reproducibility silently dies.
        import json
        rows = catalog([("FRUTA", "1.0"), ("AGUA", "2.0"),
                           ("SIN_BENCHMARK", "0.5"), ("NO_FOOD", None)])
        with tempfile.TemporaryDirectory() as tmp:
            write_seeds(tmp)
            dump = lambda: json.dumps(  # noqa: E731 - two identical calls, one line
                dp.build(
                    rows,
                    tmp,
                    customers_by_cohort=COHORT_COUNTS,
                    warehouse_regions=WAREHOUSE_REGIONS,
                ),
                sort_keys=True,
            )
            first, second = dump(), dump()
        self.assertEqual(first, second)

    def test_real_seeds_produce_a_stable_hash(self):
        self.assertEqual(dp.seeds_sha256(REAL_SEEDS), dp.seeds_sha256(REAL_SEEDS))
        self.assertEqual(len(dp.seeds_sha256(REAL_SEEDS)), 8)

class CohortTest(unittest.TestCase):
    """The cohort layer, and the two checksums the reading of the report must close.

    The realistic error in this part is not logic, it is READING: the report's demographic
    cuts are in charts, and a swapped digit in a label produces a plausible index that nothing
    fails. The two sums exist so that it stops here.
    """

    def test_volume_shares_of_the_four_bands_sum_to_one_hundred(self):
        # On the repo's REAL seeds, not only on fixtures: all 39 readings must close.
        data = dp.load_cohort_age(REAL_SEEDS)
        self.assertEqual(len(data), 39)
        for key, rows in sorted(data.items()):
            share_sum = sum(v for _pop, v in rows.values())
            self.assertLess(
                abs(share_sum - Decimal("100")), dp.COHORT_SUM_TOLERANCE,
                f"{key}: volume sums {share_sum}",
            )

    def test_a_swapped_digit_in_the_seed_fails(self):
        # The proof that the checksum is useful: without it, 42.27 -> 24.27 would pass and the
        # 65+ band would lose 43% of its propensity for fresh fruit with nothing warning.
        wrong = [dict(entry) for entry in COHORT_AGE]
        wrong[3]["volume_share_pct"] = "24.00"
        with tempfile.TemporaryDirectory() as tmp:
            write_seeds(tmp, cohort_age=wrong)
            with self.assertRaises(dp.DemandProfileError) as caught:
                dp.load_cohort_age(tmp)
        self.assertIn("sums", str(caught.exception))

    def test_population_share_is_the_same_in_every_group(self):
        # `% Poblacion` is the UNIVERSE, not a measurement of the category: it must repeat in
        # every section of the report. A divergence here is a misreading, not new data.
        #
        # Tolerance, not equality: the report itself sometimes publishes one decimal place
        # (8.9) and sometimes two (8.89), and one section — processed meat, page 206 — has
        # 30.5/31.7/29.0 where all the others have 30.3/31.3/29.4. The discrepancy is the
        # source's and is recorded in the `note` column of that row.
        for loader, expected in (
            (dp.load_cohort_age, {"LT35": "8.89", "35_49": "30.33",
                                  "50_64": "31.34", "GE65": "29.44"}),
            (dp.load_cohort_region, {"09": "16.26", "10": "11.14",
                                     "01": "17.52", "13": "13.86"}),
        ):
            data = loader(REAL_SEEDS)
            for key, rows in sorted(data.items()):
                for cohort, (population, _vol) in sorted(rows.items()):
                    self.assertLess(
                        abs(population - Decimal(expected[cohort])), Decimal("0.5"),
                        f"{key}/{cohort}: population {population}, universe {expected[cohort]}",
                    )

    def test_affinity_index_is_dimensionless(self):
        # 1.0 means "buys in proportion to its own size". There is no ceiling or floor: the
        # report measures AGUA at 4.3% of volume for 13.86% of Madrid's population, and
        # trimming that 0.31 would discard the strongest measurement the source publishes.
        self.assertEqual(
            dp.affinity_index(Decimal("20"), Decimal("20")), Decimal("1")
        )
        region = dp.load_cohort_region(REAL_SEEDS)["AGUA"]["13"]
        self.assertLess(dp.affinity_index(*region), Decimal("0.4"))

    def test_frequency_indices_sum_to_the_number_of_regions(self):
        # Renormalization is what keeps the order TOTAL where it was. Without it, the four
        # served warehouses — which consume less than the Spanish average — would produce a
        # few percent fewer orders without that meaning anything.
        reference = dp.load_region_reference(REAL_SEEDS)
        served = ["01", "09", "10", "13"]
        indices = dp.region_frequency(reference, served)
        self.assertEqual(len(indices), 4)
        self.assertAlmostEqual(float(sum(indices.values())), 4.0, places=9)
        self.assertGreater(indices["09"], indices["13"])

    def test_base_weighted_frequency_also_sums_to_one(self):
        # With uneven bases, the normalizing mean must be WEIGHTED — otherwise the weighted
        # sum of the indices stops being 1 and the order total drifts.
        reference = dp.load_region_reference(REAL_SEEDS)
        weights = {"01": 100, "09": 300, "10": 50, "13": 550}
        indices = dp.region_frequency(reference, list(weights), weights=weights)
        total = sum(Decimal(weights[c]) for c in weights)
        weighted = sum(Decimal(weights[c]) * indices[c] for c in sorted(weights)) / total
        self.assertAlmostEqual(float(weighted), 1.0, places=9)

    def test_province_without_community_fails(self):
        # No default target, for the same reason as the category mapping: a new province
        # would silently inherit another region's consumption profile.
        province_map = dp.load_province_ccaa(REAL_SEEDS)
        self.assertEqual(dp.ccaa_of(province_map, "08"), "09")
        with self.assertRaises(dp.DemandProfileError) as caught:
            dp.ccaa_of(province_map, "33")
        self.assertIn("there is no default community", str(caught.exception))

    def test_ipf_with_flat_indices_is_the_identity(self):
        # When there is nothing to correct, IPF must correct nothing. Without this, an IPF that
        # "adjusts" already-correct weights would shift the aggregate calibration on every run.
        base = {"A": Decimal("0.5"), "B": Decimal("0.3"), "C": Decimal("0.2")}
        mass = {"x": Decimal("0.6"), "y": Decimal("0.4")}
        flat = {c: {g: Decimal("1") for g in base} for c in mass}
        factor, iterations, deviation = dp.ipf_calibrate(
            base, flat, mass, Decimal("1e-9"), 200
        )
        self.assertEqual(iterations, 1)
        self.assertEqual(set(factor.values()), {Decimal("1")})
        self.assertEqual(deviation, Decimal("0"))

    def test_ipf_converges_and_reproduces_the_aggregate_weights(self):
        base = {"A": Decimal("0.5"), "B": Decimal("0.3"), "C": Decimal("0.2")}
        mass = {"x": Decimal("0.6"), "y": Decimal("0.4")}
        index = {
            "x": {"A": Decimal("1.5"), "B": Decimal("0.5"), "C": Decimal("1.0")},
            "y": {"A": Decimal("0.4"), "B": Decimal("2.0"), "C": Decimal("1.0")},
        }
        factor, _it, deviation = dp.ipf_calibrate(base, index, mass, Decimal("1e-9"), 200)
        self.assertLessEqual(deviation, Decimal("1e-9"))

        aggregate = {g: Decimal("0") for g in base}
        for cohort, weight in mass.items():
            raw = {g: base[g] * factor[g] * index[cohort][g] for g in base}
            total = sum(raw.values())
            for g in base:
                aggregate[g] += weight * raw[g] / total
        for g in base:
            self.assertAlmostEqual(float(aggregate[g]), float(base[g]), places=8)

    def test_ipf_that_does_not_converge_fails(self):
        # Tolerance below Decimal's own precision (28 digits): the target is never reached,
        # and the module must FAIL instead of returning a matrix that does not close.
        # Silently accepting it would undo the previous version's aggregate calibration.
        base = {"A": Decimal("0.5"), "B": Decimal("0.3"), "C": Decimal("0.2")}
        mass = {"x": Decimal("0.6"), "y": Decimal("0.4")}
        index = {
            "x": {"A": Decimal("40"), "B": Decimal("0.05"), "C": Decimal("1")},
            "y": {"A": Decimal("0.02"), "B": Decimal("60"), "C": Decimal("1")},
        }
        with self.assertRaises(dp.DemandProfileError) as caught:
            dp.ipf_calibrate(base, index, mass, Decimal("1e-40"), 5)
        self.assertIn("did not converge", str(caught.exception))


class ProfileByCohortTest(unittest.TestCase):
    """The full profile with the cohort layer, over the fixtures."""

    def _profile(self, **kwargs):
        rows = catalog([
            ("FRUTA", "1.0"), ("FRUTA", "1.0"),
            ("AGUA", "1.5"), ("AGUA", "1.5"),
            ("HUEVOS", None), ("HUEVOS", None),
            ("SIN_BENCHMARK", "0.5"),
            ("NO_FOOD", None), ("NO_FOOD", None),
        ])
        with tempfile.TemporaryDirectory() as tmp:
            write_seeds(tmp, **kwargs)
            return dp.build(
                rows, tmp,
                customers_by_cohort=COHORT_COUNTS,
                warehouse_regions=WAREHOUSE_REGIONS,
            )

    def test_the_aggregate_does_not_move(self):
        # THE ACCEPTANCE CRITERION OF THE WHOLE PHASE. The mean of the per-cohort weights,
        # weighted by the real cohort mass, must reproduce the aggregate weights — otherwise
        # this layer would have undone the previous one's calibration sideways, with nothing
        # failing.
        profile = self._profile()
        base = {g["demand_group"]: Decimal(g["line_weight"]) for g in profile["groups"]}
        cohorts = profile["cohorts"]
        mass = {m["cohort"]: Decimal(m["share"]) for m in cohorts["mass"]}
        weights = {
            v["cohort"]: {g["demand_group"]: Decimal(g["line_weight"]) for g in v["groups"]}
            for v in cohorts["weights"]
        }
        self.assertAlmostEqual(float(sum(mass.values())), 1.0, places=9)
        for group, expected in base.items():
            observed = sum(mass[c] * weights[c][group] for c in mass)
            self.assertAlmostEqual(float(observed), float(expected), places=8, msg=group)

    def test_each_block_slice_is_constant_across_cohorts(self):
        # NO_FOOD and SIN_BENCHMARK have a neutral index for LACK OF EVIDENCE. Without
        # confining IPF to each block, they absorbed the normalization residue and the model
        # started claiming that older people buy less drugstore — a number nobody measured,
        # and larger than most effects that are measured.
        profile = self._profile()
        blocks = {g["demand_group"]: g["block"] for g in profile["groups"]}
        weights = {
            v["cohort"]: {g["demand_group"]: Decimal(g["line_weight"]) for g in v["groups"]}
            for v in profile["cohorts"]["weights"]
        }
        for block in sorted(set(blocks.values())):
            slices = [
                sum(p[g] for g, b in blocks.items() if b == block) for p in weights.values()
            ]
            # Tolerance at Decimal's last digit, not exact equality: the division that
            # normalizes each block has 28 significant digits, and rounding residue is not the
            # defect this test looks for — the defect is the slice MOVING, at the third
            # decimal place or earlier.
            self.assertLess(
                max(slices) - min(slices), Decimal("1e-20"),
                f"{block} varies across cohorts: min {min(slices)} max {max(slices)}",
            )

    def test_different_cohorts_produce_different_weights(self):
        # The counterpart of the test above: without it, "the aggregate does not move" and
        # "the block is constant" would also pass on an implementation that returns the same
        # vector for every cohort.
        profile = self._profile()
        weights = {
            v["cohort"]: {g["demand_group"]: Decimal(g["line_weight"]) for g in v["groups"]}
            for v in profile["cohorts"]["weights"]
        }
        self.assertGreater(weights["GE65|13"]["FRUTA"], weights["LT35|13"]["FRUTA"] * 2)
        self.assertLess(weights["GE65|13"]["AGUA"], weights["LT35|13"]["AGUA"])

    def test_weightable_group_without_demographic_cut_fails(self):
        # Leaving a group silently neutral would hide an EXTRACTION gap behind a plausible
        # mix — and extraction is the part of this phase most prone to human error.
        partial = [entry for entry in COHORT_AGE if entry["mapa_key"] != "AGUA"]
        with self.assertRaises(dp.DemandProfileError) as caught:
            self._profile(cohort_age=partial)
        self.assertIn("no demographic cut", str(caught.exception))

    def test_warehouse_in_community_without_published_consumption_fails(self):
        with self.assertRaises(dp.DemandProfileError) as caught:
            self._profile(regions=[r for r in REGIONS if r["ccaa_code"] != "09"])
        self.assertIn("per-capita consumption", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
