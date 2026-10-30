"""How likely each product is to be chosen within its demand group, and how many units a line
carries — computed ONCE, here, and stamped on every catalog row of the orders reference.

WHY THIS EXISTS (CR-017)
------------------------
Until CR-017 the product within a demand group was drawn UNIFORMLY. The group mix was
calibrated against MAPA, but inside a group every SKU was equally likely, and the dashboard
showed what that means on a real catalog: perfume was the top non-food category by revenue,
make-up beat detergent, toilet paper came last, and whole Iberian hams at 368-532 EUR were
bought 1.8 at a time. Quantity was 1-4 with the same odds for water and for ham.

THE WEIGHT
----------
    weight = own_brand_weight (if a Mercadona brand) x (group median price / price) ^ elasticity

Relative to the GROUP median, so a group's share of lines — the part calibrated against MAPA
— is untouched; only the choice inside it moves.

THE ELASTICITY IS FITTED, NOT DECLARED. MAPA publishes the average price households actually
pay per kg or litre, per group. With uniform choice the simulated EUR/kg sits well above it,
because the catalog's expensive tail is drawn as often as the staples. The elasticity is the
single number, on a fixed grid, that brings the simulated EUR/kg of the benchmark groups
closest to MAPA's — mean absolute log error, EACH GROUP COUNTING ONCE. Weighting by volume
share was measured and rejected (2026-10-30): the objective went flat above 1.5 and was
pulled to the grid edge by a single group (fresh vegetables), overshooting wine, cheese and
shellfish to half of MAPA's price; unweighted, the minimum is clear and the median agrees.
The fit and its error travel in the reference header, so the number is auditable, not chosen.

QUANTITY
--------
Ceiling per product from its price (declared tiers in order_premises_seed); within the
ceiling the odds stay w(k) = 1/2^(k-1), as before.

ONE IMPLEMENTATION. The Source reads `popularity_weight` and `quantity_max` from the reference
and never recomputes them; `demand_profile` uses the same numbers for the kg-per-line that
converts MAPA volume into lines. Two implementations would drift, and both would look plausible.
"""

from __future__ import annotations

import math
from decimal import Decimal

OWN_BRANDS = ("Hacendado", "Deliplus", "Bosque Verde", "Compy")
ELASTICITY_GRID = tuple(round(step * 0.05, 2) for step in range(0, 61))  # 0.00 .. 3.00


class ProductChoiceError(Exception):
    """The catalog or the premises cannot support the product-choice weights."""


def is_own_brand(display_name: str) -> bool:
    words = display_name.replace(",", " ").split()
    text = " ".join(words)
    return any(f" {brand} " in f" {text} " for brand in OWN_BRANDS)


def quantity_max_of(price: float, premises: dict) -> int:
    """Ceiling of units per line, from the declared price tiers."""
    if price >= float(premises["quantity_high_price_eur"]):
        return int(float(premises["quantity_max_high"]))
    if price >= float(premises["quantity_mid_price_eur"]):
        return int(float(premises["quantity_max_mid"]))
    if price < float(premises["quantity_multi_price_eur"]):
        return int(float(premises["quantity_max_multi"]))
    return int(float(premises["quantity_max"]))


def expected_quantity(quantity_max: int) -> float:
    """Mean of w(k) = 1/2^(k-1) over 1..quantity_max — the generator's quantity draw."""
    weights = [1.0 / (2 ** (k - 1)) for k in range(1, quantity_max + 1)]
    return sum(k * w for k, w in zip(range(1, quantity_max + 1), weights)) / sum(weights)


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _content(value) -> float | None:
    if value in (None, ""):
        return None
    number = float(value)
    return number if number > 0 else None


def _raw_weights(rows: list[dict], elasticity: float, brand_weight: float) -> list[float]:
    medians: dict = {}
    for row in rows:
        medians.setdefault((row["wh"], row["price_as_of"], row["demand_group"]), []).append(
            float(row["unit_price"])
        )
    medians = {key: _median(values) for key, values in medians.items()}
    weights = []
    for row in rows:
        median = medians[(row["wh"], row["price_as_of"], row["demand_group"])]
        weight = (median / float(row["unit_price"])) ** elasticity
        if row["own_brand"]:
            weight *= brand_weight
        weights.append(weight)
    return weights


def _eur_per_kg(rows: list[dict], weights: list[float]) -> dict:
    """Simulated EUR per kg-or-litre per group: what the chosen basket pays per unit volume."""
    money: dict = {}
    volume: dict = {}
    for row, weight in zip(rows, weights):
        content = _content(row.get("net_content_kg_l"))
        if content is None:
            continue
        units = weight * expected_quantity(row["quantity_max"])
        group = row["demand_group"]
        money[group] = money.get(group, 0.0) + units * float(row["unit_price"])
        volume[group] = volume.get(group, 0.0) + units * content
    return {group: money[group] / volume[group] for group in money if volume[group] > 0}


def fit_elasticity(rows: list[dict], benchmark: dict, brand_weight: float) -> dict:
    """The grid value that brings simulated EUR/kg closest to MAPA's, each group counting once."""
    targets = {
        key: entry for key, entry in benchmark.items()
        if entry.get("use_as_weight") and entry.get("avg_price_eur_kg") is not None
    }
    best = None
    for elasticity in ELASTICITY_GRID:
        simulated = _eur_per_kg(rows, _raw_weights(rows, elasticity, brand_weight))
        errors = [
            abs(math.log(simulated[key] / float(entry["avg_price_eur_kg"])))
            for key, entry in targets.items() if key in simulated
        ]
        if not errors:
            raise ProductChoiceError("no benchmark group with kg content in the catalog")
        score = sum(errors) / len(errors)
        if best is None or score < best["mean_abs_log_error"]:
            best = {"elasticity": elasticity, "mean_abs_log_error": score,
                    "groups": sorted(k for k in targets if k in simulated)}
    uniform = _eur_per_kg(rows, _raw_weights(rows, 0.0, 1.0))
    fitted = _eur_per_kg(rows, _raw_weights(rows, best["elasticity"], brand_weight))
    best["by_group"] = [
        {
            "demand_group": key,
            "mapa_eur_kg": str(targets[key]["avg_price_eur_kg"]),
            "uniform_eur_kg": str(round(Decimal(uniform[key]), 3)),
            "fitted_eur_kg": str(round(Decimal(fitted[key]), 3)),
        }
        for key in best["groups"]
    ]
    return best


def stamp(rows: list[dict], premises: dict, benchmark: dict) -> dict:
    """Adds `own_brand`, `quantity_max` and `popularity_weight` to every row. Returns the header."""
    brand_weight = float(premises["own_brand_weight"])
    for row in rows:
        row["own_brand"] = is_own_brand(row["display_name"])
        row["quantity_max"] = quantity_max_of(float(row["unit_price"]), premises)
    fit = fit_elasticity(rows, benchmark, brand_weight)
    for row, weight in zip(rows, _raw_weights(rows, fit["elasticity"], brand_weight)):
        # A string with fixed decimals, like money: the Source must read the same number on
        # every machine, and repr(float) is not a contract.
        row["popularity_weight"] = f"{weight:.6f}"
    own = sum(1 for row in rows if row["own_brand"])
    return {
        "formula": "own_brand_weight x (group median price / price) ^ elasticity",
        "elasticity": fit["elasticity"],
        "elasticity_grid": [ELASTICITY_GRID[0], ELASTICITY_GRID[-1], 0.05],
        "fit_target": "MAPA 2025 average price paid per kg or litre, benchmark groups, "
                      "mean absolute log error, each group counting once",
        "fit_mean_abs_log_error": round(fit["mean_abs_log_error"], 6),
        "fit_by_group": fit["by_group"],
        "own_brand_weight": brand_weight,
        "own_brands": list(OWN_BRANDS),
        "own_brand_row_share": round(own / len(rows), 6),
    }
