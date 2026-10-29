"""Sampling of the synthetic customers. Pure logic: reference in, list of dicts out.

GOLDEN RULE: zero geographic randomness. The customer is invented; the place where they
live is not. Municipality, street, postal code and numbering range always come from a real
Callejero row; nothing here builds an address out of loose parts.

SAMPLING CHAIN (always in this order):

    warehouse -> municipality weighted by observed municipal population
              -> UNIFORM tramo among the municipality's valid candidates
              -> house number within the real range, respecting parity
              -> sex by observed municipal proportion
              -> age by the PROVINCIAL distribution (proxy)
              -> name from an embedded curated list

The tramo choice is UNIFORM on purpose, and that is NOT population density: there is no
population per street or per tramo in any source this platform ingests. Weighting tramos
by anything here would be inventing a distribution nobody measured. Population weight
enters only once, in the municipality choice.

REPRODUCIBILITY: a single `random.Random(seed)` instance per run, always consumed in the
same order. The lists traversed come from the reference JSON, whose order is fixed by
`ORDER BY` in the export — a `set` or rebuilt `dict` is never iterated, since its order
would depend on PYTHONHASHSEED and break same-seed-same-output.
"""

from __future__ import annotations

import itertools
import random

# Sex vocabulary: the EXACT labels of INE table 29005, where the proportion comes from.
# The field is called `sex_label` for the same reason as the Silver column of the same name:
# the value is the source's label, not a category this Source invented. That way the join
# back to silver_ine_population_by_municipality is direct and needs no translation.
SEX_MALE = "Hombres"
SEX_FEMALE = "Mujeres"

# Callejero numbering (TINUM field), verbatim: "0" = no real numbering exists,
# "1" = odd, "2" = even. Carried through to the customer so that the numbering rule is
# auditable by looking at customers.json alone.
NUMBERING_NONE = "0"
NUMBERING_ODD = "1"
NUMBERING_EVEN = "2"

# Literal data, not a dependency: common Spanish given names and surnames. They only give
# the record a human shape — there is no source of real names in the Lakehouse, and
# inventing one would be faking precision that does not exist.
FIRST_NAMES_MALE = (
    "Antonio", "Manuel", "José", "Francisco", "David", "Juan", "Javier", "Daniel",
    "Carlos", "Miguel", "Alejandro", "Rafael", "Pablo", "Jesús", "Ángel", "Sergio",
    "Fernando", "Pedro", "Jorge", "Alberto", "Luis", "Álvaro", "Adrián", "Diego",
    "Raúl", "Iván", "Rubén", "Óscar", "Enrique", "Andrés", "Marcos", "Víctor",
)
FIRST_NAMES_FEMALE = (
    "María", "Carmen", "Ana", "Isabel", "Laura", "Cristina", "Marta", "Lucía",
    "Elena", "Sara", "Paula", "Raquel", "Silvia", "Beatriz", "Rosa", "Patricia",
    "Nuria", "Andrea", "Alba", "Julia", "Irene", "Claudia", "Natalia", "Sofía",
    "Eva", "Pilar", "Teresa", "Susana", "Mónica", "Alicia", "Noelia", "Sandra",
)
SURNAMES = (
    "García", "Rodríguez", "González", "Fernández", "López", "Martínez", "Sánchez",
    "Pérez", "Gómez", "Martín", "Jiménez", "Ruiz", "Hernández", "Díaz", "Moreno",
    "Muñoz", "Álvarez", "Romero", "Alonso", "Gutiérrez", "Navarro", "Torres",
    "Domínguez", "Vázquez", "Ramos", "Gil", "Ramírez", "Serrano", "Blanco", "Molina",
    "Morales", "Suárez", "Ortega", "Delgado", "Castro", "Ortiz", "Rubio", "Marín",
    "Sanz", "Núñez", "Iglesias", "Medina", "Garrido", "Cortés", "Castillo", "Santos",
    "Lozano", "Guerrero", "Cano", "Prieto", "Méndez", "Cruz", "Calvo", "Gallego",
)


class GenerationError(Exception):
    """The reference does not support the requested generation."""


def pick_house_number(rng: random.Random, numbering_type, number_from, number_to):
    """House number within the tramo's real range, or None when there is none.

    Three independent reasons for there being no number, all measured in the Callejero and
    all resolved with None instead of an invented number:

      1. `numbering_type == "0"` (9,024 of the 216,594 tramos in scope): the source declares
         that the street has no numbering scheme. Drawing here would fabricate geography —
         the same prohibition as for the postal code, one level down.
      2. Degenerate range `0000..0000` with declared numbering (14 tramos with
         `numbering_type == "2"`): drawing in [0,0] would give house number 0, which does
         not exist. The numbering_type filter alone does NOT catch this case.
      3. Range without any number of the declared parity (e.g. even between 3 and 3).

    Outside those cases, the number respects parity: odd for "1", even for "2".
    """
    if numbering_type == NUMBERING_NONE:
        return None
    if numbering_type not in (NUMBERING_ODD, NUMBERING_EVEN):
        return None
    if not isinstance(number_from, int) or not isinstance(number_to, int):
        return None

    low, high = min(number_from, number_to), max(number_from, number_to)
    low = max(low, 1)  # 0 is never a real house number
    if high < low:
        return None

    parity = 1 if numbering_type == NUMBERING_ODD else 0
    first = low if low % 2 == parity else low + 1
    if first > high:
        return None
    return first + 2 * rng.randrange((high - first) // 2 + 1)


def _cumulative(weights: list[float]) -> list[float]:
    return list(itertools.accumulate(float(w) for w in weights))


def generate(reference, wh: str, count: int, seed: int, ingestion_date: str) -> list[dict]:
    """Generate `count` customers for warehouse `wh`. The same (reference, wh, count, seed,
    ingestion_date) produces exactly the same list, byte for byte."""
    if count < 1:
        raise GenerationError(f"count must be >= 1, got {count}")

    rng = random.Random(seed)
    municipalities = reference.municipalities(wh)

    # Cumulative weights computed once. `random.choices` would do the same on every call:
    # the result is identical, the cost is not.
    municipality_cum = _cumulative([m["proportion_within_wh"] for m in municipalities])

    # Cache per key already seen. Read-only: this dict is never ITERATED, only looked up,
    # so insertion order does not influence any sampling.
    candidates_cache: dict[tuple, list[int]] = {}
    ages_cache: dict[str, tuple[list[int], list[float]]] = {}

    # birth_year derives from the PARTITION date, never from the clock: otherwise the same
    # seed would give different outputs depending on the day the extraction ran.
    reference_year = int(ingestion_date[:4])

    customers: list[dict] = []
    for index in range(count):
        municipality = rng.choices(municipalities, cum_weights=municipality_cum, k=1)[0]
        province_code = municipality["province_code"]
        municipality_code = municipality["municipality_code"]

        key = (wh, province_code, municipality_code)
        if key not in candidates_cache:
            candidates_cache[key] = reference.candidates_of(*key)
        candidate_index = rng.choice(candidates_cache[key])
        candidate = reference.candidate(candidate_index)

        house_number = pick_house_number(
            rng,
            candidate["numbering_type"],
            candidate.get("number_from"),
            candidate.get("number_to"),
        )

        male = rng.random() < float(municipality["sex_hombres_proportion"])
        sex_label = SEX_MALE if male else SEX_FEMALE

        if province_code not in ages_cache:
            ages, proportions = reference.ages_of(province_code)
            ages_cache[province_code] = (ages, _cumulative(proportions))
        ages, age_cum = ages_cache[province_code]
        age = rng.choices(ages, cum_weights=age_cum, k=1)[0]

        given = FIRST_NAMES_MALE if male else FIRST_NAMES_FEMALE
        first_name = rng.choice(given)
        # Spain uses two surnames: paternal and maternal.
        last_name = f"{rng.choice(SURNAMES)} {rng.choice(SURNAMES)}"

        customers.append(
            {
                "customer_id": f"cust_{wh}_{index:06d}",
                "wh": wh,
                "province_code": province_code,
                "province_name": municipality["province_name"],
                "municipality_code": municipality_code,
                "municipality_name": municipality["municipality_name"],
                "candidate_index": candidate_index,
                "street_name": candidate["street_name"],
                "postal_code": candidate["postal_code"],
                "numbering_type": candidate["numbering_type"],
                # Key ALWAYS present, even when null: `schema.fingerprint` is the union of
                # observed keys, so an optional field would make the partition fingerprint
                # depend on the seed.
                "house_number": house_number,
                "first_name": first_name,
                "last_name": last_name,
                "sex_label": sex_label,
                "birth_year": reference_year - age,
            }
        )
    return customers
