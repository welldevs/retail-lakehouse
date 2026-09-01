"""Referencia sintetica minima, com os casos-limite que a suite precisa provar.

A fixture nao imita o volume do dado real (216 mil tramos), imita a FORMA e todos os
casos que a regra de numeracao e a coerencia geografica tem de tratar:

  * `numbering_type='0'`  -> a via nao tem numeracao real, nunca pode sair numero;
  * faixa `0..0` com `numbering_type='2'` -> existe no dado real (14 tramos) e daria
    numero de casa 0 se o codigo so filtrasse por numbering_type;
  * faixa sem numero da paridade declarada (pares entre 3 e 3) -> tambem sem numero;
  * pseudovia -> tramo que nao pendura em rua nomeada;
  * dois warehouses com municipios disjuntos -> prova que nenhum cliente cruza a AUF;
  * um municipio com peso 0,9 e outro com 0,1 -> prova que o peso e respeitado;
  * um municipio com varios candidatos de mesmo peso -> prova que a escolha do TRAMO e
    uniforme, e nao ponderada por populacao (que nao existe nessa granularidade).
"""

from __future__ import annotations

import json
import os

WH_A = "wha"
WH_B = "whb"

# Indices sao a identidade do candidato (candidate_index): a ordem desta lista importa.
CANDIDATES = [
    # 0..2: wha / provincia 01 / municipio 001 — tres tramos, para testar uniformidade
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "001",
        "municipality_name_callejero": "ALFA", "neighborhood_name": None,
        "street_name": "RUA UM", "is_pseudo_address": False, "postal_code": "01001",
        "numbering_type": "1", "number_from": 1, "number_to": 9,
    },
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "001",
        "municipality_name_callejero": "ALFA", "neighborhood_name": "NUCLEO UM",
        "street_name": "RUA DOIS", "is_pseudo_address": False, "postal_code": "01002",
        "numbering_type": "2", "number_from": 2, "number_to": 10,
    },
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "001",
        "municipality_name_callejero": "ALFA", "neighborhood_name": None,
        "street_name": "PARQUE DA PSEUDOVIA", "is_pseudo_address": True,
        "postal_code": "01003", "numbering_type": "0", "number_from": 0, "number_to": 0,
    },
    # 3..5: wha / provincia 01 / municipio 002 — so casos sem numero possivel
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "002",
        "municipality_name_callejero": "BETA", "neighborhood_name": None,
        "street_name": "RUA DEGENERADA", "is_pseudo_address": False,
        "postal_code": "01010", "numbering_type": "2", "number_from": 0, "number_to": 0,
    },
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "002",
        "municipality_name_callejero": "BETA", "neighborhood_name": None,
        "street_name": "RUA SEM PAR", "is_pseudo_address": False,
        "postal_code": "01011", "numbering_type": "2", "number_from": 3, "number_to": 3,
    },
    {
        "wh": WH_A, "province_code": "01", "municipality_code": "002",
        "municipality_name_callejero": "BETA", "neighborhood_name": None,
        "street_name": "RUA IMPAR UNICA", "is_pseudo_address": False,
        "postal_code": "01012", "numbering_type": "1", "number_from": 3, "number_to": 3,
    },
    # 6: whb / provincia 02 / municipio 010 — outro warehouse, outra provincia
    {
        "wh": WH_B, "province_code": "02", "municipality_code": "010",
        "municipality_name_callejero": "GAMA", "neighborhood_name": None,
        "street_name": "RUA GAMA", "is_pseudo_address": False, "postal_code": "02010",
        "numbering_type": "1", "number_from": 11, "number_to": 21,
    },
]

WEIGHTS = [
    {
        "wh": WH_A, "province_code": "01", "province_name": "Provincia Alfa",
        "municipality_code": "001", "municipality_name": "Alfa",
        "population_total": 9000, "proportion_within_wh": 0.9,
        "sex_hombres_proportion": 0.5, "sex_mujeres_proportion": 0.5,
    },
    {
        "wh": WH_A, "province_code": "01", "province_name": "Provincia Alfa",
        "municipality_code": "002", "municipality_name": "Beta",
        "population_total": 1000, "proportion_within_wh": 0.1,
        "sex_hombres_proportion": 0.5, "sex_mujeres_proportion": 0.5,
    },
    {
        "wh": WH_B, "province_code": "02", "province_name": "Provincia Beta",
        "municipality_code": "010", "municipality_name": "Gama",
        "population_total": 500, "proportion_within_wh": 1.0,
        "sex_hombres_proportion": 1.0, "sex_mujeres_proportion": 0.0,
    },
]

AGES = (
    [{"province_code": "01", "age": age, "proportion": 1 / 3} for age in (30, 31, 32)]
    + [{"province_code": "02", "age": age, "proportion": 0.5} for age in (40, 41)]
)

# A distribuicao entregue e a do CADASTRO: ja truncada e renormalizada pela plataforma. Todas
# as idades da fixture estao acima do minimo de proposito — quem quiser exercitar a recusa
# usa AGES_COM_MENOR abaixo, e ai a recusa e o comportamento em teste, nao um acidente.
MIN_CUSTOMER_AGE = 18
AGES_COM_MENOR = AGES + [{"province_code": "01", "age": 7, "proportion": 0.0}]

# Alvos DIFERENTES entre si e diferentes do count padrao da fixture (50): so assim um teste
# distingue "leu o alvo da referencia" de "usou o numero da linha de comando".
CUSTOMER_TARGETS = {WH_A: 37, WH_B: 11}

ALLOCATION = {
    "rule": "per_warehouse_population",
    "population_basis": "adult_resident_population",
    "min_customer_age": MIN_CUSTOMER_AGE,
    "penetration_pct": 2.2,
    "penetration_source": "demand_profile.channel_reference_pct",
    "served_population": 10500,
    "served_adult_population": 8631.0,
    "total_customers": 48,
    "by_warehouse": [
        {"wh": WH_A, "population_total": 10000, "adult_population": 8220.0, "customers": 37},
        {"wh": WH_B, "population_total": 500, "adult_population": 411.0, "customers": 11},
    ],
}


def write_reference(
    directory: str, candidates=None, weights=None, ages=None,
    allocation=..., min_customer_age=...,
) -> str:
    """Escreve os tres arquivos de referencia. Devolve o diretorio.

    `allocation` e `min_customer_age` usam `...` como ausencia em vez de `None`: `None` e um
    valor legitimo que os testes precisam poder injetar para provar que a Source recusa uma
    referencia de schema antigo.
    """
    os.makedirs(directory, exist_ok=True)
    payloads = {
        "address_candidates.json": {
            "generated_at_utc": "2026-08-27T00:00:00Z",
            "callejero_ingestion_date": "2026-08-25",
            "excluded_rows": {"no_street_or_pseudo_match": 3},
            "rows": CANDIDATES if candidates is None else candidates,
        },
        "municipality_population_weights.json": {
            "generated_at_utc": "2026-08-27T00:00:00Z",
            "population_ingestion_date": "2026-08-26",
            "population_year": 2025,
            "population_reference_date": "2024-12-31",
            "customer_allocation": ALLOCATION if allocation is ... else allocation,
            "rows": WEIGHTS if weights is None else weights,
        },
        "province_age_distribution.json": {
            "generated_at_utc": "2026-08-27T00:00:00Z",
            "population_series_ingestion_date": "2026-08-26",
            "year": 2022,
            "reference_date": "2022-06-30",
            "fk_periodo": 27,
            "excluded_age_labels": ["Total", "85 y más años"],
            "min_customer_age": (
                MIN_CUSTOMER_AGE if min_customer_age is ... else min_customer_age
            ),
            "rows": AGES if ages is None else ages,
        },
    }
    for payload in payloads.values():
        # Chave AUSENTE e diferente de chave nula: e assim que uma referencia de schema antigo
        # se parece, e e esse caso que a Source tem de recusar.
        for chave in ("customer_allocation", "min_customer_age"):
            if chave in payload and payload[chave] is None:
                payload.pop(chave)
    for name, payload in payloads.items():
        with open(os.path.join(directory, name), "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
    return directory


class Args:
    """Namespace simples, para chamar os handlers sem passar pelo argparse."""

    def __init__(self, **values):
        self.__dict__.update(values)


def extract_args(reference: str, out: str, wh: str = WH_A, **overrides) -> Args:
    values = {
        "reference": reference,
        "out": out,
        "wh": wh,
        "date": "2026-08-27",
        "count": 50,
        "seed": 7,
        "overwrite": False,
    }
    values.update(overrides)
    return Args(**values)


def validate_args(partition: str, reference: str, strict: bool = False) -> Args:
    return Args(partition=partition, reference=reference, strict=strict)
