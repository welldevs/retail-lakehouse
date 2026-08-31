"""Fixtures minimas: uma referencia completa em disco, sem rede e sem Lakehouse.

Pequena de proposito — catalogo de poucas dezenas de produtos e um punhado de clientes. O que
se testa aqui e a REGRA (o preco vem do catalogo, o cliente vem do armazem, o fold fecha), e
regra nao precisa de volume. O volume e exercitado pelo `validate` contra a particao real.
"""

from __future__ import annotations

import json
import os

WH = "mad1"
OTHER_WH = "bcn1"
ORDER_DATE = "2026-08-24"
PRICE_AS_OF = "2026-08-24"
FIRST_INGESTION = "2026-08-24"

PREMISES = {
    "daily_order_rate": "0.5",
    "basket_lines_min": "2",
    "basket_lines_mode": "3",
    "basket_lines_max": "5",
    "quantity_max": "3",
    "substitution_rate": "0.2",
    "removal_rate": "0.1",
    "payment_failure_rate": "0.05",
    "cancellation_rate": "0.05",
    "delivery_failure_rate": "0.05",
    "return_rate": "0.05",
    "order_hour_min": "8",
    "order_hour_max": "20",
    "slot_hours": "2",
    "slot_lead_hours_min": "2",
    "slot_lead_hours_max": "10",
    "minutes_to_payment_min": "1",
    "minutes_to_payment_max": "5",
    "minutes_to_cancel_min": "3",
    "minutes_to_cancel_max": "30",
    "minutes_to_picking_min": "10",
    "minutes_to_picking_max": "60",
    "minutes_per_line_picked": "2",
    "minutes_to_dispatch_min": "5",
    "minutes_to_dispatch_max": "20",
    "minutes_to_delivered_min": "10",
    "minutes_to_delivered_max": "60",
    "minutes_to_return_min": "60",
    "minutes_to_return_max": "600",
    "sla_minutes_picking": "90",
}


# Tres grupos de demanda na fixture, com pesos DESIGUAIS de proposito: com pesos iguais o
# sorteio ponderado seria indistinguivel do uniforme, e um teste que passa nas duas
# implementacoes nao testa nenhuma.
DEMAND_GROUPS = ("GRUPO_A", "GRUPO_B", "GRUPO_C")
DEMAND_WEIGHTS = {"GRUPO_A": "0.6", "GRUPO_B": "0.3", "GRUPO_C": "0.1"}


def catalog_rows(wh: str = WH, price_as_of: str = PRICE_AS_OF, total: int = 30) -> list[dict]:
    """Catalogo sintetico com subgrupos povoados, para que a substituicao tenha candidato."""
    rows = []
    for index in range(total):
        rows.append(
            {
                "wh": wh,
                "price_as_of": price_as_of,
                "source_product_id": f"p{index:04d}",
                "display_name": f"Produto {index}",
                "category_id": 10 + index % 3,
                "subgroup_id": 100 + index % 5,
                "demand_group": DEMAND_GROUPS[index % len(DEMAND_GROUPS)],
                "unit_price": f"{1 + index % 7}.{(index * 7) % 100:02d}",
                "tax_percentage": "21.000",
            }
        )
    return rows


def demand_payload(weights=None, seasonality=None, version: str = "fixture_v1") -> dict:
    """Perfil de demanda minimo, no mesmo formato que a plataforma escreve."""
    pesos = dict(DEMAND_WEIGHTS if weights is None else weights)
    return {
        "demand_model_version": version,
        "benchmark": "fixture",
        "seeds_sha256": {},
        "blocks": {},
        "seasonality_applies_to": "daily_order_rate",
        "seasonality": {str(m): "1.0" for m in range(1, 13)}
        if seasonality is None
        else {str(m): str(v) for m, v in seasonality.items()},
        "groups": [
            {"demand_group": key, "line_weight": pesos[key], "block": "benchmark"}
            for key in sorted(pesos)
        ],
    }


def customer_rows(wh: str = WH, total: int = 8, first: str = FIRST_INGESTION) -> list[dict]:
    return [
        {
            "customer_id": f"cust_{wh}_{index:06d}",
            "wh": wh,
            "province_code": "28",
            "municipality_code": "079",
            "postal_code": f"280{index:02d}",
            "first_ingestion_date": first,
        }
        for index in range(total)
    ]


def write_reference(
    directory: str,
    customers=None,
    catalog=None,
    calendar=None,
    premises=None,
    customer_ingestion_dates=None,
    demand=None,
) -> str:
    """Grava os cinco arquivos de referencia. Devolve o diretorio."""
    os.makedirs(directory, exist_ok=True)
    dates = customer_ingestion_dates or [FIRST_INGESTION]
    payloads = {
        "customers.json": {
            "customer_ingestion_dates": dates,
            "roster_ingestion_date": dates[-1],
            "warehouses": [WH],
            "rows": customer_rows() if customers is None else customers,
        },
        "catalog.json": {
            "catalog_ingestion_dates": [PRICE_AS_OF],
            "rows": catalog_rows() if catalog is None else catalog,
        },
        "calendar.json": {
            "window_from": ORDER_DATE,
            "window_to": ORDER_DATE,
            "warehouses": [WH],
            "carried_forward_rows": 0,
            "rows": calendar
            if calendar is not None
            else [
                {
                    "wh": WH,
                    "order_date": ORDER_DATE,
                    "price_as_of": PRICE_AS_OF,
                    "price_source": "observed",
                }
            ],
        },
        "premises.json": {
            "seed_path": "platform/dbt/seeds/order_premises_seed.csv",
            "seed_sha256": "0" * 64,
            "label": "synthetic",
            "values": dict(premises or PREMISES),
            "rows": [],
        },
        "demand_profile.json": demand_payload() if demand is None else demand,
    }
    for name, payload in payloads.items():
        with open(os.path.join(directory, name), "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
    return directory


class Args:
    """Namespace simples no lugar do resultado do argparse."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
