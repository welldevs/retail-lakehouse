"""Forma dos registros gerados e impressao digital de schema.

Diferente das outras tres sources, aqui o payload nao vem de fora: e esta Source que o
escreve. Mesmo assim a impressao digital continua valendo a pena — ela detecta que o
formato mudou entre snapshots sem que o consumidor precise difundir arquivo inteiro.

A impressao digital e a UNIAO das chaves observadas. E por isso que
`customers_generator` sempre emite `house_number`, mesmo nulo: se a chave fosse omitida
quando nao ha numero, a impressao digital da particao passaria a depender da seed.
"""

from __future__ import annotations

import hashlib
import json

# Campos que todo cliente carrega. As tres primeiras linhas sao identidade e linhagem; a
# quarta e o endereco atribuido; a quinta e a demografia amostrada.
CUSTOMER_FIELDS = (
    "customer_id",
    "wh",
    "province_code",
    "province_name",
    "municipality_code",
    "municipality_name",
    "candidate_index",
    "street_name",
    "postal_code",
    "numbering_type",
    "house_number",
    "first_name",
    "last_name",
    "sex_label",
    "birth_year",
)


def customers_of(payload: object) -> list:
    """Clientes de um arquivo do snapshot. Lista vazia para qualquer forma inesperada."""
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def count_customers(payload: object) -> int:
    return len(customers_of(payload))


def fingerprint(payloads) -> dict:
    """Impressao digital do schema observado nos arquivos de clientes."""
    keys: set = set()
    for payload in payloads:
        for customer in customers_of(payload):
            keys.update(customer.keys())

    ordered = {"customer_keys": sorted(keys)}
    blob = json.dumps(ordered, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ordered["sha256"] = hashlib.sha256(blob).hexdigest()
    return ordered


def missing_fields(customers) -> list[str]:
    """Campos obrigatorios ausentes em algum registro, agregados (nao um erro por linha)."""
    absent: set = set()
    for customer in customers:
        absent.update(field for field in CUSTOMER_FIELDS if field not in customer)
    return sorted(absent)
