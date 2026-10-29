"""Shape of the generated records and schema fingerprint.

Unlike the other three sources, here the payload does not come from outside: this Source
writes it. Even so the fingerprint is still worth it — it detects that the format changed
between snapshots without the consumer having to diff a whole file.

The fingerprint is the UNION of the observed keys. That is why `customers_generator`
always emits `house_number`, even when null: if the key were omitted when there is no
number, the partition fingerprint would start to depend on the seed.
"""

from __future__ import annotations

import hashlib
import json

# Fields every customer carries. The first three lines are identity and lineage; the
# fourth is the assigned address; the fifth is the sampled demographics.
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
    """Customers in a snapshot file. Empty list for any unexpected shape."""
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def count_customers(payload: object) -> int:
    return len(customers_of(payload))


def fingerprint(payloads) -> dict:
    """Fingerprint of the schema observed in the customer files."""
    keys: set = set()
    for payload in payloads:
        for customer in customers_of(payload):
            keys.update(customer.keys())

    ordered = {"customer_keys": sorted(keys)}
    blob = json.dumps(ordered, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ordered["sha256"] = hashlib.sha256(blob).hexdigest()
    return ordered


def missing_fields(customers) -> list[str]:
    """Required fields missing from any record, aggregated (not one error per row)."""
    absent: set = set()
    for customer in customers:
        absent.update(field for field in CUSTOMER_FIELDS if field not in customer)
    return sorted(absent)
