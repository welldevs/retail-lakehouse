"""Expected shape of the source payloads and schema fingerprint.

The Source does not transform the payload, but it needs to navigate it to count records and
to detect when the source changes format. Every function here is defensive: it accepts
any JSON object and never raises on an unexpected shape.

The fingerprint written into the manifest lets the consumer detect a schema change
between snapshots by comparing a single field.
"""

from __future__ import annotations

import hashlib
import json


class SchemaError(Exception):
    """Payload whose shape is incompatible with the Source contract."""


def is_mapping(value: object) -> bool:
    return isinstance(value, dict)


def groups_of(payload: object) -> list:
    """Subgroups of a catalog file. Empty list for any unexpected shape."""
    if not is_mapping(payload):
        return []
    groups = payload.get("categories")
    if not isinstance(groups, list):
        return []
    return [group for group in groups if is_mapping(group)]


def products_of(group: object) -> list:
    """Products of a subgroup. Empty list for any unexpected shape."""
    if not is_mapping(group):
        return []
    products = group.get("products")
    if not isinstance(products, list):
        return []
    return [product for product in products if is_mapping(product)]


def iter_products(payload: object):
    for group in groups_of(payload):
        for product in products_of(group):
            yield group, product


def count_products(payload: object) -> int:
    return sum(1 for _ in iter_products(payload))


def collect_product_ids(payload: object) -> set:
    return {product["id"] for _, product in iter_products(payload) if "id" in product}


def flatten_tree(tree: object) -> list[dict]:
    """Flattens the tree into level-2 categories, deduplicated by id.

    Only level-2 ids are reachable at /api/categories/{id}/; level-1 ids respond
    404/410. Nodes without a usable 'id' are dropped: without an id there is no URL.
    """
    if not is_mapping(tree):
        raise SchemaError("/categories/ response is not a JSON object")
    level1 = tree.get("results")
    if not isinstance(level1, list):
        raise SchemaError("/categories/ response has no 'results' list")

    flattened: list[dict] = []
    seen: set = set()
    for parent in level1:
        if not is_mapping(parent):
            continue
        children = parent.get("categories")
        if not isinstance(children, list):
            continue
        for child in children:
            if not is_mapping(child) or "id" not in child:
                continue
            if child["id"] in seen:
                continue
            seen.add(child["id"])
            flattened.append(
                {
                    "id": child["id"],
                    "name": child.get("name") or "",
                    "parent_id": parent.get("id"),
                    "parent_name": parent.get("name") or "",
                }
            )
    return flattened


def count_level1(tree: object) -> int:
    if not is_mapping(tree) or not isinstance(tree.get("results"), list):
        return 0
    return sum(1 for node in tree["results"] if is_mapping(node))


def fingerprint(payloads) -> dict:
    """Fingerprint of the schema observed in the catalog files.

    Records the sorted set of keys seen in 'product' and in 'price_instructions', plus a
    stable hash of both sets. A field rename or removal at the source changes the hash
    between snapshots.
    """
    product_keys: set = set()
    price_keys: set = set()
    for payload in payloads:
        for _, product in iter_products(payload):
            product_keys.update(product.keys())
            price = product.get("price_instructions")
            if is_mapping(price):
                price_keys.update(price.keys())

    ordered = {
        "product_keys": sorted(product_keys),
        "price_instruction_keys": sorted(price_keys),
    }
    blob = json.dumps(ordered, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ordered["sha256"] = hashlib.sha256(blob).hexdigest()
    return ordered
