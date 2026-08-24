"""Forma esperada dos payloads da fonte e impressao digital de schema.

A Source nao transforma o payload, mas precisa saber navega-lo para contar registros e
para detectar quando a fonte muda de formato. Todas as funcoes aqui sao defensivas:
recebem qualquer objeto JSON e nunca levantam excecao por forma inesperada.

A impressao digital gravada no manifesto permite ao consumidor detectar mudanca de
schema entre snapshots comparando um unico campo.
"""

from __future__ import annotations

import hashlib
import json


class SchemaError(Exception):
    """Payload com forma incompativel com o contrato da Source."""


def is_mapping(value: object) -> bool:
    return isinstance(value, dict)


def groups_of(payload: object) -> list:
    """Subgrupos de um arquivo de catalogo. Lista vazia para qualquer forma inesperada."""
    if not is_mapping(payload):
        return []
    groups = payload.get("categories")
    if not isinstance(groups, list):
        return []
    return [group for group in groups if is_mapping(group)]


def products_of(group: object) -> list:
    """Produtos de um subgrupo. Lista vazia para qualquer forma inesperada."""
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
    """Achata a arvore em categorias de nivel 2, deduplicadas por id.

    Apenas ids de nivel 2 sao acessiveis em /api/categories/{id}/; ids de nivel 1
    respondem 404/410. Nos sem 'id' utilizavel sao descartados: sem id nao ha URL.
    """
    if not is_mapping(tree):
        raise SchemaError("resposta de /categories/ nao e um objeto JSON")
    level1 = tree.get("results")
    if not isinstance(level1, list):
        raise SchemaError("resposta de /categories/ nao tem a lista 'results'")

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
    """Impressao digital do schema observado nos arquivos de catalogo.

    Registra o conjunto ordenado de chaves vistas em 'product' e em
    'price_instructions', mais um hash estavel dos dois conjuntos. Uma renomeacao ou
    remocao de campo na fonte altera o hash entre snapshots.
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
