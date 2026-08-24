"""Apoio da suite: cliente HTTP falso e construtores de payload.

Nenhum teste toca a rede. O FakeFetcher responde de um dicionario em memoria e conta
requisicoes, para que a suite exercite exatamente o mesmo codigo de extracao.
"""

from __future__ import annotations

import argparse


class FakeFetcher:
    """Substituto de http_client.Fetcher, alimentado por respostas em memoria."""

    responses: dict = {}
    errors: dict = {}

    def __init__(self, delay, timeout, max_retries, log, base_url="fake://") -> None:
        self.delay = delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.log = log
        self.base_url = base_url
        self.requests = 0
        self.retries = 0
        self.paths: list[str] = []

    def get_json(self, path: str, params: dict):
        self.requests += 1
        self.paths.append(path)
        if path in self.errors:
            return None, self.errors[path]
        if path in self.responses:
            value = self.responses[path]
            # Uma excecao como resposta simula interrupcao do processo no meio da etapa 2.
            if isinstance(value, BaseException):
                raise value
            return value, None
        return None, "HTTP 404"


def make_fetcher_factory(responses: dict, errors: dict | None = None):
    """Devolve uma fabrica de FakeFetcher e a lista de instancias criadas."""
    created: list[FakeFetcher] = []

    def factory(delay, timeout, max_retries, log, base_url="fake://"):
        fetcher = FakeFetcher(delay, timeout, max_retries, log, base_url)
        fetcher.responses = responses
        fetcher.errors = errors or {}
        created.append(fetcher)
        return fetcher

    return factory, created


def tree(*categories) -> dict:
    """Arvore de categorias: cada argumento e (parent_id, parent_name, [(id, name), ...])."""
    return {
        "results": [
            {
                "id": parent_id,
                "name": parent_name,
                "categories": [{"id": cid, "name": cname} for cid, cname in children],
            }
            for parent_id, parent_name, children in categories
        ]
    }


def catalog(category_id: int, name: str, *groups) -> dict:
    """Catalogo de uma categoria: cada grupo e (nome, [(product_id, nome, preco), ...])."""
    return {
        "id": category_id,
        "name": name,
        "published": True,
        "categories": [
            {
                "id": 900 + index,
                "name": group_name,
                "products": [
                    {
                        "id": str(product_id),
                        "display_name": product_name,
                        "slug": f"p-{product_id}",
                        "price_instructions": {
                            "unit_price": price,
                            "reference_price": price,
                            "reference_format": "kg",
                        },
                    }
                    for product_id, product_name, price in products
                ],
            }
            for index, (group_name, products) in enumerate(groups)
        ],
    }


def extract_args(out: str, **overrides) -> argparse.Namespace:
    defaults = {
        "out": out,
        "wh": "mad1",
        "lang": "es",
        "date": "2026-01-01",
        "delay": 0.0,
        "timeout": 5.0,
        "max_retries": 0,
        "limit": None,
        "overwrite": False,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def validate_args(partition: str, strict: bool = False) -> argparse.Namespace:
    return argparse.Namespace(partition=partition, strict=strict)
