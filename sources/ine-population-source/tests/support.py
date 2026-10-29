"""Suite support: fake HTTP client and payload builders.

No test touches the network. FakeFetcher answers from an in-memory dictionary and counts
requests, so the suite exercises exactly the same extraction code.
"""

from __future__ import annotations

import argparse


class FakeFetcher:
    """Stand-in for http_client.Fetcher, fed by in-memory responses."""

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
            if isinstance(value, BaseException):
                raise value
            return value, None
        return None, "HTTP 404"


def make_fetcher_factory(responses: dict, errors: dict | None = None):
    """Return a FakeFetcher factory and the list of instances it created."""
    created: list[FakeFetcher] = []

    def factory(delay, timeout, max_retries, log, base_url="fake://"):
        fetcher = FakeFetcher(delay, timeout, max_retries, log, base_url)
        fetcher.responses = responses
        fetcher.errors = errors or {}
        created.append(fetcher)
        return fetcher

    return factory, created


def series(cod: str, nombre: str, *points) -> dict:
    """A Tempus3 series: each data point is (year, value)."""
    return {
        "COD": cod,
        "Nombre": nombre,
        "FK_Unidad": 3,
        "FK_Escala": 1,
        "Data": [{"Fecha": 0, "FK_Periodo": 1, "Anyo": year, "Valor": value} for year, value in points],
    }


def table_payload(*series_list) -> list:
    """Response of /DATOS_TABLA/<table_id>: a list of series."""
    return list(series_list)


def extract_args(out: str, **overrides) -> argparse.Namespace:
    defaults = {
        "out": out,
        "tables": ["31304"],
        "date": "2026-01-01",
        "delay": 0.0,
        "timeout": 5.0,
        "max_retries": 0,
        "overwrite": False,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def validate_args(partition: str, strict: bool = False) -> argparse.Namespace:
    return argparse.Namespace(partition=partition, strict=strict)
