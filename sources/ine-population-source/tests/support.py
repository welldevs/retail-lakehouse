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


def series(cod: str, nombre: str, *points) -> dict:
    """Uma serie Tempus3: cada ponto e (ano, valor)."""
    return {
        "COD": cod,
        "Nombre": nombre,
        "FK_Unidad": 3,
        "FK_Escala": 1,
        "Data": [{"Fecha": 0, "FK_Periodo": 1, "Anyo": year, "Valor": value} for year, value in points],
    }


def table_payload(*series_list) -> list:
    """Resposta de /DATOS_TABLA/<table_id>: uma lista de series."""
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
