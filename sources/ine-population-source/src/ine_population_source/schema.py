"""Forma esperada dos payloads da fonte e impressao digital de schema.

A Source nao transforma o payload, mas precisa saber navega-lo para contar series e
pontos, e para detectar quando a fonte muda de formato. Todas as funcoes aqui sao
defensivas: recebem qualquer objeto JSON e nunca levantam excecao por forma inesperada.

Forma esperada de um payload de /DATOS_TABLA/<table_id>: uma lista de series, cada uma
com "COD", "Nombre" e "Data" (lista de pontos, cada um com pelo menos "Anyo" e "Valor").
A impressao digital gravada no manifesto permite ao consumidor detectar mudanca de schema
entre snapshots comparando um unico campo.
"""

from __future__ import annotations

import hashlib
import json


class SchemaError(Exception):
    """Payload com forma incompativel com o contrato da Source."""


def is_mapping(value: object) -> bool:
    return isinstance(value, dict)


def series_of(payload: object) -> list:
    """Series de um payload de tabela. Lista vazia para qualquer forma inesperada."""
    if not isinstance(payload, list):
        return []
    return [item for item in payload if is_mapping(item)]


def data_points_of(series: object) -> list:
    """Pontos de uma serie. Lista vazia para qualquer forma inesperada."""
    if not is_mapping(series):
        return []
    points = series.get("Data")
    if not isinstance(points, list):
        return []
    return [point for point in points if is_mapping(point)]


def iter_data_points(payload: object):
    for series in series_of(payload):
        for point in data_points_of(series):
            yield series, point


def count_series(payload: object) -> int:
    return len(series_of(payload))


def count_data_points(payload: object) -> int:
    return sum(1 for _ in iter_data_points(payload))


def validate_payload(payload: object) -> list:
    """Confere que o payload e uma lista de series com 'COD' e 'Data'. Levanta
    SchemaError se a forma raiz nao bater; series individuais malformadas sao apenas
    ignoradas por series_of (a Source nao inventa dado que nao veio da fonte)."""
    if not isinstance(payload, list):
        raise SchemaError("resposta de /DATOS_TABLA nao e uma lista JSON")
    series = series_of(payload)
    if not series:
        raise SchemaError("resposta de /DATOS_TABLA sem nenhuma serie reconhecivel")
    return series


def fingerprint(payloads) -> dict:
    """Impressao digital do schema observado nos arquivos de tabela.

    Registra o conjunto ordenado de chaves vistas em cada serie e em cada ponto de
    'Data', mais um hash estavel dos dois conjuntos. Uma renomeacao ou remocao de campo
    na fonte altera o hash entre snapshots.
    """
    series_keys: set = set()
    point_keys: set = set()
    for payload in payloads:
        for series, point in iter_data_points(payload):
            series_keys.update(k for k in series.keys() if k != "Data")
            point_keys.update(point.keys())

    ordered = {
        "series_keys": sorted(series_keys),
        "data_point_keys": sorted(point_keys),
    }
    blob = json.dumps(ordered, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ordered["sha256"] = hashlib.sha256(blob).hexdigest()
    return ordered
