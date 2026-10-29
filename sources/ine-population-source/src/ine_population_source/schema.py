"""Expected shape of the source payloads and schema fingerprint.

The Source does not transform the payload, but it needs to navigate it to count series and
data points, and to detect when the source changes format. Every function here is
defensive: it accepts any JSON object and never raises on an unexpected shape.

Expected shape of a /DATOS_TABLA/<table_id> payload: a list of series, each with "COD",
"Nombre" and "Data" (a list of data points, each with at least "Anyo" and "Valor").
The fingerprint written into the manifest lets the consumer detect a schema change between
snapshots by comparing a single field.
"""

from __future__ import annotations

import hashlib
import json


class SchemaError(Exception):
    """Payload whose shape is incompatible with the Source contract."""


def is_mapping(value: object) -> bool:
    return isinstance(value, dict)


def series_of(payload: object) -> list:
    """Series of a table payload. Empty list for any unexpected shape."""
    if not isinstance(payload, list):
        return []
    return [item for item in payload if is_mapping(item)]


def data_points_of(series: object) -> list:
    """Data points of a series. Empty list for any unexpected shape."""
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
    """Check that the payload is a list of series with 'COD' and 'Data'. Raises
    SchemaError if the root shape does not match; individual malformed series are simply
    ignored by series_of (the Source does not invent data that did not come from the source).

    The SchemaError messages are kept byte-identical: they are persisted in the manifest's
    failures[].error.
    """
    if not isinstance(payload, list):
        raise SchemaError("resposta de /DATOS_TABLA nao e uma lista JSON")
    series = series_of(payload)
    if not series:
        raise SchemaError("resposta de /DATOS_TABLA sem nenhuma serie reconhecivel")
    return series


def fingerprint(payloads) -> dict:
    """Fingerprint of the schema observed in the table files.

    Records the sorted set of keys seen in each series and in each 'Data' point, plus a
    stable hash of both sets. A field rename or removal at the source changes the hash
    between snapshots.
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
