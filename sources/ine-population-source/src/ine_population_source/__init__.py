"""INE Population Source.

Population data Source: extracts historical series of inhabitants by province from the
public Tempus3 API of the INE (Instituto Nacional de Estadistica) and produces partitioned,
reproducible, verifiable snapshots.

This package is a Source, not a transformation platform. See CONTRACT.md.
Standard library only.
"""

from __future__ import annotations

__version__ = "1.0.0"

SOURCE_NAME = "ine_population_api"
MANIFEST_VERSION = 1

__all__ = ["SOURCE_NAME", "MANIFEST_VERSION", "__version__"]
