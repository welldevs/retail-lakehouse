"""INE Callejero Source.

Official INE geography Source: ingests the Callejero files (SECC, UP, VIAS, PSEU, TRAM)
— there is no API; they are downloaded manually from the INE website and placed in a local
directory. Produces partitioned, reproducible, verifiable snapshots holding the same bytes
as the original files.

This package is a Source, not a transformation platform. See CONTRACT.md.
Standard library only.
"""

from __future__ import annotations

__version__ = "1.0.0"

SOURCE_NAME = "ine_callejero"
MANIFEST_VERSION = 1

__all__ = ["SOURCE_NAME", "MANIFEST_VERSION", "__version__"]
