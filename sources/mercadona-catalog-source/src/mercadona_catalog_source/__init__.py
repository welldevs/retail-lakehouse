"""Mercadona Catalog API Source.

Catalog data Source: extracts product, category and price from Mercadona's public API
and produces partitioned, reproducible, verifiable snapshots.

This package is a Source, not a transformation platform. See CONTRACT.md.
Standard library only.
"""

from __future__ import annotations

__version__ = "1.0.0"

SOURCE_NAME = "mercadona_catalog_api"
MANIFEST_VERSION = 2

__all__ = ["SOURCE_NAME", "MANIFEST_VERSION", "__version__"]
