"""Data platform on top of the Mercadona Catalog Source.

Consumes the Source's snapshots through the PHYSICAL CONTRACT (canonical JSON +
_manifest.json), never importing the Source package. See CONTRACT.md section 4,
"Consumer obligations".
"""

__version__ = "0.1.0"

# Name of this platform's original source. Kept for compatibility (test comparisons,
# docs); land.py derives the prefix from partition.source_name, not from this constant.
SOURCE_NAME = "mercadona_catalog_api"

# Manifest version this platform knows how to read, per source. CONTRACT.md section 8:
# refusing a different version is better than interpreting it by guesswork. Each source
# has its own version counter — they are independent contracts.
SUPPORTED_MANIFEST_VERSIONS = {
    "mercadona_catalog_api": 2,
    "ine_population_api": 1,
    "ine_callejero": 1,
    "simulated_oltp": 1,
    "simulated_orders": 1,
}

__all__ = ["SOURCE_NAME", "SUPPORTED_MANIFEST_VERSIONS", "__version__"]
