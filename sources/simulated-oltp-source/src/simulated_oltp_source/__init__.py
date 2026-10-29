"""Simulated OLTP Source — synthetic customers with real geography.

Generates synthetic customers anchored in REAL addresses and demographics from the
Lakehouse: the customer is invented, the place where they live is not. No postal code,
municipality or street is fabricated — all of them come from the INE Callejero, and each
municipality's weight comes from the observed municipal population.

This is the first DERIVED source in the repo: the other three are upstream of external data
(Mercadona API, INE API, Callejero files); this one consumes the Silver they produced. The
inversion is deliberate and is documented in CONTRACT.md section 2.

This package is a Source, not a transformation platform. See CONTRACT.md.
Standard library only.
"""

from __future__ import annotations

__version__ = "1.0.0"

SOURCE_NAME = "simulated_oltp"
MANIFEST_VERSION = 1

__all__ = ["SOURCE_NAME", "MANIFEST_VERSION", "__version__"]
