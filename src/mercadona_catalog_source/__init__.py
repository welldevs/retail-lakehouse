"""Mercadona Catalog API Source.

Source de dados de catalogo: extrai produto, categoria e preco da API publica da
Mercadona e produz snapshots particionados, reproduziveis e validaveis.

Este pacote e uma Source, nao uma plataforma de transformacao. Ver CONTRACT.md.
Somente biblioteca padrao.
"""

from __future__ import annotations

__version__ = "1.0.0"

SOURCE_NAME = "mercadona_catalog_api"
MANIFEST_VERSION = 2

__all__ = ["SOURCE_NAME", "MANIFEST_VERSION", "__version__"]
