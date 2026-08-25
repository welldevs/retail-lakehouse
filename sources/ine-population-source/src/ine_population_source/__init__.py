"""INE Population Source.

Source de dados de populacao: extrai series historicas de habitantes por provincia da
API publica Tempus3 do INE (Instituto Nacional de Estadistica) e produz snapshots
particionados, reproduziveis e validaveis.

Este pacote e uma Source, nao uma plataforma de transformacao. Ver CONTRACT.md.
Somente biblioteca padrao.
"""

from __future__ import annotations

__version__ = "1.0.0"

SOURCE_NAME = "ine_population_api"
MANIFEST_VERSION = 1

__all__ = ["SOURCE_NAME", "MANIFEST_VERSION", "__version__"]
