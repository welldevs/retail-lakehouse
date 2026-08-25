"""INE Callejero Source.

Source de geografia oficial do INE: incorpora os arquivos do Callejero (SECC, UP, VIAS,
PSEU) — sem API, baixados manualmente do site do INE e colocados num diretorio local.
Produz snapshots particionados, reproduziveis e validaveis, com os mesmos bytes dos
arquivos originais.

Este pacote e uma Source, nao uma plataforma de transformacao. Ver CONTRACT.md.
Somente biblioteca padrao.
"""

from __future__ import annotations

__version__ = "1.0.0"

SOURCE_NAME = "ine_callejero"
MANIFEST_VERSION = 1

__all__ = ["SOURCE_NAME", "MANIFEST_VERSION", "__version__"]
