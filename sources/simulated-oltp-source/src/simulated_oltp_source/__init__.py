"""Simulated OLTP Source — clientes sinteticos com geografia real.

Gera clientes sinteticos ancorados em enderecos e demografia REAIS do Lakehouse: o
cliente e inventado, o lugar onde ele mora nao. Nenhum CEP, municipio ou via e
fabricado — todos vem do Callejero do INE, e o peso de cada municipio vem da populacao
municipal observada.

Esta e a primeira source DERIVADA do repo: as outras tres sao upstream de dado externo
(API da Mercadona, API do INE, arquivos do Callejero), esta consome o Silver que elas
produziram. A inversao e deliberada e esta documentada no CONTRACT.md secao 2.

Este pacote e uma Source, nao uma plataforma de transformacao. Ver CONTRACT.md.
Somente biblioteca padrao.
"""

from __future__ import annotations

__version__ = "1.0.0"

SOURCE_NAME = "simulated_oltp"
MANIFEST_VERSION = 1

__all__ = ["SOURCE_NAME", "MANIFEST_VERSION", "__version__"]
