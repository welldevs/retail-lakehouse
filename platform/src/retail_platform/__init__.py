"""Plataforma de dados sobre a Mercadona Catalog Source.

Consome os snapshots da Source pelo CONTRATO FISICO (JSON canonico + _manifest.json),
nunca importando o pacote da Source. Ver CONTRACT.md secao 4, "Obrigacoes do consumidor".
"""

__version__ = "0.1.0"

# Nome da source, conforme _manifest.json -> source.name. Primeiro segmento do prefixo
# no object storage, para que uma segunda source aterrisse ao lado sem reorganizar nada.
SOURCE_NAME = "mercadona_catalog_api"

# Versao de manifesto que esta plataforma sabe ler. CONTRACT.md secao 8: recusar versao
# diferente e melhor do que interpretar por adivinhacao.
SUPPORTED_MANIFEST_VERSION = 2

__all__ = ["SOURCE_NAME", "SUPPORTED_MANIFEST_VERSION", "__version__"]
