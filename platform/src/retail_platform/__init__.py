"""Plataforma de dados sobre a Mercadona Catalog Source.

Consome os snapshots da Source pelo CONTRATO FISICO (JSON canonico + _manifest.json),
nunca importando o pacote da Source. Ver CONTRACT.md secao 4, "Obrigacoes do consumidor".
"""

__version__ = "0.1.0"

# Nome da source original desta plataforma. Mantido por compatibilidade (comparacoes em
# teste, docs); land.py deriva o prefixo de partition.source_name, nao desta constante.
SOURCE_NAME = "mercadona_catalog_api"

# Versao de manifesto que esta plataforma sabe ler, por source. CONTRACT.md secao 8:
# recusar versao diferente e melhor do que interpretar por adivinhacao. Cada source tem
# seu proprio contador de versao — sao contratos independentes.
SUPPORTED_MANIFEST_VERSIONS = {
    "mercadona_catalog_api": 2,
    "ine_population_api": 1,
    "ine_callejero": 1,
    "simulated_oltp": 1,
    "simulated_orders": 1,
}

__all__ = ["SOURCE_NAME", "SUPPORTED_MANIFEST_VERSIONS", "__version__"]
