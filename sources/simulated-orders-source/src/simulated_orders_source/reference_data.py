"""Leitura e validacao estrutural dos quatro arquivos de referencia.

A Source nao fala com o Lakehouse: le quatro JSON planos que a plataforma escreveu com
`retail-platform export-orders-reference`. Este modulo e a unica porta de entrada desse
insumo, e ele DESCONFIA do arquivo — um export truncado, de schema antigo, ou vazio para um
armazem tem de REPROVAR a geracao, nunca produzir pedidos enviesados em silencio.

ORDEM IMPORTA. Tudo aqui e lista, na ordem do arquivo — nunca `set` nem `dict` reconstruido
fora de ordem, cuja iteracao depende de PYTHONHASHSEED e quebraria a reprodutibilidade por
seed. Os indices por (armazem, data) sao consultados por chave, jamais iterados.

PRECO E STRING, E CONTINUA STRING
----------------------------------
`unit_price` chega como string e vira `decimal.Decimal`, nunca `float`. E a obrigacao 4.4 do
contrato da Mercadona: converter moeda para float perde precisao decimal. `Decimal` e stdlib,
entao a fronteira FROZEN continua de pe.
"""

from __future__ import annotations

import json
import os
from decimal import Decimal, InvalidOperation

CUSTOMERS_FILE = "customers.json"
CATALOG_FILE = "catalog.json"
CALENDAR_FILE = "calendar.json"
PREMISES_FILE = "premises.json"

REFERENCE_FILES = (CUSTOMERS_FILE, CATALOG_FILE, CALENDAR_FILE, PREMISES_FILE)

CUSTOMER_FIELDS = (
    "customer_id",
    "wh",
    "province_code",
    "municipality_code",
    "postal_code",
    "first_ingestion_date",
)
CATALOG_FIELDS = (
    "wh",
    "price_as_of",
    "source_product_id",
    "display_name",
    "category_id",
    "subgroup_id",
    "unit_price",
    "tax_percentage",
)
CALENDAR_FIELDS = ("wh", "order_date", "price_as_of", "price_source")

PRICE_OBSERVED = "observed"
PRICE_CARRIED_FORWARD = "carried_forward"
PRICE_SOURCES = (PRICE_OBSERVED, PRICE_CARRIED_FORWARD)


class ReferenceError(Exception):
    """Referencia ausente, ilegivel, incompleta ou incoerente."""


def _load_file(directory: str, name: str) -> dict:
    path = os.path.join(directory, name)
    if not os.path.exists(path):
        raise ReferenceError(
            f"arquivo de referencia ausente: {path}. Gere-o com "
            f"`make orders-export-reference` antes de extrair."
        )
    try:
        with open(path, "rb") as handle:
            payload = json.loads(handle.read().decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ReferenceError(f"{path}: JSON invalido ({exc})") from exc
    if not isinstance(payload, dict):
        raise ReferenceError(f"{path}: raiz nao e um objeto JSON")
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ReferenceError(f"{path}: sem a lista 'rows' ou lista vazia")
    return payload


def _require_fields(path: str, rows: list, fields: tuple, limit: int = 200) -> None:
    """Confere os campos obrigatorios. Amostra as primeiras linhas, nao o arquivo todo.

    `catalog.json` tem dezenas de milhares de linhas: varrer tudo a cada execucao custaria
    mais do que protege. Um export truncado ou de schema antigo se denuncia nas primeiras
    linhas, e o resto e coberto na hora de usar cada campo.
    """
    for position, row in enumerate(rows[:limit]):
        if not isinstance(row, dict):
            raise ReferenceError(f"{path}: linha {position} nao e um objeto JSON")
        missing = [field for field in fields if field not in row]
        if missing:
            raise ReferenceError(f"{path}: linha {position} sem o(s) campo(s) {missing}")


def _decimal(value, path: str, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ReferenceError(f"{path}: {field} nao e decimal: {value!r}") from exc


class Reference:
    """Indice imutavel sobre os quatro arquivos, pronto para amostragem deterministica."""

    def __init__(self, directory: str, customers: dict, catalog: dict, calendar: dict, premises: dict):
        self.directory = directory
        self.customer_ingestion_dates = list(customers.get("customer_ingestion_dates") or [])
        self.roster_ingestion_date = customers.get("roster_ingestion_date")
        self.catalog_ingestion_dates = list(catalog.get("catalog_ingestion_dates") or [])
        self.window_from = calendar.get("window_from")
        self.window_to = calendar.get("window_to")
        self.premises_sha256 = premises.get("seed_sha256")
        self.premises_seed_path = premises.get("seed_path")
        self.carried_forward_rows = calendar.get("carried_forward_rows")

        if not self.customer_ingestion_dates:
            raise ReferenceError(f"{CUSTOMERS_FILE}: sem customer_ingestion_dates no cabecalho")

        # wh -> [cliente, ...] na ordem do arquivo. A POSICAO nao e carregada para o evento
        # (diferente do candidate_index da Fase 1), mas a ordem ainda importa: e ela que a
        # amostragem por indice percorre, e ela precisa ser estavel entre execucoes.
        self._customers_by_wh: dict[str, list[dict]] = {}
        for row in customers["rows"]:
            self._customers_by_wh.setdefault(row["wh"], []).append(row)

        # (wh, price_as_of) -> [produto, ...]. Guardamos a linha inteira: o gerador precisa
        # de preco, nome, categoria e subgrupo para montar a cesta e para substituir dentro
        # do mesmo subgrupo.
        self._catalog: dict[tuple, list[dict]] = {}
        for row in catalog["rows"]:
            row = dict(row)
            row["unit_price"] = _decimal(row["unit_price"], CATALOG_FILE, "unit_price")
            if row["unit_price"] <= 0:
                raise ReferenceError(
                    f"{CATALOG_FILE}: unit_price nao positivo para "
                    f"{row.get('source_product_id')!r} em {row.get('wh')!r}"
                )
            self._catalog.setdefault((row["wh"], row["price_as_of"]), []).append(row)

        # (wh, price_as_of, subgroup_id) -> [posicao no catalogo daquele par, ...]. Indice de
        # substituicao: trocar um produto por outro do MESMO subgrupo mantem a troca dentro
        # do sortimento que aquele armazem realmente vende naquele dia.
        self._by_subgroup: dict[tuple, list[int]] = {}
        self._by_category: dict[tuple, list[int]] = {}
        for key, produtos in self._catalog.items():
            for position, row in enumerate(produtos):
                self._by_subgroup.setdefault(key + (row["subgroup_id"],), []).append(position)
                self._by_category.setdefault(key + (row["category_id"],), []).append(position)

        # (wh, order_date) -> linha do calendario.
        self._calendar: dict[tuple, dict] = {}
        for row in calendar["rows"]:
            if row["price_source"] not in PRICE_SOURCES:
                raise ReferenceError(
                    f"{CALENDAR_FILE}: price_source fora do vocabulario: "
                    f"{row['price_source']!r}"
                )
            self._calendar[(row["wh"], row["order_date"])] = row

        self.premises = dict(premises.get("values") or {})
        if not self.premises:
            raise ReferenceError(f"{PREMISES_FILE}: sem o bloco 'values'")

    # -- consultas -----------------------------------------------------------------

    def warehouses(self) -> list[str]:
        return sorted(self._customers_by_wh)

    def order_dates(self, wh: str) -> list[str]:
        return sorted(date for (w, date) in self._calendar if w == wh)

    def customers_of(self, wh: str) -> list[dict]:
        rows = self._customers_by_wh.get(wh)
        if not rows:
            raise ReferenceError(
                f"referencia sem nenhum cliente para wh={wh!r}. "
                f"Warehouses disponiveis: {self.warehouses()}"
            )
        return rows

    def calendar_of(self, wh: str, order_date: str) -> dict:
        row = self._calendar.get((wh, order_date))
        if row is None:
            raise ReferenceError(
                f"referencia sem linha de calendario para wh={wh!r} dia={order_date!r}. "
                f"A janela exportada foi {self.window_from} a {self.window_to}."
            )
        return row

    def catalog_of(self, wh: str, price_as_of: str) -> list[dict]:
        rows = self._catalog.get((wh, price_as_of))
        if not rows:
            raise ReferenceError(
                f"referencia sem catalogo para wh={wh!r} price_as_of={price_as_of!r}"
            )
        return rows

    def substitutes_in_subgroup(self, wh: str, price_as_of: str, subgroup_id) -> list[int]:
        return self._by_subgroup.get((wh, price_as_of, subgroup_id), [])

    def substitutes_in_category(self, wh: str, price_as_of: str, category_id) -> list[int]:
        return self._by_category.get((wh, price_as_of, category_id), [])

    def customer_version_at(self, first_ingestion_date: str, order_date: str) -> str | None:
        """Geracao de cliente vigente na data do pedido, ou None se ele ainda nao existia.

        A base e append-only (crescer preserva os primeiros N byte a byte), entao um cliente
        presente numa geracao esta presente em todas as posteriores; basta a maior data <=
        order_date que nao seja anterior a primeira aparicao dele.
        """
        if order_date < first_ingestion_date:
            return None
        candidatas = [d for d in self.customer_ingestion_dates if d <= order_date]
        return max(candidatas) if candidatas else None


def load(directory: str) -> Reference:
    """Le e valida os quatro arquivos de referencia de um diretorio."""
    if not os.path.isdir(directory):
        raise ReferenceError(
            f"diretorio de referencia nao encontrado: {directory}. Gere-o com "
            f"`make orders-export-reference`."
        )
    customers = _load_file(directory, CUSTOMERS_FILE)
    catalog = _load_file(directory, CATALOG_FILE)
    calendar = _load_file(directory, CALENDAR_FILE)

    path = os.path.join(directory, PREMISES_FILE)
    if not os.path.exists(path):
        raise ReferenceError(f"arquivo de referencia ausente: {path}")
    try:
        with open(path, "rb") as handle:
            premises = json.loads(handle.read().decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ReferenceError(f"{path}: JSON invalido ({exc})") from exc
    if not isinstance(premises, dict):
        raise ReferenceError(f"{path}: raiz nao e um objeto JSON")

    _require_fields(os.path.join(directory, CUSTOMERS_FILE), customers["rows"], CUSTOMER_FIELDS)
    _require_fields(os.path.join(directory, CATALOG_FILE), catalog["rows"], CATALOG_FIELDS)
    _require_fields(os.path.join(directory, CALENDAR_FILE), calendar["rows"], CALENDAR_FIELDS)
    return Reference(directory, customers, catalog, calendar, premises)
