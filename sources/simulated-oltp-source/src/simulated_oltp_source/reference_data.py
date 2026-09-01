"""Leitura e validacao estrutural do arquivo de referencia geografica/demografica.

A Source nao fala com o Lakehouse: ela le tres JSON planos que a plataforma escreveu com
`retail-platform export-oltp-reference`. Este modulo e a unica porta de entrada desse
insumo, e ele desconfia do arquivo: um export truncado, de um schema antigo, ou vazio
para um warehouse tem de REPROVAR a geracao, nunca produzir clientes enviesados em
silencio.

ORDEM IMPORTA. A posicao de cada linha em `address_candidates.json` e o `candidate_index`
que o cliente carrega, e e o que torna a linhagem auditavel ate o tramo exato. Por isso
tudo aqui e lista, na ordem do arquivo — nunca `set` nem `dict` reconstruido fora de
ordem, cuja iteracao depende de PYTHONHASHSEED e quebraria a reprodutibilidade por seed.
"""

from __future__ import annotations

import json
import os

ADDRESS_CANDIDATES_FILE = "address_candidates.json"
POPULATION_WEIGHTS_FILE = "municipality_population_weights.json"
AGE_DISTRIBUTION_FILE = "province_age_distribution.json"

REFERENCE_FILES = (
    ADDRESS_CANDIDATES_FILE,
    POPULATION_WEIGHTS_FILE,
    AGE_DISTRIBUTION_FILE,
)

CANDIDATE_FIELDS = (
    "wh",
    "province_code",
    "municipality_code",
    # O Callejero renderiza o nome do municipio de forma diferente da tabela 29005 em 370
    # de 370 casos no escopo ("BRUC (EL)" contra "Bruc, El"). Os dois nomes convivem na
    # referencia com rotulos distintos; o cliente carrega o da 29005, que e o legivel e o
    # mesmo de warehouse_service_area_seed. A chave comparavel e o CODIGO, nunca o nome.
    "municipality_name_callejero",
    "street_name",
    "is_pseudo_address",
    "postal_code",
    "numbering_type",
)
WEIGHT_FIELDS = (
    "wh",
    "province_code",
    "province_name",
    "municipality_code",
    "municipality_name",
    "population_total",
    "proportion_within_wh",
    "sex_hombres_proportion",
    "sex_mujeres_proportion",
)
AGE_FIELDS = ("province_code", "age", "proportion")

# Cabecalhos que a referencia PRECISA trazer desde que o cadastro deixou de ser uma projecao
# da populacao residente. Ausencia significa referencia de schema antigo — gerada antes de
# min_customer_age existir — e ela nao pode ser usada em silencio: produziria de novo uma base
# com titular de conta recem-nascido, que e exatamente o defeito que o corte existe para
# fechar. Ver CONTRACT.md secao 2.
MIN_AGE_HEADER = "min_customer_age"
ALLOCATION_HEADER = "customer_allocation"
ALLOCATION_ROWS = "by_warehouse"


class ReferenceError(Exception):
    """Referencia ausente, ilegivel, incompleta ou incoerente."""


def _load_file(directory: str, name: str) -> dict:
    path = os.path.join(directory, name)
    if not os.path.exists(path):
        raise ReferenceError(
            f"arquivo de referencia ausente: {path}. Gere-o com "
            f"`make oltp-export-reference` antes de extrair."
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
    """Confere os campos obrigatorios. Amostra as primeiras linhas e nao o arquivo todo.

    `address_candidates.json` tem centenas de milhares de linhas: varrer tudo a cada
    execucao custaria mais do que protege. Um export truncado ou de schema antigo se
    denuncia nas primeiras linhas, e o resto e coberto na hora de usar cada campo.
    """
    for position, row in enumerate(rows[:limit]):
        if not isinstance(row, dict):
            raise ReferenceError(f"{path}: linha {position} nao e um objeto JSON")
        missing = [field for field in fields if field not in row]
        if missing:
            raise ReferenceError(f"{path}: linha {position} sem o(s) campo(s) {missing}")


class Reference:
    """Indice imutavel sobre os tres arquivos, pronto para amostragem deterministica."""

    def __init__(self, directory: str, candidates: dict, weights: dict, ages: dict):
        self.directory = directory
        self.callejero_ingestion_date = candidates.get("callejero_ingestion_date")
        self.population_ingestion_date = weights.get("population_ingestion_date")
        self.population_series_ingestion_date = ages.get("population_series_ingestion_date")
        self.population_year = weights.get("population_year")
        self.population_reference_date = weights.get("population_reference_date")
        self.age_year = ages.get("year")
        self.age_reference_date = ages.get("reference_date")
        self.age_fk_periodo = ages.get("fk_periodo")

        # A IDADE MINIMA E DA REFERENCIA, nao desta Source. Quem decide quem pode ter cadastro
        # e a plataforma, em customer_premises_seed; aqui ela e lida, registrada no manifesto
        # e RECONFERIDA — a distribuicao entregue nao pode conter idade abaixo dela.
        self.min_customer_age = ages.get(MIN_AGE_HEADER)
        allocation = weights.get(ALLOCATION_HEADER) or {}
        self.customer_allocation = allocation
        self.allocation_rule = allocation.get("rule")
        self.penetration_pct = allocation.get("penetration_pct")
        self.penetration_source = allocation.get("penetration_source")
        self.served_population = allocation.get("served_population")
        self.served_adult_population = allocation.get("served_adult_population")
        # Dict de consulta, nunca iterado: a ordem de insercao nao influencia amostragem
        # nenhuma, e a lista ordenada de onde ele vem esta preservada em customer_allocation.
        self._targets = {
            row["wh"]: int(row["customers"])
            for row in allocation.get(ALLOCATION_ROWS, [])
            if isinstance(row, dict) and "wh" in row and "customers" in row
        }
        self.orphan_tramos_excluded = (candidates.get("excluded_rows") or {}).get(
            "no_street_or_pseudo_match"
        )

        self._candidates = candidates["rows"]

        # (wh, province_code, municipality_code) -> [candidate_index, ...], na ordem do
        # arquivo. Guardamos o INDICE, nao a linha: e ele que vai para o cliente.
        self._by_municipality: dict[tuple, list[int]] = {}
        for index, row in enumerate(self._candidates):
            key = (row["wh"], row["province_code"], row["municipality_code"])
            self._by_municipality.setdefault(key, []).append(index)

        self._weights: dict[str, list[dict]] = {}
        for row in weights["rows"]:
            self._weights.setdefault(row["wh"], []).append(row)

        self._ages: dict[str, tuple[list[int], list[float]]] = {}
        grouped: dict[str, list[dict]] = {}
        for row in ages["rows"]:
            grouped.setdefault(row["province_code"], []).append(row)
        for province, rows in grouped.items():
            self._ages[province] = (
                [int(row["age"]) for row in rows],
                [float(row["proportion"]) for row in rows],
            )

    def warehouses(self) -> list[str]:
        return sorted(self._weights)

    def municipalities(self, wh: str) -> list[dict]:
        rows = self._weights.get(wh)
        if not rows:
            raise ReferenceError(
                f"referencia sem nenhum municipio para wh={wh!r}. "
                f"Warehouses disponiveis: {self.warehouses()}"
            )
        return rows

    def candidate(self, index: int) -> dict:
        return self._candidates[index]

    def candidate_count(self) -> int:
        return len(self._candidates)

    def candidates_of(self, wh: str, province_code: str, municipality_code: str) -> list[int]:
        indexes = self._by_municipality.get((wh, province_code, municipality_code))
        if not indexes:
            raise ReferenceError(
                f"referencia sem candidato de endereco para "
                f"wh={wh!r} provincia={province_code!r} municipio={municipality_code!r}"
            )
        return indexes

    def customer_target(self, wh: str) -> int:
        """Quantos clientes este armazem deve ter, segundo a referencia.

        E o que o `extract` usa quando `--count` e omitido. NAO ha default: um numero chutado
        aqui produziria uma base dimensionada por ninguem, que e de onde vinham os 5.000 iguais
        para AUFs que diferem por 4,6x em populacao.
        """
        alvo = self._targets.get(wh)
        if alvo is None:
            raise ReferenceError(
                f"referencia sem alvo de clientes para wh={wh!r}. Armazens com alvo: "
                f"{sorted(self._targets)}. Regere a referencia com "
                f"`make oltp-export-reference`, ou passe --count explicitamente."
            )
        if alvo < 1:
            raise ReferenceError(f"alvo de clientes invalido para wh={wh!r}: {alvo}")
        return alvo

    def ages_of(self, province_code: str) -> tuple[list[int], list[float]]:
        entry = self._ages.get(province_code)
        if not entry:
            raise ReferenceError(
                f"referencia sem distribuicao etaria para a provincia {province_code!r}"
            )
        return entry


def _require_customer_scope(directory: str, weights: dict, ages: dict) -> None:
    """A referencia declara quem pode ter cadastro, e o conteudo dela tem de obedecer.

    TRES coisas, e nenhuma delas e opcional:

      1. `min_customer_age` no cabecalho da distribuicao etaria. Sem ele, esta Source nao tem
         como distinguir "distribuicao do cadastro" de "distribuicao da populacao inteira", e
         geraria de novo os 18,01% de menores de idade medidos em 2026-08-31 — 3.602 de
         20.000, com idade a partir de zero.
      2. Nenhuma idade abaixo dele nas linhas. O cabecalho e a promessa; isto e a conferencia.
         Uma referencia truncada pela metade passaria no item 1 e falharia aqui.
      3. `customer_allocation` no cabecalho dos pesos. Sem ela o `extract` sem --count nao tem
         alvo, e cair num default seria voltar ao numero digitado a mao.

    Referencia de schema antigo reprova com mensagem acionavel em vez de gerar em silencio —
    a mesma disciplina do resto deste modulo.
    """
    age_path = os.path.join(directory, AGE_DISTRIBUTION_FILE)
    minima = ages.get(MIN_AGE_HEADER)
    if not isinstance(minima, int):
        raise ReferenceError(
            f"{age_path}: sem '{MIN_AGE_HEADER}' inteiro no cabecalho. Esta referencia foi "
            f"gerada antes de o cadastro passar a excluir menores de idade; regere com "
            f"`make oltp-export-reference`."
        )
    abaixo = sorted(
        {int(row["age"]) for row in ages["rows"] if int(row["age"]) < minima}
    )
    if abaixo:
        raise ReferenceError(
            f"{age_path}: idade(s) {abaixo[:10]} abaixo do {MIN_AGE_HEADER}={minima} que o "
            f"proprio cabecalho declara."
        )

    weights_path = os.path.join(directory, POPULATION_WEIGHTS_FILE)
    allocation = weights.get(ALLOCATION_HEADER)
    if not isinstance(allocation, dict) or not allocation.get(ALLOCATION_ROWS):
        raise ReferenceError(
            f"{weights_path}: sem '{ALLOCATION_HEADER}.{ALLOCATION_ROWS}' no cabecalho. E de "
            f"la que sai quantos clientes cada armazem tem quando --count e omitido."
        )


def load(directory: str) -> Reference:
    """Le e valida os tres arquivos de referencia de um diretorio."""
    if not os.path.isdir(directory):
        raise ReferenceError(
            f"diretorio de referencia nao encontrado: {directory}. Gere-o com "
            f"`make oltp-export-reference`."
        )
    candidates = _load_file(directory, ADDRESS_CANDIDATES_FILE)
    weights = _load_file(directory, POPULATION_WEIGHTS_FILE)
    ages = _load_file(directory, AGE_DISTRIBUTION_FILE)

    _require_fields(
        os.path.join(directory, ADDRESS_CANDIDATES_FILE), candidates["rows"], CANDIDATE_FIELDS
    )
    _require_fields(
        os.path.join(directory, POPULATION_WEIGHTS_FILE), weights["rows"], WEIGHT_FIELDS
    )
    _require_fields(
        os.path.join(directory, AGE_DISTRIBUTION_FILE), ages["rows"], AGE_FIELDS
    )
    _require_customer_scope(directory, weights, ages)
    return Reference(directory, candidates, weights, ages)
