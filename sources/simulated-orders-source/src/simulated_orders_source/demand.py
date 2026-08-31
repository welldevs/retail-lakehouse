"""Sorteio ponderado do grupo de demanda.

O QUE ESTE MODULO SABE, E O QUE ELE DELIBERADAMENTE NAO SABE
-------------------------------------------------------------
Sabe: um peso por grupo, lido de `demand_profile.json`.
NAO sabe: o que e o MAPA, o que e o mapeamento de categoria, o que e inclinacao de canal,
nem por que um peso vale o que vale. Tudo isso e resolvido pela plataforma e chega aqui
como numero. E a mesma fronteira que ja vale para preco e cliente — a Source e FROZEN
(`dependencies = []`) e nao pode carregar regra de calibracao.

A CONSEQUENCIA PRATICA e que trocar a calibracao NAO exige tocar neste arquivo: edita-se o
seed, reexporta-se a referencia, e o mesmo codigo sorteia diferente.

DUAS PROPRIEDADES QUE ESTE MODULO PRECISA TER
----------------------------------------------
1. DETERMINISMO SOB PYTHONHASHSEED. A CDF e construida a partir de uma TUPLA ORDENADA de
   chaves, nunca da iteracao de um `dict`. O hash de `str` em CPython e aleatorizado por
   processo: uma CDF montada na ordem de iteracao de um dicionario faria a saida depender
   da variavel de ambiente, e o defeito so apareceria entre maquinas.
2. RENORMALIZACAO POR (ARMAZEM, DIA). Um grupo pode nao ter produto no catalogo de um
   armazem num dia. O peso dele e redistribuido entre os presentes, na proporcao dos
   respectivos pesos — e o subconjunto de grupos presentes tambem chega ordenado, pelo mesmo
   motivo do item 1.
"""

from __future__ import annotations


class DemandError(Exception):
    """Perfil de demanda ausente, incompleto ou incoerente."""


class DemandModel:
    """Pesos por grupo, prontos para sorteio."""

    def __init__(self, payload: dict):
        if not isinstance(payload, dict):
            raise DemandError("demand_profile.json: raiz nao e um objeto JSON")

        self.version = payload.get("demand_model_version")
        if not self.version:
            raise DemandError(
                "demand_profile.json sem `demand_model_version`. A versao viaja para o "
                "manifesto e e o que distingue dois dias gerados por modelos diferentes."
            )
        self.benchmark = payload.get("benchmark")
        self.seeds_sha256 = dict(payload.get("seeds_sha256") or {})
        self.blocks = dict(payload.get("blocks") or {})
        self.seasonality_applies_to = payload.get("seasonality_applies_to")

        grupos = payload.get("groups")
        if not isinstance(grupos, list) or not grupos:
            raise DemandError("demand_profile.json: sem a lista 'groups' ou lista vazia")

        pesos: dict[str, float] = {}
        for position, row in enumerate(grupos):
            if not isinstance(row, dict):
                raise DemandError(f"demand_profile.json: grupo {position} nao e um objeto")
            key = row.get("demand_group")
            if not key:
                raise DemandError(f"demand_profile.json: grupo {position} sem demand_group")
            if key in pesos:
                raise DemandError(f"demand_profile.json: grupo repetido {key!r}")
            try:
                peso = float(row["line_weight"])
            except (KeyError, TypeError, ValueError) as exc:
                raise DemandError(
                    f"demand_profile.json: line_weight ausente ou nao numerico em {key!r}"
                ) from exc
            if peso < 0:
                raise DemandError(f"demand_profile.json: line_weight negativo em {key!r}")
            pesos[key] = peso

        total = sum(pesos.values())
        # Tolerancia larga porque os pesos viajam como texto decimal e sao relidos como
        # float; estreita o suficiente para pegar um perfil truncado, que e o defeito real.
        if abs(total - 1.0) > 1e-6:
            raise DemandError(
                f"demand_profile.json: os pesos somam {total!r}, e nao 1. Perfil truncado "
                f"produziria mix plausivel e nunca falharia sozinho."
            )

        self.weights = pesos
        # Tupla ORDENADA: e ela que fixa a ordem de qualquer CDF derivada daqui.
        self.groups = tuple(sorted(pesos))

        sazonal = payload.get("seasonality") or {}
        self.seasonality: dict[int, float] = {}
        for mes, fator in sazonal.items():
            try:
                self.seasonality[int(mes)] = float(fator)
            except (TypeError, ValueError) as exc:
                raise DemandError(
                    f"demand_profile.json: fator sazonal invalido em {mes!r}"
                ) from exc
        if self.seasonality and sorted(self.seasonality) != list(range(1, 13)):
            raise DemandError(
                f"demand_profile.json: perfil sazonal precisa dos 12 meses; veio "
                f"{sorted(self.seasonality)}"
            )

    # -- consultas ------------------------------------------------------------------

    def weight_of(self, group: str) -> float:
        if group not in self.weights:
            raise DemandError(
                f"grupo {group!r} nao tem peso no perfil {self.version!r}. Nao existe peso "
                f"padrao: um grupo sem peso e uma premissa de demanda nao declarada."
            )
        return self.weights[group]

    def seasonal_factor(self, month: int) -> float:
        """Fator sobre a TAXA DE PEDIDOS do mes, nunca sobre o mix.

        A distincao vem da evidencia disponivel: o informe do MAPA publica gasto mensal do
        total da alimentacao e NAO publica perfil mensal por categoria. Um fator global
        sobre o mix se normalizaria e nao faria nada; sobre a taxa de pedidos ele faz
        exatamente o que a evidencia sustenta — e nada mais.
        """
        if not self.seasonality:
            return 1.0
        try:
            return self.seasonality[int(month)]
        except KeyError as exc:
            raise DemandError(f"perfil sazonal sem o mes {month!r}") from exc

    def cumulative(self, available: tuple) -> tuple:
        """CDF sobre o subconjunto presente, renormalizada. Ordem fixa pela chave.

        `available` chega como tupla ordenada e sai na mesma ordem; nenhum `set` e iterado
        no caminho. Devolve os limites acumulados, alinhados a `available`.
        """
        if not available:
            raise DemandError("nenhum grupo disponivel para sorteio")
        pesos = [self.weight_of(g) for g in available]
        total = sum(pesos)
        if total <= 0:
            raise DemandError(
                f"os {len(available)} grupo(s) disponiveis somam peso zero: "
                f"{available[:5]}. Sortear entre eles seria escolher ao acaso sem modelo."
            )
        acumulado, corrente = [], 0.0
        for peso in pesos:
            corrente += peso / total
            acumulado.append(corrente)
        return tuple(acumulado)

    def pick(self, rng, available: tuple, cumulative: tuple) -> str:
        """Um grupo, sorteado pela CDF. `rng.random()` uma unica vez por chamada.

        Uma so extracao por linha mantem o consumo do gerador de aleatorios previsivel: a
        reprodutibilidade por seed depende de QUANTAS vezes o rng e chamado, nao apenas de
        com que seed ele comecou.
        """
        sorteio = rng.random()
        for posicao, limite in enumerate(cumulative):
            if sorteio <= limite:
                return available[posicao]
        return available[-1]


def load(payload: dict) -> DemandModel:
    return DemandModel(payload)
