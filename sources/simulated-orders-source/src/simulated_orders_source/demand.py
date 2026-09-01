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

A COORTE, e o que continua nao morando aqui
--------------------------------------------
A partir de `mapa_2025_v2` o perfil traz um vetor de pesos POR COORTE, e nao um so. A
coorte e (faixa etaria, comunidade autonoma), e este modulo sabe apenas:

  * quais sao as faixas e onde ficam os seus limites superiores;
  * qual comunidade corresponde a cada armazem;
  * que peso cada grupo tem dentro de cada coorte.

Continua NAO sabendo o que e um indice de afinidade, o que e IPF, nem por que a faixa de
65 anos compra mais azeite. Tudo isso e resolvido pela plataforma e chega como numero — a
mesma fronteira que ja valia para preco, cliente e calibracao agregada.

A CONSEQUENCIA E A MESMA DE ANTES: trocar a propensao nao exige tocar neste arquivo.
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

        self._load_cohorts(payload.get("cohorts"))

    # -- coortes ----------------------------------------------------------------------

    def _load_cohorts(self, cohorts) -> None:
        """Faixas, comunidades e um vetor de pesos por coorte.

        RECUSA UM PERFIL SEM A SECAO. Nao ha fallback para "usa o peso agregado": um perfil
        que promete propensao por coorte no cabecalho e nao a entrega seria um no-op
        silencioso, e o mix resultante continuaria plausivel. E a mesma regra que ja vale
        para um grupo sem peso.
        """
        if not isinstance(cohorts, dict):
            raise DemandError(
                "demand_profile.json sem a secao `cohorts`. Nao existe perfil sem coorte: "
                "um vetor unico carimbado com uma versao que promete propensao por cliente "
                "produziria mix plausivel e nunca falharia sozinho."
            )

        bandas = cohorts.get("age_bands")
        if not isinstance(bandas, list) or not bandas:
            raise DemandError("demand_profile.json: `cohorts.age_bands` ausente ou vazio")
        # Ordenadas pelo TETO, com o aberto por ultimo. A ordem do JSON nao e confiavel como
        # semantica: `band_of` percorre esta tupla e devolve a primeira que cabe.
        faixas = []
        for row in bandas:
            teto = row.get("max_age")
            faixas.append((row["key"], None if teto is None else int(teto)))
        abertas = [f for f in faixas if f[1] is None]
        if len(abertas) != 1:
            raise DemandError(
                f"demand_profile.json: as faixas precisam de exatamente uma aberta no topo "
                f"(max_age nulo); vieram {len(abertas)}"
            )
        self.age_bands = tuple(
            sorted((f for f in faixas if f[1] is not None), key=lambda f: f[1])
        ) + tuple(abertas)

        self.regions = {}
        self.warehouse_region = {}
        for row in cohorts.get("regions") or []:
            code = row["ccaa_code"]
            self.regions[code] = float(row["frequency_index"])
            for wh in row.get("warehouses") or []:
                if wh in self.warehouse_region:
                    raise DemandError(f"armazem {wh!r} em duas comunidades no perfil")
                self.warehouse_region[wh] = code
        if not self.regions:
            raise DemandError("demand_profile.json: `cohorts.regions` vazio")

        pesos: dict[str, dict] = {}
        for vetor in cohorts.get("weights") or []:
            chave = vetor.get("cohort")
            if not chave:
                raise DemandError("demand_profile.json: vetor de coorte sem `cohort`")
            if chave in pesos:
                raise DemandError(f"demand_profile.json: coorte repetida {chave!r}")
            linha = {}
            for item in vetor.get("groups") or []:
                linha[item["demand_group"]] = float(item["line_weight"])
            total = sum(linha.values())
            if abs(total - 1.0) > 1e-6:
                raise DemandError(
                    f"demand_profile.json: os pesos da coorte {chave!r} somam {total!r}, "
                    f"e nao 1"
                )
            pesos[chave] = linha
        if not pesos:
            raise DemandError("demand_profile.json: `cohorts.weights` vazio")
        self.cohort_weights = pesos
        # Tupla ORDENADA, pelo mesmo motivo de `groups`: e ela que fixa qualquer ordem
        # derivada, e iterar o dicionario faria a saida depender de PYTHONHASHSEED.
        self.cohorts = tuple(sorted(pesos))

    def band_of(self, age: int) -> str:
        """A faixa etaria de uma idade EM ANOS COMPLETOS no dia do pedido."""
        for nome, teto in self.age_bands:
            if teto is None or age <= teto:
                return nome
        raise DemandError(f"idade {age!r} nao cai em nenhuma faixa do perfil")

    def region_of(self, wh: str) -> str:
        if wh not in self.warehouse_region:
            raise DemandError(
                f"o armazem {wh!r} nao tem comunidade no perfil {self.version!r}. Sem "
                f"comunidade nao ha coorte, e nao existe regiao padrao."
            )
        return self.warehouse_region[wh]

    def cohort_of(self, wh: str, age: int) -> str:
        """A chave da coorte de um cliente. Mesmo formato que a plataforma escreveu."""
        chave = f"{self.band_of(age)}|{self.region_of(wh)}"
        if chave not in self.cohort_weights:
            raise DemandError(
                f"a coorte {chave!r} nao tem vetor de pesos no perfil {self.version!r}"
            )
        return chave

    def frequency_index(self, wh: str) -> float:
        """Fator sobre QUANTOS clientes daquele armazem pedem no dia, nunca sobre a cesta.

        O informe mede consumo per capita por comunidade e nao publica frequencia de compra
        domestica; a escolha de manifestar a intensidade como frequencia e premissa
        declarada da plataforma, e chega aqui ja renormalizada sobre as comunidades
        servidas — a soma ponderada e 1, entao o total de pedidos nao se move.
        """
        return self.regions[self.region_of(wh)]

    # -- consultas ------------------------------------------------------------------

    def weight_of(self, group: str, cohort: str | None = None) -> float:
        """Peso de um grupo. Sem coorte, o AGREGADO; com coorte, a condicional.

        O agregado continua acessivel de proposito: e contra ele que se verifica que a
        camada de coorte nao deslocou a calibracao da versao anterior.
        """
        tabela = self.weights if cohort is None else self.cohort_weights.get(cohort)
        if tabela is None:
            raise DemandError(
                f"coorte {cohort!r} nao existe no perfil {self.version!r}"
            )
        if group not in tabela:
            raise DemandError(
                f"grupo {group!r} nao tem peso no perfil {self.version!r}"
                + (f" para a coorte {cohort!r}" if cohort else "")
                + ". Nao existe peso padrao: um grupo sem peso e uma premissa de demanda "
                "nao declarada."
            )
        return tabela[group]

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

    def cumulative(self, available: tuple, cohort: str | None = None) -> tuple:
        """CDF sobre o subconjunto presente, renormalizada. Ordem fixa pela chave.

        `available` chega como tupla ordenada e sai na mesma ordem; nenhum `set` e iterado
        no caminho. Devolve os limites acumulados, alinhados a `available`.
        """
        if not available:
            raise DemandError("nenhum grupo disponivel para sorteio")
        pesos = [self.weight_of(g, cohort) for g in available]
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
