"""Constroi o perfil de demanda que a Source de Orders consome.

POR QUE ESTE MODULO EXISTE, E POR QUE ELE FICA NA PLATAFORMA
------------------------------------------------------------
Mesmo motivo de `orders_reference.py`: a Source e FROZEN (`dependencies = []`, verificado
por AST) e nao fala com o Lakehouse. A PLATAFORMA le oito seeds versionados, resolve o
mapeamento contra o catalogo observado e escreve UM JSON plano; a Source o le com `json` da
stdlib e sorteia. Nenhum lado importa o codigo do outro, e nenhuma regra de calibracao mora
dentro do gerador.

O QUE ESTE MODULO CALIBRA, E O QUE ELE DELIBERADAMENTE NAO CALIBRA
-------------------------------------------------------------------
Calibra: a PROBABILIDADE DE UM GRUPO DE DEMANDA aparecer numa linha da cesta.
Nao calibra: quantos pedidos por dia, quantas linhas por cesta, quantas unidades por linha,
nem que preco tem o produto. O MAPA mede consumo domestico do residente — nao mede pedido
de loja online, nem cesta, nem cadencia de compra. Essas continuam premissas `synthetic` em
`order_premises_seed.csv`, e a fronteira e o ponto mais importante desta fase.

A CADEIA, COM PRECO FORA DO CAMINHO DA DEMANDA
-----------------------------------------------
    grupo de demanda   P(g)  <- alvo de VOLUME (kg/L) do MAPA, inclinado pelo canal
           |
    produto no grupo         <- UNIFORME (nenhuma fonte mede giro por SKU)
           |
    quantidade               <- w(k)=1/2^(k-1), inalterado
           |
    preco unitario           <- OBSERVADO (purchasable_unit_price da Mercadona)
           |
    valor do pedido          <- consequencia, nunca objetivo

Preco nao aparece em nenhuma seta que aponta para demanda. Uma categoria cara pode ter
volume baixo e receita alta, e isso passa a ser RESULTADO do modelo em vez de defeito: no
MAPA, mariscos sao 0,81% do volume e 2,88% do valor.

A PONTE ENTRE ALVO EM KG E SORTEIO DE LINHAS
---------------------------------------------
O alvo do MAPA e share de KG; o sorteio escolhe LINHAS. A ponte e o tamanho medio OBSERVADO
da embalagem, jamais o preco:

    P(grupo g)  proporcional a   alvo_volume_g / kg_medio_por_linha_g

Um grupo cujo produto tipico pesa 1 kg precisa de menos linhas para entregar o mesmo volume
que um grupo cujo produto tipico pesa 100 g. Sem essa divisao, "14% do volume em fruta"
viraria "14% das linhas em fruta", que e outra coisa.

QUANDO O ALVO DE VOLUME NAO PODE SER USADO
-------------------------------------------
`net_content_kg_l` e nulo quando `reference_format` nao e massa nem volume (`ud`, `dz`).
Um grupo cuja cobertura fica abaixo de `volume_coverage_min` NAO recebe alvo de volume: o
kg medio seria calculado sobre uma amostra pequena e enviesada do proprio grupo. Ele recebe
share de LINHAS igual ao share de volume do MAPA — fallback declarado, reportado a parte no
reality check, e e o que impede HUEVOS (vendido em 'ud') de entrar por um peso por ovo que
nenhuma fonte deste repo mede.

A CAMADA DE COORTE (mapa_2025_v2)
----------------------------------
Tudo acima descreve o AGREGADO, e continua valendo palavra por palavra. O que a v2
acrescenta e um andar acima dele: P(grupo) vira P(grupo | coorte do cliente).

A coorte tem duas dimensoes, e sao as duas UNICAS em que um atributo observado do cliente
coincide com um corte publicado do informe:

    age_band   <- birth_year do cliente, que veio da distribuicao etaria do INE 31304
    region     <- province_code do cliente, que veio do Callejero, mapeado para a CCAA

Ciclo de vida do lar (9 tipos) e nivel socioeconomico (5 niveis) sao os cortes mais ricos do
informe e ficaram de fora: o cliente nao tem composicao familiar nem renda, e atribui-las
seria inventar o atributo — a mesma proibicao que a Fase 1 aplicou a densidade por tramo.

O indice de afinidade sai do MESMO grafico para numerador e denominador, entao e
adimensional e nao depende de a nossa piramide etaria ter a forma da espanhola:

    idx(g, faixa)  =  volume_share(g, faixa) / populacao_share(faixa)
    peso_bruto(g, coorte)  =  w_g  x  idx_idade(g, faixa)  x  idx_regiao(g, ccaa)

A MULTIPLICACAO ASSUME INDEPENDENCIA entre idade e regiao. O informe publica as duas
marginais e nunca o cruzamento; e premissa declarada em `cohort_independence`, nao medicao.

POR QUE O INDICE NAO PODE SER LIDO COMO SHARE. O `% Poblacion` do MAPA e a populacao que
VIVE EM LARES cujo responsavel de compra esta naquela faixa — um lar com comprador de 40
anos carrega os filhos para dentro de `35_49`. O nosso cliente e um individuo. Por isso o
numero so entra como indice RELATIVO entre faixas, e nunca como share absoluto.

NEUTRALIDADE AGREGADA POR IPF
------------------------------
Sem correcao, o mix agregado sairia do alvo da v1 so porque a nossa distribuicao de coortes
nao e a do MAPA — a calibracao da fase anterior seria desfeita de lado, sem que nada
falhasse. `ipf_calibrate` ajusta um fator por grupo ate que a media dos pesos por coorte,
ponderada pela distribuicao real de coortes ENTRE OS PEDIDOS, reproduza `w_g` exatamente.

Isso da a esta camada um criterio limpo: O AGREGADO NAO SE MOVE. O que muda e a
condicional, e e la que se deve procurar o efeito.
"""

from __future__ import annotations

import csv
import hashlib
import os
from decimal import Decimal

DEFAULT_SEEDS_DIR = os.path.join("platform", "dbt", "seeds")

BENCHMARK_SEED = "mapa_2025_benchmark_seed.csv"
MAPPING_SEED = "demand_category_mapping_seed.csv"
PROFILE_SEED = "demand_profile_seed.csv"
SEASONALITY_SEED = "demand_seasonality_seed.csv"
COHORT_AGE_SEED = "demand_cohort_age_seed.csv"
COHORT_REGION_SEED = "demand_cohort_region_seed.csv"
REGION_SEED = "mapa_2025_region_seed.csv"
CCAA_MAP_SEED = "ine_ccaa_map_seed.csv"

SEEDS = (
    BENCHMARK_SEED,
    MAPPING_SEED,
    PROFILE_SEED,
    SEASONALITY_SEED,
    COHORT_AGE_SEED,
    COHORT_REGION_SEED,
    REGION_SEED,
    CCAA_MAP_SEED,
)

# As quatro faixas do informe, verbatim, com o LIMITE SUPERIOR de cada uma. O limite
# inferior da primeira nao mora aqui: quem pode pedir e `min_buyer_age`, premissa do
# dominio de pedidos, e misturar as duas coisas faria a definicao da faixa depender de uma
# regra de elegibilidade que nada tem a ver com consumo.
AGE_BANDS = (
    ("LT35", 34),
    ("35_49", 49),
    ("50_64", 64),
    ("GE65", None),
)

# Codigo da media nacional em `mapa_2025_region_seed`. NAO e uma regiao: e o denominador
# contra o qual as demais sao lidas, e precisa ser distinguido para nunca entrar como uma
# comunidade servivel.
NATIONAL_CODE = "00"

# Tolerancia dos dois checksums de leitura do informe. Larga o bastante para os rotulos
# de uma casa decimal (quatro faixas de 31,3 somam 100,1 sem que nada esteja errado),
# estreita o bastante para pegar um digito trocado, que e o defeito real.
COHORT_SUM_TOLERANCE = Decimal("0.5")

# Destinos que existem no mapeamento mas NAO sao categorias do MAPA. Sao rotulos de escopo,
# e o codigo precisa distingui-los de uma chave de benchmark para nao procurar share deles.
NO_FOOD = "NO_FOOD"
SIN_BENCHMARK = "SIN_BENCHMARK"
SCOPE_LABELS = (NO_FOOD, SIN_BENCHMARK)

WILDCARD = "*"

REQUIRED_PARAMS = (
    "demand_model_version",
    "food_line_share",
    "benchmark_volume_coverage_pct",
    "unbenchmarked_allocation",
    "within_group_selection",
    "volume_coverage_min",
    "channel_reference_pct",
    "channel_fresh_pct",
    "channel_rest_pct",
    "cohort_dimensions",
    "cohort_independence",
    "cohort_calibration",
    "ipf_tolerance",
    "ipf_max_iterations",
    "region_frequency_basis",
    "region_frequency_normalization",
)


class DemandProfileError(Exception):
    """Seed ausente, mapeamento incompleto ou peso incoerente."""


# --------------------------------------------------------------------------------
# 1. Leitura dos seeds
# --------------------------------------------------------------------------------

def _read_seed(seeds_dir: str, name: str) -> list[dict]:
    path = os.path.join(seeds_dir, name)
    if not os.path.exists(path):
        raise DemandProfileError(f"seed ausente: {path}")
    with open(path, encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise DemandProfileError(f"{path}: seed vazio")
    return rows


def seeds_sha256(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """sha256 de cada seed, para o manifesto.

    Sem isto, "qual perfil gerou este dia" seria respondido pela versao — que alguem pode
    esquecer de incrementar ao editar um peso. O hash nao esquece.
    """
    digests = {}
    for name in SEEDS:
        path = os.path.join(seeds_dir, name)
        if not os.path.exists(path):
            raise DemandProfileError(f"seed ausente: {path}")
        with open(path, "rb") as handle:
            digests[name] = hashlib.sha256(handle.read()).hexdigest()
    return digests


def _decimal_or_none(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except Exception as exc:  # noqa: BLE001 - o valor veio de um CSV editado a mao
        raise DemandProfileError(f"valor nao numerico no seed: {value!r}") from exc


def load_benchmark(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """mapa_key -> linha do benchmark, com os numeros ja tipados."""
    benchmark = {}
    for row in _read_seed(seeds_dir, BENCHMARK_SEED):
        key = row["mapa_key"].strip()
        if key in benchmark:
            raise DemandProfileError(f"mapa_key repetida no benchmark: {key}")
        benchmark[key] = {
            "mapa_key": key,
            "mapa_label": row["mapa_label"].strip(),
            "scope": row["scope"].strip(),
            "use_as_weight": row["use_as_weight"].strip().lower() == "true",
            "volume_share_pct": _decimal_or_none(row["volume_share_pct"]),
            "value_share_pct": _decimal_or_none(row["value_share_pct"]),
            "avg_price_eur_kg": _decimal_or_none(row["avg_price_eur_kg"]),
            "ecommerce_volume_pct": _decimal_or_none(row["ecommerce_volume_pct"]),
            "channel_basis": row["channel_basis"].strip(),
            "informe_section": row["informe_section"].strip(),
            "provenance": row["provenance"].strip(),
        }
    for key in SCOPE_LABELS:
        if key not in benchmark:
            raise DemandProfileError(
                f"o benchmark precisa declarar a linha de escopo {key}, mesmo sem numeros: "
                f"e nela que fica escrito por que aquele destino existe"
            )
    faltando = [
        row["mapa_key"] for row in benchmark.values()
        if row["use_as_weight"] and row["volume_share_pct"] is None
    ]
    if faltando:
        raise DemandProfileError(
            f"chaves marcadas use_as_weight sem volume_share_pct: {faltando}. "
            f"Um peso sem alvo viraria zero em silencio."
        )
    return benchmark


def load_mapping(seeds_dir: str = DEFAULT_SEEDS_DIR) -> list[dict]:
    return [
        {
            "l1": row["l1"].strip(),
            "l2": row["l2"].strip(),
            "l3": row["l3"].strip(),
            "mapa_key": row["mapa_key"].strip(),
        }
        for row in _read_seed(seeds_dir, MAPPING_SEED)
    ]


def load_params(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    params = {row["param_key"].strip(): row["value"].strip()
              for row in _read_seed(seeds_dir, PROFILE_SEED)}
    faltando = [k for k in REQUIRED_PARAMS if k not in params]
    if faltando:
        raise DemandProfileError(f"demand_profile_seed sem o(s) parametro(s) {faltando}")
    return params


def load_seasonality(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """month -> fator sobre a TAXA DE PEDIDOS, nao sobre o mix.

    A distincao importa e vem da evidencia: o informe publica gasto mensal do TOTAL da
    alimentacao (cinco meses, em prosa) e NAO publica perfil mensal por categoria — os
    graficos mensais sao imagens. Um fator global sobre o mix se normalizaria e nao faria
    nada; sobre a taxa de pedidos ele faz exatamente o que a evidencia sustenta.
    """
    fatores = {}
    for row in _read_seed(seeds_dir, SEASONALITY_SEED):
        mes = int(row["month"])
        if not 1 <= mes <= 12:
            raise DemandProfileError(f"mes fora de 1..12 no perfil sazonal: {mes}")
        if mes in fatores:
            raise DemandProfileError(f"mes repetido no perfil sazonal: {mes}")
        fator = _decimal_or_none(row["factor"])
        if fator is None or fator <= 0:
            raise DemandProfileError(f"fator sazonal ausente ou nao positivo no mes {mes}")
        fatores[mes] = fator
    if len(fatores) != 12:
        raise DemandProfileError(
            f"o perfil sazonal precisa dos 12 meses; vieram {sorted(fatores)}"
        )
    return fatores


def _load_cohort(seeds_dir: str, name: str, key_field: str) -> dict:
    """Le um seed de coorte e devolve mapa_key -> {chave da coorte: (populacao, volume)}.

    A CONFERENCIA MORA AQUI, e nao so no teste: um numero mal lido de um grafico do informe
    e a unica forma realista de erro nesta tabela, e ele quebra a soma. Deixar a leitura
    passar e conferir tres camadas adiante seria descobrir o defeito depois de ele ja ter
    calibrado a demanda.
    """
    saida: dict[str, dict] = {}
    for row in _read_seed(seeds_dir, name):
        chave = row["mapa_key"].strip()
        coorte = row[key_field].strip()
        populacao = _decimal_or_none(row["population_share_pct"])
        volume = _decimal_or_none(row["volume_share_pct"])
        if populacao is None or volume is None:
            raise DemandProfileError(
                f"{name}: {chave}/{coorte} sem share de populacao ou de volume. Uma celula "
                f"vazia viraria indice zero, que e uma afirmacao forte e nao declarada."
            )
        if populacao <= 0:
            raise DemandProfileError(
                f"{name}: {chave}/{coorte} tem populacao {populacao}; o indice dividiria por zero"
            )
        destino = saida.setdefault(chave, {})
        if coorte in destino:
            raise DemandProfileError(f"{name}: linha repetida em {chave}/{coorte}")
        destino[coorte] = (populacao, volume)
    return saida


def load_cohort_age(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """mapa_key -> faixa etaria -> (share de populacao, share de volume).

    Reprova se um grupo nao trouxer as QUATRO faixas, ou se os shares de volume nao
    fecharem 100. As quatro faixas sao uma particao do universo: se elas nao somam, ou
    falta uma linha ou um digito foi lido errado — e as duas coisas produziriam um indice
    plausivel que nunca falharia sozinho.
    """
    bandas = tuple(nome for nome, _ in AGE_BANDS)
    dados = _load_cohort(seeds_dir, COHORT_AGE_SEED, "age_band")
    for chave, linhas in sorted(dados.items()):
        faltando = [b for b in bandas if b not in linhas]
        if faltando:
            raise DemandProfileError(
                f"{COHORT_AGE_SEED}: {chave} sem a(s) faixa(s) {faltando}. As quatro faixas "
                f"sao uma particao; faltar uma nao e cobertura parcial, e um total errado."
            )
        for rotulo, indice in (("volume", 1), ("populacao", 0)):
            soma = sum(linhas[b][indice] for b in bandas)
            if abs(soma - Decimal("100")) > COHORT_SUM_TOLERANCE:
                raise DemandProfileError(
                    f"{COHORT_AGE_SEED}: {chave} soma {soma} de {rotulo} entre as quatro "
                    f"faixas, e nao 100. Leitura errada de um rotulo do informe."
                )
    return dados


def load_cohort_region(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """mapa_key -> ccaa_code -> (share de populacao, share de volume).

    SEM CHECKSUM DE SOMA, e a diferenca em relacao a idade e real: este seed traz so as
    comunidades servidas, que sao 4 das 17, entao nao ha particao para fechar. O que da
    para conferir e que o share de POPULACAO de cada comunidade e o mesmo em todo grupo —
    ele e o universo, nao uma medicao da categoria — e isso `assert_cohort_seeds` faz.
    """
    return _load_cohort(seeds_dir, COHORT_REGION_SEED, "ccaa_code")


def load_region_reference(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """ccaa_code -> {label, consumo per capita, gasto per capita}. Inclui a media nacional."""
    regioes = {}
    for row in _read_seed(seeds_dir, REGION_SEED):
        code = row["ccaa_code"].strip()
        if code in regioes:
            raise DemandProfileError(f"{REGION_SEED}: ccaa_code repetido {code}")
        per_capita = _decimal_or_none(row["per_capita_kg_l"])
        if per_capita is None or per_capita <= 0:
            raise DemandProfileError(
                f"{REGION_SEED}: {code} sem consumo per capita positivo"
            )
        regioes[code] = {
            "ccaa_code": code,
            "ccaa_label": row["ccaa_label"].strip(),
            "per_capita_kg_l": per_capita,
            "per_capita_eur": _decimal_or_none(row["per_capita_eur"]),
            "informe_section": row["informe_section"].strip(),
        }
    if NATIONAL_CODE not in regioes:
        raise DemandProfileError(
            f"{REGION_SEED} sem a linha {NATIONAL_CODE} (media nacional). Ela nao e uma "
            f"regiao, e o denominador contra o qual as demais sao lidas."
        )
    return regioes


def load_province_ccaa(seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """province_code -> ccaa_code. Sem destino padrao, pelo mesmo motivo do mapeamento."""
    mapa = {}
    for row in _read_seed(seeds_dir, CCAA_MAP_SEED):
        provincia = row["province_code"].strip()
        if provincia in mapa:
            raise DemandProfileError(f"{CCAA_MAP_SEED}: provincia repetida {provincia}")
        mapa[provincia] = row["ccaa_code"].strip()
    return mapa


def ccaa_of(province_ccaa: dict, province_code: str) -> str:
    """A comunidade de uma provincia, ou erro.

    SEM DEFAULT. Uma provincia nova que caisse numa comunidade padrao herdaria em silencio
    o perfil de consumo de outra regiao — e o mix resultante seria plausivel, que e
    exatamente o tipo de defeito que este repo trata como o mais caro.
    """
    codigo = (province_code or "").strip()
    if codigo not in province_ccaa:
        raise DemandProfileError(
            f"provincia {codigo!r} nao esta em {CCAA_MAP_SEED}. Acrescente a linha — nao "
            f"existe comunidade padrao."
        )
    return province_ccaa[codigo]


def age_band_of(age: int) -> str:
    """A faixa do informe para uma idade EM ANOS COMPLETOS no dia do pedido."""
    for nome, teto in AGE_BANDS:
        if teto is None or age <= teto:
            return nome
    raise DemandProfileError(f"idade {age!r} nao cai em nenhuma faixa")


# --------------------------------------------------------------------------------
# 2. Resolucao do mapeamento
# --------------------------------------------------------------------------------

def _specificity(rule: dict) -> int:
    """Quanto mais especifica a regra, maior o numero. l3 pesa mais que l2, que pesa mais que l1.

    Pesos 4/2/1 e nao 3/2/1: com potencias de dois nenhuma combinacao de niveis empata com
    outra, entao "mais especifica" nunca fica ambiguo por soma.
    """
    return ((1 if rule["l1"] != WILDCARD else 0)
            + (2 if rule["l2"] != WILDCARD else 0)
            + (4 if rule["l3"] != WILDCARD else 0))


def _matches(rule: dict, l1: str, l2: str, l3: str) -> bool:
    return ((rule["l1"] == WILDCARD or rule["l1"] == l1)
            and (rule["l2"] == WILDCARD or rule["l2"] == l2)
            and (rule["l3"] == WILDCARD or rule["l3"] == l3))


def resolve(mapping: list[dict], l1: str, l2: str, l3: str) -> str:
    """A chave do grupo de demanda para uma trinca do catalogo.

    SEM DEFAULT. Trinca sem regra levanta erro em vez de cair num balde silencioso: um
    default aqui seria uma premissa de demanda nao declarada, que e exatamente o que os
    seeds existem para impedir. E duas regras igualmente especificas tambem levantam —
    escolher a primeira faria o resultado depender da ordem do arquivo.
    """
    candidatas = [r for r in mapping if _matches(r, l1, l2, l3)]
    if not candidatas:
        raise DemandProfileError(
            f"nenhuma regra de mapeamento casa com ({l1!r}, {l2!r}, {l3!r}). "
            f"Acrescente a regra em {MAPPING_SEED} — nao existe destino padrao."
        )
    melhor = max(_specificity(r) for r in candidatas)
    vencedoras = [r for r in candidatas if _specificity(r) == melhor]
    if len(vencedoras) > 1:
        raise DemandProfileError(
            f"({l1!r}, {l2!r}, {l3!r}) casa com {len(vencedoras)} regras igualmente "
            f"especificas: {[r['mapa_key'] for r in vencedoras]}"
        )
    return vencedoras[0]["mapa_key"]


def unused_rules(mapping: list[dict], triples: list[tuple]) -> list[dict]:
    """Regras que nao vencem nenhuma trinca do catalogo.

    Regra morta nao quebra nada hoje, e por isso e perigosa: ela documenta uma decisao que
    nao esta em vigor. Quem ler o seed vai acreditar nela.
    """
    vencedoras = set()
    for l1, l2, l3 in triples:
        candidatas = [(i, r) for i, r in enumerate(mapping) if _matches(r, l1, l2, l3)]
        if not candidatas:
            continue
        melhor = max(_specificity(r) for _, r in candidatas)
        for i, r in candidatas:
            if _specificity(r) == melhor:
                vencedoras.add(i)
    return [r for i, r in enumerate(mapping) if i not in vencedoras]


# --------------------------------------------------------------------------------
# 3. Inclinacao de canal
# --------------------------------------------------------------------------------

def channel_tilt(row: dict, params: dict) -> tuple[Decimal, str]:
    """Fator que leva o share do CONSUMO DOMESTICO para o share do E-COMMERCE.

    O MAPA mede o consumo do residente, nao o pedido online. Tratar um pelo outro seria o
    erro que a fase inteira existe para evitar. O informe da tres granularidades de canal, e
    esta funcao usa a melhor disponivel para cada grupo, sempre declarando qual foi:

      fine   share de e-commerce publicado para este grupo         (18 blocos do informe)
      group  publicado para o grupo pai (as carnes herdam de CARNES)
      coarse so o corte fresca 1,1% / resto 2,8% sobre o total 2,2%

    Nao ha quarta opcao: inventar um percentual para quem nao tem linha de canal seria
    exatamente o que o §3 do plano proibiu.
    """
    referencia = Decimal(params["channel_reference_pct"])
    basis = row.get("channel_basis") or "coarse"
    ecom = row.get("ecommerce_volume_pct")
    if basis in ("fine", "group") and ecom is not None:
        return ecom / referencia, basis
    fresco = Decimal(params["channel_fresh_pct"])
    resto = Decimal(params["channel_rest_pct"])
    base = fresco if row.get("scope") == "fresh" else resto
    return base / referencia, "coarse"


# --------------------------------------------------------------------------------
# 3b. Propensao por coorte
# --------------------------------------------------------------------------------

ONE = Decimal("1")


def affinity_index(population_share: Decimal, volume_share: Decimal) -> Decimal:
    """Quanto uma coorte compra de um grupo, relativo ao que o seu tamanho faria esperar.

    Numerador e denominador saem do MESMO grafico do informe, entao o quociente e
    adimensional: ele nao depende de a nossa piramide etaria ter a forma da espanhola, e
    por isso sobrevive a diferenca de denominador descrita no cabecalho do modulo.

    1,0 significa "compra na proporcao do seu tamanho". Nao ha teto nem piso artificiais:
    HUEVOS tem 0,74 nos menores de 35 e AGUA tem 0,31 em Madrid, e as duas coisas sao
    medidas, nao ruido a ser aparado.
    """
    return volume_share / population_share


def region_frequency(
    region_reference: dict,
    served: list[str],
    weights: dict | None = None,
) -> dict:
    """ccaa_code -> indice de frequencia, renormalizado sobre as comunidades SERVIDAS.

    O informe publica consumo per capita por comunidade (secao 3). Cataluna consome 620,82
    kg ou litro por pessoa e ano contra 505,86 de Madrid — 22,7% a mais, medido.

    DUAS DECISOES, as duas declaradas em `demand_profile_seed`:

    1. A intensidade regional vira FREQUENCIA de pedido, e nao tamanho de cesta. O informe
       da kg por ano e nao publica frequencia de compra domestica ('actos de compra' so
       aparece no capitulo extradomestico), entao repartir a intensidade entre frequencia e
       cesta seria inventar a reparticao. Escolher uma e declarar e honesto; escolher e nao
       declarar nao seria.

    2. A renormalizacao e sobre as comunidades SERVIDAS, ponderada pelo tamanho da base de
       cada uma, e nao contra a media nacional de 577,32. A media nacional inclui regioes
       que nao atendemos: usa-la faria a contagem total de pedidos cair alguns por cento
       sem que isso significasse nada. Assim a soma ponderada dos indices e exatamente 1, o
       total de pedidos fica onde estava, e a fase muda so a REPARTICAO entre armazens.
    """
    if not served:
        raise DemandProfileError("nenhuma comunidade servida: nao ha o que renormalizar")
    faltando = sorted(c for c in set(served) if c not in region_reference)
    if faltando:
        raise DemandProfileError(
            f"{REGION_SEED} nao declara consumo per capita de {faltando}"
        )
    pesos = {c: Decimal(1) for c in set(served)} if not weights else {
        c: Decimal(weights.get(c, 0)) for c in set(served)
    }
    massa = sum(pesos.values(), Decimal("0"))
    if massa <= 0:
        raise DemandProfileError("as comunidades servidas somam peso zero")
    media = sum(
        pesos[c] * region_reference[c]["per_capita_kg_l"] for c in sorted(pesos)
    ) / massa
    if media <= 0:
        raise DemandProfileError("consumo per capita medio nao positivo")
    return {c: region_reference[c]["per_capita_kg_l"] / media for c in sorted(pesos)}


def ipf_calibrate(
    base_weights: dict,
    index: dict,
    cohort_mass: dict,
    tolerance: Decimal,
    max_iterations: int,
) -> tuple[dict, int, Decimal]:
    """Ajusta um fator por grupo ate que a media por coorte reproduza o peso agregado.

    O PROBLEMA QUE ISTO RESOLVE. Sem correcao,

        p(g | c)  =  w_g . i(g,c) / SOMA_h w_h . i(h,c)

    e a media `SOMA_c m_c . p(g|c)` NAO volta a ser `w_g`, porque o denominador varia de
    coorte para coorte. O desvio nao e ruido: ele desloca a calibracao agregada da fase
    anterior sem que nada falhe, e um mix deslocado continua parecendo plausivel.

    A CORRECAO. Um fator `f_g` por grupo, ajustado por iteracao proporcional:

        p(g | c)  =  w_g . f_g . i(g,c) / SOMA_h w_h . f_h . i(h,c)
        a_g       =  SOMA_c m_c . p(g|c)
        f_g       <- f_g . w_g / a_g

    Converge para uma matriz positiva e devolve `f`, o numero de iteracoes e o maior desvio
    que restou. Com a matriz de indices toda igual a 1,0 a primeira iteracao ja fecha e `f`
    fica em 1 — o IPF e a identidade quando nao ha nada a corrigir, e ha teste que prova.

    Ordem de iteracao SEMPRE por chave ordenada: a soma de Decimais nao e associativa no
    ultimo digito, e iterar um dicionario faria o resultado depender de PYTHONHASHSEED.
    """
    grupos = tuple(sorted(base_weights))
    coortes = tuple(sorted(cohort_mass))
    if not grupos:
        raise DemandProfileError("IPF sem grupo nenhum")
    if not coortes:
        raise DemandProfileError("IPF sem coorte nenhuma")

    fator = {g: ONE for g in grupos}
    pior = None
    for iteracao in range(1, max_iterations + 1):
        agregado = {g: Decimal("0") for g in grupos}
        for c in coortes:
            bruto = {g: base_weights[g] * fator[g] * index[c][g] for g in grupos}
            total = sum(bruto[g] for g in grupos)
            if total <= 0:
                raise DemandProfileError(
                    f"a coorte {c!r} soma peso zero sobre {len(grupos)} grupos"
                )
            massa = cohort_mass[c]
            for g in grupos:
                agregado[g] += massa * bruto[g] / total
        pior = max(abs(agregado[g] - base_weights[g]) for g in grupos)
        if pior <= tolerance:
            return fator, iteracao, pior
        for g in grupos:
            if agregado[g] <= 0:
                raise DemandProfileError(
                    f"o grupo {g!r} ficou com massa agregada zero durante o IPF"
                )
            fator[g] = fator[g] * base_weights[g] / agregado[g]

    raise DemandProfileError(
        f"o IPF nao convergiu em {max_iterations} iteracoes; maior desvio {pior}. "
        f"Aceitar a matriz assim desfaria a calibracao agregada da versao anterior."
    )


# --------------------------------------------------------------------------------
# 4. Construcao do perfil
# --------------------------------------------------------------------------------

def _mean(values: list[Decimal]) -> Decimal:
    return sum(values, Decimal("0")) / Decimal(len(values))


def _content(value) -> Decimal | None:
    """`net_content_kg_l` como Decimal, ou None quando nao ha conteudo utilizavel.

    Aceita None, string vazia e NaN. O NaN nao e paranoia: um DataFrame do pandas devolve
    NaN para nulo, e `Decimal('nan')` NAO levanta — ele constroi um NaN que sobrevive a
    aritmetica e envenena a media em silencio, que e pior do que o erro.
    """
    if value is None:
        return None
    texto = str(value).strip()
    if not texto or texto.lower() in ("nan", "none", "<na>"):
        return None
    try:
        numero = Decimal(texto)
    except Exception:  # noqa: BLE001 - valor vindo de CSV ou de um driver
        return None
    if not numero.is_finite() or numero <= 0:
        return None
    return numero


def cohort_key(age_band: str, ccaa_code: str) -> str:
    """A chave textual de uma coorte. Uma funcao so, usada dos dois lados da fronteira."""
    return f"{age_band}|{ccaa_code}"


def _build_cohorts(
    base_weights: dict,
    group_blocks: dict,
    customers_by_cohort: dict,
    warehouse_regions: dict,
    params: dict,
    seeds_dir: str,
) -> dict:
    """A camada de coorte sobre os pesos agregados, ja calibrada por IPF.

    `customers_by_cohort` conta os clientes ELEGIVEIS de cada (faixa, comunidade) — a
    elegibilidade e do dominio de pedidos (`min_buyer_age`) e chega resolvida. Contar aqui
    quem nao pode pedir enviesaria a massa das coortes e, por tabela, o IPF.
    """
    age = load_cohort_age(seeds_dir)
    region = load_cohort_region(seeds_dir)
    region_ref = load_region_reference(seeds_dir)

    if not warehouse_regions:
        raise DemandProfileError("nenhum armazem tem comunidade autonoma resolvida")
    servidas = sorted(set(warehouse_regions.values()))
    if NATIONAL_CODE in servidas:
        raise DemandProfileError(
            f"{NATIONAL_CODE} e a media nacional, nao uma comunidade servivel"
        )

    # --- 1. frequencia por comunidade, ponderada pela base elegivel de cada uma --------
    por_ccaa: dict[str, Decimal] = {}
    for (_banda, ccaa), quantos in customers_by_cohort.items():
        por_ccaa[ccaa] = por_ccaa.get(ccaa, Decimal("0")) + Decimal(quantos)
    orfas = sorted(c for c in por_ccaa if c not in servidas)
    if orfas:
        raise DemandProfileError(
            f"ha cliente em comunidade(s) que nenhum armazem serve: {orfas}"
        )
    frequencia = region_frequency(region_ref, servidas, weights=por_ccaa)

    # --- 2. massa de cada coorte ENTRE OS PEDIDOS, nao entre os clientes ---------------
    # A distincao e o motivo de a frequencia entrar aqui: uma coorte de Cataluna pesa mais
    # no mix agregado do que a mesma coorte em Madrid, porque coloca mais pedidos. Fazer o
    # IPF contra a distribuicao de CLIENTES fecharia a conta errada.
    bruta = {
        cohort_key(banda, ccaa): Decimal(quantos) * frequencia[ccaa]
        for (banda, ccaa), quantos in customers_by_cohort.items()
        if quantos > 0
    }
    total_massa = sum(bruta.values(), Decimal("0"))
    if total_massa <= 0:
        raise DemandProfileError("nenhum cliente elegivel em coorte nenhuma")
    massa = {c: bruta[c] / total_massa for c in sorted(bruta)}

    # --- 3. matriz de indices ----------------------------------------------------------
    grupos = tuple(sorted(base_weights))
    sem_dado = sorted(
        g for g in grupos
        if g not in SCOPE_LABELS and (g not in age or g not in region)
    )
    if sem_dado:
        raise DemandProfileError(
            f"grupo(s) com peso do MAPA mas sem corte demografico: {sem_dado}. Deixa-los "
            f"neutros em silencio esconderia uma lacuna de extracao atras de um mix "
            f"plausivel; acrescente as linhas em {COHORT_AGE_SEED} e {COHORT_REGION_SEED}."
        )

    indice: dict[str, dict] = {}
    for chave in sorted(massa):
        banda, ccaa = chave.split("|", 1)
        linha = {}
        for g in grupos:
            fator = ONE
            if g in age:
                if banda not in age[g]:
                    raise DemandProfileError(f"{COHORT_AGE_SEED}: {g} sem a faixa {banda}")
                linha_idade = age[g][banda]
                fator *= affinity_index(linha_idade[0], linha_idade[1])
            if g in region:
                if ccaa not in region[g]:
                    raise DemandProfileError(
                        f"{COHORT_REGION_SEED}: {g} sem a comunidade {ccaa}"
                    )
                linha_regiao = region[g][ccaa]
                fator *= affinity_index(linha_regiao[0], linha_regiao[1])
            linha[g] = fator
        indice[chave] = linha

    # --- 4. IPF DENTRO DE CADA BLOCO, e nunca atravessando a fronteira deles -----------
    #
    # A restricao nao e detalhe de implementacao, e foi encontrada medindo. Normalizando a
    # coorte inteira de uma vez, NO_FOOD e SIN_BENCHMARK — que tem indice 1,0 por ausencia
    # de evidencia — saiam com 0,60x da fatia na coorte de 65+ em relacao a de menos de 35.
    # Ou seja: o modelo passaria a AFIRMAR que quem tem mais de 65 anos compra 40% menos
    # drogaria por linha de cesta. Ninguem mediu isso. O efeito era puro residuo da
    # normalizacao, e era MAIOR que a maioria dos efeitos medidos.
    #
    # Fixar a fatia de cada bloco em toda coorte devolve a `food_line_share` o estatuto que
    # o seed lhe da — premissa declarada, uniforme — e confina a propensao a redistribuir
    # DENTRO do escopo onde existe evidencia. Indice neutro passa a significar de verdade
    # "sem efeito", em vez de "efeito que sobrou da conta".
    tolerancia = Decimal(params["ipf_tolerance"])
    max_iteracoes = int(params["ipf_max_iterations"])

    por_bloco: dict[str, list] = {}
    for g in grupos:
        por_bloco.setdefault(group_blocks[g], []).append(g)

    fator: dict[str, Decimal] = {}
    total_por_bloco: dict[str, Decimal] = {}
    iteracoes = 0
    desvio = Decimal("0")
    for bloco in sorted(por_bloco):
        do_bloco = sorted(por_bloco[bloco])
        total = sum(base_weights[g] for g in do_bloco)
        total_por_bloco[bloco] = total
        if total <= 0:
            for g in do_bloco:
                fator[g] = ONE
            continue
        base_bloco = {g: base_weights[g] / total for g in do_bloco}
        indice_bloco = {c: {g: indice[c][g] for g in do_bloco} for c in massa}
        parcial, its, err = ipf_calibrate(
            base_bloco, indice_bloco, massa, tolerancia, max_iteracoes
        )
        fator.update(parcial)
        iteracoes = max(iteracoes, its)
        desvio = max(desvio, err)

    pesos = []
    for chave in sorted(massa):
        linha = {}
        for bloco, do_bloco in sorted(por_bloco.items()):
            total = total_por_bloco[bloco]
            if total <= 0:
                for g in do_bloco:
                    linha[g] = Decimal("0")
                continue
            bruto = {g: base_weights[g] * fator[g] * indice[chave][g] for g in do_bloco}
            soma = sum(bruto[g] for g in do_bloco)
            for g in do_bloco:
                linha[g] = total * bruto[g] / soma
        pesos.append({
            "cohort": chave,
            "groups": [
                {"demand_group": g, "line_weight": str(linha[g])} for g in grupos
            ],
        })

    return {
        "dimensions": [d.strip() for d in params["cohort_dimensions"].split(",")],
        "independence": params["cohort_independence"],
        "calibration": params["cohort_calibration"],
        "independence_note": (
            "O informe publica as marginais de idade e de comunidade e NUNCA o cruzamento "
            "das duas. Multiplicar os dois indices assume independencia condicional: e "
            "premissa declarada, nao medicao."
        ),
        "index_note": (
            "O '% Poblacion' do MAPA e a populacao que VIVE EM LARES cujo responsavel de "
            "compra esta naquela faixa, e nao a populacao daquela idade. O nosso cliente e "
            "um individuo. Por isso o numero entra como indice RELATIVO entre faixas e "
            "nunca como share absoluto — e por isso o IPF existe."
        ),
        "age_bands": [
            {"key": nome, "max_age": teto} for nome, teto in AGE_BANDS
        ],
        "regions": [
            {
                "ccaa_code": c,
                "ccaa_label": region_ref[c]["ccaa_label"],
                "per_capita_kg_l": str(region_ref[c]["per_capita_kg_l"]),
                "warehouses": sorted(w for w, r in warehouse_regions.items() if r == c),
                "frequency_index": str(frequencia[c]),
            }
            for c in servidas
        ],
        "region_frequency_basis": params["region_frequency_basis"],
        "region_frequency_normalization": params["region_frequency_normalization"],
        "region_frequency_note": (
            "O indice pesa QUANTOS clientes pedem em cada armazem, nunca o tamanho da "
            "cesta: o informe da intensidade em kg por ano e nao publica frequencia de "
            "compra domestica. Renormalizado sobre as comunidades servidas, entao o total "
            "de pedidos nao se move e o que muda e a reparticao entre armazens."
        ),
        "neutral_groups": sorted(g for g in grupos if g in SCOPE_LABELS),
        "neutral_note": (
            "NO_FOOD e SIN_BENCHMARK ficam com indice 1,0 em toda coorte porque o informe "
            "nao os mede — drogaria, limpeza e mascotas estao fora do universo dele. Nao e "
            "a afirmacao de que todas as idades compram xampu igual; e a ausencia de "
            "qualquer medicao que sustente o contrario."
        ),
        "block_shares_are_constant": True,
        "block_shares_note": (
            "A fatia de cada bloco (calibrado, alimentar sem benchmark, nao alimentar) e a "
            "MESMA em toda coorte, e o IPF roda dentro de cada um. Sem essa fronteira, os "
            "grupos de indice neutro absorviam o residuo da normalizacao: NO_FOOD saia com "
            "0,60x da fatia em 65+ contra menos de 35, o que seria afirmar que idoso compra "
            "40% menos drogaria por linha — numero que nenhuma fonte deste repo mede, e "
            "maior que a maioria dos efeitos que sao medidos."
        ),
        "ipf_iterations": iteracoes,
        "ipf_max_abs_error": str(desvio),
        "ipf_note": (
            "Criterio de aceitacao desta camada: o mix AGREGADO nao se move. Os pesos por "
            "coorte, ponderados pela distribuicao real de coortes entre os pedidos, "
            "reproduzem os pesos da calibracao agregada. O efeito da fase esta na "
            "condicional, e e la que se deve procura-lo."
        ),
        "mass": [{"cohort": c, "share": str(massa[c])} for c in sorted(massa)],
        "weights": pesos,
    }


def build(
    catalog_rows: list[dict],
    seeds_dir: str = DEFAULT_SEEDS_DIR,
    *,
    customers_by_cohort: dict,
    warehouse_regions: dict,
) -> dict:
    """Perfil de demanda a partir do catalogo observado e dos oito seeds.

    `catalog_rows` precisa trazer `demand_group` — JA RESOLVIDO — e `net_content_kg_l`, que
    pode ser nulo. A resolucao NAO acontece aqui de proposito: ela mora em um lugar so
    (`orders_reference._stamp_demand_group`), pelo mesmo motivo que o portao do Silver mora
    em `silver_gate.plan()`. Duas resolucoes divergem no primeiro ajuste, e a divergencia
    seria invisivel porque as duas produziriam mix plausivel.

    `customers_by_cohort` mapeia (faixa etaria, ccaa) -> quantos clientes ELEGIVEIS, e
    `warehouse_regions` mapeia armazem -> ccaa. Os dois sao OBRIGATORIOS, e nao opcionais
    com fallback neutro: um perfil sem a camada de coorte carimbado com a versao v2 seria
    um no-op silencioso — o pior resultado possivel para uma fase cujo criterio de sucesso
    e justamente que o agregado nao se mova.
    """
    benchmark = load_benchmark(seeds_dir)
    params = load_params(seeds_dir)
    seasonality = load_seasonality(seeds_dir)

    if not catalog_rows:
        raise DemandProfileError("catalogo vazio: nao ha o que ponderar")

    # --- 1. agrupar o catalogo pelo grupo ja carimbado ----------------------------
    por_grupo: dict[str, dict] = {}
    for row in catalog_rows:
        key = row.get("demand_group")
        if not key:
            raise DemandProfileError(
                "linha de catalogo sem `demand_group`. A resolucao do mapeamento e "
                "responsabilidade de quem monta o catalogo, e nao acontece aqui."
            )
        alvo = por_grupo.setdefault(key, {"produtos": 0, "com_kg": [], "sem_kg": 0})
        alvo["produtos"] += 1
        conteudo = _content(row.get("net_content_kg_l"))
        if conteudo is None:
            alvo["sem_kg"] += 1
        else:
            alvo["com_kg"].append(conteudo)

    faltantes = [k for k in por_grupo if k not in benchmark]
    if faltantes:
        raise DemandProfileError(
            f"o mapeamento aponta para chave(s) que o benchmark nao declara: {faltantes}"
        )

    cobertura_min = Decimal(params["volume_coverage_min"])

    # --- 2. tres blocos: benchmark, alimentar sem benchmark, nao alimentar --------
    chaves_bench = sorted(k for k in por_grupo
                          if k not in SCOPE_LABELS and benchmark[k]["use_as_weight"])
    chaves_sem = sorted(k for k in por_grupo
                        if k == SIN_BENCHMARK or (k not in SCOPE_LABELS
                                                  and not benchmark[k]["use_as_weight"]))
    chaves_nofood = sorted(k for k in por_grupo if k == NO_FOOD)

    if not chaves_bench:
        raise DemandProfileError(
            "nenhum grupo do catalogo caiu numa chave pesavel do MAPA. O mapeamento ou o "
            "benchmark estao desalinhados, e sem isto nao ha calibracao nenhuma."
        )

    # --- 3. alvo de volume, inclinado pelo canal ---------------------------------
    grupos: dict[str, dict] = {}
    bruto_bench: dict[str, Decimal] = {}
    for key in chaves_bench:
        linha = benchmark[key]
        info = por_grupo[key]
        tilt, basis = channel_tilt(linha, params)
        alvo_volume = linha["volume_share_pct"] * tilt

        cobertos = len(info["com_kg"])
        cobertura = Decimal(cobertos) / Decimal(info["produtos"])
        if cobertura >= cobertura_min:
            kg_medio = _mean(info["com_kg"])
            peso = alvo_volume / kg_medio
            caminho = "volume"
        else:
            # Fallback DECLARADO: share de linhas igual ao share de volume. Nao e
            # equivalente, e o reality check reporta estes grupos num bloco separado.
            kg_medio = None
            peso = alvo_volume
            caminho = "lines"

        bruto_bench[key] = peso
        grupos[key] = {
            "demand_group": key,
            "block": "benchmark",
            "scope": linha["scope"],
            "mapa_volume_share_pct": str(linha["volume_share_pct"]),
            "mapa_value_share_pct": str(linha["value_share_pct"]) if linha["value_share_pct"] is not None else None,
            "mapa_avg_price_eur_kg": str(linha["avg_price_eur_kg"]) if linha["avg_price_eur_kg"] is not None else None,
            "channel_tilt": str(round(tilt, 6)),
            "channel_basis": basis,
            "target_path": caminho,
            "kg_coverage": str(round(cobertura, 4)),
            "mean_kg_per_unit": str(round(kg_medio, 6)) if kg_medio is not None else None,
            "assortment_products": info["produtos"],
            "informe_section": linha["informe_section"],
        }

    # --- 4. os tres blocos somam 1, e cada divisao tem origem escrita -------------
    #   alimentar          <- premissa declarada `food_line_share`
    #     com benchmark    <- cobertura do PROPRIO benchmark (86,12%), nao premissa nova
    #     sem benchmark    <- o complemento, dividido por tamanho de sortimento
    #   nao alimentar      <- o resto, dividido por tamanho de sortimento
    food_share = Decimal(params["food_line_share"])
    if not Decimal("0") < food_share <= Decimal("1"):
        raise DemandProfileError(f"food_line_share fora de (0, 1]: {food_share}")
    cobertura_bench = Decimal(params["benchmark_volume_coverage_pct"]) / Decimal("100")
    if not Decimal("0") < cobertura_bench <= Decimal("1"):
        raise DemandProfileError(
            f"benchmark_volume_coverage_pct fora de (0, 100]: {params['benchmark_volume_coverage_pct']}"
        )

    bloco_bench = food_share * cobertura_bench
    bloco_sem = food_share * (Decimal("1") - cobertura_bench)
    bloco_nofood = Decimal("1") - food_share

    # UM BLOCO SEM NENHUM GRUPO NO CATALOGO. Acontece de verdade com catalogo pequeno — uma
    # fixture de teste, ou uma janela em que a fonte so trouxe alimento. O peso daquele
    # bloco nao tem a quem ir, e deixa-lo cair faria os pesos somarem menos que 1.
    #
    # Redistribuir PROPORCIONALMENTE aos blocos que existem, e REGISTRAR que aconteceu. Sem
    # o registro seria uma correcao silenciosa: `demand_profile.json` mostraria pesos que
    # nao correspondem aos parametros declarados, e a diferenca ficaria sem explicacao.
    presentes = {
        "benchmark_block": (bloco_bench, bool(chaves_bench)),
        "sin_benchmark_block": (bloco_sem, bool(chaves_sem)),
        "no_food_block": (bloco_nofood, bool(chaves_nofood)),
    }
    vazios = [nome for nome, (_peso, tem) in presentes.items() if not tem]
    if vazios:
        vivo = sum(peso for peso, tem in presentes.values() if tem)
        if vivo <= 0:
            raise DemandProfileError("nenhum bloco tem grupo no catalogo")
        escala = Decimal("1") / vivo
        bloco_bench = bloco_bench * escala if chaves_bench else Decimal("0")
        bloco_sem = bloco_sem * escala if chaves_sem else Decimal("0")
        bloco_nofood = bloco_nofood * escala if chaves_nofood else Decimal("0")

    total_sem = total_nofood = Decimal("0")
    if chaves_sem:
        total_sem = sum(Decimal(por_grupo[k]["produtos"]) for k in chaves_sem)
    if chaves_nofood:
        total_nofood = sum(Decimal(por_grupo[k]["produtos"]) for k in chaves_nofood)

    total_bruto = sum(bruto_bench.values(), Decimal("0"))
    for key in chaves_bench:
        grupos[key]["line_weight"] = str(bloco_bench * bruto_bench[key] / total_bruto)

    for key in chaves_sem:
        info = por_grupo[key]
        grupos[key] = {
            "demand_group": key,
            "block": "sin_benchmark",
            "scope": benchmark[key]["scope"],
            "target_path": "assortment",
            "assortment_products": info["produtos"],
            "line_weight": str(bloco_sem * Decimal(info["produtos"]) / total_sem),
            "note": "alimentar sem alvo de volume no informe; share proporcional ao sortimento",
        }

    for key in chaves_nofood:
        info = por_grupo[key]
        grupos[key] = {
            "demand_group": key,
            "block": "no_food",
            "scope": "none",
            "target_path": "assortment",
            "assortment_products": info["produtos"],
            "line_weight": str(bloco_nofood * Decimal(info["produtos"]) / total_nofood),
            "note": "fora do universo do MAPA; share proporcional ao sortimento",
        }

    soma = sum(Decimal(g["line_weight"]) for g in grupos.values())
    if abs(soma - Decimal("1")) > Decimal("0.000001"):
        raise DemandProfileError(f"os pesos somam {soma}, e nao 1")

    return {
        "demand_model_version": params["demand_model_version"],
        "benchmark": "MAPA — Informe del Consumo Alimentario en España 2025",
        "benchmark_note": (
            "A folha de rosto do PDF diz 'Informe del consumo alimentario en España 2024'; o "
            "corpo inteiro reporta o ano 2025 ('A cierre del año 2025', 'frente a los 26.823,4 "
            "millones del año 2024'). E residuo da edicao anterior na pagina de creditos. Os "
            "numeros deste perfil vem do CORPO."
        ),
        "seeds_sha256": seeds_sha256(seeds_dir),
        "params": params,
        "blocks": {
            "food_line_share": str(food_share),
            "benchmark_block": str(bloco_bench),
            "sin_benchmark_block": str(bloco_sem),
            "no_food_block": str(bloco_nofood),
        },
        # Vazio no caso normal. Preenchido quando um bloco nao tinha nenhum grupo no
        # catalogo e o peso dele foi redistribuido — a redistribuicao fica visivel aqui, e
        # nao apenas na diferenca entre os parametros declarados e os pesos resultantes.
        "blocks_redistributed": vazios,
        "seasonality_applies_to": "daily_order_rate",
        "seasonality": {str(m): str(f) for m, f in sorted(seasonality.items())},
        "seasonality_note": (
            "Neutro por ausencia de evidencia, nao por esquecimento: os graficos mensais do "
            "informe sao imagens e nao ha perfil mensal por categoria. Aplica-se a taxa de "
            "pedidos, nunca ao mix — um fator global sobre o mix se normalizaria e nao faria "
            "nada. A janela atual cobre so agosto."
        ),
        "groups": [grupos[k] for k in sorted(grupos)],
        "cohorts": _build_cohorts(
            {k: Decimal(g["line_weight"]) for k, g in grupos.items()},
            {k: g["block"] for k, g in grupos.items()},
            customers_by_cohort,
            warehouse_regions,
            params,
            seeds_dir,
        ),
    }
