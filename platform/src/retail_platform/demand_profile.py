"""Constroi o perfil de demanda que a Source de Orders consome.

POR QUE ESTE MODULO EXISTE, E POR QUE ELE FICA NA PLATAFORMA
------------------------------------------------------------
Mesmo motivo de `orders_reference.py`: a Source e FROZEN (`dependencies = []`, verificado
por AST) e nao fala com o Lakehouse. A PLATAFORMA le quatro seeds versionados, resolve o
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

SEEDS = (BENCHMARK_SEED, MAPPING_SEED, PROFILE_SEED, SEASONALITY_SEED)

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


def build(catalog_rows: list[dict], seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """Perfil de demanda a partir do catalogo observado e dos quatro seeds.

    `catalog_rows` precisa trazer `demand_group` — JA RESOLVIDO — e `net_content_kg_l`, que
    pode ser nulo. A resolucao NAO acontece aqui de proposito: ela mora em um lugar so
    (`orders_reference._stamp_demand_group`), pelo mesmo motivo que o portao do Silver mora
    em `silver_gate.plan()`. Duas resolucoes divergem no primeiro ajuste, e a divergencia
    seria invisivel porque as duas produziriam mix plausivel.
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
    }
