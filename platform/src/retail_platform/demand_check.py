"""Reality check da demanda: ANTES · MAPA · DEPOIS, nas tres dimensoes.

POR QUE TRES DIMENSOES, E NAO SO RECEITA
-----------------------------------------
Receita sozinha e o pior indicador de realismo que existe para este problema, porque ela
mistura duas coisas que precisam ser julgadas separadamente: QUANTO se compra e QUANTO CUSTA
o que se compra. O caso que abriu esta fase e exatamente isso — mariscos apareciam com 22,8%
da receita e 3,4% das unidades, e a leitura correta ("o preco de 10 linhas esta errado por
tres ordens de grandeza") era invisivel na coluna de receita.

    unidades      quantas linhas-unidade de cada grupo saem
    kg / litro    quanto peso ou volume — a dimensao que o MAPA publica
    receita       o dinheiro, que e CONSEQUENCIA das duas acima e do preco observado

E o preco medio por kg ao lado, que e a unica coluna diretamente comparavel com o informe
sem nenhuma conversao de share.

O CONTEUDO E DERIVADO DO PRECO DA LINHA, NAO DO CATALOGO ATUAL
---------------------------------------------------------------
`kg = quantidade * (unit_price_da_linha / reference_price)`.

Parece um detalhe e nao e. Se o conteudo viesse da coluna `net_content_kg_l` do catalogo de
hoje, o ANTES seria retroativamente corrigido: as linhas de granel que o simulador cobrou a
1.084,05 apareceriam com 0,15 kg em vez dos 99 kg que aquele preco implica. O ANTES tem de
descrever o mundo como ele era, inclusive nos seus defeitos — senao a comparacao esconde
justamente o que ela existe para mostrar.

A MESMA FUNCAO MEDE OS DOIS LADOS. Se ANTES e DEPOIS fossem medidos por consultas
diferentes, parte da diferenca seria da consulta.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal

from . import demand_profile

DEFAULT_OUT = os.path.join("docs", "demand-evidence", "README.md")
DEFAULT_SNAPSHOT_DIR = os.path.join("docs", "demand-evidence")

# Consulta unica dos dois lados. O join carrega (wh, price_as_of, produto, categoria,
# subgrupo) — a mesma chave composta que o gerador usou para escolher a linha, e nao apenas
# o id do produto: um produto vive em mais de uma categoria e o join por id sozinho
# multiplicaria linhas.
MIX_SQL = """
with catalogo as (
    select
        warehouse, ingestion_date, source_product_id, category_id, subgroup_id,
        product_level1_category_name as l1,
        category_name                as l2,
        subgroup_name                as l3,
        reference_price,
        reference_format
    from silver_product_price
),
linhas as (
    select
        c.l1, c.l2, c.l3,
        l.quantity,
        l.line_amount,
        -- CONTEUDO IMPLICADO PELO PRECO DA LINHA. Nulo quando reference_format nao e massa
        -- nem volume: converter ovos para kg exigiria um peso por ovo que nenhuma fonte
        -- deste repo mede, e um numero inventado aqui contaminaria o denominador de EUR/kg.
        case
            when c.reference_format in ('kg', 'L') and c.reference_price > 0
                then l.quantity * (l.unit_price / c.reference_price)
            when c.reference_format in ('100 g', '100 ml') and c.reference_price > 0
                then l.quantity * (l.unit_price / c.reference_price) * 0.1
        end as kg_l
    from silver_order_line l
    join catalogo c
      on  c.warehouse        = l.wh
      and c.ingestion_date   = l.price_as_of
      and c.source_product_id = l.source_product_id
      and c.category_id      = l.category_id
      and c.subgroup_id      = l.subgroup_id
    where l.line_status <> 'removed'
)
select
    l1, l2, l3,
    count(*)                                     as linhas,
    sum(quantity)                                as unidades,
    sum(line_amount)                             as receita,
    sum(kg_l)                                    as kg_l,
    count(*) filter (where kg_l is null)         as linhas_sem_kg
from linhas
group by 1, 2, 3
order by 1, 2, 3
"""


# O MIX POR COORTE, que e a unica dimensao em que a Fase 5 se enxerga. O agregado nao se
# move de proposito — criterio de aceitacao do IPF — entao um relatorio que mostrasse so
# totais concluiria que a fase nao fez nada.
#
# `demand_group` vem carimbado na LINHA e `buyer_age_band` no PEDIDO, os dois de dentro do
# proprio evento. Nenhum join com o catalogo nem com o cadastro: o que se quer e o que valia
# no momento do pedido, e e exatamente isso que os dois carimbos guardam.
COHORT_SQL = """
select
    o.buyer_age_band,
    o.wh,
    l.demand_group,
    count(*)          as linhas,
    sum(l.quantity)   as unidades,
    sum(l.line_amount) as receita
from silver_order_line l
join silver_order o on o.order_id = l.order_id
where l.line_status <> 'removed'
group by 1, 2, 3
order by 1, 2, 3
"""

# Quantos pedidos cada armazem colocou. E aqui que a inclinacao regional de FREQUENCIA se
# enxerga: antes desta fase os quatro armazens tinham a mesma contagem por construcao.
WAREHOUSE_SQL = """
select wh, count(*) as pedidos, count(distinct customer_id) as clientes
from silver_order
group by 1
order by 1
"""


class DemandCheckError(Exception):
    """Nao ha o que medir, ou o snapshot pedido nao existe."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def measure(connection, seeds_dir: str = demand_profile.DEFAULT_SEEDS_DIR) -> dict:
    """Mix observado nas tres dimensoes, agregado por grupo de demanda."""
    resultado = connection.execute(MIX_SQL)
    colunas = [d[0] for d in resultado.description]
    trincas = [dict(zip(colunas, linha)) for linha in resultado.fetchall()]
    if not trincas:
        raise DemandCheckError(
            "nenhuma linha de pedido para medir. `silver_order_line` foi construido?"
        )

    mapping = demand_profile.load_mapping(seeds_dir)
    grupos: dict[str, dict] = {}
    for row in trincas:
        key = demand_profile.resolve(mapping, row["l1"], row["l2"], row["l3"])
        alvo = grupos.setdefault(
            key,
            {"linhas": 0, "unidades": Decimal("0"), "receita": Decimal("0"),
             "kg_l": Decimal("0"), "linhas_sem_kg": 0},
        )
        alvo["linhas"] += int(row["linhas"])
        alvo["unidades"] += _decimal(row["unidades"])
        alvo["receita"] += _decimal(row["receita"])
        alvo["kg_l"] += _decimal(row["kg_l"])
        alvo["linhas_sem_kg"] += int(row["linhas_sem_kg"])

    return {
        "measured_at_utc": _utc_now(),
        "groups": {
            key: {
                "linhas": valor["linhas"],
                "unidades": str(valor["unidades"]),
                "receita": str(valor["receita"]),
                "kg_l": str(valor["kg_l"]),
                "linhas_sem_kg": valor["linhas_sem_kg"],
            }
            for key, valor in sorted(grupos.items())
        },
        **_measure_cohorts(connection),
    }


def _measure_cohorts(connection) -> dict:
    """Mix por coorte e contagem por armazem, quando a janela ja os carrega.

    TOLERANTE POR UM MOTIVO ESPECIFICO, e nao por generosidade: o ANTES desta fase e uma
    janela gerada por `mapa_2025_v1`, cujo `silver_order` nao tem a coluna `buyer_age_band`.
    Ele precisa ser congelavel — sem ANTES nao ha como PROVAR que o agregado nao se moveu, e
    essa prova e o criterio de aceitacao da fase. Um erro aqui impediria justamente a
    medicao que justifica a mudanca.

    A ausencia e REGISTRADA (`cohorts: None`) em vez de virar um dicionario vazio: vazio
    seria indistinguivel de "medi e nao havia nada".
    """
    try:
        resultado = connection.execute(COHORT_SQL)
    except Exception:  # noqa: BLE001 - coluna ausente na janela anterior a esta fase
        return {"cohorts": None, "warehouses": None}

    colunas = [d[0] for d in resultado.description]
    linhas = [dict(zip(colunas, linha)) for linha in resultado.fetchall()]
    coortes: dict[str, dict] = {}
    for row in linhas:
        banda = row["buyer_age_band"]
        if banda is None:
            continue
        alvo = coortes.setdefault(banda, {})
        grupo = alvo.setdefault(
            row["demand_group"],
            {"linhas": 0, "unidades": Decimal("0"), "receita": Decimal("0")},
        )
        grupo["linhas"] += int(row["linhas"])
        grupo["unidades"] += _decimal(row["unidades"])
        grupo["receita"] += _decimal(row["receita"])

    if not coortes:
        return {"cohorts": None, "warehouses": None}

    resultado = connection.execute(WAREHOUSE_SQL)
    colunas = [d[0] for d in resultado.description]
    armazens = {
        row["wh"]: {"pedidos": int(row["pedidos"]), "clientes": int(row["clientes"])}
        for row in (dict(zip(colunas, linha)) for linha in resultado.fetchall())
    }

    return {
        "cohorts": {
            banda: {
                grupo: {
                    "linhas": valor["linhas"],
                    "unidades": str(valor["unidades"]),
                    "receita": str(valor["receita"]),
                }
                for grupo, valor in sorted(grupos.items())
            }
            for banda, grupos in sorted(coortes.items())
        },
        "warehouses": armazens,
    }


def _shares(snapshot: dict) -> dict:
    """Shares percentuais por grupo, nas tres dimensoes, mais o EUR/kg."""
    grupos = snapshot["groups"]
    total_un = sum(_decimal(g["unidades"]) for g in grupos.values())
    total_kg = sum(_decimal(g["kg_l"]) for g in grupos.values())
    total_eur = sum(_decimal(g["receita"]) for g in grupos.values())
    saida = {}
    for key, g in grupos.items():
        un, kg, eur = _decimal(g["unidades"]), _decimal(g["kg_l"]), _decimal(g["receita"])
        saida[key] = {
            "unidades": un,
            "kg_l": kg,
            "receita": eur,
            "pct_unidades": (un / total_un * 100) if total_un else None,
            "pct_kg": (kg / total_kg * 100) if total_kg else None,
            "pct_receita": (eur / total_eur * 100) if total_eur else None,
            "eur_por_kg": (eur / kg) if kg else None,
            "linhas_sem_kg": g["linhas_sem_kg"],
        }
    saida["__total__"] = {
        "unidades": total_un, "kg_l": total_kg, "receita": total_eur,
        "eur_por_kg": (total_eur / total_kg) if total_kg else None,
    }
    return saida


def save_snapshot(snapshot: dict, path: str) -> str:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")
    return path


def load_snapshot(path: str) -> dict:
    if not os.path.exists(path):
        raise DemandCheckError(
            f"snapshot ausente: {path}. Congele o ANTES com "
            f"`make demand-reality-check SNAPSHOT=before` antes de regerar a janela — "
            f"depois de regerar, o ANTES nao existe mais em lugar nenhum."
        )
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _fmt(value, casas: int = 2) -> str:
    if value is None:
        return "—"
    return f"{Decimal(value):,.{casas}f}".replace(",", " ").replace(".", ",")


def _targets(benchmark: dict, params: dict) -> dict:
    """Alvo de volume por grupo: share do MAPA inclinado pelo canal, renormalizado.

    ESTA E A COLUNA CONTRA A QUAL O MODELO DEVE SER JULGADO, e nao o share bruto do MAPA.
    O informe mede consumo domestico; o simulador representa e-commerce, e o e-commerce
    NAO consome na mesma proporcao — 1,1% do volume fresco contra 2,8% do resto, sobre uma
    media de 2,2%. Comparar o simulador com o share bruto acusaria como erro justamente a
    correcao de canal que a fase inteira existe para aplicar: fruta fresca DEVE aparecer
    abaixo dos 14,13% domesticos numa cesta online.
    """
    pesaveis = [k for k, v in benchmark.items() if v["use_as_weight"]]
    bruto = {}
    for key in pesaveis:
        tilt, _base = demand_profile.channel_tilt(benchmark[key], params)
        bruto[key] = benchmark[key]["volume_share_pct"] * tilt
    total = sum(bruto.values())
    return {k: (v / total * 100 if total else None) for k, v in bruto.items()}


def _block_shares(shares: dict, chaves: list) -> dict:
    """Renormaliza os shares dentro de um subconjunto de grupos.

    Sem isto, DEPOIS estaria sobre o total (incluindo nao-alimentar) e o alvo sobre o bloco
    calibrado: duas bases diferentes na mesma tabela, que e como se compara errado sem
    perceber.
    """
    total = sum(Decimal(shares[k]["kg_l"]) for k in chaves if k in shares)
    return {
        k: (Decimal(shares[k]["kg_l"]) / total * 100 if total else None)
        for k in chaves if k in shares
    }


def render(
    depois: dict,
    antes: dict | None,
    seeds_dir: str = demand_profile.DEFAULT_SEEDS_DIR,
    antes_nome: str | None = None,
) -> str:
    """Markdown do reality check. Nenhum numero escrito a mao."""
    benchmark = demand_profile.load_benchmark(seeds_dir)
    params = demand_profile.load_params(seeds_dir)
    sd = _shares(depois)
    sa = _shares(antes) if antes else None
    alvos = _targets(benchmark, params)
    pesaveis = sorted(alvos, key=lambda k: -(alvos[k] or 0))

    dep_bloco = _block_shares(sd, pesaveis)
    ant_bloco = _block_shares(sa, pesaveis) if sa else {}

    linhas: list[str] = []
    linhas.append("# Reality check da demanda sintetica")
    linhas.append("")
    linhas.append(
        "GERADO por `make demand-reality-check`. Nenhum numero desta pagina foi escrito a "
        "mao; refaca-a em vez de edita-la."
    )
    linhas.append("")
    linhas.append(f"- modelo de demanda: **{params['demand_model_version']}**")
    linhas.append(f"- benchmark: MAPA, Informe del Consumo Alimentario en Espana 2025")
    linhas.append(f"- medido em: {depois['measured_at_utc']}")
    if antes:
        rotulo = f" (`{antes_nome}`)" if antes_nome else ""
        linhas.append(
            f"- ANTES congelado em: {antes['measured_at_utc']}{rotulo} — o estado "
            f"IMEDIATAMENTE anterior a esta versao do modelo, e nao o mais antigo que "
            f"existe. `before_mapa_2025_v1` guarda o mix uniforme de antes da calibracao "
            f"agregada e continua no disco; misturar os dois numa coluna so faria os "
            f"efeitos de duas fases serem lidos como um."
        )
    else:
        linhas.append(
            "- ANTES: **ausente**. Sem ele esta pagina mostra so o estado atual contra o "
            "MAPA, e a coluna de efeito do modelo nao existe."
        )
    linhas.append("")
    linhas.append("## Como ler as colunas")
    linhas.append("")
    linhas.append(
        "**MAPA %** e o consumo domestico bruto. **ALVO %** e o mesmo numero inclinado pela "
        "participacao do e-commerce e renormalizado — e contra ele que o modelo deve ser "
        "julgado. Os dois diferem de proposito: o informe mede o que o residente consome, "
        "e a cesta online nao tem a mesma composicao (1,1% do volume fresco chega por "
        "e-commerce contra 2,8% do resto). Fruta fresca DEVE aparecer abaixo dos 14,13% "
        "domesticos numa loja online; se aparecesse em 14,13% o modelo estaria confundindo "
        "consumo total com canal."
    )
    linhas.append("")
    linhas.append(
        "Receita sozinha nao serve como indicador de realismo: ela mistura QUANTO se compra "
        "com QUANTO CUSTA. Foi assim que 10 linhas de catalogo com preco de teto de API "
        "responderam por 16,6% de toda a receita simulada sem que nenhum total fechasse "
        "errado."
    )
    linhas.append("")

    # --- totais ------------------------------------------------------------------
    td, ta = sd["__total__"], (sa["__total__"] if sa else None)
    linhas.append("## Totais")
    linhas.append("")
    linhas.append("| dimensao | ANTES | DEPOIS | variacao |")
    linhas.append("|---|---:|---:|---:|")
    for rotulo, chave, casas in (
        ("unidades", "unidades", 0), ("kg ou litro", "kg_l", 1),
        ("receita (EUR)", "receita", 2), ("EUR por kg", "eur_por_kg", 2),
    ):
        atual = td[chave]
        anterior = ta[chave] if ta else None
        if anterior and atual is not None and anterior != 0:
            var = f"{(Decimal(atual) / Decimal(anterior) - 1) * 100:+.1f} %".replace(".", ",")
        else:
            var = "—"
        linhas.append(f"| {rotulo} | {_fmt(anterior, casas)} | {_fmt(atual, casas)} | {var} |")
    linhas.append("")
    linhas.append(
        "O EUR/kg do MAPA para o total da alimentacao domestica em 2025 e **3,25**. O nosso "
        "cobre tambem o terco nao alimentar do catalogo, que o informe nao mede, entao os "
        "dois nao sao diretamente comparaveis — a comparacao util e grupo a grupo, mais "
        "abaixo."
    )
    linhas.append("")

    # --- blocos -------------------------------------------------------------------
    linhas.append("## Os tres blocos")
    linhas.append("")
    linhas.append(
        "O share alimentar e premissa declarada; a divisao entre calibrado e nao calibrado "
        "sai da cobertura do proprio benchmark, e nao de um numero novo."
    )
    linhas.append("")
    linhas.append("| bloco | peso declarado | % do volume observado | origem do peso |")
    linhas.append("|---|---:|---:|---|")
    total_kg = td["kg_l"]
    blocos = (
        ("calibrado pelo MAPA", "benchmark_block", pesaveis,
         f"food_line_share x cobertura do benchmark ({params['benchmark_volume_coverage_pct']}%)"),
        ("alimentar sem benchmark", "sin_benchmark_block", [demand_profile.SIN_BENCHMARK],
         "complemento, dividido por tamanho de sortimento"),
        ("nao alimentar", "no_food_block", [demand_profile.NO_FOOD],
         "1 - food_line_share; fora do universo do MAPA"),
    )
    food = Decimal(params["food_line_share"])
    cob = Decimal(params["benchmark_volume_coverage_pct"]) / 100
    declarado = {
        "benchmark_block": food * cob,
        "sin_benchmark_block": food * (1 - cob),
        "no_food_block": 1 - food,
    }
    for rotulo, chave, membros, origem in blocos:
        obtido = sum(Decimal(sd[k]["kg_l"]) for k in membros if k in sd)
        pct = (obtido / total_kg * 100) if total_kg else None
        linhas.append(
            f"| {rotulo} | {_fmt(declarado[chave] * 100)} % (linhas) | {_fmt(pct)} % (kg) | {origem} |"
        )
    linhas.append("")
    linhas.append(
        "Peso declarado e share de LINHAS; a coluna observada e share de VOLUME. Nao devem "
        "coincidir: um pacote de agua pesa 3,5 kg e um sache de tempero pesa 30 g."
    )
    linhas.append("")

    # --- volume, dentro do bloco calibrado -----------------------------------------
    linhas.append("## Volume (kg ou litro) — dentro do bloco calibrado")
    linhas.append("")
    linhas.append(
        "Todas as colunas somam 100 sobre os mesmos grupos. E a dimensao que o MAPA publica "
        "e onde a calibracao age."
    )
    linhas.append("")
    linhas.append("| grupo | ANTES % | MAPA % | ALVO % | DEPOIS % | erro | canal |")
    linhas.append("|---|---:|---:|---:|---:|---:|---|")
    erros = []
    for key in pesaveis:
        b = benchmark[key]
        _tilt, base = demand_profile.channel_tilt(b, params)
        alvo, obtido = alvos[key], dep_bloco.get(key)
        if alvo is not None and obtido is not None:
            erro = obtido - alvo
            erros.append(abs(erro))
            erro_txt = f"{erro:+.2f}".replace(".", ",")
        else:
            erro_txt = "—"
        linhas.append(
            f"| {key} | {_fmt(ant_bloco.get(key))} | {_fmt(b['volume_share_pct'])} "
            f"| {_fmt(alvo)} | {_fmt(obtido)} | {erro_txt} | {base} |"
        )
    linhas.append("")
    if erros:
        medio = sum(erros) / len(erros)
        pior = max(erros)
        linhas.append(
            f"Erro absoluto medio contra o ALVO: **{_fmt(medio, 3)}** pontos; "
            f"maior desvio: **{_fmt(pior, 3)}** pontos."
        )
        linhas.append("")
        linhas.append(
            "O objetivo NAO e minimizar este numero. Ele esta aqui para que um desvio grande "
            "seja explicavel, e nao para ser perseguido — um erro de zero significaria que o "
            "sortimento do catalogo casa perfeitamente com a cesta espanhola, o que seria "
            "suspeito, nao bom."
        )
        linhas.append("")
    fallback = [
        k for k in pesaveis
        if sd.get(k, {}).get("linhas_sem_kg", 0) > 0
    ]
    if fallback:
        linhas.append(
            "Grupos com linhas sem conversao para kg (o desvio deles contra o ALVO nao "
            "mede a calibracao, mede a cobertura da conversao): "
            + ", ".join(f"`{k}` ({sd[k]['linhas_sem_kg']} linhas)" for k in fallback)
        )
        linhas.append("")

    # --- unidades e receita ---------------------------------------------------------
    chaves_todas = [k for k in sorted(sd, key=lambda k: -(float(sd[k].get("pct_kg") or 0)))
                    if k != "__total__"]

    linhas.append("## Unidades — sobre o total, inclusive nao alimentar")
    linhas.append("")
    linhas.append(
        "Nao e comparavel ao MAPA: o informe mede peso e volume, nunca peca. Esta tabela "
        "existe para mostrar quantas LINHAS cada grupo ocupa na cesta."
    )
    linhas.append("")
    linhas.append("| grupo | ANTES % | DEPOIS % |")
    linhas.append("|---|---:|---:|")
    for key in chaves_todas:
        linhas.append(
            f"| {key} | {_fmt(sa.get(key, {}).get('pct_unidades') if sa else None)} "
            f"| {_fmt(sd[key].get('pct_unidades'))} |"
        )
    linhas.append("")

    linhas.append("## Receita — sobre o total")
    linhas.append("")
    linhas.append(
        "A coluna do MAPA e o share de VALOR domestico, sem inclinacao de canal. Ela NAO e "
        "um alvo: a receita e consequencia do volume e do preco observado da Mercadona, e "
        "converge com o MAPA so na medida em que os precos espanhois e os da Mercadona se "
        "parecem. Volume e valor devem DIVERGIR entre si — mariscos sao 0,81% do volume e "
        "2,88% do valor domestico, e um simulador que os igualasse estaria errado."
    )
    linhas.append("")
    linhas.append("| grupo | ANTES % | MAPA valor % | DEPOIS % |")
    linhas.append("|---|---:|---:|---:|")
    for key in chaves_todas:
        b = benchmark.get(key, {})
        linhas.append(
            f"| {key} | {_fmt(sa.get(key, {}).get('pct_receita') if sa else None)} "
            f"| {_fmt(b.get('value_share_pct'))} | {_fmt(sd[key].get('pct_receita'))} |"
        )
    linhas.append("")

    # --- preco medio ------------------------------------------------------------------
    linhas.append("## Preco medio por kg — a coluna comparavel sem conversao nenhuma")
    linhas.append("")
    linhas.append(
        "Share exige converter volume em linhas e inclinar por canal; EUR/kg nao exige nada. "
        "Um grupo cujo EUR/kg bate com o informe tem preco observado saudavel, "
        "independentemente de quantas linhas dele saem — e foi esta coluna que denunciou o "
        "defeito de preco de granel antes da calibracao existir."
    )
    linhas.append("")
    linhas.append("| grupo | ANTES EUR/kg | MAPA EUR/kg | DEPOIS EUR/kg | linhas sem kg |")
    linhas.append("|---|---:|---:|---:|---:|")
    for key in chaves_todas:
        b = benchmark.get(key, {})
        linhas.append(
            f"| {key} | {_fmt(sa.get(key, {}).get('eur_por_kg') if sa else None)} "
            f"| {_fmt(b.get('avg_price_eur_kg'))} | {_fmt(sd[key].get('eur_por_kg'))} "
            f"| {sd[key].get('linhas_sem_kg', 0)} |"
        )
    linhas.append("")

    # --- o que o benchmark nao alcanca -------------------------------------------------
    linhas.append("## O que o benchmark nao alcanca")
    linhas.append("")
    sem_alvo = sorted(
        k for k, v in benchmark.items()
        if not v["use_as_weight"] and v["provenance"] in ("informe_prose", "none")
        and k not in demand_profile.SCOPE_LABELS
    )
    linhas.append(
        f"- **{demand_profile.NO_FOOD}** e **{demand_profile.SIN_BENCHMARK}** nao tem alvo "
        f"do MAPA e nunca sao somados ao bloco calibrado. O informe mede alimentos e "
        f"bebidas; drogaria, limpeza, maquiagem e mascotas estao fora do universo dele."
    )
    linhas.append(
        f"- share alimentar declarado: **{params['food_line_share']}** — premissa "
        f"`synthetic`, sem benchmark. Nenhuma fonte deste repo mede a composicao alimentar "
        f"de uma cesta online, e o MAPA nao mede drogaria."
    )
    linhas.append(
        f"- cobertura do benchmark: **{params['benchmark_volume_coverage_pct']}%** do volume "
        f"domestico espanhol, somando as folhas pesaveis. O complemento vai para "
        f"`SIN_BENCHMARK`, dividido por tamanho de sortimento."
    )
    for key in sem_alvo:
        b = benchmark[key]
        secao = b["informe_section"] or "sem secao"
        linhas.append(f"- `{key}` ({b['mapa_label']}): sem share publicado — secao {secao}")
    linhas.append("")
    linhas.append("### Sazonalidade")
    linhas.append("")
    linhas.append(
        "Perfil NEUTRO nos 12 meses, por ausencia de evidencia numerica: os graficos "
        "mensais do informe sao imagens, e so ha cinco numeros mensais em prosa — todos do "
        "TOTAL da alimentacao, nunca por categoria. Alem disso a janela do simulador cobre "
        "apenas agosto, entao nao existe eixo mensal para exercer. O mecanismo existe, "
        "aplica-se a taxa de pedidos e tem teste que prova que um perfil nao neutro muda a "
        "saida; o gatilho para propor um perfil e a janela cobrir novembro e dezembro."
    )
    linhas.extend(_cohort_section(depois, antes))
    linhas.append("")
    linhas.append("## Fronteira que a calibracao nao atravessa")
    linhas.append("")
    linhas.append(
        "O MAPA mede consumo domestico do residente. NAO mede pedido de loja online, nem "
        "cesta, nem cadencia de compra, nem ticket por canal. Por isso `daily_order_rate`, "
        "`basket_lines_min/mode/max` e `quantity_max` continuam premissas `synthetic` em "
        "`order_premises_seed.csv` e NAO receberam calibracao nenhuma nesta fase. Chamar o "
        "benchmark de fonte para esses numeros seria transforma-lo numa falsa representacao "
        "da realidade."
    )
    linhas.append("")
    return "\n".join(linhas) + "\n"


def _cohort_section(depois: dict, antes: dict | None) -> list[str]:
    """A unica dimensao em que a camada de coorte se enxerga.

    O AGREGADO NAO SE MOVE DE PROPOSITO — e o criterio de aceitacao do IPF — entao uma
    pagina que so mostrasse totais concluiria que a fase nao fez nada. Esta secao existe
    porque o efeito e inteiramente condicional, e algo que so aparece na condicional precisa
    de um lugar proprio para ser visto.
    """
    coortes = depois.get("cohorts")
    linhas: list[str] = ["", "## Propensao por coorte do comprador", ""]

    if not coortes:
        linhas.append(
            "**Ausente nesta janela.** `silver_order.buyer_age_band` nao esta preenchido — "
            "a janela foi gerada por um modelo anterior a camada de coorte. Nao e uma "
            "medicao de zero, e a ausencia do carimbo."
        )
        return linhas

    linhas.append(
        "A coorte tem duas dimensoes, e sao as duas UNICAS em que um atributo observado do "
        "cliente coincide com um corte publicado do informe: **idade** (`birth_year`, da "
        "distribuicao provincial do INE) e **comunidade autonoma** (`province_code`, do "
        "Callejero). Ciclo de vida do lar e nivel socioeconomico sao os cortes mais ricos do "
        "MAPA e ficaram de fora: o cliente nao tem composicao familiar nem renda, e "
        "atribui-las seria inventar o atributo."
    )
    linhas.append("")
    linhas.append(
        "**O agregado nao se move, e isso e o criterio de aceitacao.** Os pesos por coorte, "
        "ponderados pela distribuicao real de coortes entre os pedidos, reproduzem os pesos "
        "da calibracao agregada — o IPF existe para isso. Quem procurar o efeito desta "
        "camada num total nao vai encontrar: ele esta inteiro nas colunas abaixo."
    )

    bandas = [b for b in ("LT35", "35_49", "50_64", "GE65") if b in coortes]
    totais = {
        b: sum(int(v["linhas"]) for v in coortes[b].values()) for b in bandas
    }

    linhas.append("")
    linhas.append("### Fatia de cada grupo DENTRO da coorte (% das linhas)")
    linhas.append("")
    linhas.append(
        "Denominador e a propria coorte, e nao o total: e assim que a comparacao entre "
        "faixas isola a propensao do tamanho da coorte. A coluna `x` e a razao entre a "
        "faixa mais velha e a mais nova — o resumo de uma linha inteira."
    )
    linhas.append("")
    cabecalho = "| grupo | " + " | ".join(f"{b} %" for b in bandas) + " | x GE65/LT35 |"
    linhas.append(cabecalho)
    linhas.append("|---|" + "---:|" * (len(bandas) + 1))

    grupos = sorted({g for b in bandas for g in coortes[b]})
    ordenados = []
    for grupo in grupos:
        fatias = {}
        for b in bandas:
            total = totais[b] or 1
            fatias[b] = Decimal(coortes[b].get(grupo, {}).get("linhas", 0)) * 100 / total
        razao = None
        if "GE65" in fatias and "LT35" in fatias and fatias["LT35"] > 0:
            razao = fatias["GE65"] / fatias["LT35"]
        ordenados.append((razao if razao is not None else Decimal("0"), grupo, fatias))
    ordenados.sort(reverse=True)

    for razao, grupo, fatias in ordenados:
        celulas = " | ".join(_fmt(fatias[b]) for b in bandas)
        linhas.append(f"| {grupo} | {celulas} | {_fmt(razao)} |")

    linhas.append("")
    linhas.append(
        "Uma razao de 1,00 significa que a faixa etaria nao move aquele grupo. `NO_FOOD` e "
        "`SIN_BENCHMARK` ficam perto de 1 por construcao: o informe nao os mede, o indice "
        "deles e neutro, e a fatia de cada BLOCO e mantida constante entre coortes de "
        "proposito. Sem essa fronteira eles absorviam o residuo da normalizacao e o modelo "
        "passava a afirmar que idoso compra 40% menos drogaria — numero que nenhuma fonte "
        "deste repo mede, e maior que a maioria dos efeitos que sao medidos."
    )

    armazens = depois.get("warehouses") or {}
    if armazens:
        linhas.append("")
        linhas.append("### Frequencia por comunidade autonoma")
        linhas.append("")
        linhas.append(
            "Antes desta fase os quatro armazens tinham a MESMA contagem de pedidos por "
            "construcao. O informe mede consumo per capita por comunidade, e essa diferenca "
            "passa a valer — como frequencia, nunca como tamanho de cesta, porque o informe "
            "da kg por ano e nao publica frequencia de compra domestica."
        )
        linhas.append("")
        linhas.append("| armazem | pedidos | clientes distintos | pedidos por cliente |")
        linhas.append("|---|---:|---:|---:|")
        for wh in sorted(armazens):
            dados = armazens[wh]
            por_cliente = (
                Decimal(dados["pedidos"]) / Decimal(dados["clientes"])
                if dados["clientes"] else Decimal("0")
            )
            linhas.append(
                f"| {wh} | {dados['pedidos']} | {dados['clientes']} | {_fmt(por_cliente)} |"
            )
        antes_wh = (antes or {}).get("warehouses")
        if antes_wh:
            linhas.append("")
            linhas.append(
                "O ANTES congelado tem a contagem por armazem ao lado; a diferenca entre "
                "eles e a inclinacao regional entrando em vigor."
            )
        else:
            linhas.append("")
            linhas.append(
                "O ANTES congelado nao registra contagem por armazem: ele e anterior a esta "
                "medicao existir. A comparacao util e entre os quatro armazens de HOJE, que "
                "antes eram iguais por construcao."
            )

    return linhas


def calibration_error(depois: dict, seeds_dir: str = demand_profile.DEFAULT_SEEDS_DIR) -> dict:
    """Erro do mix observado contra o ALVO, ignorando os grupos sem cobertura de kg.

    NAO E UMA NOTA. O objetivo declarado nunca foi minimizar esta distancia — um erro de
    zero significaria que o sortimento da Mercadona casa perfeitamente com a cesta
    espanhola, o que seria suspeito e nao bom. O numero existe para pegar UMA coisa: uma
    calibracao silenciosamente inerte.

    E o modo de falha e concreto. Se o perfil deixar de ser aplicado — um `demand.pick` que
    volta a ser uniforme, um export que esquece o quinto arquivo, um seed que nao recarrega
    — nada estoura. Os pedidos continuam saindo, os totais continuam fechando, e o mix volta
    a espelhar o tamanho do sortimento. Antes desta fase, MARISCOS estava a 11,44 pontos do
    alvo; a inercia se anuncia com desvios dessa ordem, nao com decimos.

    Grupos que caem no fallback de linhas (cobertura de kg abaixo do minimo) sao EXCLUIDOS:
    o desvio deles nao mede a calibracao, mede a cobertura da conversao, e mistura-los faria
    o limiar precisar ser afrouxado ate deixar de pegar o que veio pegar.
    """
    benchmark = demand_profile.load_benchmark(seeds_dir)
    params = demand_profile.load_params(seeds_dir)
    sd = _shares(depois)
    alvos = _targets(benchmark, params)
    pesaveis = list(alvos)
    dep = _block_shares(sd, pesaveis)

    desvios = {}
    for key in pesaveis:
        if sd.get(key, {}).get("linhas_sem_kg", 0):
            continue
        alvo, obtido = alvos[key], dep.get(key)
        if alvo is None or obtido is None:
            continue
        desvios[key] = abs(obtido - alvo)
    if not desvios:
        return {"grupos": 0, "medio": None, "pior": None, "pior_grupo": None}
    pior_grupo = max(desvios, key=desvios.get)
    return {
        "grupos": len(desvios),
        "medio": sum(desvios.values()) / len(desvios),
        "pior": desvios[pior_grupo],
        "pior_grupo": pior_grupo,
        "excluidos": sorted(
            k for k in pesaveis if sd.get(k, {}).get("linhas_sem_kg", 0)
        ),
    }


def write(markdown: str, path: str) -> str:
    if path == "-":
        print(markdown, end="")
        return path
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(markdown)
    return path
