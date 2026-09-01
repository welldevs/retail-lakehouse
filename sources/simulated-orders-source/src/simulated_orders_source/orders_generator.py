"""Gera o log de eventos de um (armazem, dia).

A REGRA DE OURO
---------------
Zero aleatoriedade em cliente, produto e preco. O PEDIDO e inventado; quem compra, o que se
compra e quanto custa vem da referencia, que veio do Silver, que veio do RAW. Nenhum produto,
preco, cliente ou CEP e fabricado aqui.

O QUE E SORTEADO, E O QUE ISSO SIGNIFICA
-----------------------------------------
    dia + armazem -> sub-seed proprio
                  -> quantos clientes pedem hoje (taxa x sazonal x indice regional)
                  -> quais clientes pedem hoje (UNIFORME, sem reposicao, entre os elegiveis)
                  -> qual COORTE cada um deles e (faixa etaria x comunidade autonoma)
                  -> quantas linhas tem a cesta (triangular declarada)
                  -> qual GRUPO DE DEMANDA (ponderado pelo perfil DAQUELA COORTE)
                  -> qual produto DENTRO do grupo (UNIFORME)
                  -> quantidade por linha (decrescente, derivada de quantity_max)
                  -> horario, janela de entrega e marcos (faixas declaradas)
                  -> quais linhas sao substituidas ou removidas (taxas declaradas)
                  -> qual ramo terminal o pedido segue (taxas declaradas)

A ESCOLHA DO PRODUTO DENTRO DO GRUPO CONTINUA UNIFORME, de proposito: nenhuma fonte deste
repo mede giro por SKU, e ponderar produto inventaria uma distribuicao que ninguem mediu — a
mesma proibicao que a Fase 1 aplicou a escolha do tramo ("nao existe populacao por rua nem
por tramo em nenhuma fonte que esta plataforma ingere").

O QUE MUDOU NA FASE 4 e o nivel acima. Antes, o produto era sorteado uniformemente sobre o
CATALOGO INTEIRO, e a consequencia declarada era que o mix por categoria espelhava o TAMANHO
DO SORTIMENTO. Agora existe uma ancora observacional que nao existia: o Informe del Consumo
Alimentario en España 2025 do MAPA. O grupo de demanda e sorteado com peso calibrado contra
o volume domestico espanhol, inclinado pela participacao do e-commerce; o produto dentro do
grupo continua uniforme.

A FRONTEIRA, que importa mais que a calibracao: o MAPA mede consumo domestico do residente.
NAO mede pedido de loja online, nem cesta, nem cadencia de compra. Por isso `daily_order_rate`,
`basket_lines_*` e `quantity_max` continuam premissas `synthetic` sem calibracao nenhuma, e
o terco nao alimentar do catalogo — que o informe nao cobre — recebe share declarado.

REPRODUTIBILIDADE: SUB-SEED POR (ARMAZEM, DIA)
-----------------------------------------------
Cada `(wh, order_date)` deriva a propria `random.Random` de
`sha256(seed|wh|order_date)`. Consequencia — e este e o analogo da "propriedade de prefixo"
da Fase 1, num eixo diferente:

    ACRESCENTAR UM DIA A JANELA E ADITIVO. As particoes ja geradas ficam byte a byte
    identicas, porque nenhum dia depende do sorteio de outro.

`hashlib.sha256` e usado, e nao `hash()`, porque o hash de `str` em CPython e aleatorizado por
processo: derivar a sub-seed dele faria a saida depender de PYTHONHASHSEED.

SEM RELOGIO. Todo `occurred_at` deriva da data da particao mais offsets declarados. Nenhuma
chamada a `datetime.now()` neste modulo.
"""

from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

from . import SOURCE_NAME
from .events import (
    CANCEL_REASONS,
    CANCELLED_BY,
    DECLINE_REASONS,
    DELIVERY_FAILURE_REASONS,
    LINE_FULFILLED,
    LINE_REMOVED,
    LINE_SUBSTITUTED,
    ORDER_CANCELLED,
    ORDER_DELIVERED,
    ORDER_DELIVERY_FAILED,
    ORDER_DISPATCHED,
    ORDER_LINE_REMOVED,
    ORDER_LINE_SUBSTITUTED,
    ORDER_PAYMENT_AUTHORIZED,
    ORDER_PAYMENT_FAILED,
    ORDER_PICKED,
    ORDER_PICKING_STARTED,
    ORDER_PLACED,
    ORDER_RETURNED,
    PAYMENT_METHODS,
    apply_transition,
    envelope,
)

CENTS = Decimal("0.01")


class GenerationError(Exception):
    """A referencia nao sustenta a geracao pedida."""


def day_seed(seed: int, wh: str, order_date: str) -> int:
    """Sub-seed deterministica de um (armazem, dia).

    sha256, e nao `hash()`: o hash de str em CPython e aleatorizado por processo.
    """
    blob = f"{seed}|{wh}|{order_date}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(blob).digest()[:8], "big")


def _money(value: Decimal) -> Decimal:
    return value.quantize(CENTS, rounding=ROUND_HALF_UP)


def _stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _sample_indices(rng: random.Random, population: int, wanted: int) -> list[int]:
    """`wanted` indices distintos de range(population), na ordem em que sairam.

    Escrito a mao em vez de `rng.sample`: o algoritmo interno de `sample` nao e parte do
    contrato da stdlib e pode mudar entre versoes do Python, o que quebraria a
    reprodutibilidade entre a maquina local e o container. `randrange` e estavel.

    O `set` aqui e so consultado, nunca iterado — a ordem da saida vem da sequencia de
    sorteios, entao PYTHONHASHSEED nao a alcanca.
    """
    if wanted > population:
        raise GenerationError(
            f"pedidos {wanted} indices distintos de uma populacao de {population}"
        )
    escolhidos: list[int] = []
    vistos: set = set()
    while len(escolhidos) < wanted:
        candidate = rng.randrange(population)
        if candidate in vistos:
            continue
        vistos.add(candidate)
        escolhidos.append(candidate)
    return escolhidos


def _quantity_weights(quantity_max: int) -> list[float]:
    """Pesos decrescentes para 1..quantity_max, derivados da propria premissa.

    w(k) = 1/2^(k-1): comprar uma unidade domina, e cada unidade a mais e metade tao provavel.
    Derivar da premissa em vez de tabelar evita uma segunda premissa nao declarada.
    """
    raw = [1.0 / (2 ** (k - 1)) for k in range(quantity_max)]
    total = sum(raw)
    cumulative, running = [], 0.0
    for weight in raw:
        running += weight / total
        cumulative.append(running)
    return cumulative


def _pick_weighted(rng: random.Random, cumulative: list[float]) -> int:
    draw = rng.random()
    for index, limit in enumerate(cumulative):
        if draw <= limit:
            return index
    return len(cumulative) - 1


def _order_id(wh: str, order_date: str, index: int) -> str:
    return f"ord_{wh}_{order_date.replace('-', '')}_{index:06d}"


def _basket(rng, reference, premises, wh, price_as_of, cohort):
    """Monta a cesta: linhas com produto real, preco real, quantidade sorteada.

    DOIS PASSOS, e a separacao entre eles e o que esta fase inteira introduziu:

        1. GRUPO DE DEMANDA, ponderado pelo perfil calibrado contra o MAPA — e, desde
           `mapa_2025_v2`, pelo perfil DA COORTE do cliente que esta pedindo.
        2. PRODUTO DENTRO DO GRUPO, uniforme.

    O passo 2 continua uniforme de proposito: nenhuma fonte deste repo mede giro por SKU, e
    ponderar produto inventaria uma distribuicao que ninguem mediu. O que mudou foi so o
    passo 1 — antes, o produto era sorteado uniformemente sobre o CATALOGO INTEIRO, e o mix
    por categoria espelhava o TAMANHO DO SORTIMENTO. Um catalogo tem 475 SKUs de cuidado
    facial e 162 de fruta e verdura; um domicilio nao compra nessa proporcao.

    PRECO NAO ENTRA EM NENHUM DOS DOIS SORTEIOS. Ele so aparece depois, copiado da
    referencia. Uma categoria cara pode ter volume baixo e receita alta, e isso e resultado
    do modelo, nao defeito: no MAPA, mariscos sao 0,81% do volume e 2,88% do valor.
    """
    catalog = reference.catalog_of(wh, price_as_of)
    grupos = reference.demand_groups_of(wh, price_as_of)
    cumulative = reference.demand_cdf_of(wh, price_as_of, cohort)

    low = premises.integer("basket_lines_min")
    high = premises.integer("basket_lines_max")
    mode = premises.integer("basket_lines_mode")

    wanted = int(rng.triangular(low, high + 1, mode))
    wanted = max(low, min(high, wanted))
    wanted = min(wanted, len(catalog))

    quantity_cumulative = _quantity_weights(premises.integer("quantity_max"))

    # SEM REPOSICAO, como antes: um produto aparece no maximo uma vez na cesta. A diferenca
    # e que agora a exclusao pode esgotar um GRUPO pequeno (MIEL tem 5 produtos). Quando
    # isso acontece o sorteio do grupo e refeito — e a tentativa gasta consumiu uma extracao
    # do rng, o que mantem a sequencia deterministica em vez de "pular" silenciosamente.
    # O teto de tentativas existe para que um perfil patologico falhe rapido em vez de
    # travar; nunca foi atingido na janela medida.
    positions: list[int] = []
    taken: set = set()
    tentativas = 0
    teto = max(wanted * 8, 32)
    while len(positions) < wanted and tentativas < teto:
        tentativas += 1
        grupo = reference.demand.pick(rng, grupos, cumulative)
        candidatos = reference.positions_in_demand_group(wh, price_as_of, grupo)
        livres = [p for p in candidatos if p not in taken]
        if not livres:
            continue
        escolhido = livres[rng.randrange(len(livres))]
        taken.add(escolhido)
        positions.append(escolhido)

    lines = []
    for line_no, position in enumerate(positions, start=1):
        product = catalog[position]
        quantity = _pick_weighted(rng, quantity_cumulative) + 1
        lines.append(
            {
                "line_no": line_no,
                "catalog_position": position,
                "source_product_id": product["source_product_id"],
                "display_name": product["display_name"],
                "category_id": product["category_id"],
                "subgroup_id": product["subgroup_id"],
                "demand_group": product["demand_group"],
                "quantity": quantity,
                "unit_price": product["unit_price"],
            }
        )
    return lines


def _substitute_for(rng, reference, wh, price_as_of, line, taken: set) -> dict | None:
    """Produto de troca, do MESMO armazem e da MESMA data.

    Preferencia pelo mesmo subgrupo, depois pela mesma categoria. Se nenhum dos dois tiver
    candidato livre, NAO ha substituicao e nenhum evento e emitido — a linha segue cumprida.

    Consequencia declarada: a taxa efetiva de substituicao fica ligeiramente abaixo de
    `substitution_rate` em subgrupos pequenos. Inventar um substituto fora do sortimento
    daquele armazem seria pior: seria inventar sortimento, que e a mesma proibicao do preco.
    """
    catalog = reference.catalog_of(wh, price_as_of)
    for candidates in (
        reference.substitutes_in_subgroup(wh, price_as_of, line["subgroup_id"]),
        reference.substitutes_in_category(wh, price_as_of, line["category_id"]),
        # Terceiro nivel: o mesmo GRUPO DE DEMANDA. Mais largo que a categoria da fonte e
        # ainda dentro do que a calibracao considera a mesma necessidade — trocar merluza
        # por dourada e substituicao; trocar por xampu nao seria.
        reference.positions_in_demand_group(wh, price_as_of, line["demand_group"]),
    ):
        livres = [p for p in candidates if catalog[p]["source_product_id"] not in taken]
        if livres:
            return catalog[livres[rng.randrange(len(livres))]]
    return None


def _lifecycle(rng, reference, premises, wh, order_date, order_id, customer, base_day):
    """Constroi a sequencia de eventos de UM pedido.

    O estado e avancado por `apply_transition` a cada evento: o gerador e validado pelo
    proprio vocabulario enquanto constroi, e nao apenas por um teste tres camadas adiante.
    """
    calendar = reference.calendar_of(wh, order_date)
    price_as_of = calendar["price_as_of"]
    price_source = calendar["price_source"]

    customer_version = reference.customer_version_at(customer["first_ingestion_date"], order_date)
    if customer_version is None:
        raise GenerationError(
            f"cliente {customer['customer_id']} so existe a partir de "
            f"{customer['first_ingestion_date']}, depois do dia do pedido {order_date}. "
            f"Nenhum pedido pode nascer antes do cliente."
        )

    # A COORTE E RESOLVIDA NO DIA DO PEDIDO, e nao no export: a idade muda, e uma janela
    # longa faz um cliente cruzar a fronteira de uma faixa. A conta e a diferenca de anos,
    # que e a mesma convencao com que `birth_year` foi construido na Source de OLTP
    # (`reference_year - idade`) — usar data completa aqui e ano la produziria duas idades
    # para a mesma pessoa.
    idade = int(order_date[:4]) - int(customer["birth_year"])
    cohort = reference.demand.cohort_of(wh, idade)

    lines = _basket(rng, reference, premises, wh, price_as_of, cohort)
    gross = _money(sum((line["unit_price"] * line["quantity"] for line in lines), Decimal("0")))

    placed_at = base_day + timedelta(
        hours=rng.randint(premises.integer("order_hour_min"), premises.integer("order_hour_max")),
        minutes=rng.randrange(60),
        seconds=rng.randrange(60),
    )
    slot_start = (
        placed_at
        + timedelta(
            hours=rng.randint(
                premises.integer("slot_lead_hours_min"), premises.integer("slot_lead_hours_max")
            )
        )
    ).replace(minute=0, second=0, microsecond=0)
    slot_end = slot_start + timedelta(hours=premises.integer("slot_hours"))

    events: list[dict] = []
    state = None
    sequence = 0

    def emit(event_type: str, occurred_at: datetime, payload: dict) -> None:
        nonlocal state, sequence
        state = apply_transition(state, event_type)
        sequence += 1
        events.append(
            envelope(
                order_id=order_id,
                wh=wh,
                sequence_no=sequence,
                event_type=event_type,
                occurred_at=_stamp(occurred_at),
                payload=payload,
                producer=SOURCE_NAME,
            )
        )

    emit(
        ORDER_PLACED,
        placed_at,
        {
            "customer_id": customer["customer_id"],
            "customer_ingestion_date": customer_version,
            # Carimbada no evento pelo mesmo motivo de `demand_group`: o que importa e a
            # faixa que valia NO MOMENTO DO PEDIDO. Deriva-la depois, no Silver, exigiria
            # reimplementar os limites das faixas numa segunda linguagem.
            "buyer_age_band": cohort.split("|", 1)[0],
            "province_code": customer["province_code"],
            "municipality_code": customer["municipality_code"],
            "postal_code": customer["postal_code"],
            "price_as_of": price_as_of,
            "price_source": price_source,
            "delivery_slot_start": _stamp(slot_start),
            "delivery_slot_end": _stamp(slot_end),
            "line_count": len(lines),
            "gross_amount": str(gross),
            "lines": [
                {
                    "line_no": line["line_no"],
                    "source_product_id": line["source_product_id"],
                    "category_id": line["category_id"],
                    "subgroup_id": line["subgroup_id"],
                    # No evento, e nao apenas na referencia: e assim que o Silver pode
                    # agrupar por grupo de demanda sem reimplementar o de-para, e assim que
                    # `validate` reconfere o carimbo contra a referencia em vez de confiar.
                    "demand_group": line["demand_group"],
                    "quantity": line["quantity"],
                    "unit_price": str(line["unit_price"]),
                }
                for line in lines
            ],
        },
    )

    # --- pagamento ---------------------------------------------------------------
    payment_at = placed_at + timedelta(
        minutes=rng.randint(
            premises.integer("minutes_to_payment_min"), premises.integer("minutes_to_payment_max")
        )
    )
    if rng.random() < premises.number("payment_failure_rate"):
        emit(
            ORDER_PAYMENT_FAILED,
            payment_at,
            {"decline_reason": DECLINE_REASONS[rng.randrange(len(DECLINE_REASONS))]},
        )
        return events, state

    emit(
        ORDER_PAYMENT_AUTHORIZED,
        payment_at,
        {
            "payment_method": PAYMENT_METHODS[rng.randrange(len(PAYMENT_METHODS))],
            "authorized_amount": str(gross),
        },
    )

    # --- cancelamento ------------------------------------------------------------
    if rng.random() < premises.number("cancellation_rate"):
        cancel_at = payment_at + timedelta(
            minutes=rng.randint(
                premises.integer("minutes_to_cancel_min"),
                premises.integer("minutes_to_cancel_max"),
            )
        )
        emit(
            ORDER_CANCELLED,
            cancel_at,
            {
                "cancelled_by": CANCELLED_BY[rng.randrange(len(CANCELLED_BY))],
                "reason": CANCEL_REASONS[rng.randrange(len(CANCEL_REASONS))],
            },
        )
        return events, state

    # --- separacao ---------------------------------------------------------------
    picking_at = payment_at + timedelta(
        minutes=rng.randint(
            premises.integer("minutes_to_picking_min"), premises.integer("minutes_to_picking_max")
        )
    )
    emit(ORDER_PICKING_STARTED, picking_at, {})

    per_line = premises.integer("minutes_per_line_picked")
    substitution_rate = premises.number("substitution_rate")
    removal_rate = premises.number("removal_rate")
    taken = {line["source_product_id"] for line in lines}

    for line in lines:
        line["status"] = LINE_FULFILLED
        line["effective_price"] = line["unit_price"]
        line_at = picking_at + timedelta(minutes=per_line * line["line_no"])

        # UM sorteio por linha, com dois limiares: assim as duas taxas sao exatas e
        # independentes do numero de chamadas ao rng, e nao condicionadas uma a outra.
        draw = rng.random()
        if draw < substitution_rate:
            substitute = _substitute_for(rng, reference, wh, price_as_of, line, taken)
            if substitute is not None:
                taken.add(substitute["source_product_id"])
                line["status"] = LINE_SUBSTITUTED
                line["effective_price"] = substitute["unit_price"]
                line["substitute_source_product_id"] = substitute["source_product_id"]
                emit(
                    ORDER_LINE_SUBSTITUTED,
                    line_at,
                    {
                        "line_no": line["line_no"],
                        "source_product_id": line["source_product_id"],
                        "substitute_source_product_id": substitute["source_product_id"],
                        "substitute_unit_price": str(substitute["unit_price"]),
                        "quantity": line["quantity"],
                    },
                )
        elif draw < substitution_rate + removal_rate:
            line["status"] = LINE_REMOVED
            emit(
                ORDER_LINE_REMOVED,
                line_at,
                {
                    "line_no": line["line_no"],
                    "source_product_id": line["source_product_id"],
                    "quantity": line["quantity"],
                    # "unavailable", nunca "out_of_stock": nao existe fato de estoque nesta
                    # plataforma, e nomear como se existisse prometeria um dado inexistente.
                    "reason": "unavailable",
                },
            )

    kept = [line for line in lines if line["status"] != LINE_REMOVED]
    net = _money(
        sum((line["effective_price"] * line["quantity"] for line in kept), Decimal("0"))
    )
    picked_at = picking_at + timedelta(minutes=per_line * len(lines))
    emit(
        ORDER_PICKED,
        picked_at,
        {"picked_line_count": len(kept), "picked_amount": str(net)},
    )

    # --- despacho e entrega ------------------------------------------------------
    dispatch_at = picked_at + timedelta(
        minutes=rng.randint(
            premises.integer("minutes_to_dispatch_min"),
            premises.integer("minutes_to_dispatch_max"),
        )
    )
    emit(ORDER_DISPATCHED, dispatch_at, {})

    delivery_at = dispatch_at + timedelta(
        minutes=rng.randint(
            premises.integer("minutes_to_delivered_min"),
            premises.integer("minutes_to_delivered_max"),
        )
    )
    if rng.random() < premises.number("delivery_failure_rate"):
        emit(
            ORDER_DELIVERY_FAILED,
            delivery_at,
            {"reason": DELIVERY_FAILURE_REASONS[rng.randrange(len(DELIVERY_FAILURE_REASONS))]},
        )
        return events, state

    emit(
        ORDER_DELIVERED,
        delivery_at,
        {
            # Janela de entrega e promessa comercial numa grade fixa, NUNCA rota: o Callejero
            # nao tem coordenada nem adjacencia, entao rota e tempo de rota seriam inventados.
            "delivered_within_slot": slot_start <= delivery_at <= slot_end,
            "delivered_line_count": len(kept),
            "delivered_amount": str(net),
        },
    )

    # --- devolucao ---------------------------------------------------------------
    if kept and rng.random() < premises.number("return_rate"):
        devolvida = kept[rng.randrange(len(kept))]
        return_at = delivery_at + timedelta(
            minutes=rng.randint(
                premises.integer("minutes_to_return_min"),
                premises.integer("minutes_to_return_max"),
            )
        )
        emit(
            ORDER_RETURNED,
            return_at,
            {
                "returned_line_count": 1,
                "returned_line_no": devolvida["line_no"],
                "returned_amount": str(
                    _money(devolvida["effective_price"] * devolvida["quantity"])
                ),
            },
        )
    return events, state


def generate(reference, premises, wh: str, order_date: str, seed: int) -> list[dict]:
    """Log de eventos de um (armazem, dia), ordenado por (occurred_at, order_id, sequence_no).

    A ordenacao e total e deterministica: `occurred_at` sozinho empata, e o desempate por
    order_id e sequence_no garante que os eventos de um mesmo pedido nunca saiam fora de
    ordem, que e a propriedade de que o fold depende.
    """
    rng = random.Random(day_seed(seed, wh, order_date))
    base_day = datetime.strptime(order_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # ELEGIVEIS: DUAS CONDICOES, e a segunda nasceu na Fase 5.
    #
    #   1. ja existia no dia do pedido — a base cresce, e uma geracao futura pode
    #      acrescentar clientes que nao existiam nos dias ja gerados;
    #   2. tem `min_buyer_age` anos ou mais NO DIA DO PEDIDO.
    #
    # A segunda existe porque `silver_customer` e uma projecao fiel da POPULACAO residente
    # (a idade vem da distribuicao provincial do INE, como o contrato da Source de OLTP
    # declara), e populacao inclui criancas: 18,01% da base tinha menos de 18 anos, com
    # idades a partir de zero. Enquanto a idade nao fazia nada isso era inofensivo. A partir
    # do momento em que ela governa a demanda, deixa de ser — 18% da base entraria na faixa
    # '-35 anos' do MAPA sendo crianca. Quem PODE pedir e premissa do dominio de pedidos, e
    # mora em `order_premises_seed.csv`; a base de clientes nao foi tocada.
    minima = premises.integer("min_buyer_age")
    ano = int(order_date[:4])
    eligible = [
        customer
        for customer in reference.customers_of(wh)
        if reference.customer_version_at(customer["first_ingestion_date"], order_date)
        and ano - int(customer["birth_year"]) >= minima
    ]
    if not eligible:
        raise GenerationError(
            f"nenhum cliente de wh={wh!r} existia em {order_date} com {minima} anos ou "
            f"mais. A base de clientes comeca em "
            f"{min(reference.customer_ingestion_dates)}."
        )

    # SAZONALIDADE ENTRA AQUI, NA TAXA DE PEDIDOS — nunca no mix. A distincao vem da
    # evidencia: o informe do MAPA publica gasto mensal do TOTAL da alimentacao e NAO publica
    # perfil mensal por categoria (os graficos mensais sao imagens). Um fator global sobre o
    # mix se normalizaria e nao faria nada. O perfil entregue e neutro em todos os 12 meses,
    # por ausencia de evidencia numerica e porque a janela cobre so agosto — mas o mecanismo
    # existe e tem teste que prova que um perfil nao neutro muda a saida.
    fator = reference.demand.seasonal_factor(base_day.month)

    # A INTENSIDADE REGIONAL ENTRA AQUI, ao lado da sazonalidade, e pelo mesmo motivo: ela
    # e uma propriedade de QUANTO se compra, nao de O QUE se compra. O informe mede consumo
    # per capita por comunidade — Cataluna 620,82 kg ou litro por pessoa e ano contra 505,86
    # de Madrid — e nao publica frequencia de compra domestica, entao repartir a intensidade
    # entre frequencia e tamanho de cesta seria inventar a reparticao. A plataforma escolheu
    # frequencia, declarou a escolha em `region_frequency_basis`, e ja entrega o indice
    # RENORMALIZADO sobre as comunidades servidas: a soma ponderada e 1, logo o total de
    # pedidos da janela nao se move e o que muda e a reparticao entre armazens.
    regional = reference.demand.frequency_index(wh)

    taxa = premises.number("daily_order_rate") * fator * regional
    wanted = round(len(eligible) * taxa)
    if wanted <= 0:
        raise GenerationError(
            f"daily_order_rate={premises.number('daily_order_rate')} x fator sazonal "
            f"{fator} x indice regional {regional} sobre {len(eligible)} cliente(s) "
            f"elegivel(is) da zero pedido em {order_date}"
        )

    # Sem reposicao: um cliente faz no maximo um pedido por dia. E uma simplificacao
    # declarada, e nao uma medicao — nenhuma fonte deste repo mede cadencia de compra.
    escolhidos = _sample_indices(rng, len(eligible), wanted)

    events: list[dict] = []
    for index, position in enumerate(escolhidos):
        order_id = _order_id(wh, order_date, index)
        do_pedido, _ = _lifecycle(
            rng, reference, premises, wh, order_date, order_id, eligible[position], base_day
        )
        events.extend(do_pedido)

    events.sort(key=lambda e: (e["occurred_at"], e["order_id"], e["sequence_no"]))
    return events
