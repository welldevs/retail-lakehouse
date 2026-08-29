"""Gera o log de eventos de um (armazem, dia).

A REGRA DE OURO
---------------
Zero aleatoriedade em cliente, produto e preco. O PEDIDO e inventado; quem compra, o que se
compra e quanto custa vem da referencia, que veio do Silver, que veio do RAW. Nenhum produto,
preco, cliente ou CEP e fabricado aqui.

O QUE E SORTEADO, E O QUE ISSO SIGNIFICA
-----------------------------------------
    dia + armazem -> sub-seed proprio
                  -> quais clientes pedem hoje (UNIFORME, sem reposicao)
                  -> quantas linhas tem a cesta (triangular declarada)
                  -> quais produtos (UNIFORME entre os do catalogo daquele armazem e dia)
                  -> quantidade por linha (decrescente, derivada de quantity_max)
                  -> horario, janela de entrega e marcos (faixas declaradas)
                  -> quais linhas sao substituidas ou removidas (taxas declaradas)
                  -> qual ramo terminal o pedido segue (taxas declaradas)

A ESCOLHA DO PRODUTO E UNIFORME DE PROPOSITO, e isso NAO e "cesta realista": nenhuma fonte
deste repo mede venda, giro ou composicao de cesta. Ponderar produto inventaria uma
distribuicao que ninguem mediu — a mesma proibicao que a Fase 1 aplicou a escolha do tramo
("nao existe populacao por rua nem por tramo em nenhuma fonte que esta plataforma ingere").
A consequencia declarada: o mix por categoria espelha o TAMANHO do sortimento, e isso e
consequencia de uma premissa, nao afirmacao sobre o mercado.

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


def _basket(rng, reference, premises, wh, price_as_of):
    """Monta a cesta: linhas com produto real, preco real, quantidade sorteada."""
    catalog = reference.catalog_of(wh, price_as_of)
    low = premises.integer("basket_lines_min")
    high = premises.integer("basket_lines_max")
    mode = premises.integer("basket_lines_mode")

    wanted = int(rng.triangular(low, high + 1, mode))
    wanted = max(low, min(high, wanted))
    wanted = min(wanted, len(catalog))

    quantity_cumulative = _quantity_weights(premises.integer("quantity_max"))
    positions = _sample_indices(rng, len(catalog), wanted)

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

    lines = _basket(rng, reference, premises, wh, price_as_of)
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

    # Elegiveis: quem ja existia no dia do pedido. Hoje sao todos, mas a base cresce e uma
    # geracao futura pode acrescentar clientes que nao existiam nos dias ja gerados.
    eligible = [
        customer
        for customer in reference.customers_of(wh)
        if reference.customer_version_at(customer["first_ingestion_date"], order_date)
    ]
    if not eligible:
        raise GenerationError(
            f"nenhum cliente de wh={wh!r} existia em {order_date}. A base de clientes comeca "
            f"em {min(reference.customer_ingestion_dates)}."
        )

    wanted = round(len(eligible) * premises.number("daily_order_rate"))
    if wanted <= 0:
        raise GenerationError(
            f"daily_order_rate={premises.number('daily_order_rate')} sobre {len(eligible)} "
            f"cliente(s) da zero pedido em {order_date}"
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
