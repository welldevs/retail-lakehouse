"""Orquestra a geracao de uma particao de eventos de pedido.

"extract" aqui NAO busca rede — nem esta Source nem nenhuma outra parte deste pacote abre
socket. O verbo e mantido por simetria de Makefile/DAG com as outras quatro sources. O que
ele faz e: ler os quatro arquivos de referencia que a plataforma pre-computou do Silver,
gerar o log de eventos daquele (armazem, dia) e pousar a particao com manifesto e _SUCCESS.

Ordem de gravacao, nesta ordem e sempre: dados -> manifesto -> _SUCCESS. Um processo morto no
meio nunca deixa uma particao que se anuncia completa sem estar.

O MANIFESTO DECLARA O FOLD, E ISSO NAO E REDUNDANCIA
-----------------------------------------------------
`totals` traz `order_rows`, `net_amount_picked`, `substituted_lines` e companhia — numeros que
so existem depois de dobrar o log. Nao ha `orders.json` ao lado: o estado nao e material aqui,
e o manifesto e a unica declaracao do que o log significa. E contra ela que o teste dbt
reconcilia o Silver, fechando manifesto -> RAW -> parquet.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from . import SOURCE_NAME, __version__
from .canonical import write_jsonl
from .events import EventError
from .orders_generator import GenerationError, generate
from .partition import (
    EVENTS_FILE,
    PartitionError,
    assert_writable,
    build_history,
    events_path,
    mark_success,
    partition_path,
    write_manifest,
)
from .premises import PremiseError, Premises
from .reference_data import ReferenceError, load
from .schema import fingerprint, missing_fields, totals_of

EXIT_OK = 0
EXIT_FATAL = 2


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def run(args) -> int:
    started = _now()
    order_date = args.date or started.strftime("%Y-%m-%d")

    try:
        partition = partition_path(args.out, order_date, args.wh)
        os.makedirs(partition, exist_ok=True)
        previous = assert_writable(partition, args.overwrite)
        reference = load(args.reference)
        premises = Premises(reference.premises)
        events = generate(reference, premises, args.wh, order_date, args.seed)
    except (PartitionError, ReferenceError, PremiseError, GenerationError, EventError) as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL

    ausentes = missing_fields(events)
    if ausentes:
        # Nunca deve acontecer — o gerador constroi pelo mesmo vocabulario que isto confere —
        # mas gravar uma particao com campo faltando seria pior que recusar.
        print(f"ERRO: eventos gerados sem campo declarado: {ausentes[:20]}")
        return EXIT_FATAL

    sha256, size, records = write_jsonl(events_path(partition), events)
    finished = _now()

    totals = totals_of(events)
    totals["bytes"] = size
    calendar = reference.calendar_of(args.wh, order_date)
    relative = os.path.relpath(events_path(partition), args.out)
    run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{args.wh}_{order_date.replace('-', '')}"

    manifest = {
        "run_id": run_id,
        "started_at_utc": _stamp(started),
        "finished_at_utc": _stamp(finished),
        "duration_seconds": round((finished - started).total_seconds(), 3),
        "complete": True,
        "source": {
            "name": SOURCE_NAME,
            "wh": args.wh,
            "generator_version": __version__,
        },
        "partition": {"ingestion_date": order_date, "warehouse": args.wh},
        # As TRES entradas que mudam os pedidos sob os mesmos order_id. Sem as tres
        # registradas, um --overwrite trocaria os pedidos sem deixar rastro.
        "config": {
            "seed": args.seed,
            "orders": totals["order_rows"],
            "premises_sha256": reference.premises_sha256,
            # QUARTA CONDICAO NAO-ADITIVA. Trocar o modelo de demanda troca os pedidos por
            # tras dos mesmos ids, exatamente como trocar seed, referencia ou premissas.
            # Fica no config, e nao so na referencia, para que reler uma particao antiga
            # responda com que modelo ela nasceu sem depender do export ainda existir.
            "demand_model_version": reference.demand.version,
            "reference": os.path.normpath(args.reference),
        },
        # Proveniencia do insumo: quais snapshots sustentam estes pedidos.
        "reference": {
            "customer_ingestion_dates": reference.customer_ingestion_dates,
            "roster_ingestion_date": reference.roster_ingestion_date,
            "catalog_ingestion_dates": reference.catalog_ingestion_dates,
            "price_as_of": calendar["price_as_of"],
            "price_source": calendar["price_source"],
            "window_from": reference.window_from,
            "window_to": reference.window_to,
            "premises_seed_path": reference.premises_seed_path,
            "premises_sha256": reference.premises_sha256,
            "demand_model_version": reference.demand.version,
            "demand_benchmark": reference.demand.benchmark,
            "demand_seeds_sha256": reference.demand.seeds_sha256,
        },
        "totals": totals,
        "schema_fingerprint": fingerprint(),
        "files": [
            {
                "path": relative,
                "stage": "order_events",
                "sha256": sha256,
                "bytes": size,
                "records": records,
            }
        ],
        "failures": [],
        "anomalies": [],
        "history": build_history(previous),
    }

    write_manifest(partition, manifest)
    mark_success(partition, True, run_id)

    estados = ", ".join(f"{e}={n}" for e, n in totals["orders_by_state"].items())
    print(f"particao ......... {partition}")
    print(f"run_id ........... {run_id}")
    print(f"referencia ....... {args.reference}")
    print(f"  clientes ....... geracao vigente entre {', '.join(reference.customer_ingestion_dates)}")
    print(f"  preco .......... price_as_of={calendar['price_as_of']} "
          f"({calendar['price_source']})")
    print(f"  premissas ...... sha256={reference.premises_sha256[:16]}... (todas sinteticas)")
    print(f"  demanda ........ {reference.demand.version} ({len(reference.demand.groups)} grupos)")
    print(f"seed ............. {args.seed} (sub-seed do dia derivada por sha256)")
    print(f"pedidos .......... {totals['order_rows']} de {totals['customers_used']} cliente(s)")
    print(f"eventos .......... {totals['event_rows']} em {len(totals['event_rows_by_type'])} tipos")
    print(f"linhas ........... {totals['line_rows']} "
          f"({totals['substituted_lines']} substituida(s), {totals['removed_lines']} removida(s))")
    print(f"valor ............ colocado {totals['gross_amount_placed']} / "
          f"separado {totals['net_amount_picked']}")
    print(f"estados .......... {estados}")
    print(f"arquivo .......... {EVENTS_FILE} ({size} bytes, {records} linhas)")
    print("OK: particao completa, _SUCCESS gravado por ultimo.")
    return EXIT_OK
