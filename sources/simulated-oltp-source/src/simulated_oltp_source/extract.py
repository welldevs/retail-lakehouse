"""Orquestra a geracao de uma particao de clientes sinteticos.

"extract" aqui NAO busca rede — nem esta Source nem nenhuma outra parte deste pacote abre
socket. O verbo e mantido por simetria de Makefile/DAG com as outras tres sources, e o
que ele faz e: ler o arquivo de referencia que a plataforma pre-computou do Silver, gerar
os clientes daquele warehouse e pousar a particao com manifesto e _SUCCESS. Mesmo
precedente do `extract --in <dir>` do `ine-callejero-source`, um nivel antes.

Ordem de gravacao, nesta ordem e sempre: dados -> manifesto -> _SUCCESS. Um processo
morto no meio nunca deixa uma particao que se anuncia completa sem estar.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from . import SOURCE_NAME, __version__
from .canonical import write_json
from .customers_generator import GenerationError, generate
from .partition import (
    CUSTOMERS_FILE,
    PartitionError,
    assert_writable,
    build_history,
    customers_path,
    mark_success,
    partition_path,
    write_manifest,
)
from .reference_data import ReferenceError, load
from .schema import count_customers, fingerprint

EXIT_OK = 0
EXIT_FATAL = 2


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def run(args) -> int:
    started = _now()
    ingestion_date = args.date or started.strftime("%Y-%m-%d")

    try:
        partition = partition_path(args.out, ingestion_date, args.wh)
        os.makedirs(partition, exist_ok=True)
        previous = assert_writable(partition, args.overwrite)
        reference = load(args.reference)
        customers = generate(
            reference, args.wh, args.count, args.seed, ingestion_date
        )
    except (PartitionError, ReferenceError, GenerationError) as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL

    sha256, size = write_json(customers_path(partition), customers)
    finished = _now()

    # Par (provincia, municipio), nao so o codigo: codigo de municipio e unico DENTRO da
    # provincia. Hoje cada warehouse cai numa provincia so, mas a AUF oficial de Madrid
    # tem 38 municipios em Avila/Guadalajara/Toledo que ficaram de fora por falta de
    # Callejero — se um dia entrarem, contar so o codigo passaria a subcontar em silencio.
    municipalities_used = len(
        {(c["province_code"], c["municipality_code"]) for c in customers}
    )
    house_number_null = sum(1 for c in customers if c["house_number"] is None)
    pseudo_rows = sum(
        1 for c in customers if reference.candidate(c["candidate_index"])["is_pseudo_address"]
    )
    relative = os.path.relpath(customers_path(partition), args.out)
    run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{args.wh}"

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
        "partition": {"ingestion_date": ingestion_date, "warehouse": args.wh},
        # A seed vive no manifesto e nao em cada registro: e constante para a particao
        # inteira. Sem ela registrada aqui, a geracao seria irreproduzivel.
        "config": {
            "count": args.count,
            "seed": args.seed,
            "reference": os.path.normpath(args.reference),
        },
        # Proveniencia do insumo: quais snapshots do Silver sustentam estes clientes.
        "reference": {
            "callejero_ingestion_date": reference.callejero_ingestion_date,
            "population_ingestion_date": reference.population_ingestion_date,
            "population_series_ingestion_date": reference.population_series_ingestion_date,
            "population_year": reference.population_year,
            "population_reference_date": reference.population_reference_date,
            "age_year": reference.age_year,
            "age_reference_date": reference.age_reference_date,
            "age_fk_periodo": reference.age_fk_periodo,
            "address_candidates": reference.candidate_count(),
            "orphan_tramos_excluded": reference.orphan_tramos_excluded,
        },
        "totals": {
            "customer_rows": len(customers),
            "municipalities_used": municipalities_used,
            "house_number_null": house_number_null,
            "pseudo_address_rows": pseudo_rows,
            "bytes": size,
        },
        "schema_fingerprint": fingerprint([customers]),
        "files": [
            {
                "path": relative,
                "stage": "customers",
                "sha256": sha256,
                "bytes": size,
                "records": count_customers(customers),
            }
        ],
        "failures": [],
        "anomalies": [],
        "history": build_history(previous),
    }

    write_manifest(partition, manifest)
    mark_success(partition, True, run_id)

    print(f"particao ......... {partition}")
    print(f"run_id ........... {run_id}")
    print(f"referencia ....... {args.reference}")
    print(f"  callejero ...... ingestion_date={reference.callejero_ingestion_date}")
    print(f"  populacao ...... ingestion_date={reference.population_ingestion_date} "
          f"year={reference.population_year}")
    print(f"  idade .......... year={reference.age_year} "
          f"fk_periodo={reference.age_fk_periodo} (proxy provincial)")
    print(f"seed ............. {args.seed}")
    print(f"clientes ......... {len(customers)} em {municipalities_used} municipio(s)")
    print(f"sem numero ....... {house_number_null} (numeracao inexistente no Callejero)")
    print(f"pseudovia ........ {pseudo_rows}")
    print(f"arquivo .......... {CUSTOMERS_FILE} ({size} bytes)")
    print("OK: particao completa, _SUCCESS gravado por ultimo.")
    return EXIT_OK
