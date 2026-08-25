"""Construcao de particoes sinteticas em disco, para testar sem rede e sem a Source."""

from __future__ import annotations

import hashlib
import json
import os

CANONICAL = dict(ensure_ascii=False, sort_keys=True, indent=2)


def canonical_bytes(payload) -> bytes:
    """Mesma forma canonica da Source: o sha256 declarado tem de ser o deste blob."""
    return (json.dumps(payload, **CANONICAL) + "\n").encode("utf-8")


def write_canonical(path: str, payload) -> tuple[str, int]:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    blob = canonical_bytes(payload)
    with open(path, "wb") as handle:
        handle.write(blob)
    return hashlib.sha256(blob).hexdigest(), len(blob)


def build_partition(root: str, ingestion_date="2026-08-24", warehouse="mad1", *,
                    complete=True, manifest_version=2, failures=None,
                    extra_manifest=None, write_success=None, category_ids=(112,)):
    """Cria uma particao valida em disco e devolve o caminho dela.

    O manifesto declara files[].path relativo a RAIZ DO SNAPSHOT, nao a particao — e a
    obrigacao 4.1 do contrato, e a que um consumidor erra com mais facilidade.
    """
    partition = os.path.join(root, f"ingestion_date={ingestion_date}", f"wh={warehouse}")
    tail = f"ingestion_date={ingestion_date}/wh={warehouse}"
    files = []

    tree = {"results": [{"id": 12, "name": "N1", "categories":
                         [{"id": cid, "name": f"N2-{cid}"} for cid in category_ids]}]}
    sha, size = write_canonical(os.path.join(partition, "categories", "categories.json"), tree)
    files.append({"path": f"{tail}/categories/categories.json", "sha256": sha,
                  "bytes": size, "records": 1, "stage": "categories"})

    for cid in category_ids:
        payload = {"id": cid, "name": f"N2-{cid}", "categories": [
            {"id": 420, "name": "sub", "products": [
                {"id": "4241", "display_name": "P", "categories": [{"id": 12, "name": "N1"}],
                 "price_instructions": {"unit_price": "17.75"}}]}]}
        sha, size = write_canonical(
            os.path.join(partition, "catalog", f"category_id={cid}.json"), payload)
        files.append({"path": f"{tail}/catalog/category_id={cid}.json", "sha256": sha,
                      "bytes": size, "records": 1, "stage": "catalog", "category_id": cid})

    manifest = {
        "manifest_version": manifest_version,
        "run_id": f"2026{ingestion_date.replace('-', '')}T000000Z_{warehouse}",
        "complete": complete,
        "source": {"name": "mercadona_catalog_api", "lang": "es", "wh": warehouse},
        "partition": {"ingestion_date": ingestion_date, "warehouse": warehouse},
        "totals": {"product_rows": len(category_ids), "unique_product_ids": 1,
                   "catalog_files": len(category_ids)},
        "schema_fingerprint": {"sha256": "deadbeef"},
        "files": files,
        "failures": failures or [],
        "anomalies": [],
    }
    if extra_manifest:
        manifest.update(extra_manifest)
    write_canonical(os.path.join(partition, "_manifest.json"), manifest)

    with open(os.path.join(partition, "_run.log"), "w", encoding="utf-8") as handle:
        handle.write("log\n")

    should_write = complete if write_success is None else write_success
    if should_write:
        with open(os.path.join(partition, "_SUCCESS"), "w", encoding="utf-8") as handle:
            handle.write(manifest["run_id"] + "\n")

    return partition


def build_single_axis_partition(root: str, ingestion_date="2026-08-24", *,
                                source_name="ine_population_api", manifest_version=1,
                                complete=True, failures=None, extra_manifest=None,
                                write_success=None, table_ids=(31304,)):
    """Cria uma particao sintetica SEM segundo eixo (formato de uma source como o INE,
    que devolve todas as provincias num unico payload por table_id)."""
    partition = os.path.join(root, f"ingestion_date={ingestion_date}")
    tail = f"ingestion_date={ingestion_date}"
    files = []

    for tid in table_ids:
        payload = {"table_id": tid, "series": [{"COD": "DPOP1", "Nombre": "Total. Madrid. Ambos sexos. Población. Número.",
                   "Data": [{"Anyo": 2025, "Valor": 6779888}]}]}
        sha, size = write_canonical(
            os.path.join(partition, "tables", f"table_id={tid}.json"), payload)
        files.append({"path": f"{tail}/tables/table_id={tid}.json", "sha256": sha,
                      "bytes": size, "records": 1, "stage": "tables"})

    manifest = {
        "manifest_version": manifest_version,
        "run_id": f"{ingestion_date.replace('-', '')}T000000Z",
        "complete": complete,
        "source": {"name": source_name, "lang": ""},
        "partition": {"ingestion_date": ingestion_date},
        "totals": {"series": len(table_ids)},
        "schema_fingerprint": {"sha256": "deadbeef"},
        "files": files,
        "failures": failures or [],
        "anomalies": [],
    }
    if extra_manifest:
        manifest.update(extra_manifest)
    write_canonical(os.path.join(partition, "_manifest.json"), manifest)

    with open(os.path.join(partition, "_run.log"), "w", encoding="utf-8") as handle:
        handle.write("log\n")

    should_write = complete if write_success is None else write_success
    if should_write:
        with open(os.path.join(partition, "_SUCCESS"), "w", encoding="utf-8") as handle:
            handle.write(manifest["run_id"] + "\n")

    return partition
