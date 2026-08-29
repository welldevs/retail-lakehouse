"""Deriva, para os NOMES de municipio que se repetem na Espanha inteira, qual serie do
Tempus3 pertence a qual municipio — pelo CODIGO OFICIAL do INE, nao por heuristica.

POR QUE ISTO EXISTE
-------------------
`silver_ine_population_by_municipality` resolve o codigo do municipio juntando o NOME que
vem em "Nombre" (table_id=29005 nao traz codigo) com `ine_municipality_codes_seed`. O seed
esta escopado a 08/28/41/46 e nao tem nenhuma colisao interna de nome — medido. Mas o lado
RAW e NACIONAL (~8.200 municipios), e la existem homonimos em outras provincias com texto
de "Nombre" IDENTICO. O join por nome casa a linha da nossa provincia E a homonima de fora,
e o resultado e fanout silencioso:

    "Torrent. Total. Total habitantes. Personas. "  ->  DPOP21778 = 90.928 (Valencia, 46244)
    "Torrent. Total. Total habitantes. Personas. "  ->  DPOP7960  =    182 (Girona,  17197)

O docstring de scripts/derive_municipality_codes.py ja previa exatamente isso ("um join
Espanha-inteira por nome sozinho fosse ambiguo para 3 desses 18 nomes"); o que passou
despercebido e que o modelo faz esse join Espanha-inteira, porque o RAW nao e filtrado na
extracao. O teste de grao existente nao pega, porque as duas series tem series_code
DIFERENTE — o grao (ingestion_date, series_code, year, fk_periodo) continua unico.

COMO A AMBIGUIDADE E RESOLVIDA
------------------------------
Fonte: `GET /ES/VALORES_SERIE/{COD}` da API Tempus3 (a mesma API que
ine-population-source ja consome). Para cada serie devolve os VALORES das variaveis que a
definem, e o valor de municipio traz `Codigo` = provincia(2)+municipio(3) — o codigo
oficial, o mesmo esquema do Callejero e dos seeds warehouse_*. Confirmado ao vivo nas duas
series de Torrent: 46244 e 17197.

So sao consultadas as series cujo nome de municipio e ambiguo NO RAW e existe no seed das
4 provincias — hoje 3 nomes, ~18 chamadas. Nao ha como derivar isto offline: o payload de
DATOS_TABLA/29005 tem apenas COD, Nombre, FK_Escala, FK_Unidad e Data. Sem codigo, sem
provincia, sem MetaData (verificado).

Uso:
    python3 scripts/derive_ambiguous_series.py
"""

from __future__ import annotations

import csv
import glob
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

SERIES_ENDPOINT = "https://servicios.ine.es/wstempus/js/ES/VALORES_SERIE"
USER_AGENT = "spark-retail-lakehouse-scripts/1.0 (derive_ambiguous_series; contato via repositorio)"

RAW_GLOB = "data/ine/ingestion_date=*/tables/table_id=29005.json"
CODES_SEED = Path("platform/dbt/seeds/ine_municipality_codes_seed.csv")
OUT_PATH = Path("platform/dbt/seeds/ine_ambiguous_series_seed.csv")

DELAY_SECONDS = 0.5


def latest_raw() -> Path:
    """A particao mais recente ja landada. O RAW e imutavel: reler nao muda nada."""
    found = sorted(glob.glob(RAW_GLOB))
    if not found:
        raise SystemExit(
            f"nenhum payload em {RAW_GLOB}. Rode `make ine-refresh` antes deste script."
        )
    return Path(found[-1])


def municipality_of(series_name: str) -> str:
    return series_name.split(". ")[0].strip()


def sex_of(series_name: str) -> str:
    parts = series_name.split(". ")
    return parts[1].strip() if len(parts) > 1 else ""


def fetch_official_code(series_code: str) -> str:
    """Codigo oficial (provincia+municipio) da serie, pela propria API do INE."""
    request = urllib.request.Request(
        f"{SERIES_ENDPOINT}/{series_code}", headers={"User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))

    # O valor de municipio e o unico cujo Codigo tem 5 digitos numericos: os outros sao
    # sexo ("0"), medida ("0") e unidade (""). Verificado nas series ambiguas reais.
    candidates = [
        item.get("Codigo")
        for item in payload
        if isinstance(item, dict)
        and isinstance(item.get("Codigo"), str)
        and item["Codigo"].isdigit()
        and len(item["Codigo"]) == 5
    ]
    if len(candidates) != 1:
        raise SystemExit(
            f"{series_code}: esperava exatamente um codigo de municipio de 5 digitos, "
            f"achei {candidates}. A forma da resposta mudou — nao vou adivinhar."
        )
    return candidates[0]


def main() -> None:
    raw_path = latest_raw()
    print(f"lendo {raw_path} ...")
    with raw_path.open(encoding="utf-8") as handle:
        raw = json.load(handle)

    with CODES_SEED.open(encoding="utf-8") as handle:
        seed_names = {row["municipality_name"] for row in csv.DictReader(handle)}
    print(f"seed das 4 provincias: {len(seed_names)} nomes")

    # Agrupa por (nome, sexo): mais de uma serie no mesmo par = homonimo nacional.
    grouped: dict[tuple, list[str]] = {}
    for series in raw:
        if not isinstance(series, dict) or "Nombre" not in series:
            continue
        key = (municipality_of(series["Nombre"]), sex_of(series["Nombre"]))
        grouped.setdefault(key, []).append(series["COD"])

    ambiguous = {
        key: sorted(codes)
        for key, codes in grouped.items()
        if len(codes) > 1 and key[0] in seed_names
    }
    nomes = sorted({key[0] for key in ambiguous})
    total_series = sum(len(codes) for codes in ambiguous.values())
    print(f"nomes ambiguos que afetam o escopo: {len(nomes)} -> {nomes}")
    print(f"series a consultar: {total_series}\n")

    if not ambiguous:
        print("nenhuma ambiguidade: o seed sai vazio (so cabecalho), e o modelo nao filtra nada.")

    rows = []
    for (name, sex), codes in sorted(ambiguous.items()):
        for series_code in codes:
            official = fetch_official_code(series_code)
            rows.append(
                {
                    "series_code": series_code,
                    "municipality_name": name,
                    "sex_label": sex,
                    "province_code": official[:2],
                    "municipality_code": official[2:],
                }
            )
            print(f"  {series_code:12} {name!r:20} {sex:8} -> {official}")
            time.sleep(DELAY_SECONDS)

    rows.sort(key=lambda r: (r["municipality_name"], r["sex_label"], r["series_code"]))

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "series_code",
                "municipality_name",
                "sex_label",
                "province_code",
                "municipality_code",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    dentro = sum(1 for r in rows if r["province_code"] in {"08", "28", "41", "46"})
    print(f"\nOK: {len(rows)} linhas escritas em {OUT_PATH}")
    print(f"     {dentro} dentro do escopo (mantidas pelo modelo), "
          f"{len(rows) - dentro} de fora (descartadas pelo modelo)")


if __name__ == "__main__":
    main()
