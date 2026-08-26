"""Deriva o codigo INE (provincia+municipio) de cada municipio dentro do escopo desta
plataforma (08/28/41/46), para juntar com series do INE Tempus3 que so trazem o NOME do
municipio em "Nombre" (ex.: table_id=29005) — mesmo espirito dos outros scripts derive_*,
aplicado a uma fonte ao vivo em vez de um arquivo baixado manualmente.

Fonte: `GET /ES/VALORES_VARIABLE/19` da API Tempus3 do INE (mesma API que
ine-population-source ja consome) — variavel 19 = "Municipios", devolve numa chamada so
`{"Nombre", "Codigo", ...}` para todos os ~8.200 municipios da Espanha. `Codigo` e
provincia(2)+municipio(3), mesmo esquema que o Callejero e os seeds warehouse_* ja usam.
Confirmado ao vivo: a grafia de `Nombre` aqui bate exatamente com a de
DATOS_TABLA/29005 (0 divergencias numa amostra de 1.501 nomes) — NAO bate com a grafia do
Callejero (silver_callejero_population_units.municipality_name), que vem em MAIUSCULA e
move o artigo definido para sufixo entre parenteses (ex. "AMETLLA DEL VALLES (L')" vs
"L'Ametlla del Valles" aqui) — por isso este script busca o codigo na MESMA familia de
endpoint que table_id=29005 usa, em vez de reaproveitar o Callejero ja landado.

Nome de municipio NAO e chave segura no escopo nacional: medido que 18 dos ~8.200
municipios da Espanha compartilham o nome com outro municipio em OUTRA provincia (ex.:
"Arroyomolinos" existe em Madrid [28015] e em Caceres [10023]). Nenhuma colisao acontece
DENTRO das 4 provincias desta plataforma (verificado contra o payload completo antes de
escrever este script) — por isso o seed abaixo, escopado a 08/28/41/46, pode ser juntado
por nome sem ambiguidade real, embora um join Espanha-inteira por nome sozinho fosse
ambiguo para 3 desses 18 nomes.

Uso:
    python3 scripts/derive_municipality_codes.py
"""

from __future__ import annotations

import csv
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

ENDPOINT = "https://servicios.ine.es/wstempus/js/ES/VALORES_VARIABLE/19"
USER_AGENT = "spark-retail-lakehouse-scripts/1.0 (derive_municipality_codes; contato via repositorio)"

# Mesmas 4 provincias ja landadas pelo Callejero (CONTRACT.md da ine-callejero-source) —
# escopo desta plataforma, nao um recorte arbitrario.
TARGET_PROVINCES = {"08", "28", "41", "46"}

TIMEOUT_SECONDS = 120.0
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 5.0


def fetch_municipios() -> list[dict]:
    """GET VALORES_VARIABLE/19 com retry — payload de ~900KB, a rede ja mostrou
    truncamento silencioso em respostas grandes desta API durante esta investigacao."""
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        request = urllib.request.Request(
            ENDPOINT, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                break
            backoff = BACKOFF_BASE_SECONDS * (2**attempt)
            print(f"  tentativa {attempt + 1}/{MAX_RETRIES} falhou ({exc}); aguardando {backoff:.0f}s")
            time.sleep(backoff)
    raise SystemExit(f"nao consegui buscar {ENDPOINT} apos {MAX_RETRIES + 1} tentativas: {last_error}")


def main() -> int:
    print(f"buscando {ENDPOINT} ...")
    municipios = fetch_municipios()
    print(f"{len(municipios)} municipios recebidos (Espanha inteira)")

    codigos = [m["Codigo"] for m in municipios]
    if len(codigos) != len(set(codigos)):
        raise SystemExit("codigo duplicado no payload — inesperado, nao vou gravar um seed inconsistente")
    if any(len(c) != 5 for c in codigos):
        raise SystemExit("codigo fora do padrao de 5 digitos — inesperado, confira o payload")

    in_scope = [m for m in municipios if m["Codigo"][:2] in TARGET_PROVINCES]
    print(f"{len(in_scope)} municipios dentro do escopo (provincias {sorted(TARGET_PROVINCES)})")

    names_in_scope = [m["Nombre"] for m in in_scope]
    if len(names_in_scope) != len(set(names_in_scope)):
        raise SystemExit(
            "nome de municipio duplicado DENTRO do escopo — inesperado (medido antes que nao "
            "acontece); um join por nome deixaria de ser seguro, nao vou gravar o seed assim"
        )

    rows = sorted(
        (
            {
                "province_code": m["Codigo"][:2],
                "municipality_code": m["Codigo"][2:],
                "municipality_name": m["Nombre"],
            }
            for m in in_scope
        ),
        key=lambda r: (r["province_code"], r["municipality_code"]),
    )

    out_path = Path("platform/dbt/seeds/ine_municipality_codes_seed.csv")
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["province_code", "municipality_code", "municipality_name"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nOK: {len(rows)} linhas (municipio) escritas em {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
