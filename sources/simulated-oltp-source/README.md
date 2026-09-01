# Simulated OLTP Source

Gera clientes sintéticos **geograficamente coerentes**: o cliente é inventado, o lugar
onde ele mora não. Cada cliente nasce numa via real de um município real, com o CEP real
daquele tramo, num município que pertence de fato à Área Urbana Funcional do seu armazém.

Pacote irmão isolado, **sem nenhuma dependência de runtime** — só biblioteca padrão. Ver
[CONTRACT.md](CONTRACT.md) para o contrato completo.

## De onde vêm os dados

Esta é a primeira source **derivada** do repositório. As outras três são upstream de dado
externo; esta consome o Silver que elas produziram:

| Insumo | Origem | O que fornece |
|---|---|---|
| `silver_callejero_tramos` + `streets`/`pseudo_addresses`/`population_units` | INE Callejero | via, CEP, faixa de numeração, município |
| `silver_ine_population_by_municipality` (tabela 29005) | INE Tempus3 | peso do município e proporção de sexo |
| `silver_ine_population_series` (tabela 31304) | INE Tempus3 | distribuição etária provincial (proxy) |
| `warehouse_service_area_seed`, `warehouse_province_map_seed` | seeds do dbt | quais municípios cada armazém atende |

**A Source não fala com o Lakehouse.** Toda Source deste repo é FROZEN
(`dependencies = []`, verificado por AST), e `duckdb`/`boto3` são dependências exclusivas
da plataforma por design. A ponte é um contrato **físico**, não um import: a plataforma
materializa o que a Source precisa em três JSON planos, e a Source os lê com `json` da
stdlib.

```
Silver  ──[ retail-platform export-oltp-reference ]──>  data/oltp-reference/
                                                                 │
                                                        (3 JSON planos)
                                                                 │
                                                                 v
                                        [ simulated_oltp_source extract ]  ──> customers.json
```

Mesmo precedente que `ine-callejero-source` já usa, um nível antes: lá `extract` também
não busca rede, incorpora arquivos já preparados em `--in`.

## Requisitos

Python 3.12+. Nada além disso: `PYTHONPATH=src` basta para rodar tudo. Os alvos de venv
existem para empacotar, não para executar.

Antes do primeiro `extract`, o Callejero e a população precisam já estar no Lakehouse
(`make callejero-refresh` e `make ine-refresh` na raiz do repositório).

## Uso

Pela raiz do repositório (recomendado):

```bash
make oltp-export-reference          # uma vez: materializa o Silver (cobre os 4 armazéns)
make oltp-refresh WH=mad1           # extract -> validate -> land -> verify-landing
make oltp-refresh-all               # o mesmo para mad1, bcn1, svq1 e vlc1
```

Direto, sem o Makefile da raiz:

```bash
PYTHONPATH=src python3 -m simulated_oltp_source extract \
  --reference ../../data/oltp-reference/ingestion_date=2026-08-27 \
  --out data/oltp --wh mad1 --date 2026-08-27 --seed 20260827

PYTHONPATH=src python3 -m simulated_oltp_source validate \
  data/oltp/ingestion_date=2026-08-27/wh=mad1 \
  --reference ../../data/oltp-reference/ingestion_date=2026-08-27 --strict
```

### `extract`

| Flag | Padrão | Papel |
|---|---|---|
| `--reference` | — (obrigatório) | diretório dos três JSON de referência |
| `--wh` | — (obrigatório) | `mad1`, `bcn1`, `svq1` ou `vlc1` |
| `--out` | `data/oltp` | raiz do snapshot |
| `--date` | hoje em UTC | `ingestion_date` da partição |
| `--count` | **sem default** | clientes a gerar. Omitido, usa o alvo de `customer_allocation` da referência — população adulta do armazém × taxa de penetração. Um default aqui reintroduziria em silêncio a base dimensionada por ninguém |
| `--seed` | `20260827` | semente: a mesma seed reproduz a mesma saída |
| `--overwrite` | desligado | reescreve partição completa |

### `validate`

`--reference` é **obrigatório** aqui, diferente das outras três Sources. A garantia
central — todo cliente mora num endereço real da AUF do seu armazém — só é verificável
relendo a mesma referência que gerou a partição. `--strict` promove avisos de
distribuição a falha.

## Estrutura

```
sources/simulated-oltp-source/
├── CONTRACT.md
├── README.md
├── Makefile
├── pyproject.toml            (dependencies = [])
├── requirements.txt          (vazio, de propósito)
├── src/simulated_oltp_source/
│   ├── cli.py                argumentos e códigos de saída
│   ├── reference_data.py     lê e desconfia dos três JSON
│   ├── customers_generator.py  amostragem pura, determinística
│   ├── extract.py            orquestra partição + manifesto + _SUCCESS
│   ├── validate.py           integridade + coerência geoespacial
│   ├── partition.py          caminho, tokens, imutabilidade (eixo wh=)
│   ├── schema.py             campos e impressão digital
│   └── canonical.py          forma canônica + escrita atômica
└── tests/                    124 testes, sem rede
```

## Forma dos arquivos

`customers.json` é um array JSON de objetos. Exemplo real, gerado contra o Lakehouse:

```json
{
  "customer_id": "cust_vlc1_000000",
  "wh": "vlc1",
  "province_code": "46",
  "province_name": "Valencia/València",
  "municipality_code": "244",
  "municipality_name": "Torrent",
  "candidate_index": 207249,
  "street_name": "PARE MÉNDEZ",
  "postal_code": "46900",
  "numbering_type": "1",
  "house_number": 37,
  "first_name": "Eva",
  "last_name": "González Gil",
  "sex_label": "Mujeres",
  "birth_year": 1987
}
```

`candidate_index` aponta a linha exata de `address_candidates.json` que forneceu o
endereço — é o único caminho de auditoria exato até o tramo de origem (CEP + rua
identificam sozinhos apenas 13,3% dos tramos; ver CONTRACT.md seção 4).

`house_number` é `null` quando a fonte não sustenta um número real — nunca um número
inventado. O motivo fica legível no campo companheiro `numbering_type` ao lado.

## Comportamento operacional

- **Idempotente por partição.** Uma partição completa é imutável; reexecutar sem
  `--overwrite` recusa em vez de reescrever.
- **Determinístico.** Mesma referência + mesma seed + mesma data ⇒ `customers.json` byte
  a byte idêntico. Verificado inclusive com `PYTHONHASHSEED` diferente entre execuções.
- **Ordem de gravação.** Dados → manifesto → `_SUCCESS`, sempre. Um processo morto no
  meio nunca deixa partição que se anuncia completa sem estar.
- **A seed vive no manifesto**, não repetida em cada registro, e `history[]` guarda a
  seed de cada execução anterior.

## Restrições da fonte

Resumo; os números medidos e a explicação completa estão em
[CONTRACT.md seção 6](CONTRACT.md).

- A **idade é provincial**, usada como proxy: não há faixa etária por município nas
  tabelas ingeridas.
- **Vintages diferentes** entre os dois insumos demográficos: defasagem real de 2 anos e
  6 meses, medida por `reference_date`.
- **Naturezas diferentes**: a idade vem de estimativa (`fk_tipo_dato=2`), o peso
  municipal vem de cifras oficiales.
- **Sexo e idade são amostrados independentemente**; a correlação idade×sexo existente na
  fonte é descartada por construção.
- A escolha do tramo dentro do município é **uniforme, não ponderada** — não existe
  população nessa granularidade, e chamar isso de densidade seria falso.
- 4,2% dos tramos não têm numeração real; 3 são órfãos; 14 têm faixa degenerada.

## Validação

`validate` recalcula tudo em vez de confiar no manifesto, e depois relê a referência:

```
particao ......... data/oltp/ingestion_date=2026-08-27/wh=vlc1
run_id ........... 20260827T130256Z_vlc1
source ........... simulated_oltp
warehouse ........ vlc1
seed ............. 20260827
completa ......... True
arquivos ......... 1/1 lidos e conferidos
orfaos ........... 0
coerencia ........ 200/200 clientes com endereco real na AUF de vlc1

OK: integridade, schema, totais e coerencia geoespacial validados.
```

Injetar um CEP de fora da AUF na partição faz a validação reprovar com código 1 —
verificado ponta a ponta, não só em teste unitário.

## Dependências e container

`requirements.txt` está vazio de propósito e `[project.dependencies]` é `[]`. A promessa
é **verificada, não afirmada**: `tests/test_dependencies.py` percorre a AST de todos os
módulos do pacote e dos testes e reprova qualquer import fora da stdlib, além de conferir
`pyproject.toml`, `requirements.txt` e os metadados instalados.

Um container que rode esta Source precisa apenas de Python 3.12 e do diretório `src/` no
`PYTHONPATH` — mais o diretório de referência montado.

## Testes

```bash
make test                 # 124 testes, sem rede
```

Cobrem, entre outros: mesma seed ⇒ saída byte-idêntica (inclusive sob `PYTHONHASHSEED`
diferente, em subprocesso); seeds diferentes ⇒ saída diferente; 100% dos clientes na AUF
correta; nenhum `numbering_type='0'` gera número; `house_number` nunca é 0; peso
municipal respeitado e escolha de tramo uniforme; referência ausente ou inconsistente
reprova a geração; manifesto e totais batem com o arquivo produzido; e um teste negativo
para cada forma de corromper a partição.
