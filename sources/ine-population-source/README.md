# INE Population Source

Source de dados de população. Extrai **séries de população por província** da API
pública Tempus3 do INE (`https://servicios.ine.es/wstempus/js`) e produz snapshots
particionados, reproduzíveis e validáveis.

Este repositório é uma **Source**, não uma plataforma de transformação. O contrato
completo com o consumidor está em [CONTRACT.md](CONTRACT.md).

Segunda source do monorepo, ao lado de
[`mercadona-catalog-source`](../mercadona-catalog-source/). As duas são pacotes irmãos
independentes — nenhum código é compartilhado entre elas, cada uma com seu próprio
`pyproject.toml`, `CONTRACT.md` e suíte de testes. Ver
[ARCHITECTURE.md](../../ARCHITECTURE.md) para a razão dessa escolha.

- **Faz:** conecta à API Tempus3 · busca a série completa de um ou mais `table_id` ·
  controla a taxa de requisição · executa retries com backoff · retoma partições
  incompletas · produz snapshots particionados · registra metadata e proveniência · valida
  integridade, cobertura, schema e qualidade.
- **Não faz:** deduplicação · modelagem dimensional · normalização · Silver/Gold ·
  extração de código de província · cruzamento com outra fonte · métricas · Spark · dbt ·
  Airflow.

## Requisitos

Python 3.12+ e nada mais. Somente biblioteca padrão. `dependencies = []` no
[pyproject.toml](pyproject.toml) — não há o que instalar. A API Tempus3 é JSON puro sobre
HTTPS, sem necessidade de cliente de terceiros nem de chave de API.

## Localização

Esta Source vive em `sources/ine-population-source/` de um monorepo, ao lado da camada de
plataforma que a consome. Ela **permanece independente**: `pyproject.toml` próprio,
`dependencies = []`, suíte própria, e nada fora daqui é importado.

Comandos com `make` neste diretório continuam funcionando isolados, e é assim que a
fronteira é verificada. Mas no dia a dia use o **Makefile da raiz**, que aponta `--out`
para o `data/` compartilhado do monorepo:

```bash
make -C ../.. ine-extract ine-validate      # da raiz: escreve em <raiz>/data/ine
make test                                   # daqui: suíte da Source, sem rede
```

## Uso

```bash
make test                  # suíte, sem rede e sem dependências
make extract               # snapshot de hoje (UTC) da tabela 31304
make validate              # valida a partição de hoje em modo --strict
make check                 # test + validate
```

Sem `make`, com `PYTHONPATH=src`:

```bash
PYTHONPATH=src python3 -m ine_population_source extract --out data/ine
PYTHONPATH=src python3 -m ine_population_source validate \
    "data/ine/ingestion_date=$(date -u +%F)" --strict
PYTHONPATH=src python3 -m unittest discover -s tests -t .
```

Instalando o pacote, o console script `ine-population-source` fica disponível com os
mesmos subcomandos, sem precisar de `PYTHONPATH`:

```bash
make install               # cria venv/, instala ferramentas de build e o pacote (-e .)
./venv/bin/ine-population-source --version
```

### `extract`

| Flag | Padrão | Efeito |
|---|---|---|
| `--out` | `data/ine` | diretório raiz do snapshot |
| `--tables` | `31304` | `table_id`(s) do INE Tempus3, separados por vírgula. `31304` é o único cujo formato de `Nombre` foi confirmado — ver [CONTRACT.md § 2](CONTRACT.md) |
| `--date` | hoje (UTC) | data da partição. Validada como `YYYY-MM-DD` |
| `--delay` | `0.5` | intervalo mínimo entre requisições, em segundos. Boa prática de cliente HTTP, não resposta a bloqueio medido — ver "Restrições da fonte" |
| `--timeout` | `30.0` | timeout de cada requisição |
| `--max-retries` | `5` | tentativas extras por requisição |
| `--overwrite` | desligado | reescreve a partição, inclusive se estiver completa |

### `validate`

| Argumento | Efeito |
|---|---|
| `partition` | caminho da partição (posicional, obrigatório) |
| `--strict` | além da integridade, falha se houver série sem nenhum ponto com `Valor` |

## Estrutura

```
.
├── CONTRACT.md                      # contrato com o consumidor
├── Makefile                         # test / extract / validate / check / venv / install / freeze
├── pyproject.toml                   # metadados; dependencies = []
├── requirements.txt                 # runtime: vazio por construção, verificado por teste
├── requirements-build.txt           # setuptools / wheel / packaging
├── src/ine_population_source/
│   ├── __init__.py                  # nome da source, versão do manifesto
│   ├── __main__.py                  # python -m ine_population_source
│   ├── cli.py                       # subcomandos e códigos de saída
│   ├── canonical.py                 # forma canônica + escrita atômica
│   ├── http_client.py               # cliente sequencial, throttle e backoff
│   ├── schema.py                    # navegação defensiva e fingerprint
│   ├── partition.py                 # identidade, imutabilidade, _SUCCESS — sem eixo de armazém
│   ├── extract.py                   # uma busca por table_id, sem crawl de árvore
│   └── validate.py                  # validação independente
├── tests/                           # stdlib unittest, sem rede
└── venv/                            # ambiente de build, criado por `make venv`; ignorado
```

Os snapshots **não ficam aqui**. A partição que a plataforma consome vive em
`<raiz>/data/ine/ingestion_date=…/`, um nível acima (não dois: sem eixo de armazém):

```
data/ine/ingestion_date=2026-08-25/
├── tables/table_id=31304.json
├── _manifest.json
├── _SUCCESS
└── _run.log
```

## Forma dos arquivos

Os arquivos contêm o payload da API **íntegro em conteúdo** — nenhum campo é adicionado,
removido, renomeado ou convertido de tipo — mas **reserializados de forma canônica**:
`json.dumps(ensure_ascii=False, sort_keys=True, indent=2)` mais newline final. Portanto
**não são os bytes originais da resposta HTTP**; o SHA-256 registrado é o do arquivo
canônico produzido pela Source, não o da resposta HTTP.

Forma de uma série (`tables/table_id=31304.json` é uma **lista** dessas):

```json
{
  "COD": "DPOP1",
  "Nombre": "Total. Madrid. Ambos sexos. Población. Número.",
  "FK_Unidad": 3,
  "FK_Escala": 1,
  "Data": [
    {"Fecha": 1735686000000, "FK_Periodo": 1, "Anyo": 2025, "Valor": 6779888}
  ]
}
```

`Data[]` cobre muitos anos numa única resposta — cada snapshot já é a série histórica
inteira, não um incremento.

### `_manifest.json`

Registra `manifest_version`, `run_id`, janela de execução, duração, `complete`, `source`,
`partition`, `config`, `totals`, `schema_fingerprint` e a lista de arquivos com `sha256`,
`bytes` e `records` (contagem de séries). `table_id`s que falharam ficam em `failures[]`;
eventos tratados durante a extração ficam em `anomalies[]`; o resumo das execuções
anteriores sobre a mesma partição fica em `history[]`.

`files[].path` é relativo à **raiz do snapshot** (`--out`), não à partição: começa em
`ingestion_date=…/`. Para resolvê-lo a partir do caminho da partição, suba **um** nível
(não dois: não há eixo de armazém entre a raiz e a partição).

## Comportamento operacional

**Throttle.** Requisições sequenciais, limitadas a `1/--delay` req/s. Ao contrário da
Mercadona Catalog Source, isto **não é resposta a um bloqueio medido** — não há
concorrência nenhuma aqui para medir, já que uma execução busca um punhado de `table_id`
sequencialmente. É boa prática de cliente HTTP, mantida por padrão.

**Retry.** Status `408, 429, 500, 502, 503, 504` e qualquer erro não-HTTP (timeout, reset
de conexão, corpo JSON inválido) são repetidos com backoff exponencial
`5s → 10s → 20s → 40s → 60s` (teto de 60 s), até `--max-retries`. `403` **não** está entre
os retentáveis aqui — diferente da Mercadona, onde há evidência medida de bloqueio
intermitente sob rajada; sem essa evidência para o INE, tratar `403` como transitório
seria inventar uma política sem base.

**Retomada.** Um arquivo já presente na partição não é rebaixado, salvo com `--overwrite`.
É rebaixado da fonte automaticamente quando está ilegível, truncado, com forma inesperada,
fora da forma canônica ou divergente do manifesto anterior — nos três últimos casos o
evento entra em `anomalies[]`.

**Identidade antes do fan-out.** Um manifesto preliminar (`complete: false`) é gravado
assim que a partição é criada, antes de qualquer `table_id` ser buscado.

**Inventário.** Ao final, a extração varre `tables/` e declara no manifesto os arquivos
válidos que aquela execução não visitou (caso de um `table_id` removido de `--tables`
entre execuções, mas cujo arquivo permanece em disco).

**Códigos de saída.** Ver [CONTRACT.md § 7](CONTRACT.md).

## Restrições da fonte

- **Rate-limit não foi medido contra esta fonte.** Ao contrário da Mercadona (403
  intermitente sob concorrência, medido e documentado), esta Source não tem cenário local
  de fan-out concorrente que motivasse medir isso ainda — uma execução é sequencial por
  natureza. Se um dia isso mudar (ex.: muitos `table_id` em paralelo), meça antes de
  presumir que o throttle atual basta.
- **Só `table_id: 31304` foi confirmado.** Consultei `GRUPOS_TABLA/31304` (dimensões:
  `Sexo`, `Edad`, `Provincias`) e `DATOS_TABLA/31304` (formato de `Nombre`) diretamente na
  API antes de fixar este contrato. Outros `table_id` do INE (ex.: 9688–9691) podem ser
  passados via `--tables`, mas o formato de `Nombre` para eles não foi verificado — pode
  divergir do documentado em [CONTRACT.md § 2](CONTRACT.md).
- **Cadência de publicação é irregular.** O INE atualiza Cifras de População de forma não
  fixa — historicamente da ordem de 1-2 vezes ao ano, às vezes com meses entre uma
  publicação e outra. Isto molda a orquestração desta Source na plataforma (sem cron
  fixo; ver `orchestration/`), não o comportamento desta Source em si — ela extrai o que
  a API tiver no momento em que for chamada.
- **Sem autenticação.** A API Tempus3 é pública; nenhuma chave ou credencial é enviada.

## Validação

`validate` relê e reparseia todos os arquivos declarados no manifesto, recalculando
checksums, contagens e fingerprint em vez de aceitar os valores registrados, e varre o
diretório em busca de arquivos não declarados.

**Integridade** — sempre fatal: manifesto ausente, ilegível ou sem a lista `files` ·
`manifest_version` incompatível · entrada de manifesto malformada (sem `path`) · entrada
apontando para fora da raiz do snapshot · arquivo declarado e ausente · SHA-256 ou tamanho
divergente · JSON inválido · contagem de registros divergente · arquivo em disco fora do
manifesto, em qualquer profundidade · `_SUCCESS` incoerente com `complete` · `table_id`
configurado sem arquivo · `schema_fingerprint` divergente · `totals.series_count`,
`totals.data_point_count`, `totals.table_files` ou `totals.bytes` divergentes da releitura
· falha registrada em `failures[]`.

**Avisos** — reportados, nunca fatais: cada entrada de `anomalies[]`.

**Qualidade** — medida e reportada; fatal apenas com `--strict`: série sem nenhum ponto
com `Valor`.

## Dependências e container

**Runtime: nenhuma.** Todo import do pacote e dos testes é da biblioteca padrão do
Python 3.12, e `[project.dependencies]` está vazio. `tests/test_dependencies.py` percorre
a AST de todos os módulos e falha se aparecer qualquer import de terceiro.

| Arquivo | Conteúdo | Quando é necessário |
|---|---|---|
| [requirements.txt](requirements.txt) | vazio, só comentários | nunca — não há dependência de runtime |
| [requirements-build.txt](requirements-build.txt) | `setuptools`, `wheel`, `packaging` | apenas para `pip install .` / construir a wheel |

### Container

A imagem **não precisa instalar nenhum pacote de terceiros**, pelo mesmo motivo da
Mercadona Catalog Source: `dependencies = []` significa que o código roda com
`PYTHONPATH=src python -m ine_population_source` no próprio interpretador do Airflow, sem
`pip install` nenhum. Não existe `Dockerfile` dedicado, e não é necessário pelo mesmo
raciocínio documentado no README da Mercadona Catalog Source.

## Testes

Suíte em `unittest` da biblioteca padrão, sem acesso à rede — o cliente HTTP é substituído
por um duplo que responde de memória, exercitando o mesmo código de extração. Cobre:
escrita atômica sob falha e sob `KeyboardInterrupt`, determinismo da forma canônica,
recusa de travessia de caminho, imutabilidade, retomada, arquivo truncado/vazio/adulterado,
mudança de forma da fonte, detecção de órfãos, coerência de `_SUCCESS`, adulteração de
totais e de fingerprint, e ausência de dependência de terceiros.

**Validação contra a API real** ainda não foi registrada neste README com números —
diferente da Mercadona Catalog Source, que tem execuções reais documentadas. A tabela
`GRUPOS_TABLA/31304` e uma amostra de `DATOS_TABLA/31304` foram consultadas para fixar o
contrato (ver [CONTRACT.md § 2](CONTRACT.md)), mas uma extração `make extract` completa
contra a rede real, seguida de `make validate --strict`, ainda não foi executada e
registrada aqui. Faça isso antes de depender desta Source em produção, e atualize esta
seção com o resultado — mesmo padrão de honestidade que o resto deste monorepo.
