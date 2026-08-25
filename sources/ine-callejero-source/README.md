# INE Callejero Source

Source de geografia oficial do INE. Incorpora **seções censitárias, unidades
populacionais e vias** (`SECC`, `UP`, `VIAS`, `PSEU`) do Callejero, baixado
manualmente do site do INE — **não há API** para este dataset — e produz snapshots
particionados, reproduzíveis e validáveis, byte a byte idênticos ao download.

Este repositório é uma **Source**, não uma plataforma de transformação. O contrato
completo com o consumidor está em [CONTRACT.md](CONTRACT.md).

Terceira source do monorepo, ao lado de
[`mercadona-catalog-source`](../mercadona-catalog-source/) e
[`ine-population-source`](../ine-population-source/). Pacote irmão independente —
nenhum código é compartilhado. Ver [ARCHITECTURE.md](../../ARCHITECTURE.md).

- **Faz:** localiza os arquivos esperados por província num diretório de entrada ·
  copia verbatim (sem reserialização) · calcula checksum · retoma partições incompletas
  · produz snapshots particionados · registra metadata e proveniência · valida
  integridade, cobertura e coerência estrutural.
- **Não faz:** requisição de rede · parsing de campo · deduplicação · modelagem
  dimensional · Silver/Gold · mapeamento warehouse → província · ingestão de `TRAM` ·
  Spark · dbt · Airflow.

## Requisitos

Python 3.12+ e nada mais. Somente biblioteca padrão. `dependencies = []` no
[pyproject.toml](pyproject.toml) — não há o que instalar, e não há cliente HTTP porque
não há API.

## Localização

Esta Source vive em `sources/ine-callejero-source/` de um monorepo, ao lado da camada
de plataforma que a consome. Ela **permanece independente**: `pyproject.toml` próprio,
`dependencies = []`, suíte própria.

```bash
make -C ../.. callejero-extract IN=../../temp     # da raiz: escreve em <raiz>/data/callejero
make test                                          # daqui: suíte da Source, sem rede
```

## Uso

```bash
make test                        # suíte, sem rede e sem dependências
make extract IN=../../temp       # incorpora os arquivos de IN para a partição de hoje
make validate                    # valida a partição de hoje em modo --strict
make check                       # test + validate
```

Sem `make`, com `PYTHONPATH=src`:

```bash
PYTHONPATH=src python3 -m ine_callejero_source extract --in /caminho/do/download --out data/callejero
PYTHONPATH=src python3 -m ine_callejero_source validate \
    "data/callejero/ingestion_date=$(date -u +%F)" --strict
PYTHONPATH=src python3 -m unittest discover -s tests -t .
```

Instalando o pacote, o console script `ine-callejero-source` fica disponível:

```bash
make install
./venv/bin/ine-callejero-source --version
```

### `extract`

**Não busca rede.** Localiza, dentro de `--in`, os arquivos de cada (província,
dataset) configurados e copia verbatim para a partição.

| Flag | Padrão | Efeito |
|---|---|---|
| `--in` | *(obrigatório)* | diretório onde os arquivos do Callejero foram baixados manualmente. Varrido recursivamente — a estrutura de pastas dos downloads originais é aceita como está |
| `--out` | `data/callejero` | diretório raiz do snapshot |
| `--provinces` | `08,28,41,46` | código(s) de província do INE, separados por vírgula. As 4 dos warehouses |
| `--date` | hoje (UTC) | data da partição. Validada como `YYYY-MM-DD` |
| `--overwrite` | desligado | reescreve a partição, inclusive se estiver completa |

### `validate`

| Argumento | Efeito |
|---|---|
| `partition` | caminho da partição (posicional, obrigatório) |
| `--strict` | além da integridade, falha se houver aviso de largura de linha divergente |

## Estrutura

```
.
├── CONTRACT.md                      # contrato com o consumidor
├── Makefile                         # test / extract / validate / check / venv / install / freeze
├── pyproject.toml                   # metadados; dependencies = []
├── requirements.txt                 # runtime: vazio por construção, verificado por teste
├── requirements-build.txt           # setuptools / wheel / packaging
├── src/ine_callejero_source/
│   ├── __init__.py                  # nome da source, versão do manifesto
│   ├── __main__.py                  # python -m ine_callejero_source
│   ├── cli.py                       # subcomandos e códigos de saída
│   ├── canonical.py                 # manifesto JSON canônico + copia verbatim atômica
│   ├── schema.py                    # fatos estruturais (contagem/largura de linha), sem parsing de campo
│   ├── partition.py                 # identidade, imutabilidade, nome oficial do INE — sem eixo de armazém
│   ├── intake.py                    # localizar em --in, copiar, sem rede
│   └── validate.py                  # validação independente
├── tests/                           # stdlib unittest, sem rede, sem os 110 MB reais
└── venv/                            # ambiente de build, criado por `make venv`; ignorado
```

Os snapshots **não ficam aqui**. A partição que a plataforma consome vive em
`<raiz>/data/callejero/ingestion_date=…/`:

```
data/callejero/ingestion_date=2026-08-25/
├── provinces/
│   ├── province=08/{SECC,UP,VIAS,PSEU}.P08.D<data>.G<geracao>
│   ├── province=28/{SECC,UP,VIAS,PSEU}.P28.D<data>.G<geracao>
│   ├── province=41/...
│   └── province=46/...
├── _manifest.json
├── _SUCCESS
└── _run.log
```

## Forma dos arquivos

**Bytes originais, sem reserialização.** Ao contrário das outras duas Sources (que
reserializam JSON numa forma canônica), os 4 datasets são texto de largura fixa em
ISO-8859-1 (Latin-1) com CRLF — copiados exatamente como o INE os distribui. O SHA-256
registrado é sobre o arquivo original, não sobre uma transformação desta Source.

O layout de coluna medido de cada dataset — reconstruído por evidência, sem "Diseño de
Registro" oficial anexado ao download — está em
[CONTRACT.md § 2](CONTRACT.md#2-conteúdo-garantido). Resumo: `SECC` é só um código de
10 dígitos (província+município+distrito+seção); `VIAS`/`PSEU` têm código + nome de
via/pseudovia em 2-3 formas redundantes; `UP` tem 604 caracteres com o nome do
município numa posição e o nome da unidade populacional (núcleo) noutra. Nenhum CEP,
nenhum bairro oficial.

### `_manifest.json`

Registra `manifest_version`, `run_id`, janela de execução, duração, `complete`,
`source`, `partition`, `config` (`in_dir`, `provinces`, `datasets`, `overwrite`),
`totals` e a lista de arquivos com `sha256`, `bytes`, `line_count` e
`dominant_line_width`. Combinações (província, dataset) que não foram encontradas em
`--in` ficam em `failures[]`; o resumo das execuções anteriores sobre a mesma partição
fica em `history[]`.

`files[].path` é relativo à **raiz do snapshot** (`--out`), começando em
`ingestion_date=…/`.

## Comportamento operacional

**Sem rede, sem throttle, sem retry.** `extract` só lê arquivos locais.

**Retomada.** Um arquivo já landado numa partição incompleta é reaproveitado se ainda
bater com o arquivo atual em `--in`; se divergir, vira falha (`--overwrite` resolve a
favor do que está em `--in` agora).

**Identidade antes do fan-out.** Um manifesto preliminar (`complete: false`) é gravado
assim que a partição é criada, antes de qualquer arquivo ser copiado.

**Códigos de saída.** Ver [CONTRACT.md § 7](CONTRACT.md).

## Restrições da fonte

- **Sem API.** O Callejero só é distribuído para download manual, no site do INE.
- **Publicação semestral**, sem garantia de conteúdo diferente entre publicações —
  molda a orquestração desta Source na plataforma (sem cron; sob demanda), não o
  comportamento desta Source em si.
- **`TRAM` nunca foi inspecionado.** Existe no download; esta Source não sabe nada
  sobre sua estrutura além do nome do arquivo.
- **Layout não documentado oficialmente.** A tabela em CONTRACT.md § 2 foi
  reconstruída medindo os arquivos reais das 4 províncias dos warehouses; pode não
  generalizar para outras províncias sem revalidação.
- **Sem CEP, sem bairro oficial** nos 4 datasets incorporados — confirmado por
  inspeção, não presumido por ausência de busca.

## Validação

`validate` relê todos os arquivos declarados no manifesto, recalculando checksum e
estatísticas de linha em vez de aceitar os valores registrados, e varre o diretório em
busca de arquivos não declarados.

**Integridade** — sempre fatal: manifesto ausente, ilegível ou sem a lista `files` ·
`manifest_version` incompatível · entrada malformada · entrada apontando para fora da
raiz do snapshot · arquivo declarado e ausente · SHA-256, tamanho ou contagem de linhas
divergente · arquivo em disco fora do manifesto · `_SUCCESS` incoerente com `complete`
· combinação (província, dataset) configurada sem arquivo · `totals.files_landed` ou
`totals.bytes` divergentes da releitura · falha registrada em `failures[]`.

**Avisos** — reportados, fatais só com `--strict`: largura de linha dominante diferente
da registrada no manifesto (sinal de possível mudança de layout do INE).

## Dependências e container

**Runtime: nenhuma.** `tests/test_dependencies.py` percorre a AST de todos os módulos
e falha se aparecer qualquer import de terceiro.

| Arquivo | Conteúdo | Quando é necessário |
|---|---|---|
| [requirements.txt](requirements.txt) | vazio, só comentários | nunca |
| [requirements-build.txt](requirements-build.txt) | `setuptools`, `wheel`, `packaging` | apenas para `pip install .` / construir a wheel |

### Container

A imagem **não precisa instalar nenhum pacote de terceiros** — `dependencies = []`
significa que o código roda com `PYTHONPATH=src python -m ine_callejero_source` no
próprio interpretador do Airflow. Sem `Dockerfile` dedicado.

## Testes

Suíte em `unittest` da biblioteca padrão, sem rede e sem os 110 MB de arquivos reais —
`tests/support.py` gera arquivos de amostra com a mesma largura fixa medida contra os
arquivos de produção. Cobre: cópia atômica sob falha, preservação de bytes (inclusive
sequências inválidas em UTF-8, já que o encoding real é Latin-1), recusa de nome de
arquivo fora do padrão do INE, imutabilidade, retomada de partição incompleta,
divergência entre arquivo já landado e `--in` atual, exclusão de `TRAM`, detecção de
órfãos, coerência de `_SUCCESS`, adulteração de totais, e ausência de dependência de
terceiros.

**Validação contra os arquivos reais do Callejero** foi feita manualmente durante o
desenvolvimento desta Source (medição de encoding, largura, offsets de coluna — ver
CONTRACT.md § 2), mas uma execução `make extract`/`make validate --strict` completa
contra os arquivos de produção (110 MB, 4 províncias) ainda não foi registrada aqui com
números. Faça isso antes de depender desta Source em produção, e atualize esta seção —
mesmo padrão de honestidade que o resto deste monorepo.
