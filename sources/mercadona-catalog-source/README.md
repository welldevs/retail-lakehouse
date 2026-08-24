# Mercadona Catalog API Source

Source de dados de catálogo. Extrai **produto, categoria e preço** da API pública da
Mercadona (`https://tienda.mercadona.es/api`) e produz snapshots particionados,
reproduzíveis e validáveis.

Este repositório é uma **Source**, não uma plataforma de transformação. O contrato completo
com o consumidor está em [CONTRACT.md](CONTRACT.md).

- **Faz:** conecta à API · descobre a árvore de categorias · extrai produtos e preços ·
  controla a taxa de requisição · executa retries com backoff · retoma partições
  incompletas · produz snapshots particionados · registra metadata e proveniência · valida
  integridade, cobertura, schema e qualidade.
- **Não faz:** deduplicação · modelagem dimensional · normalização · Silver/Gold ·
  enriquecimento · histórico · métricas · Spark · dbt · Airflow · Kafka · Snowflake · S3.

## Requisitos

Python 3.12+ e nada mais. Somente biblioteca padrão. `dependencies = []` no
[pyproject.toml](pyproject.toml) — não há o que instalar.

## Localização

Esta Source vive em `sources/mercadona-catalog-source/` de um monorepo, ao lado da camada
de plataforma que a consome. Ela **permanece independente**: `pyproject.toml` próprio,
`dependencies = []`, suíte própria, e nada fora daqui é importado.

Comandos com `make` neste diretório continuam funcionando isolados, e é assim que a
fronteira é verificada. Mas no dia a dia use o **Makefile da raiz**, que aponta `--out`
para o `data/` compartilhado do monorepo:

```bash
make -C ../..  extract validate      # da raiz: escreve em <raiz>/data/source
make test                            # daqui: suíte da Source, sem rede
```

Rodar `make extract` **daqui** cria um `data/source/` dentro deste diretório, separado do
`data/` da raiz. Útil para teste isolado, mas não é a partição que a plataforma consome.

## Uso

```bash
make test                  # 145 testes, sem rede e sem dependências
make extract               # snapshot de hoje (UTC), ~4 min
make validate              # valida a partição de hoje em modo --strict
make check                 # test + validate
```

Sem `make`, com `PYTHONPATH=src`:

```bash
PYTHONPATH=src python3 -m mercadona_catalog_source extract  --out data/source
PYTHONPATH=src python3 -m mercadona_catalog_source validate \
    "data/source/ingestion_date=$(date -u +%F)/wh=mad1" --strict
PYTHONPATH=src python3 -m unittest discover -s tests -t .
```

Instalando o pacote, o console script `mercadona-catalog-source` fica disponível com os
mesmos subcomandos, sem precisar de `PYTHONPATH`:

```bash
make install               # cria venv/, instala ferramentas de build e o pacote (-e .)
./venv/bin/mercadona-catalog-source --version
./venv/bin/mercadona-catalog-source validate "data/source/ingestion_date=$(date -u +%F)/wh=mad1" --strict
```

### `extract`

| Flag | Padrão | Efeito |
|---|---|---|
| `--out` | `data/source` | diretório raiz do snapshot |
| `--wh` | `mad1` | armazém. Validado como token `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`; a existência do código só é conhecida na resposta da API. Valores que respondem `200`: `mad1`, `mad2`, `bcn1`, `vlc1`, `svq1`, `alc1`, `zgz1` |
| `--lang` | `es` | idioma da API. **Fixo por partição**: reabrir com outro idioma é recusado com código 2, salvo `--overwrite`, que reescreve a partição inteira |
| `--date` | hoje (UTC) | data da partição. Validada como `YYYY-MM-DD` |
| `--delay` | `1.5` | intervalo mínimo entre requisições, em segundos |
| `--timeout` | `30.0` | timeout de cada requisição |
| `--max-retries` | `5` | tentativas extras por requisição |
| `--limit` | — | processa apenas as N primeiras categorias de nível 2. **`N ≥ 1`**; `0` e negativos são recusados com código 2. Marca a partição como `complete: false` |
| `--overwrite` | desligado | reescreve a partição, inclusive se estiver completa |

### `validate`

| Argumento | Efeito |
|---|---|
| `partition` | caminho da partição (posicional, obrigatório) |
| `--strict` | além da integridade, falha se houver registro com campo de escopo ausente |

## Estrutura

```
.
├── CONTRACT.md                      # contrato com o consumidor
├── Makefile                         # test / extract / validate / check / venv / install / freeze
├── pyproject.toml                   # metadados; dependencies = []
├── requirements.txt                 # runtime: vazio por construção, verificado por teste
├── requirements-build.txt           # setuptools / wheel / packaging, congelados
├── src/mercadona_catalog_source/
│   ├── __init__.py                  # nome da source, versão do manifesto
│   ├── __main__.py                  # python -m mercadona_catalog_source
│   ├── cli.py                       # subcomandos e códigos de saída
│   ├── canonical.py                 # forma canônica + escrita atômica
│   ├── http_client.py               # cliente sequencial, throttle e backoff
│   ├── schema.py                    # navegação defensiva e fingerprint
│   ├── partition.py                 # identidade, imutabilidade, _SUCCESS
│   ├── extract.py                   # as duas etapas de extração
│   └── validate.py                  # validação independente
├── tests/                           # 145 testes, stdlib unittest, sem rede
└── venv/                            # ambiente de build, criado por `make venv`; ignorado
```

Os snapshots **não ficam aqui**. A partição que a plataforma consome vive em
`<raiz>/data/source/ingestion_date=…/wh=…/`, dois níveis acima, com esta forma:

```
data/source/ingestion_date=2026-08-24/
└── wh=mad1/
    ├── categories/categories.json
    ├── catalog/category_id=<id>.json     (151 arquivos)
    ├── _manifest.json
    ├── _SUCCESS
    └── _run.log
```

O `.gitignore` fica na raiz do monorepo e cobre `venv/`, `data/` e os temporários da
escrita atômica.

## Forma dos arquivos

Os arquivos contêm o payload da API **íntegro em conteúdo** — nenhum campo é adicionado,
removido, renomeado ou convertido de tipo (preços permanecem strings, ex.: `"17.75"`) — mas
**reserializados de forma canônica**: `json.dumps(ensure_ascii=False, sort_keys=True,
indent=2)` mais newline final.

Portanto **não são os bytes originais da resposta HTTP**. Não se preservam: ordem original
das chaves, formatação, escapes Unicode e a grafia léxica de floats; chaves duplicadas, se
existirem, colapsam na última ocorrência. Nada da camada HTTP (headers, `ETag`,
`Content-Encoding`) é guardado.

O **SHA-256 registrado é o do arquivo canônico produzido pela Source**, não o da resposta
HTTP. É a canonicalização que torna o digest estável entre execuções com o mesmo conteúdo —
bytes crus produziriam digests diferentes para dados idênticos a cada reordenação de chaves
do servidor. A escrita é atômica (temporário + `fsync` + `os.replace`): um processo
interrompido nunca deixa arquivo parcial no lugar final.

Exemplo real de `category_id=112.json`: nível 2 `Aceite, vinagre y sal` → subgrupo
`Aceite de oliva` → produto `Aceite de oliva 0,4º Hacendado` (id `4241`, 5 L) →
`unit_price` `"17.75"`, `reference_price` `"3.550"` por `L`. O mesmo `display_name` aparece
no id `4240` (1 L, `"3.80"`): a chave é `id`, não o nome. Nenhum dos 153 arquivos contém
`currency`, `EUR` ou `€`.

### `_manifest.json`

Registra `manifest_version`, `run_id`, janela de execução, duração, `complete`, `source`,
`partition`, `config`, `totals`, `schema_fingerprint` e a lista de arquivos com `sha256`,
`bytes` e `records`. Categorias que falharam ficam em `failures[]`; eventos tratados
durante a extração ficam em `anomalies[]`; o resumo das execuções anteriores sobre a mesma
partição fica em `history[]`.

`files[].path` é relativo à **raiz do snapshot** (`--out`), não à partição: começa em
`ingestion_date=…/wh=…/`. Para resolvê-lo a partir do caminho da partição, suba dois
níveis.

## Execução registrada

Ambiente: Python 3.12.7, Linux. Três partições em disco, todas `wh=mad1`, todas validadas.

| Métrica | `2026-08-15` | `2026-08-16` | `2026-08-24` |
|---|---|---|---|
| `run_id` | `20260815T143445Z_mad1` | `20260816T214655Z_mad1` | `20260824T140006Z_mad1` |
| Requisições HTTP | 152 (1 árvore + 151 categorias) | 152 | 152 |
| Retries / Falhas | 0 / 0 | 0 / 0 | 0 / 0 |
| Duração | 227,3 s | 227,0 s | 227,8 s |
| Categorias nível 1 / nível 2 | 26 / 151 | 26 / 151 | 26 / 151 |
| Arquivos de catálogo | 151 | 151 | 151 |
| Linhas de produto | 4.600 | 4.599 | 4.581 |
| Produtos únicos | 4.329 | 4.328 | 4.311 |
| Volume | 8.291.070 bytes | 8.289.262 bytes | 8.261.218 bytes |
| `schema_fingerprint` | `37a3d95d…` | `37a3d95d…` | `37a3d95d…` (igual nas três) |
| `complete` | `true`, com `_SUCCESS` | `true`, com `_SUCCESS` | `true`, com `_SUCCESS` |

Todas em `manifest_version: 2`, todas com 152 arquivos declarados. A partição de
`2026-08-15` foi gravada antes da introdução de `anomalies[]` e não tem o campo — leia como
lista vazia, conforme [CONTRACT.md § 1](CONTRACT.md).

**Há um vão de 8 dias** entre `2026-08-16` e `2026-08-24`. Os dias `08-17` a `08-23` **não
existem e não podem ser recuperados**: a API serve apenas o preço de hoje, e a Source não
tem como reconstruir o passado. É a razão pela qual a extração precisa ser agendada, e não
disparada à mão.

**Validação** (`--strict`, todas): 152/152 arquivos conferidos por checksum, 0 órfãos,
0 anomalias, cobertura 151/151 categorias, schema igual, totais reconferidos, 0 produtos
sem nome, 0 sem preço, 0 sem categoria — 100,00% de completude. Saída `OK`, código 0.

**Suite**: 145 testes, `OK`, sem acesso à rede.

**Imutabilidade**: reexecutar `extract` sobre uma partição completa retorna código 2 com
`particao ja esta completa e e imutavel`.

### Variação observada entre partições

Mudança temporal na fonte, medida sobre os snapshots — não inferida.

**Janela de 1 dia**, `2026-08-15` → `2026-08-16`:

- **17 preços alterados**, quase todos hortifruti: `Coliflor` 3.50 → 3.63, `Patata`
  0.42 → 0.44, `Mango` 1.50 → 1.46, `Piña` 3.72 → 3.66, `Melocotón amarillo` 0.57 → 0.60.
- **2 produtos saíram**, **1 entrou**: `Chicles gragea original frutas sin azúcar Hacendado`
  trocou de `id` 65135 (1.00) para 24537 (1.95) — o mesmo `display_name`, na mesma categoria
  (95) e no mesmo subgrupo (389), com outra chave e outro preço. Nem o nome nem o `id` servem
  como identidade de negócio: a fonte não diz se é o mesmo item rechaveado ou um item
  retirado e outro lançado.
- `schema_fingerprint` idêntico: a forma da resposta não mudou entre os dois dias.

**Janela de 8 dias**, `2026-08-16` → `2026-08-24` — a mesma fonte, medida num intervalo
maior, muda muito mais:

- **152 preços alterados** entre os 4.295 ids presentes nos dois snapshots.
- **33 ids saíram**, **16 entraram**. Destes 16, **5 têm `display_name` que já existia** em
  `08-16` — ou seja, a fonte reemitiu itens sob outra chave, sem sinalizar. Ver
  [CONTRACT.md § 4.5](CONTRACT.md): `source_product_id` não é identidade de negócio.
- `schema_fingerprint` idêntico também aqui.

**`price_decreased` é falso em 100% das linhas nas três partições**, apesar dos 152 preços
alterados. O campo que a fonte oferece para sinalizar variação não sinaliza nada: a única
forma de detectar mudança de preço é o **diff de snapshots**. Fato da fonte, registrado
aqui; a Source não o interpreta.

A Source **não interpreta** isso: ela entrega os snapshots. Qualquer leitura de
histórico, SCD ou variação de preço é da camada seguinte.

### Repetição de linhas

Números da partição `2026-08-15`. As 4.600 linhas contêm 4.329 produtos únicos, e as
**271 linhas excedentes** são:

- **261** — o mesmo produto em **duas categorias de nível 2 diferentes** (4.068 produtos em
  1 categoria, 261 em 2; nenhum em 3 ou mais).
- **10** — o mesmo produto em **dois subgrupos do mesmo arquivo**: 8 refrigerantes de cola
  em `Cola sin cafeína` / `Cola zero` (categoria 158), 1 em `Cepillo de dientes` /
  `Pasta de dientes` (186) e 1 em `Pollo` / `Pavo y otras aves` (38).

Semântica da fonte, preservada deliberadamente. A resolução é da camada posterior.

### Campos de preço observados

Presentes em 100% dos registros das três partições: `unit_price`, `reference_price`,
`reference_format`, `bulk_price`, `previous_unit_price`, `price_decreased`,
`tax_percentage`.

Preenchimento medido:

| | `2026-08-15` | `2026-08-16` | `2026-08-24` |
|---|---|---|---|
| `unit_price` string, não-nulo | 100% | 100% | 100% |
| `tax_percentage` não-nulo | 100% | 100% | 100% |
| `previous_unit_price` não-nulo | 205 (4,5%) | 211 (4,6%) | 194 (4,2%) |
| `price_decreased` verdadeiro | **0** | **0** | **0** |

Duas observações factuais que o consumidor precisa saber, ambas medidas e nenhuma
interpretada aqui:

1. **`previous_unit_price` vem com espaços à esquerda** — `"       18.75"` — em **100% dos
   não-nulos** (205/205, 211/211, 194/194). Aparar antes de converter é obrigação do
   consumidor; motores diferentes toleram o padding de formas diferentes.
2. **`iva` é nulo em 100% das linhas** enquanto `tax_percentage` está preenchido. O campo
   útil é o segundo.

## Comportamento operacional

**Throttle.** Requisições sequenciais, limitadas a `1/--delay` req/s, medindo do início da
requisição anterior. Com o padrão de 1,5 s, a execução registrada levou 227,3 s para 152
requisições, com 0 bloqueios.

**Retry.** Status `403, 408, 429, 500, 502, 503, 504` e qualquer erro não-HTTP (timeout,
reset de conexão, corpo JSON inválido) são repetidos com backoff exponencial
`5s → 10s → 20s → 40s → 60s` (teto de 60 s), até `--max-retries`. Demais status falham
imediatamente — é o caso de `404`/`410` dos ids de nível 1.

**Retomada.** Um arquivo já presente na partição não é rebaixado, salvo com `--overwrite`.
É rebaixado da fonte automaticamente quando está ilegível, truncado, com forma inesperada,
**fora da forma canônica** ou **divergente do manifesto anterior** — nos três últimos casos
o evento entra em `anomalies[]`. Uma partição é imutável: conteúdo alterado em disco não é
insumo válido e não é promovido a nova verdade por uma reexecução.

**Identidade antes do fan-out.** Um manifesto preliminar (`complete: false`) é gravado logo
após a árvore de categorias. Sem ele, uma execução interrompida deixaria a partição sem
manifesto — e as travas de imutabilidade e de idioma fixo, que dependem dele, ficariam
desligadas na retomada.

**Inventário.** Ao final, a extração varre `catalog/` e declara no manifesto os arquivos
válidos que aquela execução não visitou (caso de `--limit`, ou de categoria que saiu da
árvore da fonte). Assim o manifesto descreve a partição, não apenas o que a execução tocou.

**Códigos de saída.** Ver [CONTRACT.md § 7](CONTRACT.md). São do processo que executa; o
consumidor a jusante decide por `_SUCCESS` e `_manifest.json`.

## Restrições da fonte

Verificadas contra a API, não presumidas:

- **Só ids de categoria de nível 2** são acessíveis em `/api/categories/{id}/`. Ids de
  nível 1 respondem `404`/`410`. A árvore é achatada para nível 2, com deduplicação por id.
- **Comportamento sob carga.** Com 8 threads, 152 requisições em 9 s produziram ~20% de
  `403`, e o host passou a responder `403` de forma intermitente por alguns minutos. As
  mesmas 152 requisições sequenciais a 1,5 s tiveram 0 falhas, em execuções repetidas.
  **Comportamento além de 152 requisições por execução não foi medido**, e não se
  distinguiu WAF de rate limit de borda — observou-se apenas o status.
- **`wh` altera o sortimento.** Na amostra medida — categoria 112, `mad1` vs `bcn1` — os 35
  produtos comuns têm preço idêntico, e a diferença está em quais produtos existem (1
  exclusivo em `mad1`, 3 em `bcn1`). É observação de **1 categoria e 2 armazéns**, não
  propriedade demonstrada da fonte. `pmi1` responde `404`.
- **`robots.txt` de `tienda.mercadona.es` declara `Disallow: /api`**, e o cliente envia um
  **User-Agent de navegador Chrome**, não um identificador de robô
  ([http_client.py](src/mercadona_catalog_source/http_client.py)). Usar esta rota como
  fonte é decisão do operador, tomada com esses dois fatos à vista. A Source opera no menor
  impacto possível — sequencial, 152 requisições por execução, sem paralelismo — mas isso
  não anula a diretiva. Note ainda que `403` está entre os status retentáveis: um bloqueio
  deliberado é repetido até 5 vezes. Alternativa liberada por `robots.txt` (`Disallow:`
  vazio): o espelho `mercadonacercademi.com`, ao custo de ~2 h por extração e sem preço nas
  páginas de listagem.
- **Sem chave cross-source.** `ean`, `gtin`, `barcode`, `brand` e `origin` não existem nos
  payloads deste endpoint; `product.id` é a **chave da fonte**, sem garantia de estabilidade
  temporal — identifica o registro dentro do snapshot, mas a fonte pode reemitir o mesmo item
  comercial sob outro id sem sinalizar ([CONTRACT.md § 4.5](CONTRACT.md)). O endpoint
  `/api/products/{id}/` os traria, ao custo de +4.329 requisições (~1h50 no throttle
  seguro) contra um host com `Disallow: /api`, e está fora do escopo produto/categoria/preço.
- **Sem fato transacional.** Nenhum endpoint conhecido da API expõe venda, pedido ou
  estoque.

## Validação

`validate` relê e reparseia todos os arquivos declarados no manifesto, recalculando
checksums, contagens e fingerprint em vez de aceitar os valores registrados, e varre o
diretório em busca de arquivos não declarados.

**Integridade** — sempre fatal: manifesto ausente, ilegível ou sem a lista `files` ·
`manifest_version` incompatível · entrada de manifesto malformada (sem `path`) · entrada
apontando para fora da raiz do snapshot · arquivo declarado e ausente · SHA-256 ou tamanho
divergente · JSON inválido · contagem de registros divergente, inclusive a da árvore ·
arquivo em disco fora do manifesto, em qualquer profundidade · `_SUCCESS` incoerente com
`complete` · árvore de categorias ausente ou ilegível · categoria de nível 2 sem arquivo
(quando a partição não é parcial) · `schema_fingerprint` divergente ·
`totals.product_rows`, `totals.unique_product_ids`, `totals.catalog_files` ou
`totals.bytes` divergentes da releitura · nenhum produto lido havendo arquivos de catálogo
· falha registrada em `failures[]`.

**Avisos** — reportados, nunca fatais: cobertura incompleta em partição `--limit`; cada
entrada de `anomalies[]`.

**Qualidade** — medida e reportada; fatal apenas com `--strict`: produto sem
`display_name`, sem `price_instructions.unit_price` ou sem linhagem de categoria.

Numa partição parcial (`--limit`), a cobertura incompleta vira **aviso**, não erro.

## Dependências e container

**Runtime: nenhuma.** Todo import do pacote e dos testes é da biblioteca padrão do
Python 3.12, e `[project.dependencies]` está vazio. Isso não é uma afirmação de
documentação: `tests/test_dependencies.py` percorre a AST de todos os módulos e falha se
aparecer qualquer import de terceiro, além de conferir que `requirements.txt` não lista
pacote nenhum e que o metadata instalado declara `requires: None`.

| Arquivo | Conteúdo | Quando é necessário |
|---|---|---|
| [requirements.txt](requirements.txt) | vazio, só comentários | nunca — não há dependência de runtime |
| [requirements-build.txt](requirements-build.txt) | `setuptools==84.0.0`, `wheel==0.48.0`, `packaging==26.3` | apenas para `pip install .` / construir a wheel |

`make freeze` regrava o segundo arquivo a partir do `venv/`, excluindo o `pip` e o próprio
pacote — que não existe no PyPI e quebraria um `pip install -r`.

### Container

A imagem **não precisa instalar nenhum pacote de terceiros**. Dois modos, ambos válidos:

| Modo | Como | Custo |
|---|---|---|
| Sem instalação | copiar `src/` e rodar com `PYTHONPATH=src python -m mercadona_catalog_source` | zero `pip install`; imagem = `python:3.12-slim` + código |
| Com instalação | `pip install -r requirements-build.txt` e `pip install --no-build-isolation .` | traz o console script; build tooling não precisa ficar na imagem final |

Verificado: um venv limpo, com apenas `requirements-build.txt` instalado, constrói e instala
o pacote com `--no-build-isolation` (sem buscar dependência de build na rede) e o console
script responde.

**Não existe `Dockerfile` para esta Source, e por ora não é necessário.** O monorepo tem
`infra/Dockerfile.airflow`, que é a imagem do *orquestrador*: nela a Source roda no próprio
interpretador do Airflow, via `PYTHONPATH`, **sem instalar nada** — exatamente o "modo sem
instalação" da tabela acima. É o retorno prático de `dependencies = []`: zero dependência
significa que ela cabe em qualquer imagem sem negociar versão com ninguém.

Uma imagem dedicada só passa a valer se a Source for executada fora do orquestrador; nesse
caso o modo sem instalação permite um estágio final `distroless`/`slim` sem `pip`.

Pontos que o container vai precisar respeitar: `data/` deve ser volume, não camada da
imagem (o snapshot é artefato, não código); o processo precisa de saída de rede para
`tienda.mercadona.es`; e os códigos de saída 0/1/2/3 do [CONTRACT.md § 7](CONTRACT.md) são
o contrato com o orquestrador.

## Testes

145 testes em `unittest` da biblioteca padrão, sem acesso à rede — o cliente HTTP é
substituído por um duplo que responde de memória, exercitando o mesmo código de extração.
Cada garantia do contrato tem teste correspondente: escrita atômica sob falha e sob
`KeyboardInterrupt`, determinismo da forma canônica, recusa de travessia de caminho,
imutabilidade, idioma fixo, retomada, arquivo truncado/vazio/adulterado, mudança de forma
da fonte, detecção de órfãos, coerência de `_SUCCESS`, adulteração de totais e de
fingerprint, partição sem nenhum produto, e ausência de dependência de terceiros.

## Nota sobre o diretório raiz

O diretório do workspace ainda se chama `Spark`, o que induz a interpretação de que há
Apache Spark aqui — não há, e o [ARCHITECTURE.md](../../ARCHITECTURE.md) registra sob que
condição passaria a haver.

A sugestão anterior deste arquivo era renomear para `mercadona-catalog-source`. **Ela ficou
obsoleta**: esse nome agora pertence a este subdiretório, e a raiz abriga a plataforma
inteira. Um nome adequado para a raiz seria `retail-lakehouse`. O rename é ação do dono do
workspace (afeta a sessão da IDE) e não foi executado; nada no código, no pacote ou nos
dados depende dele.
