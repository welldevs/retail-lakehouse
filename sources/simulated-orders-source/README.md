# Simulated Orders Source

Quinta Source do repositório, e a **segunda derivada**: consome o Silver que as outras
produziram e devolve uma RAW nova. Entrega um **log de eventos de pedido**, não uma
fotografia de pedidos.

> **A regra de ouro, deslocada um nível:** o *pedido* é inventado; quem compra, o que se
> compra, quanto custa e onde mora não.

Cliente vem de `silver_customer` (pessoa sintética, endereço real do Callejero). Produto e
preço vêm de `silver_product_price` do **mesmo armazém na mesma data**. Nada aqui fabrica
produto, preço, cliente ou CEP.

Contrato completo em [CONTRACT.md](CONTRACT.md).

## Como uma Source FROZEN lê o Lakehouse sem quebrar a fronteira

Ela não lê. A plataforma materializa antes, com `retail-platform export-orders-reference`,
cinco JSON planos — `customers.json`, `catalog.json`, `calendar.json`, `premises.json`,
`demand_profile.json` — e esta Source os consome só com a stdlib. Nenhum lado importa o
código do outro.

`dependencies = []`, verificado por AST em `tests/test_dependencies.py`. Em particular **não**
importa `confluent-kafka`, `psycopg` nem `pyiceberg`: o broker, o OLTP e a projeção viva
existem um nível adiante, do lado da plataforma.

## Requisitos

Python 3.12+ e nada mais. Somente biblioteca padrão: `dependencies = []` no
[pyproject.toml](pyproject.toml) — não há o que instalar. `PYTHONPATH=src` basta para rodar
tudo; os alvos de venv existem para empacotar, não para executar.

Antes do primeiro `extract`, o cliente (`simulated-oltp-source`) e o catálogo
(`mercadona-catalog-source`) precisam já estar aterrissados no Lakehouse, e a referência
precisa ter sido exportada com `make orders-export-reference` na raiz do repositório.

## Comandos

```bash
# na raiz do repo
make orders-export-reference ORDERS_FROM=2026-08-24 ORDERS_TO=2026-08-27
make orders-refresh-all      ORDERS_FROM=2026-08-24 ORDERS_TO=2026-08-27

# aqui dentro, sem venv e sem instalar nada
make test
make extract  DATE=2026-08-24 WH=mad1 REFERENCE=../../data/orders-reference/ingestion_date=2026-08-27
make validate DATE=2026-08-24 WH=mad1 REFERENCE=../../data/orders-reference/ingestion_date=2026-08-27
```

## Estrutura

```
sources/simulated-orders-source/
├── CONTRACT.md                      # contrato com o consumidor
├── Makefile                         # test / extract / validate / check / venv / install / freeze
├── pyproject.toml                   # metadados; dependencies = []
├── requirements.txt                 # runtime: vazio por construção, verificado por teste
├── requirements-build.txt           # setuptools / wheel / packaging, congelados
├── src/simulated_orders_source/
│   ├── __init__.py                  # nome da source, versão do manifesto
│   ├── __main__.py                  # python -m simulated_orders_source
│   ├── cli.py                       # subcomandos e códigos de saída
│   ├── reference_data.py            # lê e desconfia dos cinco JSON de referência
│   ├── demand.py                    # modelo de demanda calibrado (mapa_2025_v2) e coortes
│   ├── premises.py                  # acesso tipado às premissas, todas sintéticas
│   ├── events.py                    # vocabulário de eventos + máquina de estados (o contrato em código)
│   ├── orders_generator.py          # amostragem determinística do log, um (armazém, dia) por vez
│   ├── extract.py                   # orquestra partição + manifesto + _SUCCESS
│   ├── validate.py                  # integridade + coerência contra a referência
│   ├── partition.py                 # caminho, tokens, imutabilidade (eixos wh= e ingestion_date=)
│   ├── schema.py                    # campos do envelope, totais reconferíveis e fingerprint
│   └── canonical.py                 # forma canônica do NDJSON + escrita atômica
└── tests/                           # 163 testes, stdlib unittest, sem rede
```

Os snapshots **não ficam aqui**. A partição que a plataforma consome vive em
`<raiz>/data/orders/ingestion_date=…/wh=…/`, dois níveis acima. A referência que o
`extract` lê vive em `<raiz>/data/orders-reference/ingestion_date=…/`, gerada por
`make orders-export-reference` na raiz do repositório.

## O que sai

```
data/orders/ingestion_date=2026-08-24/wh=mad1/
├── order_events.jsonl      NDJSON, uma linha por evento
├── _manifest.json
└── _SUCCESS
```

**Não existe `orders.json` ao lado**, e isso não é omissão: duas representações da mesma
verdade divergem. O estado do pedido é o **fold** dos seus eventos, e o fold mora no Silver.

## Por que o fold não é trivial

Substituição e remoção de linha alteram a cesta **depois** da colocação. Logo `picked_amount`
não é derivável de `gross_amount` — o valor do pedido só existe depois de dobrar o log. Se
fosse derivável, o log seria um carimbo de data e o modelo de eventos seria enfeite.

Medido numa partição real (`mad1`, `2026-08-24`, 400 pedidos): colocado `48.251,11`, separado
`45.553,00`, 269 substituições e 145 remoções em 7.506 linhas.

## Reprodutibilidade

Cada `(armazém, dia)` deriva a própria semente de `sha256("<seed>|<wh>|<order_date>")` —
`sha256` e não `hash()`, porque o hash de `str` em CPython é aleatorizado por processo.
Consequência, verificada ponta a ponta:

> **Acrescentar um dia à janela é aditivo.** As partições já geradas ficam byte a byte
> idênticas, porque nenhum dia depende do sorteio de outro.

As três condições que **não** são aditivas: outra `seed`, outra referência (clientes ou
catálogo reingeridos) e outra tabela de premissas. O manifesto registra as três, e `history[]`
guarda a seed e o digest de premissas de cada execução anterior.

## Premissas: sintéticas e declaradas

Nenhuma fonte deste repo mede venda, cesta, cadência ou disponibilidade. Toda premissa vive em
`platform/dbt/seeds/order_premises_seed.csv`, é rotulada `synthetic` — um rótulo diferente
**reprova** o export — e seu `sha256` viaja até o manifesto. Não há default para nenhuma: uma
chave ausente reprova a geração, porque um default escondido no gerador seria uma premissa não
declarada.

## Restrições da fonte

- Não existe fato transacional em nenhuma fonte externa: o contrato da Mercadona registra que
  nenhum endpoint expõe venda, pedido ou estoque.
- Não existe fato de estoque: por isso a remoção de linha diz `unavailable`, e não
  `out_of_stock`.
- A escolha do produto é **uniforme**, de propósito. Ponderar inventaria uma distribuição que
  ninguém mediu.
- A janela de entrega é promessa comercial numa grade fixa, **nunca** rota: o Callejero não tem
  coordenada nem adjacência.

## Validação

`validate` relê o arquivo declarado no manifesto, recalculando checksum, contagem, impressão
digital e todos os totais em vez de aceitar os valores registrados, varre a partição em busca
de arquivos não declarados e reconfere a ordenação total do log.

Além disso — e é aqui que mora o valor — relê a mesma referência que gerou a partição e
reconfere, pedido a pedido: o cliente pertence ao armazém da partição; todo produto pedido
existe no catálogo daquele `(armazém, data)`; o preço pago é o observado, comparado como
`Decimal` e nunca como `float`; a sequência de eventos é contígua e aceita pela mesma máquina
de estados que o gerador usa; `gross_amount` fecha com as linhas colocadas e `picked_amount`
fecha com as linhas cumpridas depois de substituições e remoções.

**Integridade e coerência** — sempre fatais: manifesto ausente/ilegível · `manifest_version`
incompatível · entrada de manifesto malformada ou apontando para fora da raiz do snapshot ·
arquivo declarado e ausente · checksum ou tamanho divergente · contagem de registros
divergente · arquivo em disco fora do manifesto · `_SUCCESS` incoerente com `complete` ·
campo do envelope ausente · `event_id` que não deriva de `(order_id, sequence_no)` ·
`schema_fingerprint` divergente · log fora de ordem · `sequence_no` não contíguo · transição
de estado impossível · cliente, produto ou preço divergente da referência · totais
divergentes da releitura · referência com outro modelo de demanda ou outra tabela de
premissas.

**Avisos de distribuição** — reportados, fatais só com `--strict`: nenhum pedido teve linha
substituída ou removida (o fold fica trivial e o log deixa de justificar o modelo de
eventos), ou todos os pedidos terminaram no mesmo evento.

```bash
make validate DATE=2026-08-24 WH=mad1 REFERENCE=../../data/orders-reference/ingestion_date=2026-08-27
```

`--reference` é **obrigatório**, como na Source de clientes e pelo mesmo motivo: as garantias
centrais só são verificáveis relendo a mesma referência que gerou a partição. Um validador que
só confere checksum provaria integridade, não coerência.

## Dependências e container

**Runtime: nenhuma.** `[project.dependencies]` está vazio no [pyproject.toml](pyproject.toml),
e a promessa é verificada, não apenas afirmada: `tests/test_dependencies.py` percorre a AST de
todos os módulos do pacote e dos testes e reprova qualquer import fora da biblioteca padrão —
em particular, nenhum import de `confluent-kafka`, `psycopg`, `pyiceberg`, `duckdb`, `boto3`
nem de nada do dbt: o broker, o OLTP e a projeção viva existem um nível adiante, do lado da
plataforma.

| Arquivo | Conteúdo | Quando é necessário |
|---|---|---|
| [requirements.txt](requirements.txt) | vazio, só comentários | nunca — não há dependência de runtime |
| [requirements-build.txt](requirements-build.txt) | `packaging`, `setuptools`, `wheel` | apenas para `pip install .` / construir a wheel |

### Container

A imagem **não precisa instalar nenhum pacote de terceiros**, pelo mesmo motivo das outras
quatro Sources: `dependencies = []` significa que o código roda com
`PYTHONPATH=src python -m simulated_orders_source` no próprio interpretador do Airflow, sem
nenhum `pip install`. Não existe `Dockerfile` dedicado, e por ora não é necessário.

## Testes

```bash
make test                 # 163 testes, sem rede
```

Cobrem, entre outros: o vocabulário de eventos e a máquina de estados (transições válidas e
inválidas); o fold — substituição e remoção mudando a cesta, `picked_amount` fechando com o
que sobrou depois delas; reprodutibilidade por seed com sub-seed por dia, inclusive sob
`PYTHONHASHSEED` diferente em subprocesso; a propriedade de aditividade entre dias e as
quatro condições que não são aditivas; o modelo de demanda calibrado (`mapa_2025_v2`) e a
propensão por coorte de idade/região; premissas declaradas como `synthetic`, sem nenhum
default; desconfiança da referência (arquivo ausente, `rows` vazio, campo faltando, preço não
positivo, `price_source` fora do vocabulário); escrita atômica e forma canônica do NDJSON;
ordenação total do log; imutabilidade e retomada; e ausência de dependência de terceiros.
