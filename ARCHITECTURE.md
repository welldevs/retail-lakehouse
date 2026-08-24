# Decisões de arquitetura

Registro datado do que foi adotado, do que **não** foi, e do gatilho objetivo de cada
tecnologia ainda ausente. Existe porque "não usamos Iceberg" sem motivo escrito é
indistinguível de esquecimento.

Data: 2026-08-24. Todos os números abaixo foram medidos nas três partições em disco.

## Escala real

| Métrica | Valor |
|---|---|
| Volume por partição | ~8,3 MB, 152 arquivos |
| Linhas por partição | ~4.600 (4.329 produtos únicos) |
| Crescimento | ~3 GB/ano se rodar todo dia |
| Duração de uma extração | 227 s (152 requisições a 1,5 s) |
| Silver completo (3 partições, 4 modelos, 36 testes) | ~6 s |

Nenhuma tecnologia distribuída é justificada por este volume. O que segue não é recusa —
é a condição em que cada uma passa a valer.

## Camadas adotadas

```
L0  Source        API -> partição canônica em disco, manifesto, validate
                  sources/mercadona-catalog-source (FROZEN, dependencies = [])
L1  RAW           partição byte-idêntica no object storage, sha256 conferido pós-PUT
                  s3://retail-raw/mercadona_catalog_api/ingestion_date=…/wh=…/
L2  Silver        parquet tipado + 1 modelo temporal
                  s3://retail-lakehouse/silver/…
```

O RAW é o ponto de não-retorno: tudo a jusante é reconstruível a partir dele sem tocar a
API novamente. Isso importa mais aqui do que no caso geral, porque **esta fonte não
permite releitura do passado** — ver "Backfill" abaixo.

## Motor: DuckDB + dbt-duckdb

Adotado. In-process, lê o MinIO direto via `httpfs`, escreve parquet, roda os 4 modelos em
~6 s. O layout hive que a Source já produz (`ingestion_date=…/wh=…`) é consumido com
`hive_partitioning=1`, então `ingestion_date` e `wh` viram colunas **sem parsing manual** —
a obrigação 4.3 do contrato sai de graça.

O SQL do dbt é o ativo portável: os mesmos modelos rodam em `dbt-spark` e `dbt-snowflake`
sem reescrita. É isso que torna as trocas abaixo configuração, e não projeto novo.

## O que não entrou, e quando entra

| Tecnologia | O que compra | Por que não agora | Gatilho | Onde a troca acontece |
|---|---|---|---|---|
| **Iceberg** | `MERGE`/ACID, time travel, evolução de schema, interop entre engines | Partição de data imutável + snapshot diário completo + manifesto com sha256 já dão reprodutibilidade e time travel efetivo. Não há escritor concorrente nem update de linha no Silver. | Quando o Gold `dim_product` precisar de SCD2 por `MERGE` em tabela existente, ou um segundo engine escrever a mesma tabela. | Só o formato de saída de L2/L3. DuckDB lê Iceberg; pyiceberg escreve. L0 e L1 não mudam. |
| **Kafka** | Transporte de eventos | Não há stream. É batch diário de catálogo, sem CDC e sem produtor de evento, e o contrato registra que a fonte **não tem fato transacional**. Publicar o próprio output batch num tópico e consumir de volta adicionaria um broker para manter e zero informação. | Uma source genuinamente event-driven: POS, webhook, CDC de um OLTP. | Nova ingestão em paralelo a L0. Nada existente muda. |
| **Spark** | Processamento acima de um nó | 8 MB por partição. A JVM sobe em mais tempo do que o job roda, e nenhum shuffle real é exercitado. | Partição que o DuckDB não segura em memória, ou join pesado entre múltiplas sources. | `dbt-spark` sobre os mesmos modelos. |
| **Snowflake** | SQL governado para terceiros | ~3 GB/ano e um único consumidor. | Alguém além do autor consultar os dados. | `dbt-snowflake` + `COPY INTO`. **Atrito a registrar:** o Snowflake não alcança um MinIO local — exigiria S3 real ou stage gerenciado. |
| **Airflow** | Retry, exit codes, pools, SLA, histórico de execução | — | **Adotado.** Pesado para um job diário de 4 min, e assumido com essa consciência: o valor está no contrato operacional (o pool de 1 slot e o tratamento de exit code não têm equivalente em cron). | — |

## Quatro restrições medidas que moldaram o desenho

Não são preferências. São comportamento observado, e cada uma está imposta em código.

### 1. Backfill é impossível

A API serve apenas o preço de **hoje**. Um DAG run datado de 2026-08-17 gravaria os preços
de hoje sob a chave de 08-17 — dado silenciosamente errado, e internamente consistente, o
que significa que nenhum teste a jusante o pegaria.

Há um vão real de 8 dias entre `2026-08-16` e `2026-08-24`: os dias 08-17 a 08-23
**não existem e não podem ser recuperados**. É a razão prática do agendamento — cada dia
sem ele é histórico de preço que não volta.

Imposto por três camadas, verificadas: o Airflow recusa `execution_date` no futuro;
`catchup=False` + `start_date` impedem runs anteriores ao início; e `guard_date` recusa
disparo manual de qualquer data ≠ hoje. Enquanto `start_date` == hoje a terceira é
redundante — ela passa a ser a única proteção no dia seguinte.

### 2. O throttle é por processo, não entre processos

O cliente da Source limita a `1/delay` req/s medindo do início da requisição anterior —
dentro do **processo**. Dois `extract` concorrentes dobram a taxa real contra o host.

Medido: 152 requisições em 9 s produziram ~20% de `403` e bloqueio intermitente por
minutos; sequencial a 1,5 s deu 0 falhas em execuções repetidas.

Com mais de um armazém, o pool do Airflow com **1 slot** é a única coisa preservando a taxa
segura. Não é enfeite de configuração.

O DAG hoje cobre **um** armazém (`WAREHOUSES = ["mad1"]`), então o pool ainda não está sob
pressão real — ele existe antes de ser necessário, de propósito, porque acrescentar um
armazém é uma linha e o dano de esquecê-lo é um bloqueio da fonte. Dimensionamento se
vários entrarem: 7 armazéns × 152 requisições × 1,5 s ≈ 27 min sequenciais, o que cabe
folgado numa janela diária.

### 3. Reexecutar `extract` numa partição completa retorna exit 2

A partição completa é imutável (garantia 5 do contrato). Um retry ingênuo do orquestrador
marcaria como falha um dia que deu certo. Por isso a idempotência vem do marcador
`_SUCCESS`, e `retries=0` no `extract`.

O gate do DAG olha o **destino**, não o disco local: reusa `verify-landing` como
short-circuit. Exit 0 → nada a fazer; exit 1 → aterrissada mas divergente, segue e o
`land` conserta; exit 2 → nem existe localmente, segue para extract. Checar o `_SUCCESS`
local ali seria um erro — pularia o `land` de uma partição extraída à mão e nunca
aterrissada.

### 4. A Source grava com modo 600, e isso define o UID do container

`canonical.py` faz escrita atômica com `tempfile.mkstemp()`, que cria o arquivo com modo
**0600**, e `os.replace` preserva esse modo. Medido: **153 dos 155 arquivos** de uma
partição são `-rw-------` (só `_run.log`, escrito com `open()` comum, é 664).

A consequência é operacional e não tem meio-termo: **qualquer consumidor precisa rodar com
o UID do dono dos arquivos.** Não existe fallback por grupo. Por isso o container do
Airflow roda como `${AIRFLOW_UID}` e não como o `airflow` (50000) padrão da imagem.

Duas armadilhas conhecidas nesse caminho, ambas encontradas em execução:

- **O compose não lê o `.env` da raiz por conta própria.** O project dir é `infra/`, então
  `${AIRFLOW_UID}` caía no default 50000 e o container não conseguia ler a partição. O
  `Makefile` passa `--env-file .env` explicitamente.
- **Sobrescrever `entrypoint` num serviço do Airflow quebra o usuário.** O `/entrypoint`
  da imagem é quem cria a entrada em `/etc/passwd` para o UID escolhido; sem ela o Airflow
  morre em `getpass.getuser()`. Use `command`, nunca `entrypoint`.

Alterar o modo na Source resolveria de forma mais direta, mas ela está FROZEN — então o
UID é que se ajusta.

## Verificação em vez de confiança

Padrão herdado do `validate.py` da Source, que recalcula em vez de aceitar valores
gravados. Repetido em cada fronteira:

| Fronteira | O que é reprocessado |
|---|---|
| disco → RAW | sha256 do arquivo local conferido **antes** do PUT; `ChecksumSHA256` validado no servidor |
| RAW | `verify-landing` baixa todo objeto e recalcula sha256, tamanho e inventário |
| RAW → Silver | teste dbt reconcilia a contagem derivada contra `totals` do manifesto |
| Silver | 36 testes (7 singulares + 29 do schema), incluindo unicidade do grão composto e linhagem redundante |

Cada verificação foi provada capaz de **falhar**: adulterar um byte no destino reprova o
`verify-landing` (exit 1); remover um objeto de catálogo reprova o teste de reconciliação.
Verificação que nunca falhou não é verificação.

## Fronteira Source ↔ plataforma

A Source está FROZEN e mantém `dependencies = []`, imposto por AST em
`tests/test_dependencies.py`. A plataforma tem `pyproject.toml` e venv próprios.

A fronteira não é a pasta. É imposta por:

1. **A plataforma nunca importa `mercadona_catalog_source`.** Consome o contrato físico
   (JSON canônico + `_manifest.json`), como um consumidor externo. `platform/…/manifest.py`
   implementa as obrigações da seção 4 do contrato como código, não como comentário.
2. **`make source-test` roda no Python do sistema, sem venv.** Se passar, a Source
   continua sem dependência de terceiros. É a fronteira verificada, não afirmada.
3. **Zero dependência tem retorno prático:** a Source roda no próprio interpretador do
   worker do Airflow via `PYTHONPATH`, sem conflitar com as dependências pinadas dele.

## Dívida técnica conhecida

Registrada em vez de esquecida. Nada aqui bloqueia o uso da plataforma hoje.

### Cobertura de teste

| Módulo | Testes | Situação |
|---|---|---|
| `manifest.py` | 17 | Obrigações do contrato e recusas |
| `land.py` | 10 | Upload, idempotência, auto-correção, abortar antes de `_SUCCESS` |
| `verify.py` | 7 | Adulteração, objeto ausente, órfão, manifesto divergente |
| `config.py` | 8 | Precedência de credencial e o `.env` não sobrepor o ambiente |
| `query.py` | 2 | Conversão de endpoint com e sem esquema |

Fechado com um duplo de cliente S3 em memória (`platform/tests/fake_s3.py`), no espírito do
duplo de HTTP que a Source já usa. O duplo **valida o `ChecksumSHA256` declarado**, como o
servidor real faz: um erro na conversão hex→base64 falharia em teste, não em produção.

As verificações que antes eram manuais agora são reexecutáveis, e foram confirmadas
não-vazias por mutação: desligar a comparação de sha256 em `verify.py` faz
`test_catches_a_tampered_object` falhar.

**O que a suíte ainda não cobre:** o caminho `dbt` (os 36 testes de dados exigem object
storage de pé) e o DAG (nenhum teste importa o módulo do Airflow).

### Caminho de extração não exercitado em container

Todas as execuções do DAG até agora curto-circuitaram em `skip_if_landed`, porque a
partição do dia já estava aterrissada. `extract → validate → land` foi verificado no host,
mas **não dentro do container**. A diferença que importa: o modo `600` dos arquivos e o
`AIRFLOW_UID`. Só será exercitado numa partição nova.

### Sem CI

Nenhum `.github/workflows`. A fronteira depende de alguém rodar `make test` — e o alvo
`source-test` existe exatamente para ser um job que não instala nada. Dois jobs
(Source sem dependência, plataforma com venv) tornariam a fronteira verificada a cada push
em vez de por disciplina.

### Ambientes redundantes em disco

| Caminho | Tamanho | Situação |
|---|---|---|
| `venv/` | 21 MB | **Quebrado.** Ficou apontando para o `src/` antigo depois do move; o console script dá traceback. Recriável com `make -C sources/mercadona-catalog-source venv` |
| `orchestration/.venv` | 245 MB | **Redundante** desde que o Airflow passou a rodar em container. Só serve para `airflow dags test` no host |
| `platform/.venv` | 384 MB | Em uso |

Os dois primeiros estão no `.gitignore` e podem ser removidos sem perda.

### Credenciais de desenvolvimento no `.env.example`

`minioadmin/minioadmin`, `admin/admin` na UI do Airflow e
`AIRFLOW_SECRET_KEY=dev-only-not-a-secret`. Aceitável para um stack local e sinalizado como
tal no arquivo, mas nada disso pode sobreviver a uma exposição de rede.

### Divergência possível de `data/`

O `Makefile` da raiz escreve em `<raiz>/data/source`. O `Makefile` da Source tem
`ROOT ?= data/source` relativo ao próprio diretório, então um `make extract` rodado de
dentro de `sources/mercadona-catalog-source/` cria uma segunda árvore de partições que a
plataforma não consome. Documentado no README da Source; não impedido por código.

## Fora de escopo

**Gold.** `dim_product` SCD2 e `fct_price_daily`. Quando entrar, registrar: um SCD2
chaveado em `source_product_id` modela o ciclo de vida **da chave da fonte**, não do item
comercial. Nos 6 casos medidos de `name_seen_before` ele produz "um produto morreu, outro
nasceu" — pode estar certo, mas é escolha a ser nomeada, não efeito colateral.

**Fato de venda.** A fonte não expõe venda, pedido ou estoque em nenhum endpoint conhecido.
O Gold aqui é histórico de preço. Qualquer fato transacional seria sintético, e isso precisa
estar dito, não implícito.

**Moeda.** A fonte não a declara. O Silver não inventa; a var `currency` do projeto dbt
existe para o Gold e é premissa do consumidor, não dado da fonte.
