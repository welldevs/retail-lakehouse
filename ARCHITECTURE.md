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
                  5 pacotes FROZEN (dependencies = []) em sources/
                  4 entregam FOTOGRAFIA; simulated_orders entrega LOG DE EVENTOS
L1  RAW           partição byte-idêntica no object storage, sha256 conferido pós-PUT
                  s3://retail-raw/mercadona_catalog_api/ingestion_date=…/wh=…/
    ┌─────────── plano operacional, sob demanda (`make stream-up`) ────────────┐
    │ OLTP    Postgres `oltp`: orders, order_line, outbox                     │
    │         `orders-apply` — estado + outbox NA MESMA transação, por evento │
    │         o outbox reconstitui o log byte a byte (sha256 do manifesto)    │
    │ Broker  Kafka  retail.orders.events.v1  (4 partições, key = order_id)   │
    │         `orders-publish` — at-least-once, e a janela é demonstrada      │
    │ Read    Postgres `projection` OU Iceberg `projection.live_order_state`  │
    │ model   `orders-project` — dedup por (order_id, sequence_no)            │
    │         `orders-rebuild-projection` — o SEGUNDO escritor da tabela      │
    │         fusão monotônica: o que não avança `last_sequence_no` cai       │
    └──────────────────────────────────────────────────────────────────────────┘
L2  Silver        parquet tipado + 1 modelo temporal          · DuckDB
                  s3://retail-lakehouse/silver/…                3.796.213 linhas · 44 MB
─────────────────── fronteira física: COPY INTO, nunca ref() ───────────────────
L3  Stage         espelho 1:1 de um RECORTE do Silver         · Snowflake
                  RETAIL.STAGE.STG_*                            10% do Silver
L4  Gold          DIM_* / FACT_* conformados, SCD2            · dbt-snowflake
L5  Mart          MART_*, grão declarado por tabela           · dbt-snowflake
```

O RAW é o ponto de não-retorno: tudo a jusante é reconstruível a partir dele sem tocar a
API novamente. Isso importa mais aqui do que no caso geral, porque **esta fonte não
permite releitura do passado** — ver "Backfill" abaixo.

A fronteira L2→L3 é **física, não lógica**, e é o mesmo mecanismo da fronteira
Source↔plataforma um nível acima: nenhum modelo do Snowflake pode `ref()` um modelo do
Silver, porque adaptadores diferentes não se cruzam numa execução do dbt. A ligação é
`COPY INTO` mais `source()`. O Snowflake é, na prática, **o quarto consumidor que não
alcança o Lakehouse** — os outros três são as Sources FROZEN.

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
| **Iceberg** | Isolamento de snapshot entre escritores concorrentes, time travel, interop entre engines | — | — | **Adotado em 2026-08-28** (Fase 3, Marco 6). O gatilho que disparou foi o literal — *"um segundo engine precisar escrever a mesma tabela"*: `live_order_state` é escrita pelo consumidor em streaming e pela reconstrução em lote, com o DuckDB lendo enquanto os dois escrevem. **Disparou por CONCORRÊNCIA, não por volume** — neste volume um parquet com `os.replace` atômico serviria. Precedido por `make spike-iceberg`, um experimento fechado que mediu catálogo, upsert, conflito, isolamento e leitura pelo DuckDB antes de a projeção existir. O gatilho antigo (`dim_product` SCD2 por `MERGE`) continua sem disparar: o SCD2 é derivado da história completa, não acumulado. |
| **Kafka** | Transporte de eventos, replay, ponto de desacoplamento | — | — | **Adotado em 2026-08-28** (Fase 3, Marco 5). O gatilho que disparou foi o literal — *"CDC de um OLTP"*: o evento nasce na transação que muda o pedido (Marco 4) e um consumidor stateful mantém um read model abaixo do lote. Ficou provada a **semântica de transporte**: at-least-once demonstrado reproduzindo a janela de duplicação, consumo idempotente sem conjunto que cresce, buraco recusado, replay sem efeito, 16 sha256 reproduzidos. **Não** ficou provado, e está escrito: que alguém precise da latência, e que o broker seja a origem — o log canônico continua nascendo em disco. |
| **Spark** | Processamento acima de um nó | 8 MB por partição. A JVM sobe em mais tempo do que o job roda, e nenhum shuffle real é exercitado. | Partição que o DuckDB não segura em memória, ou join pesado entre múltiplas sources. | `dbt-spark` sobre os mesmos modelos. |
| **Snowflake** | SQL governado, RBAC, conectividade BI | — | — | **Adotado em 2026-08-27** (Fase 2). Recebe 10% do Silver, não o Silver inteiro. O atrito antigo — "não alcança um MinIO local" — foi resolvido sem S3 real nem storage integration: **stage interno** (`PUT file://`) inverte o sentido, e quem empurra os bytes é o processo local, que enxerga os dois lados. |
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

O DAG cobre **quatro** armazéns (`WAREHOUSES = ["mad1", "bcn1", "vlc1", "svq1"]`), então o
pool está sob pressão real: são 4 × 152 ≈ 608 requisições, ~15 min sequenciais. Ele existia
antes de ser necessário, de propósito. Teto conhecido: os 7 armazéns que servem catálogo
dariam ≈ 27 min, o que ainda cabe folgado numa janela diária.

### 2.1. Quais armazéns, e por quê esses

A fonte serve **7 armazéns** — `mad1`, `mad2`, `mad3`, `bcn1`, `vlc1`, `svq1`, `alc1` — mais
`vlc2` e `pmi1`, que o servidor reconhece mas que não têm catálogo. Medido em `2026-08-24`.

A escolha dos quatro é por **divergência de sortimento**, não por tamanho de mercado, porque
sortimento e preço se comportam em níveis diferentes:

- **Sortimento é propriedade da cidade.** Os três armazéns de Madrid têm conjuntos de produto
  **idênticos** entre si (0 exclusivos); `bcn1` difere de `mad1` em 12,0% (551 produtos).
- **Preço varia dentro da mesma cidade.** Medido nas categorias de perecíveis, `mad3` diverge
  de `mad1` em 18,4% dos preços, contra 26,4% de Madrid↔Barcelona **nas mesmas categorias** —
  ou seja, 70% da magnitude entre cidades acontece dentro de uma só. Esses dois números são
  comparáveis entre si por serem do mesmo recorte; não são comparáveis com os da matriz
  abaixo, que é de catálogo inteiro.

Logo um segundo armazém da mesma cidade paga 152 requisições para agregar um eixo só. Entre
cidades, os dois variam.

Matriz par a par, **calculada sobre o catálogo inteiro a partir do Silver** em `2026-08-24`
(reproduzível com uma query sobre `silver_product_price`):

| par | sortimento | preço |
|---|---|---|
| bcn1 / svq1 | 13,8% | 3,3% |
| svq1 / vlc1 | 13,6% | 2,8% |
| mad1 / svq1 | 13,0% | 2,3% |
| mad1 / vlc1 | 12,4% | 2,3% |
| bcn1 / mad1 | 12,0% | 3,1% |
| bcn1 / vlc1 | 10,0% | 3,1% |

`svq1` aparece nos **três pares mais divergentes**, e sua menor divergência ao resto do
conjunto é 13,0% — a maior mínima disponível. É o que justifica tê-lo escolhido.

**`alc1` foi descartado** por duplicar `vlc1` — mesma comunidade autônoma, 170 km. Ressalva
honesta sobre esse número: `alc1` **não foi extraído**, então ele não está nesta matriz. A
comparação vem de uma sondagem em 2 categorias, onde `vlc1/alc1` deu 34,4% contra 52,3% de
`vlc1/svq1`. Aquela sondagem tinha **viés de seleção** — as categorias foram escolhidas por
concentrarem exclusivos entre `mad1` e `bcn1`, o que inflou esse par especificamente (deu
78,6% lá contra 12,0% aqui). O viés não atinge o par `alc1` vs `svq1`, medido nas mesmas
categorias sem ser critério de seleção, então a **ordenação** entre os dois se sustenta; as
magnitudes daquela sondagem, não. `alc1` segue como o candidato óbvio se um quinto entrar, e
medi-lo direito exigiria extraí-lo.

### 2.2. Dois fatos da fonte que mudam como se opera isto

**`wh` inválido não falha.** A fonte não recusa código desconhecido: devolve `200` caindo em
`vlc1`, que é o seu default. Medido — `zzz9`, `mad9` e a requisição *sem `wh` nenhum* devolvem
conteúdo idêntico. A consequência operacional é que um erro de digitação em `WAREHOUSES` não
produz erro nenhum: produz uma partição rotulada `wh=<erro>` **contendo dados de Valência**,
internamente consistente e portanto invisível para todo teste a jusante. Um código reconhecido
porém sem catálogo se distingue por `/categories/?wh=X` responder `content-length: 52` (árvore
vazia) em vez da árvore cheia.

**A árvore de categorias é byte-idêntica nos 7 armazéns.** É estrutura nacional, não regional.
Cada armazém gasta 1 requisição numa árvore já conhecida, e `silver_category` carrega 151
linhas redundantes por armazém. Inofensivo no volume atual, e registrado aqui como desperdício
conhecido em vez de descoberto depois.

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

## Segunda source: população do INE

[sources/ine-population-source/](sources/ine-population-source/) foi adicionada em
2026-08-25, junto de três mudanças na plataforma que a fronteira Source ↔ plataforma
acima não previa, porque só existia uma source quando foi escrita.

**`platform/…/manifest.py` generalizado para um segundo eixo opcional.** Antes, `Partition`
exigia `warehouse` e computava a raiz do snapshot subindo exatamente dois níveis fixos —
`SOURCE_NAME` era uma constante única em `__init__.py`, e `land.py` a usava direto em vez
de `partition.source_name` (que já existia no dataclass, populado do manifesto, mas nunca
usado para esse fim — resíduo de um comentário que já prometia isso sem o código cumprir).
Uma source sem eixo de armazém, como o INE, quebrava em
`"manifesto sem partition.ingestion_date ou partition.warehouse"`. Generalizado para
`axis_name`/`axis_value` (`None` quando não há eixo) e `SUPPORTED_MANIFEST_VERSIONS` como
dicionário por `source_name`, mantendo a propriedade `warehouse` como compatibilidade para
não tocar em `raw_manifest.sql` nem nos testes da Mercadona. Land e verify passaram a
derivar o prefixo de `partition.source_name` — a mudança que torna real a promessa do
comentário original ("para que uma segunda source aterrisse ao lado sem reorganizar
nada").

**`dbt build` compila o projeto inteiro, então uma source vazia derrubava as demais.**
Medido: `read_json` do DuckDB levanta erro fatal sobre um glob sem nenhum arquivo
correspondente — o estado normal de uma source recém-adicionada antes do primeiro land, ou
de um clone novo do repositório. Sem tratamento, isso quebraria `make daily` da Mercadona
inteiro só por causa do modelo do INE, mesmo em quem nunca tocou nele. A tentativa óbvia —
fazer o SQL do modelo fingir uma relação vazia com `WHERE false` — não resolve: o próprio
`external` materialization do dbt-duckdb, ao lidar com uma relação vazia, grava uma linha
sentinela (todas as colunas `NULL`) num arquivo `__HIVE_DEFAULT_PARTITION__` para preservar
o schema do parquet, e só filtra essa linha na *view* daquela mesma execução — o arquivo
físico persiste, e a primeira execução seguinte com dado real lê o `location` inteiro de
volta, incluindo a linha fantasma. A correção ficou fora do SQL: `retail-platform has-data
<prefixo>` (novo subcomando, genérico — qualquer source) confere se existe algum objeto
aterrissado, e `make silver` passa `--exclude` para o modelo de uma source sem dado ainda,
em vez de fazer o modelo mentir sobre ter uma partição vazia.

**Sources irmãs, não uma abstração "multi-source".** Quando um segundo dataset do INE
entrar (cogitado: o Callejero do Censo Eleitoral, ver histórico do projeto), o padrão é
outro pacote irmão completo em `sources/`, não uma camada compartilhada dentro do pacote
do INE atual. Os dois datasets são estruturalmente distintos (API JSON pequena e instantânea
vs. ZIP semestral de arquivos ASCII de largura fixa por província) — forçar uma interface
comum agora encaixaria mal nos dois, e cada pacote mantém `dependencies = []` de forma
independente e verificável por AST. Extrair um helper compartilhado só valeria a pena
depois de existirem dois casos concretos para comparar, não antes.

## Extensão: população por município (Fase A)

Adicionada em 2026-08-26, sobre `ine-population-source` já existente (não uma quarta
source) — o mecanismo de fetch já era genérico por `table_id` desde o dia 1 (ver seção
anterior), então acrescentar `table_id=29005` (população por **município**, INE) ao lado
de `31304` (população por **província**) foi extensão de configuração, confirmada por
auditoria de código antes de implementar: `http_client.py`/`extract.py`/`partition.py`
não conhecem nenhum `table_id` específico.

**Motivação, não estética.** `31304` sozinho é inadequado para densidade: medido que
"Valencia/València" nessa tabela é a **província inteira** (2,6 milhões de habitantes,
266 municípios), não a cidade — distribuir esse número entre ruas seria dado fabricado.
`29005` dá o número real por município, cruzável com `warehouse_service_area` (Callejero)
para densidade de verdade.

**Colisão real entre as duas tabelas, medida antes de acontecer em produção:** "Sevilla"
é ao mesmo tempo nome de província (lista fechada de 52 em `silver_ine_population_series`)
E nome do município capital dessa província. Um glob genérico (`table_id=*.json`) sobre
as duas tabelas juntas faria o classificador de `31304` (por vocabulário) capturar
"Sevilla. Total. Total habitantes. Personas." (de `29005`) como se fosse uma linha de
província, com sexo/idade errados. Corrigido restringindo cada modelo Silver ao seu
próprio `table_id`, glob literal, sem wildcard compartilhado — nenhuma abstração "um
modelo lê todas as tabelas de população", porque as duas tabelas têm vocabulário de
`Nombre` incompatível (ver CONTRACT.md da source, § 2).

**Nome de município não é chave seciável de fora do escopo desta plataforma.** `29005`
não traz código, só o nome por extenso. Medido contra o payload completo de
`VALORES_VARIABLE/19` (variável "Municipios" da mesma API Tempus3): 18 dos ~8.200
municípios da Espanha compartilham nome com outro município em provincia diferente (ex.
"Arroyomolinos" existe em Madrid [28015] e Cáceres [10023]) — um join por nome
Espanha-inteira seria ambíguo para esses casos. Nenhuma colisão acontece **dentro** das 4
províncias desta plataforma (verificado antes de escrever `ine_municipality_codes_seed`),
então escopar o seed a 08/28/41/46 (mesmo raciocínio de `warehouse_service_area`)
resolve isso estruturalmente, não por sorte.

**Testado e descartado: reaproveitar o Callejero já landado para o código, em vez de uma
chamada nova a `VALORES_VARIABLE/19`.** `silver_callejero_population_units` já tem
`(province_code, municipality_code, municipality_name)` — parecia redundante buscar outra
fonte. Descartado depois de consultar os dois ao vivo: a grafia diverge estruturalmente,
não é só maiúscula/minúscula — o Callejero grava o nome todo em CAIXA ALTA e move o
artigo definido para sufixo entre parênteses (`"AMETLLA DEL VALLÈS (L')"`), enquanto o
Tempus3 (tanto `29005` quanto `VALORES_VARIABLE/19`) usa o nome natural com o artigo como
prefixo (`"L'Ametlla del Vallès"`). Um join direto entre as duas grafias erraria
silenciosamente. `VALORES_VARIABLE/19` foi escolhido por estar na MESMA família de
endpoint que `29005` — confirmado 0 divergências de grafia numa amostra de 1.501 nomes.

**Faixa etária por município ficou de fora, deliberadamente.** Existe uma família de
dezenas de `table_id` do INE com município+idade (confirmado que pelo menos um,
`33956`, é populado — mas só para a província de Zamora, sugerindo um `table_id` por
província, não descoberto para as 4 províncias desta plataforma). Perseguir isso agora
seria proporcional a criar outra integração inteira sem necessidade comprovada ainda.
Decisão: manter `31304` (única fonte de estrutura etária, nível província) e `29005`
(único fonte de densidade real, nível município) como **complementares**, não tentar
substituir um pelo outro — se a simulação de clientes sintéticos precisar de pirâmide
etária por município no futuro, essa família de tabelas é o próximo lugar a investigar,
não antes.

**A extração de produção real (2026-08-26) confirmou dois problemas que só apareceram
rodando de verdade, não em amostra.** (1) O payload sem filtro de `31304` é grande o
bastante (~264 MB no formato canônico) pra corromper em trânsito antes de terminar — a
primeira tentativa falhou com `JSONDecodeError` no byte 154.057.166 depois de ~33 min; o
retry automático do `Fetcher` (já existia, tratando corpo JSON inválido como erro
retentável) resolveu na 2ª tentativa. Total pousado: ~402 MB, 40.791 séries, 2.253.624
pontos — ver CONTRACT.md da source para os números completos. (2) `validate --strict`
reprovou a partição por 6 séries de `29005` sem nenhum ponto de dado — 2 municípios
(`Gatova`/Castellón, `Palmerola`/Girona) fora das 4 províncias desta plataforma. Como
`--strict` é uma checagem opcional por contrato (não integridade), e reprovar toda
extração por município fora de escopo não protegeria nada real, `--strict` foi removido de
`make ine-validate` e do DAG — mas não do Makefile interno da própria source (que continua
buscando só `31304` por padrão, onde essa checagem nunca falhou). A contagem de séries sem
valor continua reportada no output do `validate`, só deixou de ser fatal.

## Terceira source: Callejero do INE

[sources/ine-callejero-source/](sources/ine-callejero-source/), adicionada em
2026-08-25, confirma a previsão da seção anterior: pacote irmão completo, não uma
extensão do pacote de população. **Zero mudança estrutural na plataforma** foi
necessária além de registrar o nome em `SUPPORTED_MANIFEST_VERSIONS` — `land.py`,
`verify.py` e `manifest.py`, generalizados na fase anterior, funcionaram sem tocar
código, incluindo a partição de eixo único (`ingestion_date` só, sem warehouse nem
província como eixo formal).

**Sem API — a primeira source deste tipo.** O Callejero só é publicado para download
manual, semestral. Isso quebrou uma suposição implícita das duas sources anteriores (que
"extract" busca dado por rede): aqui `extract` incorpora arquivos que um humano já
baixou, sem nenhuma requisição HTTP. O verbo foi mantido por uniformidade de CLI, com o
significado documentado explicitamente no `CONTRACT.md` da source — trocar o verbo
quebraria a simetria do Makefile/DAG sem ganho real.

**Layout de arquivo não documentado — medido, não presumido, com um gotcha de
ferramental no meio do caminho.** O download não trouxe "Diseño de Registro" nenhum.
Encoding real é ISO-8859-1 (Latin-1), confirmado com `file`; um `grep` direto nos
arquivos brutos (antes de descobrir isso) retornava vazio mesmo com o texto lá —
descoberto depois que o `grep` deste ambiente é um wrapper de `ugrep -I`, que **ignora
arquivos que parecem binário**, e um arquivo Latin-1 com bytes altos (acentos) dispara
essa heurística. A correção foi checar com `grep -a` ou converter com `iconv` antes.
O mesmo encoding, ao ler os arquivos de volta no DuckDB via `read_csv`, precisou do nome
exato `'latin-1'` (com hífen) — `'latin1'` é rejeitado com uma lista de ~700 encodings
suportados, nenhum deles com esse nome exato. Nenhuma das duas pegadinhas seria pega sem
testar contra o arquivo real.

**Layout de coluna por arquivo** (offsets de byte, sem delimitador — lido no DuckDB via
`read_csv(..., delim=E'\x01', hive_partitioning=1, filename=true)`, um delimitador que
nunca aparece no dado, para trazer a linha inteira como uma coluna): `SECC` é só um
código de 10 dígitos; `VIAS`/`PSEU` têm código + nome em 2-3 formas redundantes (larguras
diferentes, mesmo texto); `UP` é o mais complexo — 604 caracteres, com o nome do
MUNICÍPIO numa posição (`[94:314]`) e o nome do NÚCLEO/entidade dentro dele noutra
(`[459:529]`), confirmado comparando conteúdo real (`"ABRERA"` repetido para várias
entidades, cada uma com um nome de núcleo diferente — `"CAN VILALBA"`, `"SANT MIQUEL"`,
`"*DISEMINADO*"`), não assumido por semelhança de posição com os outros arquivos. Ver
`CONTRACT.md § 2` da source para a tabela completa.

**`TRAM` foi incorporado numa segunda rodada, depois de ficar deliberadamente de fora na
primeira.** A decisão original era não inspecionar `TRAM` (o mais pesado dos 5 — 14-28 MB
por província) até a simulação de Orders precisar de granularidade de número de porta.
Reabriu quando ficou claro que nenhum dos outros 4 arquivos tem código postal (CEP) nem
"bairro" com significado real fora de Valencia — e a página oficial do INE sobre o
Callejero confirma explicitamente que é `TRAM` quem carrega "el distrito postal de cada
tramo".

**Decodificado por medição, confirmado contra doc oficial — não presumido nos dois
sentidos.** Layout novo (273 chars) inspecionado do zero: código de seção em `[0:10]`
(mesmo formato de `SECC`), sufixo de entidade/núcleo em `[13:20]` (mesmo formato de
`UP`), id de via em `[20:25]` (mesmo formato de `VIAS`) OU id de pseudovia em `[25:30]`
(mesmo formato de `PSEU`) — mutuamente exclusivos, confirmado sem exceção em 305 mil
linhas reais das 4 províncias. O bloco intermediário (`[42:58]`, 16 chars) resistiu a
uma primeira leitura por regex ingênua (`\S+` colava campos adjacentes sem espaço).
Achamos o PDF oficial ["Diseños de registro de los ficheros de intercambio de
información INE-Ayuntamientos"](https://idapadron.ine.es/repositorio/DisReg/disregok.PDF)
(IDA-Padrón, 2015) via busca — descreve o formato de *intercâmbio* de variações
INE↔Ayuntamentos, não o snapshot que baixamos, mas nomeia os campos do "Tramero" na
mesma ordem: `CUN CVIA CPSVIA MANZ CPOS TINUM EIN CEIN ESN CESN`. Usando essa ordem pra
recortar os bytes, bateu: `CPOS` (código postal, 5 dígitos) sempre com o prefixo
correto da província em 304.905/304.952 linhas (99,985% — as 47 exceções só em
Barcelona), `TINUM` nunca fora de `{0,1,2}`, e a faixa `EIN`/`ESN` sempre respeitando a
paridade que `TINUM` declara (par/ímpar) — zero exceções nas quatro. Validado também
contra geografia real: Valencia cidade tem 30 CEPs distintos (46001-46026 + exceções),
e pedanias específicas batem com o CEP real da área (Pinedo/El Saler → 46012, zona sul
da cidade). O resto do registro (parte de `[58:273]`) continua não decodificado —
inclui um campo repetido no fim que espelha `CPOS`+`TINUM`+`EIN`+`ESN` já capturados no
início; nada além disso é extraído ou afirmado no Silver.

**`warehouse_province_map` não é gerado por esta source.** O seed
(`platform/dbt/seeds/warehouse_province_map_seed.csv`) foi derivado do Callejero
manualmente durante o desenvolvimento (`scripts/derive_warehouse_province_map.py`,
cruzando o nome do município em `UP` contra a existência de seções em `SECC`), e existe
independente da source em si. A source do Callejero não sabe que warehouses existem —
produz `province_code`/`municipality_code` como o INE os publica, sem nenhuma referência
a `mad1`/`bcn1`/`svq1`/`vlc1`. O vínculo é uma decisão desta plataforma, não uma
propriedade do INE, e só entra via `JOIN` no Silver/Gold.

**`warehouse_service_area` — mesmo mecanismo, pergunta diferente.** `município = wh`
(1:1) não é o mesmo que "área que o warehouse atende" (N municípios vizinhos). Consultar
só `warehouse_province_map` faz um município real e adjacente (ex. Albal, vizinho de
Valencia, mas administrativamente independente — prefeitura, CEP e código de município
próprios) parecer "não existir" na geografia do warehouse, quando na verdade só estava
fora do escopo da consulta. A fonte da lista não podia ser inventada nem estimada por
proximidade (o Callejero não tem coordenada nem adjacência) — usamos a "Área Urbana
Funcional" do INE (AUF, ex-LUZ): metodologia oficial única para o país inteiro (um
município entra na AUF de uma cidade se ≥15% da população empregada comuta pra lá por
trabalho), baixada como Excel de `ine.es` e parseada com `zipfile`+`xml.etree` da stdlib
(um `.xlsx` é só um zip de XML — nenhuma dependência nova precisou entrar). Cada um dos
370 municípios resultantes foi cross-validado contra o Callejero real antes de entrar no
seed (`scripts/derive_warehouse_service_area.py`): confirmado que existe pelo menos uma
seção `SECC` com aquele prefixo de província+município, e o nome usado é o do `UP` real
(não o texto do Excel do INE), pra bater exatamente com
`silver_callejero_population_units.municipality_name`. **Limitação aceita
conscientemente**: a AUF oficial de Madrid tem 166 municípios, mas 38 caem em Ávila,
Guadalajara ou Toledo — províncias que esta plataforma nunca baixou do Callejero (só
08/28/41/46). Barcelona tem o mesmo problema em menor escala (2 de 135, em Tarragona).
Sevilla (46/46) e Valencia (63/63) não têm essa lacuna — a AUF de ambas cabe inteira nas
provincias já landadas. Decisão explícita do usuário: usar o que já está baixado agora,
em vez de estender a source do Callejero pra mais 4 províncias só pelos municípios de
fronteira.

## Quarta source: OLTP simulado (Fase 1 — Customers)

[sources/simulated-oltp-source/](sources/simulated-oltp-source/), adicionada em
2026-08-27. É a **primeira source derivada** do repositório: as outras três são upstream
de dado externo, esta consome o Silver que elas produziram e devolve uma RAW nova. A
inversão de direção é o ponto — sem as três anteriores, gerar um cliente sintético
significaria inventar endereço; com elas, o cliente é inventado e o lugar onde ele mora
não.

**A tensão que precisou ser resolvida antes de escrever uma linha.** Toda Source deste
repo é FROZEN (`dependencies = []`, imposto por AST em `tests/test_dependencies.py`), e
`duckdb`/`boto3` são dependências exclusivas da plataforma por design explícito
(`platform/pyproject.toml`: "os dois conjuntos nunca se encontram"). Uma Source que
precisa de dado do Lakehouse não pode abrir conexão sem quebrar essa fronteira.

A saída reusa o precedente que o Callejero já tinha criado, um nível antes na cadeia: lá
`extract` não busca rede, incorpora arquivos já preparados em `--in`. Aqui a **plataforma**
ganhou um subcomando (`retail-platform export-oltp-reference`) que consulta o Silver e
escreve três JSON planos; o `extract` da Source os lê só com a stdlib. Nenhum lado importa
o código do outro — compartilham um contrato **físico**, o mesmo mecanismo que
`land.py`/`manifest.py` já usam com o manifesto de qualquer Source. **Zero mudança
estrutural na plataforma** além de registrar o nome em `SUPPORTED_MANIFEST_VERSIONS`.

**O que a revisão adversarial pegou antes da implementação.** Três rodadas de revisão
contra o Lakehouse real, e a maior parte do valor veio de medir em vez de presumir:

- **O join de nome de município zerava Valência.** O desenho inicial casava
  `tramos.unit_code` com a linha agregada de `population_units`. Medido: **0 de 7.194**
  tramos de `vlc1` — València é o único dos 4 municípios-sede com núcleos/pedanias reais,
  então nenhum tramo pendura na linha agregada. Corrigido para casar por
  `(province_code, municipality_code)`: 7.194/7.194.
- **Os seeds do dbt não são alcançáveis por `connect_lakehouse()`.**
  `warehouse_service_area_seed` e `warehouse_province_map_seed` não têm `location =` no
  `dbt_project.yml`, então o dbt-duckdb os materializa dentro do `retail.duckdb` local e
  eles nunca viram parquet sob `silver/`, que é tudo que aquela função enxerga. O export
  lê os dois CSV direto.
- **Os 103 `age_label` da tabela 31304 não formam uma partição.** Junto das 101 idades
  simples convivem dois agregados sobrepostos, `Total` e `85 y más años`. Somar os 103
  ingenuamente dá **2,03×** o valor correto (medido em Madrid/2022: 27.724.421 contra
  13.650.010, que é exatamente o rótulo `Total`). Sem `where age_label not in (...)`, a
  pirâmide etária de todos os clientes sairia errada e **nenhum teste do plano anterior a
  pegaria**.
- **`silver_ine_population_series` está duplicada em duas `ingestion_date`** com linhas
  idênticas (1.547.496 cada). O export fixa `max(ingestion_date)`.
- **Filtrar por `numbering_type='0'` não basta para não fabricar número.** Existem 14
  tramos com `numbering_type='2'` e faixa `0000..0000`: sortear em `[0,0]` daria número de
  casa 0. O 0 é excluído do conjunto amostrável.
- **A rastreabilidade que o plano prometia era falsa.** `(street_code, postal_code)`
  identifica sozinho apenas **13,3%** dos tramos do escopo (105.112 combinações para
  216.594 tramos; a maior cobre 70). Nem uma chave de 7 colunas basta — o grão real tem
  11. Resolvido com `candidate_index`: um inteiro que aponta a linha exata da referência.
- **Nenhuma das duas fontes do INE grafa município igual.** Em **370 de 370** casos o
  Callejero usa caixa alta com artigo entre parênteses (`BRUC (EL)`) e a tabela 29005 usa
  caixa mista com artigo posposto (`Bruc, El`). Isso foi descoberto porque a validação
  reprovou 200/200 clientes na primeira execução real. Os dois nomes convivem na
  referência com rótulos distintos, e o que se compara é sempre o **código**.

**Um defeito do nosso próprio Silver, encontrado de raspão — e corrigido.** Três
municípios recebiam duas linhas de população (Arroyomolinos e El Molar em `mad1`, Torrent
em `vlc1`). A causa medida não é anomalia do INE nem colisão do seed (0 colisões nas 4
províncias): a tabela RAW 29005 é nacional e traz duas séries com `Nombre` idêntico para
municípios homônimos de províncias diferentes, e o `inner join ... on municipality_name`
casa só por nome. Foi registrado como dívida técnica na entrega da Fase 1 e **fechado no
mesmo dia**, pelo código oficial do INE em vez de heurística — ver "Fanout de homônimo no
Silver de população", abaixo.

**Escolhas de vocabulário: preservar, não renomear.** O primeiro desenho criava um campo
`address_precision` com valores `house`/`street` para dizer se o endereço tinha número. O
repo já tinha a resposta: `numbering_type`, com `accepted_values ["0","1","2"]`, modela
exatamente esse fato. Inventar vocabulário novo colapsaria `1` e `2` numa coisa só,
apagando a paridade — que é justamente a garantia a auditar. O cliente carrega
`numbering_type` verbatim, e `house_number` fica `null` (nunca ausente) quando não há
número: uma chave opcional faria o `schema_fingerprint` da partição depender da seed,
porque a impressão digital é a união das chaves observadas.

**Reprodutibilidade teve de ser construída, não herdada.** Esta é a primeira source com
aleatoriedade — não havia nenhum uso de `random` no repo. Dois riscos reais: nenhum dos 6
modelos Silver da junção tem `ORDER BY` (a ordem vem do scan do DuckDB e não é estável),
e o hash de `str` em CPython é aleatorizado por processo. As três queries do export levam
`ORDER BY` explícito, e o gerador nunca itera `set` nem `dict` reconstruído. A garantia é
verificada rodando a mesma seed em subprocessos com `PYTHONHASHSEED` diferente.

**Resultado medido na primeira execução real** (Callejero `2026-08-25`, população
`2026-08-26`): 216.591 candidatos de endereço (3 órfãos excluídos), 370 municípios, 800
clientes em 4 partições, **200/200 coerentes em cada armazém**, 14 sem número de casa, 2
em pseudovia. Injetar um CEP de fora da AUF faz `oltp-validate` reprovar com código 1 —
verificado ponta a ponta, não só em teste unitário.

**Deliberadamente fora da Fase 1:** modelo Silver e DAG do Airflow. Nenhuma source deste
repo ganhou modelo Silver antes de ter um consumidor. **Ambos entraram na Fase 2**
(`silver_customer`, `silver_oltp_manifest`, `simulated_oltp_customers.py`), junto do
consumidor que faltava — o modelo dimensional.

**A base é recarregável, e isso é uma propriedade do código, não uma promessa.** Verificado
lendo `customers_generator.py`: o laço é `for index in range(count)` sobre uma única
`random.Random(seed)` consumida em ordem fixa, e **nada antes do laço depende de `count`**.
Logo `generate(ref, wh, N, seed, data)[:M] == generate(ref, wh, M, seed, data)` — crescer a
base é **append-only**, sem tocar a Source congelada. Provado ponta a ponta: regerar de 200
para 5.000 clientes preservou os 200 originais **byte a byte** nos quatro armazéns, com os
sha256 conferidos. Cobertura da AUF de `mad1` subiu de 46 para 125 dos 128 municípios.

As duas condições que **não** são aditivas, ditas explicitamente: outra `ingestion_date`
preserva a idade amostrada e desloca `birth_year` (a mesma pessoa, mais velha); outra seed
troca as pessoas por trás dos mesmos ids. O manifesto registra as duas em `history`, com a
seed e o `count` de cada execução anterior. É por isso que `DIM_CUSTOMER` é SCD2 e não uma
tabela fixa.

Orders, estoque e entrega continuam fora — ver "Fase 2: camada analítica no Snowflake".

## Consolidação operacional da Fase 1

Três problemas que só apareceram ao perguntar "o que acontece se eu rodar isto de novo?" —
nenhum era visível numa execução única, e os três davam resultado errado em silêncio.

**O timeout padrão não servia para estas tabelas, e nem o Makefile nem a DAG corrigiam.**
A Source usa `DEFAULT_TIMEOUT = 30.0`, dimensionado para uma chamada de API comum. Estas
não são comuns: 264 MB (31304) e 125 MB (29005) num único `GET`. A extração que funcionou
nesta máquina foi uma invocação **manual** com `--timeout 240 --max-retries 3` — o caminho
automatizado teria estolado. Os dois caminhos agora passam os mesmos valores
(`INE_TIMEOUT`/`INE_MAX_RETRIES` no Makefile, `RETAIL_INE_TIMEOUT`/`RETAIL_INE_MAX_RETRIES`
no compose e na DAG). O default genérico da Source fica como está: **quem sabe o tamanho da
tabela é quem a pede**, e é no chamador que isso está escrito.

**Reextrair a mesma publicação duplicava linhas no Silver, sem erro visível.** Os modelos
de referência empilham todas as `ingestion_date` de propósito — o histórico é deliberado —
mas nada marcava qual era a atual. Medido: `silver_ine_population_series` com 1.547.496
linhas em **cada** uma de duas datas, ou seja, 3.094.992 no total; qualquer contagem sem
filtro saía dobrada. Os sete modelos passaram a expor `is_latest_ingestion`
(`ingestion_date = max(ingestion_date) over ()`), um teste dbt garante que a flag marca
exatamente uma data por modelo, e `export-oltp-reference` trocou seus `max(ingestion_date)`
espalhados por `where is_latest_ingestion`. Os modelos da Mercadona **não** ganharam a
coluna: lá as várias datas são o produto, não um efeito colateral.

**`data/` crescia sem limite e nada nunca era apagado.** Depois de `land` +
`verify-landing`, a cópia local é redundante. `retail-platform prune-local` remove a
partição local, mas só depois de **duas** conferências: a cópia local contra o próprio
manifesto e o destino contra esse mesmo manifesto. A primeira não é redundante — o smoke
test contra o MinIO real mostrou que sem ela um arquivo local corrompido era apagado como
se estivesse íntegro, porque `verify-landing` compara o objeto com o manifesto e o objeto
continuava certo. Não há `--force`, e o alvo nunca entra num `*-refresh`: apagar dado é
decisão de quem opera.

## Fase 2: camada analítica no Snowflake

Adicionada em 2026-08-27. É a primeira vez que este repositório atravessa a fronteira
entre dois motores de banco.

**A pergunta não foi "como levo dado para o Snowflake", foi "quanto não deveria ir".**
Medido quando a decisão foi tomada (2026-08-27): o Silver tinha 3.796.213 linhas, das
quais **3.094.992 (81,5%)** eram `silver_ine_population_series` — população **nacional**,
das quais apenas **57.072 (1,8%)** no escopo das 4 províncias. Outras 517 mil são resolução
de endereço do Callejero, que serve ao gerador de clientes, não ao analista. Atravessavam
**146.240 linhas, 3,85%**, e ficavam no S3 os outros 95%. Carregar o Silver inteiro seria
pagar armazenamento por 27× o dado útil.

**A razão NÃO é uma propriedade do pipeline, e dizer "oscila em torno de 5%" foi um erro
de leitura que a Fase 3 desfez.** Medido em 2026-08-29, com Orders no destino: o Silver tem
4.087.507 linhas e atravessam **406.855 — 9,95%**. A razão dobrou, e não porque o recorte
tenha ficado mais frouxo: ela é função de **quanto de cada source cai dentro do escopo**. A
população do INE é nacional e entrega 1,8%; os pedidos são gerados dentro das quatro AUFs
por construção e entregam ~100%. Sem Orders, o recorte continua em 6,0%.

O número de qualquer momento sai de `make warehouse-evidence`, não deste parágrafo. O que
**não** muda com o tempo é a estrutura da decisão: o recorte é escopo geográfico + última
ingestão + dedup de grão, e nenhuma regra de negócio.

**O recorte é deliberadamente burro:** escopo geográfico, última ingestão, dedup de grão.
Nenhuma regra de negócio — se aparecer um `case when` de domínio em `snowflake_export.py`,
está no lugar errado. É o que impede a mesma lógica existir em dois motores e divergir.

**A única exceção declarada** é excluir agregados que a fonte mistura com o detalhe
(`sex_label = 'Total'`). Não é regra de negócio, é evitar dupla contagem: somar os três
rótulos dá o dobro da população. É a mesma armadilha que na Fase 1 inflou a pirâmide etária
em 2,03× com `age_label`, e ela nunca falha — produz um número plausível.

**O DDL vem da própria query.** Um DDL escrito à mão é um segundo lugar onde o schema vive,
e os dois divergem no primeiro dia em que alguém acrescenta uma coluna ao recorte. Como
STAGE é espelho 1:1, seu schema *é* o resultado da consulta.

### O gatilho do Iceberg não disparou na Fase 2 — e disparou na Fase 3

Registrado aqui como estava, porque a sequência importa: durante a Fase 2 o gatilho
pré-escrito era *"quando o Gold `dim_product` precisar de SCD2 por `MERGE` em tabela
existente"*, e **ele não disparou** — o SCD2 é derivado da história completa, não acumulado
por MERGE, porque o RAW guarda todos os snapshots e a dimensão é sempre reconstruível. O
Snowflake absorveu o resto (MERGE nativo, RBAC, BI) sem broker nem catálogo novo.

O gatilho que sobrou — *"um segundo engine precisar **escrever** a mesma tabela"* — disparou
no Marco 6 da Fase 3, e por **concorrência, não por volume**. Ver a seção da projeção
concorrente, mais abaixo.

### Quatro defeitos que só apareceram executando

Nenhum deles gera SQL inválido. Todos geram SQL **válido apontando para o lugar errado**,
que é a categoria que nenhum teste offline pega.

- **`@%TABELA` seguia o schema errado.** O stage de tabela resolve contra o schema
  *corrente da sessão*, e `create schema` do Snowflake **troca** o schema corrente. Como
  `ensure_schemas` cria STAGE, GOLD e MART nessa ordem, a sessão terminava em MART e o
  `PUT` procurava `RETAIL.MART.%STG_PRODUCT_PRICE`. Corrigido qualificando sempre; virou
  teste em `test_snowflake_load.py`.
- **`GOLD_GOLD` e `GOLD_MART`.** O dbt **concatena** o schema do profile com o do modelo
  por padrão — comportamento pensado para vários desenvolvedores num banco compartilhado.
  Aqui GOLD/MART/STAGE são as *camadas*, com nome fixo e alvo de grant. A macro
  `generate_schema_name` passou a usar o nome absoluto; o isolamento certo, quando fizer
  falta, é por **database**, não por prefixo de schema.
- **Um `source_name` inventado.** O teste que liga preço a execução de ingestão filtrava
  por `'mercadona_catalog'`; o valor real, medido no manifesto, é
  `'mercadona_catalog_api'`. Errar isso não reprova uma partição: faz o join inteiro não
  casar e as **14** reprovarem de uma vez, como se o modelo estivesse quebrado.
- **`FILTER (WHERE …)` e `WINDOW … AS (…)` não existem no Snowflake.** O DuckDB aceita os
  dois, então o SQL passou no `dbt parse` local e só quebrou no motor de verdade. Trocados
  por `count_if()` e por janelas repetidas.

### Governança verificada, não afirmada

Três papéis, um por **verbo** do pipeline: `RETAIL_LOADER` escreve o STAGE e não lê o
GOLD; `RETAIL_TRANSFORMER` lê o STAGE e escreve GOLD/MART; `RETAIL_READER` só lê o MART.

**A verificação foi mais importante que os grants**, e por dois motivos medidos:

1. **`DEFAULT_SECONDARY_ROLES = ('ALL')`** é o padrão de contas Snowflake modernas: a
   sessão ativa *todos* os papéis do usuário além do primário. Como este usuário também tem
   ACCOUNTADMIN, `RETAIL_READER` lia GOLD e STAGE sem problema — enquanto
   `show grants to role RETAIL_READER` continuava mostrando apenas MART. **Uma verificação
   de RBAC feita da sessão de um admin sem desligar isso passa por engano, sempre.** A
   checagem roda com `use secondary roles none`.
2. **`grant all on schema` não alcança as tabelas que já existem** dentro dele — elas
   pertencem a quem as criou. O `RETAIL_TRANSFORMER` podia criar tabelas em GOLD e não
   conseguia ler as que já estavam lá. Isso só apareceu porque a verificação existia; foi
   ela que reprovou.

`snowflake-bootstrap` aplica os grants **e prova a matriz de isolamento** antes de
retornar sucesso.

### Resultado medido

STAGE reconferido contagem a contagem (146.240 linhas na entrega da fase); **102 testes
dbt** no target `snowflake`, 0 erros; **215** no target `dev`, inalterados. O fechamento cruza os três
caminhos: soma de `MART_MARKET_COVERAGE.customers` = `DIM_CUSTOMER` vigente = base do
STAGE = **20.000**. `DIM_PRODUCT` tem 4.962 versões para 4.959 produtos (3 com mais de uma
versão, 7 marcados como identidade ambígua). E a lacuna de 08-17 a 08-23 aparece como sete
dias com zero em `DIM_DATE` — que é exatamente o que o calendário completo e o
`FACT_INGESTION_RUN` existem para tornar visível.

### Fora desta fase, com o gatilho escrito

Orders, Order Items, Stock, Replenishment, Delivery, Events, Kafka, Spark, Iceberg,
`BRIDGE_PRODUCT_CATEGORY`, `DIM_CENSUS_SECTION`, `DIM_ADDRESS`.

**Orders, Order Items e Events entraram na Fase 3** — como Source, RAW e Silver no Marco 3,
e a árvore `models/warehouse/` no Marco 7. **Kafka e Iceberg também saíram desta lista**, nos
Marcos 5 e 6, cada um com o gatilho que disparou escrito. Restam Stock, Replenishment,
Delivery, Spark e as três dimensões — ver a seção da Fase 3.

**Bloqueio real para `FACT_DELIVERY`:** rota e tempo de entrega exigem coordenada, e o
Callejero não tem coordenada nem adjacência — já registrado neste documento. Sem
geocodificação, "rota" seria inventada, exatamente o que a regra de ouro da Fase 1 proíbe.
Proxy honesto: distância entre centróides de CEP/município, rotulada como proxy. Gatilho
para o real: uma quinta source com coordenada (CartoCiudad do IGN, ou OSM).

## Fase 3: Orders como eventos (quinta source)

Adicionada em 2026-08-28. É a primeira vez que este repositório modela **fluxo** em vez de
fotografia, e a primeira source cuja partição **não contém estado**.

**A regra de ouro, deslocada um nível.** A Fase 1 dizia "o cliente é inventado; o lugar onde
ele mora não". Aqui: **o pedido é inventado; quem compra, o que se compra, quanto custa e
onde mora não.** Cliente vem de `silver_customer`, produto e preço vêm de
`silver_product_price` do mesmo armazém na mesma data. Nada aqui fabrica produto, preço,
cliente ou CEP — e `orders-validate` reconfere as três coisas, pedido a pedido, contra a
mesma referência que gerou a partição.

### O que impediria isto de ser teatro

Este documento já tinha recusado a versão fácil de um modelo de eventos: *"publicar o próprio
output batch num tópico e consumir de volta adicionaria um broker para manter e zero
informação"*. Três decisões existem só para não cair nessa armadilha, e as três são
verificadas, não afirmadas.

**1. O fold é não-trivial.** Substituição e remoção de linha alteram a cesta **depois** da
colocação, então `net_amount` não é derivável de `gross_amount_placed` — o valor do pedido só
existe depois de dobrar o log. Medido na janela de 2026-08-24 a 08-27, sobre 120.693 linhas:

| Mecanismo | Linhas | Efeito no valor |
|---|---|---|
| cumprida sem alteração | 108.194 | 0,00 |
| substituída | 4.670 | **+10.318,99** |
| removida | 2.321 | **−15.153,72** |

Um teste dbt **invertido** (`assert_order_fold_is_not_trivial`) reprova quando *nenhuma* cesta
muda — irmão de `assert_geography_postal_code_is_not_a_key`, e pelo mesmo motivo: ele vigia a
justificativa do desenho, não o dado.

**2. Há uma única verdade.** A partição RAW contém `order_events.jsonl` e mais nada. Não
existe `orders.json` com o estado dobrado ao lado, de propósito: duas representações da mesma
verdade divergem. O estado mora em `silver_order` e é sempre reconstruível — o mesmo princípio
que faz `DIM_PRODUCT` ser SCD2 **derivado** da história em vez de acumulado por `MERGE`.

**3. Kafka será justificado pelo consumidor, não pelo produtor.** O gatilho escrito exige as
duas metades — um simulador que emite continuamente **e** um consumidor que precise de
latência abaixo do lote. Esta fase entrega a primeira; **o broker não entra até a segunda
existir**, e é por isso que a linha do Kafka na tabela de gatilhos continua como está.

### Duas decisões de partição que precisam estar escritas

**`ingestion_date` é a data do PEDIDO, não a do evento.** Todo evento de um pedido mora na
partição do dia em que ele foi colocado, mesmo atravessando a meia-noite. Medido: **4.567 de
44.456 eventos (10,3%) ocorrem depois da meia-noite do dia do pedido.** A alternativa —
particionar por data do evento — deixaria a partição de um dia impossível de fechar: ela só
estaria completa dois dias depois, quando o último pedido daquele dia terminasse, e `_SUCCESS`
perderia o significado. O Silver expõe `ingestion_date` **e** `event_date`, porque as duas
perguntas são legítimas e diferentes.

**O arquivo é NDJSON, e é o único desvio de forma canônica das cinco Sources.** As outras
quatro gravam um array JSON com `indent=2`. Um log é lido linha a linha e cresce por append;
cada linha é um evento completo e independente — o mesmo byte no disco e, mais adiante, no
tópico — e `read_json(format='newline_delimited')` do DuckDB o consome direto. A garantia que
importa é preservada: mesmo conteúdo ⇒ mesmos bytes ⇒ mesmo SHA-256, porque cada linha é
canônica e a ordem das linhas é determinística.

### Reprodutibilidade num eixo novo

A Fase 1 provou que **crescer a base de clientes é aditivo**. Aqui o que é aditivo é o **eixo
do tempo**: cada `(armazém, dia)` deriva a própria semente de `sha256("<seed>|<wh>|<dia>")`, e
nenhum dia depende do sorteio de outro. Consequência verificada ponta a ponta:

> Acrescentar um dia à janela deixa as partições já geradas **byte a byte idênticas**.

`sha256` e não `hash()`, porque o hash de `str` em CPython é aleatorizado por processo —
derivar a sub-seed dele faria a partição inteira depender de `PYTHONHASHSEED`. Verificado em
subprocesso com três valores.

**As três condições que NÃO são aditivas**, ditas explicitamente: outra `seed`, outra
referência, e outra **tabela de premissas**. As três trocam os pedidos por trás dos mesmos
`order_id`, e as três estão registradas em `config` e em `history`. A terceira é nova nesta
fase, e é por isso que o `sha256` do seed de premissas viaja até o manifesto.

### Premissas: sintéticas, declaradas, e sem default

Nenhuma fonte ingerida por esta plataforma mede venda, cesta, cadência de compra ou
disponibilidade de produto. Toda premissa vive em `platform/dbt/seeds/order_premises_seed.csv`
e é rotulada `synthetic` — um rótulo diferente **reprova o export**, porque chamar qualquer
uma destas de `observed` ou `proxy` prometeria um dado que ninguém mediu.

**Não existe default para nenhuma premissa.** Uma chave ausente reprova a geração, em vez de
o gerador escolher um número. Um default escondido no código seria, por definição, uma
premissa não declarada.

`daily_order_rate` é a única sem **nenhuma** âncora observacional: é por isso que ela mora num
seed que qualquer um edita e reconstrói, e não numa constante.

### Escolhas de vocabulário, outra vez: preservar em vez de prometer

`order_line_removed` carrega `reason = "unavailable"`, e **não** `"out_of_stock"`. Não existe
fato de estoque em nenhuma fonte desta plataforma; nomear como se existisse prometeria um dado
que ninguém mediu. É a mesma disciplina que fez a Fase 1 preservar `numbering_type` do INE em
vez de inventar um `address_precision`.

Pelo mesmo motivo a entrega é modelada como **janela** (`delivery_slot`, uma promessa
comercial numa grade fixa) e nunca como rota: o Callejero não tem coordenada nem adjacência, e
`FACT_DELIVERY` continua bloqueado exatamente onde estava.

### O que a revisão adversarial pegou antes e durante a implementação

- **A escolha do produto tinha de ser uniforme.** O desenho inicial ponderaria produto por
  categoria. Nenhuma fonte deste repo mede venda, giro ou cesta — ponderar inventaria uma
  distribuição que ninguém mediu, que é a mesma proibição que a Fase 1 aplicou ao tramo.
  Consequência declarada: **o mix por categoria espelha o tamanho do sortimento**, e isso é
  consequência de uma premissa, não afirmação sobre o mercado.
- **O `payload` não pode ser inferido.** Deixar o DuckDB inferir produz um `STRUCT` com a
  união dos campos dos 12 tipos — medido, 32 campos — e essa união depende do que a amostragem
  viu. Um dia sem nenhuma devolução não teria `returned_amount` no struct, e um modelo a
  jusante que o referenciasse **deixaria de compilar por sorteio**. O schema é declarado em
  `columns =` e o payload atravessa como JSON.
- **A impressão digital de schema tinha o mesmo problema, ao contrário.** Nas outras quatro
  Sources ela é a união das chaves *observadas*; aqui isso faria duas partições corretas
  divergirem quando um dia não tivesse devolução. Ela passou a cobrir o **vocabulário
  declarado inteiro**, e muda quando o código muda — que é o que ela existe para detectar.
- **`totals_of` não podia levantar exceção.** A primeira versão dobrava o log para contar, e
  um log adulterado fazia `validate` sair com **código 3 (exceção não tratada)** em vez de
  **1 (validação reprovou)** — apagando a diferença entre dado ruim e bug do validador.
  `fold(strict=False)` devolve um sentinela `INVALID` e a divergência de totais denuncia.
- **Duas das oito provas de reprovação eram falsas.** Ao provar que cada teste dbt novo é
  capaz de falhar, duas injeções não pegaram — e o defeito estava nas **injeções**, não nos
  testes: uma usava um valor que podia coincidir com o dado real, e a outra escrevia
  `select *, 0 as substituted_lines`, onde o `*` já trazia a coluna e o alias colidia. Vale
  registrar porque é o modo de falha de uma verificação de verificação.

### Uma característica da fonte que muda como se mede valor **[fonte]**

O `unit_price` da Mercadona cobre **quatro ordens de grandeza**: mediana 2,25, p90 6,15,
máximo 3.663,00. Os extremos são reais — marisco congelado e presunto ibérico vendidos por
peso (`Alistado mediano congelado` 3.663,00; `Jamón de bellota ibérico 100%` 532,00).

Consequência medida: **9 das 4.670 substituições caíram acima de 100,00 e respondem por 42%
do valor substituído.** Excluindo essas nove, a média do substituto (3,288) e a do original
(3,258) são praticamente iguais — ou seja, **não há viés na regra de substituição**, há uma
cauda. Qualquer mart que use média aritmética de valor de cesta será dominado por punhado de
linha; a orientação é mediana ou percentil, e está escrita no `schema.yml` do modelo.

### Resultado medido

Janela de 2026-08-24 a 2026-08-27, 4 armazéns, 16 partições:

| Métrica | Valor |
|---|---|
| Pedidos | 6.400 |
| Eventos | 44.456, em 12 tipos |
| Linhas de pedido | 120.693 |
| Valor colocado / separado | 879.449,74 / 821.121,93 |
| Partições reconciliadas manifesto ↔ Silver | 16 de 16, zero divergência |
| Coerência (cliente, produto e preço reais) | 16 de 16 partições, `--strict` |
| Suítes das Sources | 631 (145 + 136 + 95 + 125 + **130**) |
| Testes da plataforma | 178 no Marco 3, 210 no Marco 4, 239 no Marco 5, 262 no Marco 6, 273 no Marco 7, **288** no Marco 8 |
| Nós dbt no target `dev` | 303 no Marco 3, 312 no Marco 6, **319** no Marco 7 (era 215) |
| Nós dbt no target `snowflake` | 102 antes da Fase 3, **170** no Marco 7 |

### Fora desta fase, com o gatilho escrito

Stock, Replenishment, Delivery, Spark, `BRIDGE_PRODUCT_CATEGORY`,
`DIM_CENSUS_SECTION`, `DIM_ADDRESS`, e Debezium/Kafka Connect.

**O OLTP com outbox, o Kafka e o Iceberg saíram desta lista** — entraram nos Marcos 4, 5 e 6,
depois de o portão abrir. **A árvore `models/warehouse/` de Orders saiu no Marco 7.** Restam
Stock, Replenishment, Delivery e Spark, com os gatilhos intactos.

**O portão é deliberado.** Kafka e Iceberg existem para servir o fold, e o fold acabou de
ficar de pé. Ligá-los antes de o caminho em lote estar provado faria `orders-reconcile`
comparar duas coisas erradas e **passar** — o mesmo modo de falha que
`DEFAULT_SECONDARY_ROLES` produziu na Fase 2, quando uma verificação de RBAC passava por
engano.

## Fase 3, segunda metade: o OLTP e o outbox transacional

Data: 2026-08-28. **Marco 4 do plano.** A regra que passou a governar os marcos seguintes:
*cada etapa prova a propriedade que justifica a tecnologia da etapa seguinte.* O Marco 3
provou event sourcing não-trivial; este prova **atomicidade + outbox**.

### O gatilho literal, e o que ainda falta dele

O gatilho escrito para o Kafka é *"uma source genuinamente event-driven: POS, webhook,
**CDC de um OLTP**"*, mais *"emite continuamente **e** um consumidor abaixo do lote"*.
O que este marco entrega é a **primeira metade da primeira metade**: o evento passou a
nascer dentro da transação que muda o pedido.

Isso não é detalhe de implementação — é a diferença entre um outbox e um *dual-write*.
Republicar o estado depois de gravá-lo é ter duas escritas que podem discordar; gravar as
duas na mesma transação é ter uma. **A linha do Kafka na tabela de gatilhos continua não
adotada**, e continuará até o Marco 5 rodar: declarar adoção antes de a coisa existir é a
mesma classe de defeito que este plano fechou em outro lugar.

### O que exatamente se prova, e onde cada prova mora

| Propriedade | Onde é provada | Por que não pode ser provada no outro lugar |
|---|---|---|
| A fronteira da transação: outbox e estado entre o mesmo início e o mesmo `commit`, sem commit no meio | `platform/tests/fake_pg.py`, offline, em `make test` | É o defeito que se comete de verdade — um `commit()` a mais — e ele é de **forma**, visível no diário de chamadas |
| Que `rollback` **desfaz** | `make orders-prove-atomicity`, contra Postgres real | Atomicidade é propriedade do motor. Um duplo que desfaz só demonstra que o duplo desfaz |
| Que o outbox não perdeu, não duplicou e não alterou | `orders-outbox --verify`, nas 16 partições | Contar linhas não prova; reproduzir o **sha256 do manifesto** prova |
| Idempotência do replay | as duas | `event_id unique` é do banco; pular em vez de recusar é do código |
| Recusa de evento fora de ordem | as duas | É a guarda que dá sentido a `key = order_id` no Marco 5 |

A injeção da prova real **não mexe no código da plataforma — mexe no banco**: um trigger que
levanta exceção no `insert`. É uma falha que o applier não pode prever nem tratar, que é
exatamente o tipo de falha contra a qual a transação existe.

E ela é feita **nas duas direções**, o que é a metade que se esquece:

1. o insert no `outbox` explode → nenhum pedido, nenhuma linha, nenhum evento sobrevivem;
2. o insert em `orders` explode → **nenhuma linha de outbox sobrevive**.

A segunda importa tanto quanto a primeira: um outbox que sobrevivesse a um estado que não
mudou **publicaria um evento que nunca aconteceu**. Provar só um lado provaria metade do
padrão. Verificado injetando o dual-write no applier: a prova (1) continua passando e a
prova (2) reprova com `outbox=1` — e a partição seguinte falha em cascata, porque o outbox
acha que aplicou o que o estado não tem.

### Três guardas independentes que precisam concordar

| # | Guarda | Recusa |
|---|---|---|
| 1 | `outbox.event_id` UNIQUE | reaplicar o mesmo evento — e o replay **pula**, não falha |
| 2 | `orders.last_sequence_no` | evento fora de ordem |
| 3 | `orders.status` em `from_states` | transição inválida |

A guarda 2 é a que **converte a chave de partição do Kafka de preferência em exigência**.
Se o OLTP aceitasse evento fora de ordem, preservar ordem por `order_id` no broker seria
enfeite. É porque ele recusa que `key = order_id` passa a significar alguma coisa.

A máquina de estados é **redeclarada** aqui, a partir do `CONTRACT.md` §5 da Source — nunca
importada. Mesmo princípio de `verify.py` reler o objeto em vez de confiar no que acabou de
escrever. Se as duas declarações divergirem, `orders-apply` reprova na primeira transição
afetada.

### O outbox reconstitui o log byte a byte

`outbox.event_json` guarda **a linha canônica do log, verbatim** — não uma reserialização.
As colunas do envelope existem para **rotear** (chave, ordem, filtro), e quatro `CHECK`
amarram cada uma ao próprio JSON: uma linha não consegue ser roteada sob uma chave que
discorda do payload que ela carrega. É o motor que garante, não a convenção.

Consequência: reordenando as linhas do outbox pela ordem canônica da Source e recompondo o
arquivo, o `sha256` bate com o manifesto da partição. **16 de 16.** É a prova mais forte
que este marco tem para dar, e não custou nada.

`payload` deliberadamente **não** é coluna: `event_json::jsonb -> 'payload'` responde
qualquer consulta sem guardar uma segunda cópia. Guardar `jsonb` em vez do texto teria sido
pior de forma não óbvia — `jsonb` reordena chaves pelo próprio critério, e o `event_id` e o
`sha256` deixariam de ser verificáveis ponta a ponta.

### Uma transação por evento, e não por pedido nem por partição

Uma transação por partição provaria *"o dia inteiro é atômico"*, que nenhuma loja garante.
O recorte tem de ser o mesmo de um OLTP de verdade: **uma mudança de estado é um negócio
fechado**. Custo medido: 44.456 transações em 85 s, ~520 eventos/s. É lento em comparação
com um `COPY`, e é o preço de a propriedade significar o que promete.

### O que o OLTP achou no Silver — dois folds independentes discordando

Aqui está o retorno concreto da regra do usuário. Replicar o mesmo log por um caminho
completamente diferente — incremental e transacional, em vez de window function sobre o
log inteiro — e comparar os dois estados achou **dois defeitos no Marco 3** que nenhum teste
existente pegava, porque ambos eram internamente consistentes.

**1. `net_amount` respondia duas perguntas com o mesmo nome.** 298 dos 6.400 pedidos (196
cancelados, 102 com pagamento recusado) morrem antes da separação. O Silver deixa
`net_amount` nulo — *"não houve separação"*. O OLTP, na primeira versão, o inicializava com
o valor colocado — *"quanto o pedido ainda vale"*. Duas perguntas legítimas, um nome só.
`orders-reconcile`, no Marco 6, teria comparado as duas achando que comparava duas
respostas. Corrigido no OLTP: a coluna nasce **indeterminada** e só é preenchida quando um
evento a determina.

**2. O Silver afirmava separação que o log nunca declarou.** `line_status` era
`case ... else 'fulfilled'`: toda linha que não fosse substituída nem removida virava
*cumprida* — **inclusive as 5.508 linhas dos 298 pedidos que nunca chegaram à separação**.
`order_picked` é o único evento que declara separação, e ele não ocorre nesses pedidos. O
`else` era uma afirmação **do modelo**, não da fonte — a violação mais direta possível da
regra de ouro deste repositório, e ela passou por três revisões porque o número fechava com
tudo o mais.

O vocabulário passou a ter cinco valores, **idênticos nos dois lados**:
`placed | fulfilled | substituted | removed | not_picked`. `placed` cobre o pedido ainda em
voo — não ocorre nesta janela, e existe para o vocabulário ser completo em vez de completo
por sorte. Vocabulário igual dos dois lados não é estética: uma tabela de tradução entre
dois folds é exatamente onde *"compara duas coisas erradas e passa"* mora.

Depois da correção, os dois folds concordam em **tudo**:

| Comparação | Linhas | Atributos | Divergências |
|---|---|---|---|
| `orders` (OLTP) × `silver_order` | 6.400 | status, net, gross, linhas, separadas, cliente, wh | **0** |
| `order_line` (OLTP) × `silver_order_line` | 120.693 | status, valor, produto cumprido, qtd, preço, produto | **0** |

Nenhum dos dois defeitos apareceria adicionando mais um teste ao Silver: os dois eram
internamente coerentes, e o teste que os pegaria teria de conhecer a resposta certa. O que
os achou foi **uma segunda implementação independente do mesmo fold**. É o argumento a favor
do par lambda que o Marco 6 vai montar, feito antes de o Marco 6 existir.

### Decisões de infraestrutura

**Postgres separado do metadado do Airflow.** Não por organização: o OLTP é um componente
*modelado* da simulação, precisa de `wal_level=logical` próprio (que exige restart e afeta o
servidor inteiro), e *"reiniciar o OLTP"* não pode significar *"reiniciar o cérebro do
Airflow"*.

**`wal_level=logical` ligado agora, sem uso agora.** O outbox é drenado por polling — lê a
tabela, não o WAL. A configuração está ligada porque é a única que exige restart do
servidor: deixá-la ligada desde o início permite plugar Debezium (Marco 4b) sem derrubar o
banco e sem perder o que estiver no outbox. Custo: alguns bytes por escrita.

**`profiles: ["stream"]`.** `make up` continua subindo só o MinIO — promessa do README. A
separação mora no arquivo do compose, não na memória de quem opera.

**`orders-oltp-init --reset` existe, e `prune-local --force` não.** A assimetria é
deliberada: `prune-local` tem como alvo uma partição aterrissada, que pode ser a única
cópia; o OLTP é uma **réplica** do log, que `orders-apply` reconstrói em 85 s. Nada nele é a
única cópia de nada.

### O que ficou de fora deste marco

O DAG de replay. `orders-apply` já é um verbo limitado e idempotente, próprio para tarefa de
Airflow, mas o grafo útil (`apply >> wait_for_drain >> reconcile`) precisa dos Marcos 5 e 6
para existir. Escrever agora um DAG de uma tarefa só seria fachada.

## Fase 3, terceira metade: transporte, replay e consumo idempotente

Data: 2026-08-28. **Marco 5.** O enquadramento importa mais que o software: isto é
*transporte + replay + semântica de entrega + consumo idempotente*, e não "subir um broker e
publicar mensagens". Contar mensagens prova que algo trafegou; não prova que trafegou
intacto, nem o que acontece quando alguém morre no meio, nem que reprocessar é seguro. As
quatro propriedades são independentes e cada uma precisou de prova própria.

### Semântica de entrega: a escolha, e o lado em que se erra

O pipeline é **at-least-once do outbox ao broker**, e não exactly-once. A razão é estrutural
e não tem conserto barato: marcar `published_at` no Postgres e receber o ack do Kafka são
duas escritas em dois sistemas, e não existe transação entre eles. A escolha está em qual
lado errar:

| Ordem | Morrer no meio produz | |
|---|---|---|
| publicar → ack → marcar | **duplicata** | escolhido |
| marcar → publicar | **perda** | recusado |

Perder é irreversível; duplicar é absorvível a jusante. Por isso o consumidor tem de ser
idempotente **por obrigação, não por elegância**.

**`enable.idempotence=true` não resolve isso, e conflatar as duas coisas é o erro mais comum
aqui.** Ele elimina duplicata gerada por *retry dentro da sessão do produtor*. Duplicata
gerada por o processo morrer entre o ack e o commit do outbox está fora do alcance dele — é
do desenho, não do transporte.

E isso não é uma ressalva de documentação: `make orders-prove-stream` **reproduz a janela**.
Devolve 500 linhas do outbox para a fila (exatamente o que uma queda depois do ack produz),
republica, e o tópico passa a ter mais mensagens do que o log tem eventos. Medido:
44.456 → 44.956.

### Deduplicação sem conjunto que cresce

O consumidor não guarda um conjunto de `event_id`. Ele compara `sequence_no` com o
`last_sequence_no` que já está no read model:

| Comparação | Verdito |
|---|---|
| `seq <= last` | duplicata — descarta em silêncio |
| `seq == last + 1` | aplica |
| `seq > last + 1` | **buraco — recusa em voz alta** |

Duas consequências que valem mais que a economia de memória:

**É limitado por construção.** Um inteiro por pedido. Um conjunto de `event_id` cresce sem
limite e obriga a inventar uma política de expiração — e toda política de expiração é uma
janela em que a duplicata volta a passar.

**Faz `key = order_id` virar peça de carga.** A comparação só é válida porque a ordem por
chave é garantida: uma duplicata sempre chega *depois* do original. Se não chegasse, ela
seria classificada como buraco. A chave deixa de ser detalhe de configuração e passa a ser a
premissa de uma função — e a prova contra o broker confere as duas coisas: todo pedido numa
única partição, e toda repetição em offset maior que o seu original.

**Buraco é perda, e perda tem de doer.** `seq > last + 1` significa que um evento não chegou.
Avançar o offset por cima tornaria a perda permanente e invisível. O consumidor para.

### O offset é commitado depois da escrita

`enable.auto.commit` é **false**, e essa é a segunda escolha que decide tudo: o commit
automático anda no *timer*, não na escrita, e entrega at-most-once sem ninguém ter escolhido.
A ordem é escrever a projeção → commitar a projeção → commitar o offset. Morrer no meio
reentrega o lote, e a deduplicação o descarta: **at-least-once na entrega, efeito
exactly-once na projeção**.

Nenhum teste de contagem enxerga essa ordem. Por isso ela é asserida contra duplos que
gravam um **diário compartilhado** — a intercalação entre os dois lados é a propriedade, e
ela não existe em dois diários separados.

### Onde cada prova mora, outra vez

| Propriedade | Offline (`make test`) | Contra o broker (`make orders-prove-stream`) |
|---|---|---|
| Verdito de duplicata / buraco | `decide` é pura, testada sem nada | — |
| Ordem escrita → commit de offset | diário compartilhado dos duplos | — |
| Chave = `order_id`, nada marcado antes do ack | duplo de produtor | — |
| Ordem por chave, repetição depois do original | — | só o broker pode garantir |
| Transporte fiel byte a byte | — | 16 sha256 |
| At-least-once real | — | a janela é reproduzida |
| Replay sem efeito | duplos | e contra o tópico inteiro |

Seis inversões de semântica foram injetadas no código e cada uma reprovou o teste certo:
offset antes da escrita, commit assíncrono, marcar antes do flush, marcar só o que passou,
chave constante, buraco como no-op.

### Replay: rebobinar sem apagar

`orders-replay` rebobina o grupo para o início e **não apaga a projeção**. Isso é
deliberado: reprocessar o tópico inteiro *por cima* do estado existente é o que prova consumo
idempotente. Apagar antes provaria que o fold é determinístico — que é outra coisa, e já
estava provada desde o Marco 2.

Medido: 44.956 mensagens reprocessadas, **0 aplicadas**, digest da projeção idêntico. E o
plano inteiro reconstruído a partir de volumes vazios produz o **mesmo digest**
(`d769f727f805736a…`): a projeção é reconstruível do zero, não só estável.

### Três folds independentes, agora

`silver_order` (window function sobre o log), `orders` no OLTP (incremental, transacional) e
`live_order_state` (incremental, em memória, alimentado pelo broker). Três caminhos, um
número: **6.400 pedidos, zero divergências** em estado, sequência, contagens e valores.

O Marco 4 já mostrou o que isso compra — dois folds discordando acharam dois defeitos que
nenhum teste pegava. O terceiro fold não achou defeito novo, e isso também é informação.

### Dois achados que valem mais escritos que corrigidos

**1. A premissa `sla_minutes_picking = 90` é inalcançável por construção.** O consumidor
calcula o tempo de separação e marca `sla_breached` — o mecanismo funciona e está testado.
Mas `basket_lines_max × minutes_per_line_picked = 40 × 2 = 80 min`, e a maior separação
observada em 6.400 pedidos foi exatamente **80,00 min**. O limiar não pode disparar.

Foi deixado como está. Ajustar uma premissa declarada até a verificação acender é o oposto
de verificar — e "o processo está confortavelmente dentro do SLA" é um estado legítimo do
mundo, não um defeito. Fica registrado que o alerta **não é exercido por estes dados**, e
portanto não conta como prova de nada.

**2. A distribuição perfeitamente uniforme entre partições é artefato da chave.** Medido:
1.600 pedidos em cada uma das 4 partições, e exatamente 100 em cada uma dentro de *cada* um
dos 16 grupos (armazém, dia). Isso não é mérito do particionador: a parte variável de
`order_id` é um contador sequencial denso com zeros à esquerda, e os bits baixos do murmur2
acompanham os últimos dígitos de forma linear — o resultado é um sistema completo de
resíduos. Com `order_id` esparso ou em UUID o equilíbrio viraria apenas estatístico. **Não é
garantia e não deve virar premissa.**

### Um defeito na prova, não no sistema

A primeira versão da conferência de ordem exigia que a lista de `sequence_no` de cada pedido,
ordenada por offset, fosse crescente. Ela passou no primeiro run e **reprovou 121 pedidos no
segundo** — porque o segundo run tinha as duplicatas do primeiro no tópico, e uma duplicata
republicada foi produzida *depois*, então aparecer depois é o comportamento correto. A
sequência crua lê `1, 2, …, 1`.

A prova estava reprovando o broker por fazer exatamente o certo. A garantia do Kafka é sobre
a **ordem de produção**, não sobre a lista de offsets. Corrigida para as duas propriedades
que a deduplicação realmente usa: a *primeira* aparição de cada `sequence_no` vem em ordem, e
toda repetição vem depois do seu original.

Vale o registro porque é a terceira vez neste projeto que a verificação estava errada e o
sistema certo — e as três só apareceram porque a verificação foi rodada mais de uma vez,
contra estado que já não era limpo.

### O que Kafka comprou, e o que continua hipotético

**Comprou, e é medível**: um ponto de desacoplamento onde um segundo consumidor entra sem
tocar no produtor; replay a partir de offset arbitrário; e um lugar onde at-least-once mais
idempotência são *exercidos* em vez de assumidos.

**Continua hipotético**: que alguém precise da latência. O gatilho escrito pedia "um
consumidor cuja utilidade EXPIRE se chegar no lote do dia seguinte". Existe agora um
consumidor que mantém estado vivo abaixo do lote — o mecanismo. Se alguma decisão muda por o
número chegar em segundos em vez de no dia seguinte é pergunta de produto, e **replay de
dado histórico não pode respondê-la**. Está escrito assim de propósito.

**E o broker não é a origem.** O log canônico continua sendo escrito em disco antes de
entrar no OLTP e no tópico. Isso é deliberado — trocar reprodutibilidade byte a byte pelo
broker como fonte da verdade seria um mau negócio, e o replay determinístico é justamente o
que permite rodar o mesmo dia cem vezes e comparar. Mas significa que este pipeline
demonstra **semântica de transporte**, não uma origem genuinamente event-driven. Quem ler
isto procurando o segundo não vai encontrar.


## Fase 3, quarta metade: a projeção concorrente em Iceberg

Data: 2026-08-28. **Marco 6.** O gatilho era *"um segundo engine precisar **escrever** a mesma
tabela"*, e agora `live_order_state` tem dois escritores por desenho: o consumidor em
streaming (`orders-project --sink iceberg`) e a reconstrução em lote
(`orders-rebuild-projection`), com o DuckDB lendo a mesma tabela enquanto os dois escrevem.

**O gatilho disparou por CONCORRÊNCIA, não por volume.** Neste volume um parquet reescrito
com `os.replace` atômico funcionaria. O que o Iceberg compra aqui é isolamento de snapshot
entre dois escritores e um leitor, mais time travel na projeção. Escrever isso é a diferença
entre uma decisão e uma moda.

### O experimento fechado veio antes

`make spike-iceberg` respondeu nove perguntas contra o stack de verdade **antes de uma linha
da projeção existir**, porque o plano registrou "o DuckDB pode não ler o catálogo SQL do
pyiceberg" como a premissa mais frágil. Se ela caísse no meio da construção, o retrabalho
seria caro e a tentação pior: contornar com um caminho que quase funciona e chamar de
projeção.

Duas respostas mudaram o desenho:

**1. O DuckDB lê pelo `metadata_location`, e só por ele.** Ele se recusa a descobrir qual é o
metadado corrente varrendo o storage — *"globbing the filesystem to locate the latest version
is disabled by default as this is considered unsafe and could result in reading uncommitted
data"*. O atalho existe (`SET unsafe_enable_version_guessing = true`), foi medido, funciona, e
foi **recusado**: ler metadado não commitado é exatamente o que uma leitura concorrente com
dois escritores não pode fazer.

Quem sabe qual metadado é o corrente é o **catálogo**. `make silver` pergunta a ele e passa a
resposta como var. A autoridade continua num lugar só, sem flag insegura e sem reimplementar
convenção de catálogo.

**2. O experimento reprovou a minha asserção, não o Iceberg.** A primeira versão da Q5 exigia
que "as duas escritas sobrevivessem" a duas referências carregadas ao mesmo tempo, e deu
`CommitFailedException`. Isso é o controle otimista funcionando: o segundo commit parte de um
snapshot que já não é o corrente e **tem** de ser recusado. Se passasse calado, seria lost
update — e aí sim havia motivo para não usar Iceberg. Reescrita para a propriedade correta:
recusa → recarrega → retry commita → ambas presentes.

**Uma armadilha de dependência, medida:** `pyiceberg[s3fs]` arrasta um `aiobotocore` que fixa
um botocore antigo e quebra o `boto3` que a plataforma usa para o RAW. O pip aceita instalar e
o estrago aparece noutro módulo. `PyArrowFileIO` faz o mesmo trabalho.

### Retry não basta: a fusão precisa ser monotônica

Esta é a parte que o experimento não pega e que decide se dois escritores funcionam.

Recarregar e tentar de novo resolve o conflito de **commit** — e ainda assim perde dado. Se o
outro escritor já gravou o pedido no `sequence_no` 7 e a nossa tentativa carrega o 5, o retry
cego escreve o 5 por cima. **O commit passa. A tabela regride. Nada reprova**, porque do ponto
de vista do Iceberg não há nada errado: o branch estava onde se esperava.

Por isso cada tentativa relê o estado das chaves afetadas e descarta as próprias linhas que
não avançam. É a mesma guarda de `last_sequence_no` que protege o OLTP (Marco 4) e o
consumidor (Marco 5), agora protegendo a escrita concorrente — a terceira vez que o mesmo
invariante paga.

Medido na prova: um escritor com `seq=5` contra uma tabela em `seq=7` produz
`rows_dropped_as_stale=1, rows_written=0` e **nenhum upsert é emitido**. Não escrever é o
resultado correto, não uma falha.

### O par lambda, com o recorte que ele deveria ter

`orders-rebuild-projection --through 2026-08-26` cobre a história assentada; o streaming cobre
a cauda viva. Medido:

| | Pedidos | Tempo |
|---|---|---|
| Lote (12 partições, 33.349 eventos) | 4.800 | **3,2 s** |
| Streaming (o tópico inteiro por cima) | 1.600 | 60 s |

`written_by` na tabela: `{rebuild: 4800, stream: 1600}` — os dois escritores marcaram
presença, e a coluna é **proveniência consultável**, não afirmação sobre log. O streaming
descartou 33.849 eventos como duplicata: eram os pedidos que o lote já tinha trazido ao estado
final, e a dedup do consumidor os reconheceu lendo o estado que o **outro** escritor gravou.
Os dois mecanismos compõem.

### Quatro caminhos, um digest

O read model tem hoje quatro produções independentes, e todas dão o mesmo `d769f727f805736a…`:

| Caminho | Como |
|---|---|
| `live_order_state` no Postgres | fold incremental, sink Postgres |
| `live_order_state` no Iceberg, só streaming | mesmo fold, outro armazenamento |
| `live_order_state` no Iceberg, lote + streaming | dois escritores, fusão monotônica |
| Reconstrução do zero, volumes vazios | Marco 5, partida a frio |

**Dois motores de armazenamento diferentes produzindo digest byte a byte igual** é um
resultado mais forte do que qualquer contagem.

### O que o acordo entre os dois escritores NÃO prova

`orders-rebuild-projection` e `orders-project` compartilham `fold_event`. Concordarem mostra
que **não se atropelam** — não que estão certos. Confundir as duas coisas seria o mesmo
defeito que este projeto já registrou duas vezes: uma verificação que compara algo consigo
mesmo e passa.

A evidência de correção vem de `orders-reconcile`, que compara com `silver_order` — window
function em SQL sobre o log inteiro, sem uma linha de código em comum com as outras duas.
**Três folds independentes, 6.400 pedidos, zero divergências.** E o mesmo invariante virou
teste dbt (`assert_live_projection_matches_batch_fold`), porque um pipeline em que a
divergência só aparece quando alguém lembra de rodar um comando não tem verificação — tem
hábito.

### Um defeito real que a prova encontrou

`IcebergProjection._refresh()` recarregava `TABLE_NAME` — um identificador **cravado**.
Enquanto só existiu uma tabela, funcionou. No primeiro teste que usou uma tabela de sonda, um
conflito fez o escritor da sonda recarregar a tabela de **produção** e gravar nela: quatro
linhas sintéticas entraram em `live_order_state`, e quem apontou foi a reconciliação, três
passos adiante.

Um identificador cravado numa função de refresh é sempre isto: funciona até existir um segundo
objeto, e aí escreve no lugar errado sem erro nenhum.

Duas consequências viraram permanentes:

- a prova ganhou uma **sentinela** — conta as linhas da tabela de produção antes e depois das
  partes que usam a sonda. Uma prova que usa uma sonda tem de vigiar o alvo que ela *não*
  deveria tocar;
- a verificação da injeção passou a exigir que a reconciliação reprove **pela linha
  adulterada**, e a pular a injeção se a base já estiver suja. Na rodada com o defeito, ela
  "passou" porque `not ok` já era verdade — um falso-positivo clássico.

### O custo medido do copy-on-write

O sink Iceberg processou o tópico inteiro em **4 min 48 s** contra **14 s** do sink Postgres —
~20×. A causa não é o formato em si: o `upsert` do pyiceberg é copy-on-write, então cada lote
reescreve os arquivos de dados, e a guarda monotônica lê a tabela antes de cada tentativa.
Com 90 lotes de 500 mensagens, isso é ~90 leituras e ~90 reescritas de 6.400 linhas.

Números para dimensionar: 268 snapshots numa passada, 80 na passada com o recorte lambda.

**Não foi otimizado, e o motivo está aqui:** os dois sinks respondem perguntas diferentes. O
Postgres é o read model de baixa latência; o Iceberg é o que aceita dois escritores e guarda
história. Ajustar o lote do Iceberg para ficar perto do Postgres trocaria latência por
throughput sem que ninguém tivesse pedido. **Gatilho para mexer**: a projeção sair da ordem de
10⁴ linhas, quando o custo por commit deixa de ser desprezível e merge-on-read (delete
posicional) passa a valer o que custa em complexidade.


## Fase 3, quinta metade: os pedidos no warehouse

O último salto da fase é o mais convencional — três STAGE, três FACT, três MART — e foi onde
apareceu o defeito mais caro dela. Vale contar nessa ordem.

### O que entrou

| Camada | Objetos |
|---|---|
| STAGE | `STG_ORDER` (6.400) · `STG_ORDER_LINE` (120.693) · `STG_ORDER_EVENT` (44.456) · `STG_ORDER_PREMISE` (30) |
| GOLD | `FACT_ORDER` · `FACT_ORDER_ITEM` · `FACT_ORDER_EVENT` · `FACT_ORDER_PREMISE` |
| MART | `MART_ORDER_FUNNEL` · `MART_FULFILLMENT_SLA` · `MART_BASKET_DAILY` |

O destino saiu de 236.797 para **406.855 linhas**; `DIM_CUSTOMER` dobrou para 40.000 porque a
base de 2026-08-24, gerada no Marco 0, nunca tinha atravessado a fronteira.

`FACT_ORDER` é **accumulating snapshot**, e o padrão só existe porque há eventos: uma linha
por pedido que se preenche conforme ele avança, com onze marcos e as durações entre eles. Uma
fotografia de estado diria *onde* o pedido está; nunca *quanto tempo levou para chegar lá*.

### Um timestamp 56 milhões de anos no futuro, e 166 nós verdes por cima dele

A primeira carga do STAGE pôs **todo** timestamp no ano **56.648.666**. O DuckDB anota a
unidade do timestamp só no `LogicalType` moderno do parquet e deixa o `ConvertedType` legado
em `NONE`; o leitor do Snowflake ignora o primeiro por padrão, cai no segundo, não acha
unidade nenhuma e assume **milissegundos**. 1,787×10¹⁵ microssegundos viram 1,787×10¹⁵
milissegundos.

O que **não** pegou:

- a reconferência do carregador — compara **contagem** de linhas, e ela estava certa;
- os 166 nós do dbt, **todos verdes** — as durações viraram números grandes, não nulos nem
  erros, e nenhum tipo mudou;
- `assert_gold_grains_are_unique` — o grão continuou único;
- `assert_order_funnel_totals_match_fact_order` — um funil é feito de
  `count_if(marco is not null)`, e "não nulo" continua exato quando o instante está deslocado;
- `assert_fact_order_amount_equals_sum_of_items` — dinheiro não passa por timestamp.

Quem apontou foi um humano lendo **80.000.060 minutos de separação** num mart.

Só apareceu agora porque era a **primeira vez que um `TIMESTAMP` cruzava a fronteira**: até o
Marco 6 o recorte inteiro só tinha `DATE`, que viaja como `date32` sem ambiguidade de unidade.

A correção é `use_logical_type = true` no `file_format`. O que ficou permanente é o teste:
`assert_order_milestones_are_plausible_against_the_order_date` ancora cada marco contra
`order_date` — que chegou por **outro caminho**. Comparar marcos entre si passaria alegremente,
porque todos estavam deslocados pelo mesmo fator e a ordem relativa continuava certa.

O teste foi provado contra o defeito **real**, não contra uma injeção: o STAGE ainda estava
corrompido quando ele rodou pela primeira vez, e reprovou nos 6.400 pedidos.

### O DDL derivado achou uma contagem virando `FLOAT`

Menor, mesma família. `sum()` sobre `INTEGER` devolve `HUGEINT` no DuckDB; o parquet não tem
`INT128`, então a escrita rebaixa a coluna para `DOUBLE` — e `substituted_lines` e
`removed_lines`, que são contagens de eventos, chegariam ao warehouse declaradas como `FLOAT`.
Nenhum valor foi corrompido (nenhum passa de 40), mas o tipo passa a afirmar "isto pode ter
parte fracionária", que é falso.

Quem achou foi o DDL ser **derivado do próprio recorte**. Um DDL escrito à mão teria dito
`NUMBER(38,0)`, e a divergência entre o que o arquivo tem e o que a tabela declara só
apareceria no `COPY INTO` — ou nunca.

### O SCD2 finalmente paga por si, e dois caminhos independentes concordam

Até aqui `DIM_CUSTOMER` e `DIM_PRODUCT` eram SCD2 **sem nenhum fato apontando para uma
versão**. `FACT_ORDER` resolve a versão de cliente vigente na data do pedido por *range join*;
`FACT_ORDER_ITEM` resolve duas versões de produto — a do pedido e a do **cumprido**, que
diferem nas 4.670 linhas substituídas.

E há uma coincidência que virou verificação: a versão resolvida pelo *range join* é a mesma
que a Source gravou em `customer_ingestion_date` dentro do evento `order_placed`, nos
**6.400** pedidos. São dois caminhos que não se tocam — a escolha do roster em Python, no
momento da geração, e uma junção por intervalo em SQL, no warehouse. Enquanto coincidirem, o
SCD2 está sendo resolvido do jeito que a fase prometeu; quando divergirem, um dos dois lados
mudou de ideia sobre o que "versão vigente" significa, e isso precisa ser falha e não
descoberta em dashboard.

### Um funil que se apoia em marco, e os 61 pedidos que provam por quê

`order_status` guarda o estado do **último** evento. Um pedido devolvido tem status
`RETURNED` — **e foi entregue**. Medido: contar `order_status = 'DELIVERED'` dá **5.985**;
contar `delivered_at is not null` dá **6.046**. São os 61 devolvidos, e um funil montado sobre
status produziria uma taxa de entrega 1% menor que a real sem nada reprovar.

Marco é monotônico; status não é. Um funil é por definição uma contagem de etapas
**alcançadas**, então a coluna certa é o instante.

### Dois achados registrados em vez de corrigidos

**`sla_minutes_picking = 90` é inalcançável por construção.** A separação leva
`minutes_per_line_picked` (2) × número de linhas, e `basket_lines_max` é 40 — teto de 80. p50
= 36, p90 = 62, **máximo = 80**. Zero violações, e não porque a operação seja boa: porque as
três premissas não se cruzam. Baixar o limiar até o alerta acender seria adaptar a premissa ao
resultado desejado. O mart carrega `sla_minutes`, `max_picking_minutes` e
`orders_breaching_sla` **lado a lado** — quem lê vê 90, vê 80 e vê 0, e entende o zero.

**A janela de entrega quase nunca é cumprida, e o desvio é para CEDO.** Das 6.046 entregas,
**5.166 chegam antes de a janela abrir**, 471 dentro, 409 depois. Mediana de 4,6 h até a
entrega contra 12,8 h até o início da janela: `slot_lead_hours` sorteia 2–24 h enquanto a soma
dos marcos entrega em ~4,6 h. Duas premissas declaradas separadamente e nunca conciliadas.

O que mudou não foi o seed, foi **o que se publica**: `orders_delivered_before_slot` e
`orders_delivered_after_slot` viajam separados, porque chegar cedo e chegar tarde são
problemas operacionais **opostos** e "fora da janela" não diz qual dos dois está acontecendo.

### As premissas atravessam a fronteira, e não uma cópia delas

`sla_minutes_picking` tem dono: o seed que o gerador leu, cujo `sha256` está no manifesto de
cada partição do RAW. Reescrevê-lo como var do dbt criaria a segunda cópia que diverge na
primeira edição — e **nada reprovaria**, porque contar zero violação contra o limiar errado
tem exatamente a aparência de contar zero contra o certo. Daí `STG_ORDER_PREMISE` e
`FACT_ORDER_PREMISE`: metadado promovido a fato pelo mesmo motivo de `FACT_INGESTION_RUN`.

A var `currency` continua sendo var, e a diferença é o ponto: a Mercadona não declara moeda em
campo nenhum, então a premissa **não tem outra casa**.

Uma armadilha medida no caminho: `dbt seed` só recria a tabela com `--full-refresh`. Trocar
`column_types` num seed que já existe é um no-op silencioso até alguém forçar.

### Cada teste foi visto vermelho

`make warehouse-prove-tests` injeta, no dado **real** do warehouse, o defeito específico que
cada um dos cinco testes diz pegar; exige o vermelho; desfaz; e exige o verde de volta. Termina
rodando a suíte inteira e conferindo uma sentinela de contagens. Só toca GOLD e MART, que são
inteiramente reconstruíveis a partir do STAGE.

Depois do que aconteceu com os timestamps, um teste verde que nunca foi visto vermelho não é
evidência de nada.

### `make stream-evidence`

Espelha `make warehouse-evidence`, com um gatilho diferente: lá o motivo é **expiração** (a
conta é trial); aqui é que a metade em streaming **não é coberta offline** — `make test` roda
sem rede, e broker, OLTP e Iceberg só existem enquanto `make stream-up` estiver de pé.

`docs/stream-evidence/README.md` registra os três planos e os três folds concordando em 6.400
pedidos. Nenhum número escrito à mão. É **tolerante a plano desligado de propósito**: cada
seção ausente aparece como ausência declarada, nunca como zero — *"o outbox tem 0 eventos"* e
*"o OLTP não respondeu"* cabem na mesma célula de tabela e significam coisas opostas.


## Dívida técnica

Revisada em 2026-08-31. Seis itens em aberto, todos deliberados e com
o motivo escrito abaixo.

| Item | Situação |
|---|---|
| Cobertura de teste | **Fechada.** 19 → 131 testes na plataforma, com duplo de S3 em memória |
| Caminho de extração em container | **Fechado.** `bcn1` extraído, validado, aterrissado e transformado dentro do container |
| Ambientes redundantes | **Removidos.** 265 MB (`venv/` quebrado e `orchestration/.venv`) |
| Credenciais de desenvolvimento | **Endurecidas.** Portas em loopback, chaves aleatórias, compose recusa subir sem elas |
| Divergência de `data/` | **Contida.** O padrão `data/` do `.gitignore` casa em qualquer nível |
| Fanout de homônimo em `silver_ine_population_by_municipality` | **Fechado** em 2026-08-27, no mesmo dia em que foi achado |
| Modelo Silver e DAG do OLTP simulado | **Fechados** na Fase 2 (`silver_customer`, `silver_oltp_manifest`, `simulated_oltp_customers.py`) |
| Var `currency` declarada para o Gold e nunca usada | **Fechada.** `FACT_PRICE_SNAPSHOT` carrega a coluna: a premissa viaja junto do número |
| Papéis do Snowflake criados e verificados, mas não vestidos | **Fechada.** A carga roda como `RETAIL_LOADER` e o dbt como `RETAIL_TRANSFORMER`; quatro defeitos apareceram ao vestir |
| Identidade da conta cravada no `profiles.yml` | **Fechada.** `SNOWFLAKE_ACCOUNT`/`SNOWFLAKE_USER` sem default; a conta é trocável por `.env.snowflake` |
| Warehouse com `auto_suspend` de 300 s | **Fechada.** O `bootstrap` fixa X-Small e 60 s, como o plano da Fase 2 previa e nunca aplicou |
| `warehouse_load` nunca tinha rodado em container | **Fechada.** Imagem sem as dependências da Fase 2 e sem credencial; a lista de dependências deixou de ser duplicada |
| **Árvore `models/warehouse/` sem teste offline** | **Em aberto**, e é consequência de uma escolha. Mitigada por `make warehouse-evidence` |
| **Conta Snowflake é trial** | **Em aberto por natureza**, e o destino é trocável — verificado, não afirmado |
| Aviso `CustomKeyInConfigDeprecation` do dbt | **Em aberto, cosmético.** Config do `dbt-duckdb`, sem forma suportada ainda |
| **CI** | **Em aberto.** Cobriria a metade offline (992 testes + `make silver`), nunca a metade Snowflake |
| Modelo Silver e DAG dos pedidos simulados | **Fechados** na Fase 3 (4 modelos, 8 testes singulares, `simulated_orders_events.py`) |
| **Metade em streaming sem teste offline** | **Parcialmente fechada** nos Marcos 4, 5 e 6: `fake_pg.py` cobre a fronteira da transação, `fake_kafka.py` a ordem entre escrita e commit de offset, e `fake_iceberg.py` a fusão monotônica e o laço de retry — offline, em `make test`. Continua em aberto o que nenhum duplo cobre: que `rollback` desfaz, que o broker preserva ordem por chave, e que o Iceberg recusa commit de snapshot velho. Isso é `make orders-prove-atomicity`, `make orders-prove-stream` e `make orders-prove-projection` |
| **Silver de pedidos afirmava separação que o log não declara** | **Fechada** no Marco 4, no dia em que foi achada: 5.508 linhas de 298 pedidos. Achada por dois folds independentes discordando, não por teste |
| Gold e marts dos pedidos | **Fechados** no Marco 7: 4 STAGE, 4 FACT, 3 MART e 5 testes, cada um provado capaz de reprovar por `make warehouse-prove-tests` |
| **`TIMESTAMP` atravessava a fronteira 56 milhões de anos no futuro** | **Fechada** no Marco 7, no dia em que foi achada. `use_logical_type = true` no `COPY INTO`; o DuckDB anota a unidade só no `LogicalType` moderno e o Snowflake caía no `ConvertedType` legado. **166 nós do dbt construíram em verde por cima do defeito** — quem apontou foi um humano lendo um mart. Guardado agora por `assert_order_milestones_are_plausible_against_the_order_date` |
| Contagem de eventos virando `FLOAT` no parquet | **Fechada** no Marco 7. `sum()` devolve `HUGEINT`, o parquet não tem `INT128`, a escrita rebaixa para `DOUBLE`. Achada pelo DDL ser derivado do próprio recorte |
| **Premissa do gerador vivendo em dois lugares** | **Evitada** no Marco 7 em vez de fechada: `STG_ORDER_PREMISE`/`FACT_ORDER_PREMISE` carregam o seed inteiro para o warehouse, então `MART_FULFILLMENT_SLA` mede contra o mesmo número que gerou as durações. Uma var do dbt teria criado a cópia |
| **O portão do `dbt build` do Silver morava em seis arquivos** | **Fechado em 2026-08-31**, no dia em que a DAG reprovou. Ver abaixo |
| Papel `RETAIL_READER` criado, verificado e sem nenhum consumidor | **Fechada em 2026-08-31.** O painel Streamlit é o primeiro a vesti-lo, e prova a recusa em GOLD/STAGE na própria tela |
| **Nenhum mart junta cliente com pedido** | **Em aberto, e é a lacuna mais acionável do modelo.** Sem ela não há recompra, LTV, coorte nem receita por cliente. O elo existe em `FACT_ORDER.customer_sk`, no GOLD, fora do alcance do papel de BI. Não exige fonte nova — exige um mart com grão de cliente |
| **Metade em streaming sem registro de execução real** | **Fechada** no Marco 8. `make stream-evidence` escreve `docs/stream-evidence/README.md` a partir dos três planos vivos — nenhum número à mão, e seção ausente aparece como ausência declarada, nunca como zero |

### Vestir os papéis: o que só aparece quando se para de rodar como administrador

Os três papéis existiam desde a Fase 2, com os grants certos e a matriz de isolamento
verificada por `check_isolation`. E **nenhuma execução passava por eles** — a carga e o dbt
rodavam como `ACCOUNTADMIN`. É a diferença entre governança verificada e governança
adotada, e ela custou quatro defeitos, todos invisíveis enquanto o administrador rodava
tudo:

| Sintoma | Causa | Por que não aparecia antes |
|---|---|---|
| `No active warehouse selected` na carga | Os papéis não tinham `usage` no **warehouse** | Administrador enxerga todo warehouse. Dado sem compute não se move, e o erro aponta para a sessão, não para o grant |
| `schema ausente: GOLD, MART` | `require_schemas` conferia os três schemas | Era o **isolamento funcionando**: `information_schema` devolve só o que o papel vê, e o carregador não vê GOLD. Verificação mais ampla que a necessidade transforma controle em falha |
| 8 modelos com `must have OWNERSHIP granted on TABLE` | `grant all` concede os privilégios **aplicáveis**, e posse não é um deles | `create or replace table` exige posse. Numa conta nova é inócuo — quem cria já nasce dono; só aparece em conta onde o admin criou antes |
| `information_schema` vazio para o administrador | Os papéis customizados não estavam pendurados em `SYSADMIN` | Só surgiu **depois** de a posse sair do admin e os papéis secundários serem desligados. Não dá erro: apenas apaga os objetos da vista de quem administra |

O quarto é o mais instrutivo dos quatro, porque é o único que **não falha** — herança de
papel sobe (`SYSADMIN` passa a ver `MART`) e nunca desce (`RETAIL_READER` continua sem
`GOLD`), então a correção não afrouxa nada, e a ausência dela teria passado como "está
tudo certo" até alguém precisar administrar a conta.

Os quatro viraram teste em `test_snowflake_load.py` (16 → 29), pelo mesmo critério do
resto do arquivo: nenhum falha de forma óbvia se voltar atrás.

**O que ainda não é isolamento de verdade:** há um usuário só, com os três papéis. O
isolamento real seria um usuário de serviço por papel, sem `ACCOUNTADMIN` — mas isso é
decisão de quem administra a conta, não do repositório. O que o repositório garante é que
`default_secondary_roles = ()` está aplicado: sem isso, contas Snowflake modernas ativam
**todos** os papéis do usuário além do primário, e vestir o papel seria decorativo. Medido
nesta conta antes da correção: `current_secondary_roles()` devolvia
`ORGADMIN, RETAIL_READER, RETAIL_TRANSFORMER, RETAIL_LOADER`.

### `warehouse_load` compilava e não rodava

A DAG foi escrita na Fase 2 e verificada como as outras quatro: importa, monta o grafo,
`airflow dags list` a enxerga. **Compilar não é executar** — e só o caminho pelo host
(`make warehouse-refresh`) tinha sido exercitado de verdade. Na primeira execução real ela
parou em `load_stage` com `exit 2`:

```
ERRO: snowflake-connector-python nao esta instalado neste venv.
```

Duas causas independentes, e a segunda só apareceria depois de corrigida a primeira:

1. **`infra/Dockerfile.airflow` duplicava a lista de dependências** de
   `platform/pyproject.toml`. A Fase 2 acrescentou `dbt-snowflake` e
   `snowflake-connector-python` ao pyproject; a imagem continuou com a lista da Fase 1. A
   duplicação fez o que duplicação faz, e o custo foi pago em runtime, dias depois, com o
   dado parado no meio do caminho.
2. **Não havia credencial no container.** `~/.snowflake` não estava montado, então nem o
   conector nem o dbt teriam como autenticar.

A correção da primeira não é "acrescentar dois pacotes": é **ler o `pyproject.toml`** no
build, o que elimina a classe inteira do problema, e importar os dois adaptadores mais o
conector como último passo do `RUN` — se uma dependência sumir, o **build** quebra, não a
DAG.

A da segunda monta `~/.snowflake` **no mesmo caminho** de dentro e de fora
(`${HOME}/.snowflake:${HOME}/.snowflake:ro`), com `SNOWFLAKE_HOME` apontando para lá. É o
que faz `private_key_file` do `config.toml` e `SNOWFLAKE_PRIVATE_KEY_PATH` do
`.env.snowflake` resolverem idênticos nos dois lados — nenhum caminho precisa ser
reescrito em lugar nenhum. Só leitura: o orquestrador lê a chave e nunca a reescreve, e
como o container roda com `AIRFLOW_UID` (o uid do host), nenhuma permissão do arquivo modo
600 precisa ser afrouxada.

A identidade da conta entra por `env_file` com `required: false`, e **não** por
`environment:` — no bloco de mapa, uma variável ausente vira presente-e-vazia no
container, e `env_var('SNOWFLAKE_ACCOUNT')` sem default devolveria o vazio em vez de
abortar. É a mesma armadilha já documentada no Makefile, um nível acima. Com
`required: false`, quem não tem conta Snowflake continua subindo `make up` e as quatro DAGs
de source normalmente.

Medido depois da correção: a DAG fecha as quatro tarefas, e o `query_history` mostra a
separação de papéis no **tipo** de query — `RETAIL_LOADER` com `PUT_FILES`/`COPY`,
`RETAIL_TRANSFORMER` com `CREATE_TABLE_AS_SELECT`, `RETAIL_READER` só com `SELECT`.

### O destino é trocável — verificado, não afirmado

A conta é um trial, e a resposta a isso não é evitar depender dela: é garantir que trocá-la
seja barato. Duas coisas contradiziam isso e foram corrigidas.

`profiles.yml` trazia `SNOWFLAKE_ACCOUNT` e `SNOWFLAKE_USER` **cravados como default**.
Quem clonasse o repositório sem configurar nada não receberia "configure a conta" —
receberia uma tentativa de conexão contra a conta de outra pessoa, falhando com
`Object does not exist` bem longe da causa. Para o MinIO o default faz sentido (`minioadmin`
é convenção local que o `.env.example` repete); para uma conta Snowflake não existe default
que sirva a outro. Sem default, o dbt aborta dizendo `Env var required but not provided:
'SNOWFLAKE_ACCOUNT'`.

Trocar de conta hoje é: editar [.env.snowflake.example](.env.snowflake.example) copiado
para `.env.snowflake`, o bloco correspondente de `~/.snowflake/config.toml`, e rodar
`make warehouse-bootstrap`. **Nenhum modelo, nenhum SQL e nenhum teste muda** — é a
fronteira física L2→L3 pagando por si.

Verificado nos dois sentidos: `dbt parse --target dev` passa com todas as variáveis do
Snowflake ausentes, e `--target snowflake` falha nomeando a que falta.

### `models/warehouse/` sem teste offline — e por que a resposta não é um espelho DuckDB

Um espelho em DuckDB dos 21 modelos teria **passado** nos dois erros que quebraram a
primeira execução real: `FILTER (WHERE ...)` e `WINDOW ... AS`, que o DuckDB aceita e o
Snowflake não. Um teste que não reproduz o modo de falha não é teste — é uma segunda
implementação para manter, e daria confiança falsa exatamente onde não há.

A mitigação é outra: [`make warehouse-evidence`](docs/warehouse-evidence/README.md)
registra o resultado da execução **real** — posse objeto a objeto, volume, matriz de
isolamento e amostra de cada mart — com data e identidade da conta. Converte "código sem
teste" em "código executado, com a prova anexada e datada". É regenerável: vincular outra
conta e rodar de novo produz a evidência daquela conta.

O que continua verdadeiro: o recorte (24 testes) e o transporte (29) são cobertos offline,
e é neles que moram os erros silenciosos — agregado somado junto do detalhe, escopo
esquecido, coluna casada por posição. O SQL do warehouse falha alto quando falha.

### Fanout de homônimo no Silver de população

`silver_ine_population_by_municipality.sql` casava a tabela RAW com o seed de códigos
usando `inner join ... on e.municipality_name = c.municipality_name` — **só por nome**. O
comentário do próprio modelo justificava que isso era seguro porque "o seed já vem escopado
a 08/28/41/46, onde nenhuma colisão foi medida". A medição confirma que o seed realmente
não tem colisão interna (0 nas 4 províncias), mas **a conclusão não seguia**: o lado RAW é
**nacional** (~8.200 municípios) e traz homônimos de outras províncias com `Nombre`
idêntico. O docstring de `scripts/derive_municipality_codes.py` já antecipava exatamente
isso ("um join Espanha-inteira por nome sozinho fosse ambíguo para 3 desses 18 nomes"); o
que passou despercebido é que o modelo faz esse join Espanha-inteira, porque a extração
não filtra nada. Três municípios recebiam duas linhas:

| Município na AUF | Série correta | Série intrusa |
|---|---|---|
| Arroyomolinos (28/015) | `DPOP12967` = 38.075 | `DPOP4729` = 816 (Cáceres, 10023) |
| Molar, El (28/086) | `DPOP13174` = 9.999 | `DPOP19423` = 295 (Tarragona, 43085) |
| Torrent (46/244) | `DPOP21778` = 90.928 | `DPOP7960` = 182 (Girona, 17197) |

Efeito: `mad1` devolvia 130 linhas para 128 municípios e `vlc1`, 64 para 63.

**Por que o teste de grão não pegou.** O grão testado é
`(ingestion_date, series_code, year, fk_periodo)`, e as duas séries têm `series_code`
diferente — o grão continuava único. O teste que faltava, agora existe:
`assert_ine_population_by_municipality_has_one_series_per_municipality` afirma uma linha
por `(município, sexo, ano)`, que é o invariante real.

**Correção: pelo código oficial da própria fonte, não por heurística.** O payload de
`DATOS_TABLA/29005` só tem `COD`, `Nombre`, `FK_Escala`, `FK_Unidad` e `Data` — sem
província, sem `MetaData` (verificado). Mas `GET /ES/VALORES_SERIE/{COD}` devolve, para
cada série, o **código oficial INE do município** (província+município), o mesmo esquema do
Callejero e dos seeds `warehouse_*`. [scripts/derive_ambiguous_series.py](scripts/derive_ambiguous_series.py)
identifica offline quais nomes são ambíguos no RAW *e* existem no seed das 4 províncias
(hoje 3 nomes, 18 séries), consulta essas séries e grava
`ine_ambiguous_series_seed.csv`. O modelo passa a filtrar: série de nome ambíguo só entra
se o código oficial bater com o do seed; nome não ambíguo continua resolvido por nome.

Medido depois da correção: 128/133/46/63 municípios por warehouse, uma série cada. Os
`customers.json` das quatro partições saíram **byte a byte idênticos** aos anteriores — a
defesa provisória do export ("fica o maior valor") vinha acertando, mas por coincidência
de porte, não por saber qual série era qual. `export-oltp-reference` deixou de deduplicar:
agora apenas **reconfere** o invariante e **recusa** se ele cair, em vez de escolher um
valor por conta própria.

### Cobertura de teste

| Módulo | Testes | Situação |
|---|---|---|
| `manifest.py` | 17 | Obrigações do contrato e recusas |
| `land.py` | 10 | Upload, idempotência, auto-correção, abortar antes de `_SUCCESS` |
| `verify.py` | 7 | Adulteração, objeto ausente, órfão, manifesto divergente |
| `config.py` | 8 | Precedência de credencial e o `.env` não sobrepor o ambiente |
| `query.py` | 2 | Conversão de endpoint com e sem esquema |
| `oltp_reference.py` | 33 | As 3 queries do export contra fixtures DuckDB reais |
| `prune_local` | 10 | Só apaga a cópia local depois de duas conferências |
| `snowflake_export.py` | 24 | O recorte: agregado `'Total'`, escopo AUF, dedup, DDL derivado |
| `snowflake_load.py` | 29 | Stage qualificado, `OVERWRITE`, casamento por nome, reconferência, e os 4 defeitos de papel |
| `snowflake_evidence.py` | 10 | Totais somados e não escritos, isolamento quebrado em destaque, amostra que falhou não vira vazia |

Fechado com um duplo de cliente S3 em memória (`platform/tests/fake_s3.py`), no espírito do
duplo de HTTP que a Source já usa. O duplo **valida o `ChecksumSHA256` declarado**, como o
servidor real faz: um erro na conversão hex→base64 falharia em teste, não em produção.

Confirmado não-vazio por mutação: desligar a comparação de sha256 em `verify.py` faz
`test_catches_a_tampered_object` falhar.

**O que ainda não é coberto**, e a lista cresceu com a Fase 2:

- **O caminho `dbt` do Silver** — os 293 testes de dados exigem object storage de pé.
- **As DAGs** — nenhum teste importa o módulo do Airflow. As seis compilam via `DagBag`
  no container, o que pega erro de import mas não comportamento.
- **A árvore `models/warehouse/`** — os 21 modelos e 149 testes só rodam **contra o
  Snowflake**. Não há equivalente offline, e não é oversight: um espelho em DuckDB seria
  uma segunda materialização da mesma verdade, e foi justamente a diferença entre os dois
  motores (`FILTER`, `WINDOW`) que os quebrou na primeira execução — um espelho DuckDB teria
  passado e escondido exatamente esses erros. **A consequência é real e fica registrada: sem
  conta Snowflake, `make warehouse` não roda e essa metade do projeto não é verificável.**
  O que atenua é que o recorte que a alimenta (`snowflake_export.py`, 24 testes) e o
  transporte (`snowflake_load.py`, 16) são cobertos offline, e é neles que moram os erros
  silenciosos — o SQL do Gold falha alto quando falha.

Os três foram verificados manualmente, em execução real.

### Endurecimento do stack local

- **Portas em `127.0.0.1`** por padrão (`BIND_ADDR`). Antes escutavam em `0.0.0.0` com
  `minioadmin/minioadmin` e `admin/admin` — qualquer máquina da rede alcançava o console do
  MinIO e a UI do Airflow.
- **`AIRFLOW_SECRET_KEY` e `AIRFLOW_FERNET_KEY` aleatórias**, geradas por `make secrets` no
  `.env` (modo 600, fora do versionamento). O compose usa interpolação `:?` e **recusa
  subir sem elas**, de modo que não existe caminho em que um valor de exemplo vire a chave
  real por esquecimento.
- **`DUCKDB_PATH` do orquestrador vive dentro do container**, não no repositório montado. O
  DuckDB é single-writer: com o arquivo compartilhado, uma janela de DBeaver esquecida no
  host derrubaria a tarefa `silver` do DAG.

### Sem CI

Nenhum `.github/workflows`. A fronteira depende de alguém rodar `make test` — e o alvo
`source-test` existe exatamente para ser um job que não instala nada. Dois jobs
(Source sem dependência, plataforma com venv) tornariam a fronteira verificada a cada push
em vez de por disciplina. Adiado para quando o repositório subir.

### A conta Snowflake é um trial — **aberta, por natureza**

Trial de 14 dias a partir de 2026-08-27. Quando expirar, `make warehouse` para de rodar e
com ele os 21 modelos e 149 testes do Gold/Mart. **O lakehouse não é afetado**: `make silver`
e as 992 suítes Python continuam offline, sem credencial e sem custo — foi para isso que a
fronteira L2→L3 é física. Um trial anterior já expirou durante esta fase e o sintoma foi
`390913`, com o login autenticando e nenhum warehouse disponível.

### Aviso de depreciação do dbt — **aberta, cosmética**

`dbt build` emite 16 ocorrências de `CustomKeyInConfigDeprecation` por causa de
`+format: parquet` em `dbt_project.yml`. É config do `dbt-duckdb`, não do dbt-core, e mover
para `config.meta` como o aviso sugere pode quebrar a materialização `external`. Deixado
como está até o `dbt-duckdb` publicar a forma suportada; o aviso é ruído, não sintoma.

## Painel de conferência: o terceiro papel finalmente vestido

Streamlit sobre o `MART`, 16 indicadores em 6 grupos. O propósito declarado não é *mostrar
dados* — é **conferir os indicadores antes de reconstruí-los no Power BI**, que é uma
ferramenta onde a medida obviamente errada e a certa têm exatamente a mesma aparência.

### O papel de BI deixa de ser decorativo

Os três papéis existem desde a Fase 2. A carga passou a vestir `RETAIL_LOADER` e o dbt
`RETAIL_TRANSFORMER` quando aquela dívida foi fechada; **`RETAIL_READER` continuava sem
nenhum consumidor**. Este painel é o primeiro, e a consequência é concreta: ele lê `MART` e
é *recusado pelo motor* em `GOLD` e `STAGE` — verificado ao vivo, na própria tela, com
`use secondary roles none`.

Isso tem um efeito de projeto que vale mais que a conveniência: quando um indicador pede algo
que o papel não alcança, isso é **informação**, não obstáculo. Foi assim que a maior lacuna
do modelo apareceu (abaixo).

### O CONTRACT é gerado, não escrito

`streamlit/indicators.py` carrega, para cada indicador, a pergunta, o grão, o tipo
(observado/sintético) e o SQL **no mesmo objeto**. `streamlit/CONTRACT.md` é derivado dele.

O motivo é o de sempre neste repositório, e aqui ele morde mais: o CONTRACT existe para
alguém ler a consulta ao lado da explicação e decidir se o indicador está certo. Se os dois
morassem em arquivos separados, divergiriam no primeiro ajuste — e **a conferência continuaria
passando**, porque ninguém lê um SQL e um texto lado a lado procurando desacordo. Um teste
offline reprova se o arquivo no disco não for o que o gerador produz, e foi provado capaz de
reprovar.

### As três armadilhas que o painel existe para publicar

| Armadilha | Medido |
|---|---|
| **Perda de valor tem duas causas** | `SUM(gross) − SUM(net)` = 58.327,81 mistura cesta que encolheu na separação (4.834,73) com pedido que morreu antes dela (53.493,08). A soma fecha exatamente; um número único esconde qual está acontecendo, e são áreas diferentes — operação de loja contra pagamento |
| **Ticket médio tem dois denominadores** | receita/separados = 134,57; receita/colocados = 128,30. O segundo divide a receita de quem foi separado pelo total incluindo quem nunca chegou lá |
| **`orders_touching_category` não é aditivo** | somar as 151 categorias de um dia dá muito mais que os 1.600 pedidos daquele dia |

E as duas que a Fase 3 já havia registrado voltam aqui como aviso na tela, porque é onde
alguém as leria errado: `orders_breaching_sla = 0` só é legível ao lado do limiar (90) e do
máximo observado (80); e a aderência à janela de 8% precisa das três contagens, porque
**5.166 das 6.046 entregas chegam antes de a janela abrir** — chegar cedo e chegar tarde são
problemas opostos.

### A lacuna que o exercício revelou

**Nenhum mart junta cliente com pedido.** `MART_CUSTOMER_BASE` tem cliente sem pedido;
`MART_ORDER_FUNNEL` e `MART_BASKET_DAILY` têm pedido agregado sem cliente. O elo existe em
`FACT_ORDER.customer_sk`, no GOLD — fora do alcance de `RETAIL_READER`.

Consequência: **não há recompra, LTV, coorte, RFM nem receita por cliente**, e são
exatamente os indicadores que um painel estratégico costuma ser cobrado de ter. Não exige
fonte nova — exige um mart com grão de cliente e medidas de pedido. Fica registrado como a
ausência mais acionável da lista, com o gatilho escrito, em vez de aproximada por algum
número que *pareceria* responder.

### Onde as dependências ficam, e por quê

`streamlit`, `pandas`, `pyarrow` e `altair` vão em `[project.optional-dependencies]` de
`platform/pyproject.toml`, **fora** de `dependencies`. O `infra/Dockerfile.airflow` instala
exatamente aquela lista, e ~150 MB de UI não têm o que fazer numa imagem que não renderiza
dashboard. Mesmo venv, porém: o app precisa do conector do Snowflake que já está lá, e um
segundo venv duplicaria o conector só para não duplicar o Streamlit.

### Por que o smoke test não é um `curl`

O Streamlit devolve **HTTP 200 com o esqueleto da página mesmo quando o script morre no
primeiro `select`** — a renderização é no cliente. `make dashboard-check` roda o script de
verdade via `AppTest` e exige zero exceção; é a única forma de as 19 consultas serem
exercitadas. Fica fora de `make test` porque exige conta viva.

## O portão do Silver morava em seis arquivos, e nenhum concordava com o outro

Achado em 2026-08-31 pela execução real: `mercadona_catalog_daily` reprovava **todo dia** na
última tarefa. Os quatro armazéns extraíam, validavam, aterrissavam e verificavam com
sucesso; o `silver` caía com *"no version-hint could be found"*.

**O dado estava certo o tempo inteiro. O portão é que morava no arquivo errado.**

### A decisão, e as seis cópias dela

Nem todos os 21 modelos do Silver podem ser construídos sempre, e os dois motivos são
legítimos: uma source que ainda não aterrissou nada faz `read_json` **falhar** (não devolver
zero linhas), e `silver_live_order_state` só pode ser lido quando o catálogo Iceberg
responde, porque o caminho do metadado vem dele e nunca de uma varredura do storage.

Essa decisão existia em seis lugares:

| Onde | Portões que tinha |
|---|---|
| alvo `silver` do Makefile | os cinco — quatro sources + Iceberg |
| `simulated_orders_events` | um — a própria source |
| `ine_population_on_demand` | um — a própria source |
| `ine_callejero_on_demand` | um — a própria source |
| `simulated_oltp_customers` | um — a própria source |
| **`mercadona_catalog_daily`** | **nenhum** |

A Mercadona sempre tem dado, então ninguém sentiu falta do portão dela — até o Marco 6
criar um modelo que **não tem nada a ver com a source daquela DAG** e que ela passou a
tentar construir todo dia.

### Por que nada pegou

`make silver` passava — tem o portão completo. `make test` passava — não olha DAG. A suíte
do dbt nunca chegava a rodar. Só a execução real reprovava, e um dia depois, o que é a
distância máxima entre a causa e o sintoma neste repositório.

### O que ficou

Um verbo: `retail_platform silver-build`, sobre `silver_gate.py`. `plan()` é **pura** —
recebe o que foi observado e devolve `--exclude`/`--vars` — então a decisão inteira é
exercitável sem MinIO e sem catálogo. Makefile e as cinco DAGs chamam o mesmo verbo.

E uma checagem de fonte, porque **um portão único só vale enquanto for o único**: a suíte
exige que nenhuma DAG do Silver monte o próprio `dbt build`, e que todo modelo de source
esteja atribuído a alguma source em `SOURCE_MODELS`. A segunda é a que pega o modelo *novo*
— quem criar um e esquecer de registrá-lo reproduz este defeito exatamente. As duas foram
provadas capazes de reprovar antes de serem aceitas.

### O segundo achado, que o primeiro escondia

Com o portão certo, o container do Airflow passou a **excluir** a projeção — e a excluir
sempre. `orders_projection.catalog()` cai num default `localhost:5433`, que dentro do
container é o próprio container.

**Isso não reprova nada**: o build fica verde com uma tabela a menos, que é o pior tipo de
sucesso. Resolvido dando ao serviço o seu próprio endereço
(`ICEBERG_CATALOG_URI: postgresql+psycopg://oltp:oltp@oltp-postgres:5432/iceberg_catalog`),
que é a diferença entre *"não pode"* e *"não tentou"*.

E, ao verificar, apareceu o terceiro: a **imagem do Airflow em execução era anterior ao
Marco 5** — sem `psycopg`, `confluent-kafka` nem `pyiceberg`. O `Dockerfile.airflow` já
tinha a verificação de import que quebra o build quando uma dependência some; ela estava
certa e ninguém a executou. Reconstruída, o Airflow constrói os 319 nós.

## Dois defeitos de orquestração que só apareceram com dois armazéns

Ambos invisíveis com um único armazém, e ambos silenciosos: o DAG terminava `success`
enquanto deixava de transformar dado recém-aterrissado.

1. **`trigger_rule` padrão do `silver`.** `all_success` faz a tarefa compartilhada ser
   pulada quando *qualquer* upstream é pulado. Com o `mad1` curto-circuitando por já estar
   aterrissado, o `silver` era pulado mesmo com o `bcn1` tendo acabado de aterrissar.
   Corrigido para `NONE_FAILED_MIN_ONE_SUCCESS`.

2. **`ignore_downstream_trigger_rules` do `ShortCircuitOperator`.** O padrão é `True`, e
   com ele o short-circuit pula **todo** o downstream **ignorando a `trigger_rule` de cada
   tarefa** — inclusive a que acabara de ser corrigida. O sintoma foi exatamente o mesmo, o
   que torna o segundo defeito fácil de confundir com a correção do primeiro ter falhado.
   Com `False`, o gate pula apenas o próprio ramo e o `silver` volta a decidir pela própria
   regra.

Verificado no cenário misto: `mad1` curto-circuita, `bcn1` percorre
`extract → validate → land → verify`, e o `silver` **roda**.

## Calibração da demanda contra o MAPA 2025 (Fase 4)

Até aqui o simulador de Orders escolhia produto **uniformemente sobre o catálogo**, e isso
estava declarado como premissa: *"o mix por categoria espelha o TAMANHO do sortimento"*.
Deixou de valer quando apareceu uma âncora observacional que não existia — o **Informe del
Consumo Alimentario en España 2025** do MAPA, que mede volume, valor, preço médio e canal do
consumo doméstico espanhol.

### O achado que abriu a fase não era de demanda

A investigação começou por um sintoma: "Marisco y pescado" tinha **3,38% das unidades e
22,82% da receita**, com preço médio pago de **27,09 €** contra um catálogo cujo produto mais
caro em mad1 custava **24,05 €**. Um preço médio acima do máximo do sortimento não pode vir de
escolha de produto.

O RAW resolveu. Quando `selling_method = 1` e `unit_size` é nulo, a API da Mercadona devolve
`unit_price = reference_price × 99` — o preço do **teto do seletor de peso**, não de nada que
um domicílio compre. O fator é exatamente `99,000` em **10 combinações produto×armazém**, e a
porção realmente comprável está em `min_bunch_amount`, um campo **que estava no RAW desde a
primeira partição e que o Silver descartava**.

| faixa de preço | produtos | unidades | receita | % da receita |
|---|---|---|---|---|
| ≤ 30 € | 4.921 | 204.393 | 622.812,08 | 75,85 % |
| 30–100 € | 6 | 203 | 8.818,30 | 1,07 % |
| **> 100 €** | **12** | **275** | **189.491,55** | **23,08 %** |

**12 produtos em 4.939 — 0,24% do sortimento — produziam 23% da receita**, e nenhum dos 947
testes reprovava, porque o número continuava internamente consistente. É a mesma classe de
defeito do carimbo de tempo em milissegundos do Marco 7: uma unidade de medida errada não
quebra nenhum total.

`unit_price` **permanece intacto** em `silver_product_price` — projeção fiel da fonte é
invariante. O que entrou foram colunas derivadas ao lado: `purchasable_unit_price`,
`price_basis`, `net_content_kg_l`, e os três campos de granel que o modelo jogava fora.

### A resposta à pergunta que foi feita

*"Produtos de preço elevado estão recebendo demanda excessiva porque aumentam o valor da
Order?"* — **Não.** Preço não entra em nenhum sorteio, nem antes nem depois desta fase. O
mecanismo era o oposto: a escolha era *indiferente* ao preço, e foi a indiferença, sobre um
catálogo com 12 preços mal escalados, que concentrou a receita.

### O que a calibração faz, e o que ela recusa fazer

```
grupo de demanda   P(g)  <- alvo de VOLUME (kg/L) do MAPA, inclinado pelo canal e-commerce
       |
produto no grupo         <- UNIFORME (nenhuma fonte mede giro por SKU)
       |
quantidade / preço       <- inalterado / OBSERVADO
       |
valor do pedido          <- consequência, nunca objetivo
```

Preço não aparece em nenhuma seta que aponta para demanda. **Volume e valor divergem de
propósito**: no MAPA, mariscos são 0,81% do volume e 2,88% do valor, e um simulador que os
igualasse estaria errado.

**A fronteira, que vale mais que a calibração.** O MAPA mede consumo doméstico do residente —
não mede pedido de loja online, nem cesta, nem cadência. Por isso `daily_order_rate`,
`basket_lines_*` e `quantity_max` **continuam `synthetic` e não receberam calibração nenhuma**.
Transformar o benchmark em fonte para esses números seria transformá-lo numa falsa
representação da realidade.

### Três coisas que o informe não sustenta, registradas em vez de inventadas

| Dado | Por que não | O que foi feito |
|---|---|---|
| **Sazonalidade mensal por categoria** | Os gráficos mensais são **imagens**: só os rótulos dos eixos saem no texto. Há cinco números mensais em prosa, todos do total. E a janela cobre apenas agosto. | Slot criado **neutro** nos 12 meses, aplicado à taxa de pedidos (nunca ao mix, onde um fator global se normalizaria). Um par de testes prova que o mecanismo funciona *e* que o perfil entregue está neutro. |
| **E-commerce por categoria** | Só 18 dos 102 blocos trazem a linha de canal. | Inclinação **fina** nos 18, **grossa** (1,1% fresca / 2,8% resto sobre 2,2% total) nos demais. Cada grupo carrega `channel_basis` e o relatório reporta qual regra o produziu. |
| **Não-alimentar (~30% das unidades)** | Fora do universo do informe. | Fatia mantida com premissa agregada declarada, **nunca somada** ao bloco calibrado. |

Também registrado: a folha de rosto do PDF diz *"Informe del consumo alimentario en España
2024"* enquanto o corpo inteiro reporta **2025**. É resíduo da edição anterior. Os números
vêm do corpo, e a discrepância está no CONTRACT — não se ajusta a fonte, registra-se o achado.

### O que mudou, medido na mesma janela

| dimensão | ANTES | DEPOIS |
|---|---:|---:|
| unidades | 204.871 | 204.824 |
| kg ou litro | 126.550 | 144.425 |
| receita | 821.121,93 | 583.154,43 |
| EUR/kg | 6,49 | 4,04 |

| grupo, % do volume | ANTES | ALVO | DEPOIS |
|---|---:|---:|---:|
| MARISCOS_MOLUSCOS_CRUSTACEOS | 11,44 | 0,43 | 0,43 |
| FRUTAS_FRESCAS | 3,11 | 9,17 | 9,51 |
| HORTALIZAS_FRESCAS | 2,16 | 6,02 | 5,62 |
| PATATAS | 1,22 | 4,53 | 4,67 |

Erro absoluto médio contra o alvo: **0,098 ponto**. **A queda de 29% na receita é a correção
funcionando, não uma regressão** — 23% dela eram 12 produtos com preço de teto de API.

### Uma decisão de desenho que só apareceu ao rodar

A projeção Iceberg funde estado de forma **monotônica**, descartando linha com
`last_sequence_no` menor — é assim que os dois escritores convivem. Essa fusão assume, sem
dizer, que um `order_id` sempre se refere ao mesmo pedido. Trocar `demand_model_version` é a
**quarta condição não-aditiva** do CONTRACT, e ali a premissa cai: o rebuild descartou **4.028
linhas** como "mais velhas" e deixou a projeção com dois universos misturados.
`orders-reconcile` pegou. `--reset` passou a existir por causa disso, é destrutivo de
propósito e nunca acontece sozinho.

### O que ficou verificável

- `make demand-check-mapping` — as **444 trincas** do catálogo casam exatamente **uma** regra
  do de-para; zero sem regra, zero ambíguas, zero regras mortas. Sem default silencioso.
- A cobertura é conferida contra a **árvore de categorias**, não contra o recorte: o dedup do
  catálogo esconde trincas que existem na fonte, e conferir contra ele mediria o desempate.
  Três regras corretas pareceram mortas antes disso ser percebido.
- `make demand-reality-check` gera `docs/demand-evidence/README.md` com ANTES · MAPA · ALVO ·
  DEPOIS nas três dimensões, e **sai 1** se o pior desvio passar de um limiar largo e
  declarado. O limiar não é nota de qualidade: existe para pegar calibração silenciosamente
  inerte. Provado — o ANTES reprova com 11,007 pontos; o estado atual passa com 0,660.

## Perfil de consumo do cliente (Fase 5)

A Fase 4 calibrou a demanda **agregada**. O que ficou de fora era que todos os clientes
compravam a mesma cesta esperada: um cliente de 22 anos em Sevilha e um de 78 em Barcelona
sorteavam da mesma distribuição. Esta fase troca `P(grupo)` por `P(grupo | coorte)`.

### O achado que abriu a fase, outra vez, não era de demanda

**18,01% dos clientes tinham menos de 18 anos** — 3.602 de 20.000, com `age_at_ingestion`
indo de 0 a 100. Havia titular de conta recém-nascido.

Isso **não era defeito da Source de OLTP**: o contrato dela declara que a idade vem da
distribuição *populacional* provincial do INE (tabela 31304), e é exatamente isso que ela
entrega — uma projeção fiel da população residente. O que nunca fora declarado era a
diferença entre **residente** e **quem coloca um pedido**.

Enquanto a idade não fazia nada, isso era inofensivo. É a mesma forma do achado da fase
anterior: um número internamente consistente que só vira erro quando alguém passa a usá-lo.
Ao ligar a idade à demanda, 18% da base entraria na faixa `-35 anos` do MAPA sendo criança, e
a calibração ficaria errada por construção sem que nenhum total quebrasse.

A correção mora onde a pergunta mora: `min_buyer_age = 18` em `order_premises_seed.csv`, uma
premissa do **domínio de pedidos**. A base de clientes não foi tocada e continua sendo o que
o contrato dela diz que é.

### Duas pontes, e três recusas

| corte do MAPA | o cliente tem? | veredito |
|---|---|---|
| idade do responsável de compra (4 faixas) | `birth_year`, do INE 31304 | **usado** — governa o mix |
| comunidade autónoma (17) | `province_code`, do Callejero | **usado** — mix e frequência |
| ciclo de vida do lar (9 tipos) | não tem composição familiar | **recusado** |
| nível socioeconómico (5 níveis) | não tem renda | **recusado** |
| sexo do comprador | tem — mas o informe só o publica para consumo *extradoméstico* | **recusado** |

O ciclo de vida é o corte mais rico do informe e vem completo. O bloqueio não é o dado, é o
atributo: atribuir composição familiar a um cliente que não a tem seria **inventar o
atributo** — a mesma proibição que a Fase 1 aplicou à densidade por tramo. **Gatilho
registrado:** se uma fase futura ingerir lares por província do INE, o corte abre, e o dado
do MAPA já estará no seed.

### A extração: os números estavam em gráficos, e os gráficos têm rótulo

Só **17 das seções** trazem a tabela demográfica em texto; as demais são imagens. Mas os
gráficos carregam **rótulo numérico impresso**, e ler um rótulo é extração, não estimativa.
Foram lidas ~45 páginas para cobrir os 39 grupos pesáveis, cada leitura conferida por dois
checksums independentes: as quatro faixas de volume somam 100,00, e as de população somam
`8,89 + 30,33 + 31,34 + 29,44` — os mesmos quatro números em **toda** seção, porque são o
universo. Um dígito mal lido quebra uma das duas somas, e `load_cohort_age` reprova.

Duas discrepâncias da própria fonte ficaram registradas em vez de aparadas: a página 206
publica `30,5 / 31,7 / 29,0` de população onde todas as outras publicam `30,3 / 31,3 / 29,4`,
e a página 84 rotula Madrid com `13,78` onde as demais rotulam `13,86`.

### O agregado não se move, e esse é o critério de aceitação

Sem correção, o mix agregado sairia do alvo só porque a nossa pirâmide etária não é a do
MAPA — a calibração da fase anterior seria desfeita de lado, sem nada falhar. Um *iterative
proportional fitting* ajusta um fator por grupo até que a média dos pesos por coorte,
ponderada pela distribuição real de coortes **entre os pedidos**, reproduza os pesos da `v1`.

Medido: convergência em **6 iterações**, maior desvio **1,0×10⁻¹⁰**. O erro contra o alvo do
MAPA ficou em 0,075 ponto médio, contra 0,098 antes — a diferença é ruído de amostragem.

Isso dá à fase um critério limpo, e uma consequência para quem for lê-la: **procurar o efeito
num total não encontra nada.** Ele está inteiro na condicional, e é por isso que
`MART_DEMAND_COHORT` e a seção de coorte do reality check existem.

### A restrição que só apareceu ao medir

Normalizando a coorte inteira de uma vez, `NO_FOOD` e `SIN_BENCHMARK` — que têm índice neutro
por **ausência de evidência** — saíam com **0,60×** da fatia em 65+ contra menos de 35. O
modelo passaria a afirmar que quem tem mais de 65 anos compra 40% menos drogaria por linha de
cesta. Ninguém mediu isso: era resíduo da normalização, e era **maior que a maioria dos
efeitos que são medidos**.

O IPF passou a rodar **dentro de cada bloco**, com a fatia de cada um constante em toda
coorte. Isso devolve a `food_line_share` o estatuto que o seed lhe dá — premissa declarada,
uniforme — e faz índice neutro significar de verdade "sem efeito", em vez de "efeito que
sobrou da conta".

### O que mudou, medido na mesma janela

| | ANTES (v1) | DEPOIS (v2) |
|---|---:|---:|
| pedidos | 6.400 | **5.248** (−18,0%) |
| unidades | 204.824 | 169.445 (−17,3%) |
| receita (EUR) | 583.154,43 | 481.201,94 (−17,5%) |
| **EUR por kg** | 4,04 | **4,06** (+0,5%) |

As três primeiras caem pelo mesmo ~18%: são os menores de idade deixando de comprar. A quarta
fica parada, e é ela que prova que o **mix** não se moveu — uma queda de volume sem mudança
de composição.

Os quatro armazéns deixaram de ser cópias: **bcn1 coloca 1.436 pedidos contra 1.176 de
mad1**, 22,1% a mais, contra os 22,7% que o consumo per cápita das duas comunidades prevê
(Cataluña 620,82 kg-L por pessoa/ano · Madrid 505,86). O índice é renormalizado sobre as
quatro comunidades servidas, então o **total** da janela não se move — o que muda é a
repartição.

E a condicional, que é o produto da fase:

| grupo | LT35 % | GE65 % | × |
|---|---:|---:|---:|
| CARNE_CONEJO | 0,01 | 0,08 | 6,08 |
| VINO | 0,50 | 2,44 | 4,89 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0,25 | 0,80 | 3,16 |
| … | | | |
| PASTAS | 2,34 | 0,94 | 0,40 |
| ARROZ | 1,40 | 0,44 | 0,31 |

### O que ficou verificável

- `make demand-reality-check` ganhou a seção **Propensão por coorte**, com a tabela acima e a
  contagem por armazém. A página declara a ausência quando a janela é anterior à camada —
  `cohorts: None`, e não um dicionário vazio, que seria indistinguível de "medi e não havia
  nada".
- O ANTES padrão passou a ser o **estado imediatamente anterior**
  (`before_mapa_2025_v2`). Manter `before_mapa_2025_v1` como padrão faria a queda de receita
  da correção de preço da Fase 4 ser lida como se fosse desta fase.
- `assert_buyer_age_band_matches_the_customer_birth_year` recalcula a faixa contra
  `birth_year` — `not_null` e `accepted_values` passariam com um carimbo trocado, porque um
  carimbo trocado continua sendo uma das quatro faixas válidas.
- `assert_no_order_comes_from_a_minor` lê o limiar do seed **e** guarda um piso de 18. A
  primeira metade sozinha passa se alguém baixar a premissa para zero — foi medido ao
  escrever o teste, e a segunda metade existe por causa disso.
- `assert_buyer_age_band_is_stable_across_the_window` pega a janela regerada pela metade, e
  aceita aniversário: exige que a transição seja para a faixa **seguinte** e para frente no
  tempo.

## Fora de escopo

**Gold — ENTROU na Fase 2**, e o que estava registrado aqui como "a nomear quando entrar"
foi cumprido: o SCD2 de `DIM_PRODUCT` é chaveado em `source_product_id` e portanto modela o
ciclo de vida **da chave da fonte**, não do item comercial. Os casos de `name_seen_before`
(7 na base atual) produzem "um produto morreu, outro nasceu" — a dimensão carrega
`identity_ambiguous` para que isso seja consultável em vez de herdado sem saber. O nome
`fct_price_daily` foi **descartado**: virou `FACT_PRICE_SNAPSHOT`, porque "daily"
prometeria uma continuidade que a fonte não tem.

**Fato de venda — ENTROU na Fase 3, e continua sintético.** A fonte não expõe venda, pedido
ou estoque em nenhum endpoint conhecido; isso não mudou e não vai mudar. O que mudou é que o
fato transacional agora existe, **e está dito em vez de implícito**: o pedido é sintético, e
o cliente, o produto e o preço que ele carrega são observados. A tabela de premissas
(`order_premises_seed.csv`) é a lista fechada do que foi inventado, rotulada `synthetic` linha
a linha, e o export recusa qualquer outro rótulo.

O Gold **os recebeu** no fim da Fase 3: `FACT_ORDER` (accumulating snapshot),
`FACT_ORDER_ITEM`, `FACT_ORDER_EVENT` e `FACT_ORDER_PREMISE`, mais três marts. É o que
finalmente faz o SCD2 pagar por si — até então as duas dimensões versionadas não tinham
nenhum fato apontando para uma versão.

**Moeda.** A fonte não a declara. O Silver não inventa. A var `currency` do projeto dbt
existe como **premissa do consumidor** e é o Gold que a materializa: `FACT_PRICE_SNAPSHOT`
carrega uma coluna `currency` com o valor da var, para que a premissa viaje junto do número
em vez de morar só num arquivo de configuração. Trocar de moeda é editar a var, não caçar
`EUR` espalhado por modelo.
