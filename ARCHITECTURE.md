# Arquitetura — o estado corrente

O que a plataforma **é hoje**: as camadas, o motor, as tecnologias adotadas com a data e o
gatilho de cada uma que continua ausente, as restrições medidas que moldaram o desenho, e a
dívida técnica declarada.

**Este documento guarda estado. A história mora em [DECISIONS.md](DECISIONS.md)** — o que se
decidiu, contra que evidência, e o que se perdeu. A separação é verificada por teste, e
existe porque os dois cresciam juntos: o mesmo texto aparecia nos dois lugares e envelhecia
em ritmos diferentes. Três revisões de documentação foram gastas nisso.

**Onde um número pode morar.** Estrutural — grão, invariante, razão por construção — pode
ficar aqui. Propriedade **desta captura** (contagens, percentuais medidos) só em página
gerada, ou datada explicitamente. O RAW não é reproduzível; numa máquina nova os números
mudam, e [`docs/FREEZE.md`](docs/FREEZE.md) é o que dá nome à captura que produziu os que
estão publicados.

**Relógio de parede é o caso extremo dessa regra**, e ele já produziu divergência entre dois
documentos deste repositório: o mesmo comando medido duas vezes dá dois números. Por isso a
prosa aqui carrega a **magnitude** — "segundos", "~1,5 s" — e o decimal mora só na página
gerada que o mediu. Ver [DECISIONS.md § "O que a medição publica contra o Spark"](DECISIONS.md).

Última revisão: **2026-09-02** — fechamento documental da Fase 7. O estado técnico é o de
2026-09-01; o que mudou depois foi só documentação, evidência e a estrutura da dívida.

## Escala real

**Medida em 2026-08-24**, sobre as três partições do catálogo que existiam então. É a escala
que justificou o motor, e continua sendo a pergunta certa — o volume nunca cresceu o
bastante para mudar a resposta:

| Métrica | Valor |
|---|---|
| Volume por partição | ~8,3 MB, 152 arquivos |
| Linhas por partição | ~4.600 (4.329 produtos únicos) |
| Crescimento | ~3 GB/ano se rodar todo dia |
| Duração de uma extração | 227 s (152 requisições a 1,5 s) |
| Silver completo (3 partições, 4 modelos, 36 testes) | ~6 s |

**Onde está hoje, medido em 2026-09-01**, na captura selada por
[`docs/FREEZE.md`](docs/FREEZE.md) (`capture_id cec10cb5…`):

| Métrica | Valor |
|---|---|
| Clientes (`silver_customer`) | 286.826, em 4 AUFs |
| Pedidos, janela de 9 dias | 206.523 · 3.892.062 linhas · 1.433.723 eventos |
| Ledger de estoque | 173.970 linhas · 17.397 séries · 10 dias |
| RAW selado | 81 partições · 2.221.069 registros |
| `make silver` | 25 modelos, 16 seeds, 391 nós — **53 s** |
| `make warehouse` | 24 modelos, 199 nós — **131 s** |
| Suítes Python, offline | 1.095 |

**Nenhuma tecnologia distribuída é justificada por este volume, e isso é resultado e não
premissa.** Os pedidos cresceram 32× desde que a Fase 3 os mediu (6.400 → 206.523), e o
`dbt build` do Silver inteiro continua em menos de um minuto. O self-join de cesta — o
candidato natural a "grande demais para um nó" — dobrou para 37,9 M pares e continua em
**~1,5 s** (o decimal corrente está em [`docs/spark-evidence/`](docs/spark-evidence/README.md)).

**Onde o volume DOEU, e é um só lugar.** A reconstrução da projeção Iceberg custou **414
commits e ~55 min** para os 206.523 pedidos: cada lote faz `upsert` contra a tabela inteira,
então o custo cresce com o que já foi escrito. Na Fase 3, com 6.400 pedidos, isso levava
segundos e era invisível. A ironia vale escrita: a justificativa do Spark diz que o gatilho de
volume não disparou, e ele disparou aqui — no caminho em Python, e não na análise. Correção
declarada no [BACKLOG.md](BACKLOG.md).

O que segue não é recusa — é a condição em que cada tecnologia passa a valer.

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
                  s3://retail-lakehouse/silver/…                7.098.881 linhas · 205 MB
─────────────────── fronteira física: COPY INTO, nunca ref() ───────────────────
L3  Stage         espelho 1:1 de um RECORTE do Silver         · Snowflake
                  RETAIL.STAGE.STG_*                            3.327.809 linhas (46,9%)
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
| **Spark** | Um motor fora do Python escrevendo o catálogo, e uma forma de cálculo que o SQL não expressa | — | — | **Adotado em 2026-09-01** (Fase 7). **O gatilho declarado NÃO disparou, e isso está medido**: o self-join de cesta — 37,9 M pares, o candidato natural a "partição que o DuckDB não segura" — roda em **~1,5 s e ~2,4 GB** num nó. A janela final dobrou esse número em relação à intermediária (18,3 M) e o tempo continuou em segundos, o que torna a afirmação mais forte e não mais fraca. Entrou por outras duas razões. **Primeira, a interop:** o Iceberg foi justificado por *interop entre engines* desde a Fase 3, e essa metade estava afirmada e nunca demonstrada — os dois escritores eram Python. **Segunda, a forma:** o ledger de estoque é uma soma corrida cujas *entradas são geradas por decisões tomadas a partir do próprio estado* (saldo baixo → ordem → chegada em N dias → muda o saldo seguinte); window function lê a partition mas não escreve de volta nela, e isso foi medido — a soma corrida em SQL diverge em 14 de 30 dias do caso de teste e chega a −70 de saldo. Precedido por `make spike-spark-iceberg`, com **os dois desfechos declarados antes**: se o Spark não lesse o catálogo do pyiceberg, ele não entraria **e** a cláusula de interop sairia desta tabela. `make spark-evidence` publica o mesmo job nos dois motores, **inclusive quando o Python puro ganha**. **Não** ficou provado, e está escrito: escala. Ele roda `local[*]`, sem shuffle entre nós. |
| **Snowflake** | SQL governado, RBAC, conectividade BI | — | — | **Adotado em 2026-08-27** (Fase 2). Recebe um recorte por escopo, não o Silver inteiro — a razão medida saiu de 3,85% para 46,9% entre a Fase 2 e a Fase 6 sem nenhuma regra mudar, porque ela é função de quais sources cabem no escopo. O atrito antigo — "não alcança um MinIO local" — foi resolvido sem S3 real nem storage integration: **stage interno** (`PUT file://`) inverte o sentido, e quem empurra os bytes é o processo local, que enxerga os dois lados. |
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

## Painel de conferência (Streamlit sobre o MART)

Bancada de **conferência** dos indicadores antes de reconstruí-los no Power BI, e não uma
entrega de BI. São 22 indicadores em 7 grupos. Lê **somente** `RETAIL.MART`, com `RETAIL_READER` e
`use secondary roles none` — a sessão é recusada em GOLD e STAGE, e o painel roda uma
sonda ao vivo que demonstra a recusa em vez de afirmá-la.

A fonte única é [`streamlit/indicators.py`](streamlit/indicators.py): o SQL e a
explicação moram juntos, e [`CONTRACT.md`](streamlit/CONTRACT.md) é **gerado** dele. Se a
explicação vivesse num markdown escrito à mão, os dois divergiriam no primeiro ajuste de
SQL — e a conferência continuaria passando, porque ninguém lê um SQL e um texto lado a
lado procurando desacordo.

Cada indicador carrega **armadilhas**: os casos em que a medida óbvia produz um número
plausível e errado. É a única classe de erro que nenhum teste pega, e é o que torna o
painel útil para quem vai reconstruir o modelo em outra ferramenta.

A lista *Fora de alcance* é parte da entrega: cada ausência traz o **gatilho** que a
destravaria. Ausência sem gatilho é desculpa; com gatilho é decisão.

`make dashboard-check` roda o app de verdade via `AppTest` e exige zero exceção contra a
conta viva: é a única forma de as 25 consultas serem exercidas como o Streamlit as executa,
com os parâmetros ligados, em vez de conferidas como texto.

## Dívida técnica

Revisada em **2026-09-01**, depois da Fase 7, e **reestruturada em 2026-09-02** para que cada
item declare status e próximo passo em vez de só motivo. **Oito itens em aberto**, todos
deliberados. O resto da tabela é histórico: fica porque o que foi fechado e *como* foi fechado
é a parte que se aprende.

**A contagem subiu de cinco para oito, e isso é resultado e não regressão.** Três dos itens
novos foram *descobertos* pela Fase 7 — dois deles medindo o que ela mesma construiu. Uma
lista de dívidas que só encolhe é sinal de que ninguém está procurando.

Cada item traz **problema, impacto, status e próximo passo**, e nada mais. O status é um de
quatro: **mitigado** (o dano está contido, a causa não), **aceito** (não vai ser corrigido, e o
motivo está escrito), **aberto** (falta trabalho identificado) ou **fora do escopo**.

| # | Problema | Impacto | Status | Próximo passo |
|---|---|---|---|---|
| 1 | `models/warehouse/` não tem teste offline | Dois defeitos reais só apareceram na primeira execução contra a conta viva; um espelho DuckDB os teria pegado | **Mitigado** por `make warehouse-evidence` — evidência datada, não um segundo motor | Nenhum. A resposta **não** é um espelho: ver a seção abaixo |
| 2 | A conta Snowflake é um trial | Expira, e com ela toda a metade da direita do pipeline | **Aceito** — é aberta por natureza | Nenhum. O destino é trocável por `.env.snowflake`, e isso está verificado |
| 3 | Não há CI | A suíte offline depende de alguém rodar `make test` | **Aberto**, bloqueado por não haver remoto — escrever um workflow que nunca rodou seria afirmar uma verificação que ninguém viu | O repositório ganhar um remoto; o workflow cobre `make test` + `make silver`, nunca a metade Snowflake |
| 4 | Aviso `CustomKeyInConfigDeprecation` no `dbt build` | Ruído no log | **Aceito** — cosmético e alheio: config do `dbt-duckdb`, sem forma suportada publicada | Acompanhar o `dbt-duckdb` |
| 5 | Nenhum mart junta cliente com pedido | Sem recompra, RFM, LTV nem coorte. O elo existe em `FACT_ORDER.customer_sk`, no GOLD, fora do alcance do papel de BI | **Aberto** — é a lacuna funcional mais acionável, e a única que se fecha escrevendo SQL | Um mart com grão de cliente (`MART_CUSTOMER_ORDERS`), com o rótulo `synthetic` viajando em cada coluna. Ver [BACKLOG.md](BACKLOG.md) |
| 6 | A reconstrução da projeção é **O(n²)** | 414 commits e ~55 min para 206.523 pedidos: cada lote faz `upsert` contra a tabela inteira. Numa máquina nova é um imposto de 55 min | **Aberto.** É o **único lugar do projeto onde volume realmente doeu** — e a ironia vale registrar: a justificativa do Spark diz que o gatilho de volume não disparou, e ele disparou aqui, no caminho em Python | Um caminho de `append` em lote único quando `--reset` é usado: a tabela começa vazia e não há escritor concorrente, então não há contra o que fazer `upsert`. Ver [BACKLOG.md](BACKLOG.md) |
| 7 | A ruptura do ledger é independente das linhas `unavailable` do pedido | Uma não causa a outra, e cruzá-las produziria uma correlação inventada | **Aceito**, e declarado no dado: o gerador remove linha a taxa fixa sorteada, sem olhar saldo | Um gerador de segunda passada que releia o saldo. **Inverteria a dependência do projeto** — hoje pedido gera estoque —, e é isso que segura o item |
| 8 | O parquet do Silver sobrevive à exclusão do modelo pelo portão | Aconteceu de verdade: `MART_STOCK_HEALTH` descreveu 5 dias enquanto os outros marts descreviam 9, **sem um único teste reprovar** — cada domínio fechava sozinho | **Mitigado por domínio, classe em aberto.** `assert_stock_ledger_covers_the_order_window` compara as janelas dos dois domínios no warehouse. A classe é geral: **qualquer** modelo excluído deixa parquet velho para trás | Uma verificação genérica — para cada modelo que o portão exclui, comparar a idade do parquet contra a do build corrente. Não foi feita porque exigiria o portão publicar o que excluiu, e a fase fechou |

| Item | Situação |
|---|---|
| Cobertura de teste | **Fechada.** 19 → 416 testes na plataforma, com duplo de S3 em memória |
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
| **CI** | **Em aberto, e bloqueada por não haver remoto.** Cobriria a metade offline (`make test` + `make silver`), nunca a metade Snowflake |
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
| Todo cliente comprava a mesma cesta esperada | **Fechada** na Fase 5: o mix passou a ser condicional à coorte (idade × comunidade), calibrado por IPF para o agregado não se mover |
| **Recém-nascido com cadastro de titular** | **Fechada** na Fase 6, e ela corrigiu o domínio que a Fase 5 errou. A Fase 5 barrou o menor no *pedido* (`min_buyer_age`) e deixou o cadastro intacto; `min_customer_age` mora agora em `customer_premises_seed.csv`, e 3.602 menores viraram 0 |
| **Base de clientes sem densidade** | **Fechada** na Fase 6. Eram 5.000 por armazém para AUFs que diferem por 4,6× em população — nada reprovava, porque densidade não aparece em nenhum total. Hoje é `população municipal × share adulto da província × 2,2%`, e o total é consequência, não cota |
| Lista de prints do Snowflake, 1 de 6 capturados | **Fechada em 2026-09-01.** Cinco dos seis itens já eram cobertos pela evidência gerada; o sexto virou a seção **Papéis em execução** de `make warehouse-evidence`, lida do `query_history`. `PRINTS.md` foi removido: era uma lista de tarefas morando no repositório |
| Variáveis de ambiente lidas pelo código e declaradas em lugar nenhum | **Fechada em 2026-09-01.** Dezenove delas — de `AWS_ACCESS_KEY_ID` a `RETAIL_DASHBOARD_TTL`. Todas têm default no código, então nada quebrava: elas simplesmente não existiam para quem clonasse o repositório. Estão em `.env.example` como sobrescritas comentadas, e `TodaVariavelDeAmbienteEDeclarada` varre o código atrás de `os.environ`/`getenv` e reprova se aparecer uma nova sem declaração |
| `streamlit/CONTRACT.md` eternamente "modificado" no git | **Fechada em 2026-09-01.** O cabeçalho trazia a data da geração, então o arquivo derivado mudava a cada execução e o teste de sincronia precisava **isentar aquela linha** — uma faixa cega dentro do próprio teste que existe para não haver faixa cega. Passou a trazer o sha256 de `indicators.py`: a comparação virou byte a byte |
| Quatro seeds versionados sem procedência executável | **Fechada em 2026-09-01.** Os `scripts/derive_*.py` existiam, com docstring bom, e **nenhum alvo no Makefile** — a origem de quatro CSVs só se descobria abrindo um arquivo que o README não dizia como executar. Viraram `make seed-province-map`, `seed-service-area`, `seed-municipality-codes` e `seed-ambiguous-series`; os quatro reproduziram o CSV versionado byte a byte |
| Documentação conferida só por leitura | **Fechada em 2026-09-01.** `test_documentacao.py` varre o que dá para verificar por máquina: todo caminho da árvore do README existe, todo link relativo resolve, nada de log/artefato versionado, todo alvo do Makefile aparece no `make help`, todo script tem alvo, toda variável do Makefile é usada. Seis injeções vistas vermelhas |
| **Interop entre engines: afirmada por quatro fases, nunca demonstrada** | **Fechada na Fase 7.** O Iceberg foi justificado por interop desde a Fase 3 e os dois escritores eram Python, usando a mesma biblioteca. `make spike-spark-iceberg` mediu 18 perguntas contra o stack real, com **os dois desfechos declarados antes**: se reprovasse, o Spark não entraria **e** a cláusula sairia desta tabela. Hoje o catálogo tem três escritores, e `written_by` torna isso consultável |
| **Premissas do pedido contradizendo umas às outras** | **Fechada na Fase 7**, depois de três fases "registradas em vez de corrigidas". 84% das entregas chegavam antes de a janela abrir; o limiar de SLA valia 90 contra um teto possível de 80. Faltava a distinção entre *ajustar até a saída agradar* e *tornar duas premissas coerentes* — a primeira se recusa, a segunda é correção de modelo. Guardada por `assert_order_premises_are_internally_coherent`, que afere a **derivação** e nunca o resultado |
| **Estoque, ruptura, giro e cobertura fora de alcance** | **Fechada na Fase 7, com ressalva que viaja no dado.** O gatilho declarado era "uma fonte de saldo ou movimento" e ele **não** foi cumprido: o ledger é *calculado* a partir do consumo observado mais uma política declarada. `stock_label = 'synthetic'` está em toda linha do mart |
| **"Não mexa no RAW depois de fechar" era disciplina, não verificação** | **Fechada na Fase 7.** Três revisões de documentação existiram porque uma regeração mudou números já escritos e nada avisou. `make freeze` sela a captura e `make freeze-check` reprova se ela mudar. O selo cobre o **dado**, não a execução: `run_id` e timestamps ficam de fora, senão um re-land byte-idêntico quebraria o selo |
| **Referência por nome de seção e âncora nunca eram conferidas** | **Fechada em 2026-09-01.** `LinksRelativosTest` confere que o ARQUIVO existe, e uma âncora quebrada aponta para um arquivo que existe — então ela passava, e o leitor caía no topo do documento. Achado ao mover 17 seções para `DECISIONS.md`: uma referência ficou órfã. Dois testes novos cobrem rótulo `§ "…"` e âncora, em todos os níveis de título e nas âncoras HTML explícitas do CONTRACT |
| **README e ARCHITECTURE explicando a mesma coisa duas vezes** | **Fechada em 2026-09-01.** 1.435 + 2.475 linhas, com o mesmo assunto em dois lugares envelhecendo em ritmos diferentes. A narrativa foi para `DECISIONS.md`, o escopo futuro para `BACKLOG.md`, e um teto de linhas testado impede os dois de voltarem a crescer sem que seja uma decisão |
| **O parquet do Silver sobrevive à exclusão do modelo pelo portão** | **Em aberto, mitigada.** Quando `silver_gate` tira `silver_stock_ledger` (ou `silver_live_order_state`) do build, o parquet da última construção bem-sucedida **fica** no object storage — e o export para o Snowflake o lê sem saber que é velho. Aconteceu de verdade: `MART_STOCK_HEALTH` descreveu uma janela de 5 dias enquanto todos os outros marts descreviam 9, sem um único teste reprovar. Mitigada por `assert_stock_ledger_covers_the_order_window`, que compara as janelas dos dois domínios no warehouse — que é onde eles finalmente se encontram. Não fechada porque a mitigação é por domínio, e a classe é geral: qualquer modelo excluído deixa parquet velho para trás |

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

Um espelho em DuckDB dos 24 modelos teria **passado** nos dois erros que quebraram a
primeira execução real: `FILTER (WHERE ...)` e `WINDOW ... AS`, que o DuckDB aceita e o
Snowflake não. Um teste que não reproduz o modo de falha não é teste — é uma segunda
implementação para manter, e daria confiança falsa exatamente onde não há.

A mitigação é outra: [`make warehouse-evidence`](docs/warehouse-evidence/README.md)
registra o resultado da execução **real** — posse objeto a objeto, volume, matriz de
isolamento e amostra de cada mart — com data e identidade da conta. Converte "código sem
teste" em "código executado, com a prova anexada e datada". É regenerável: vincular outra
conta e rodar de novo produz a evidência daquela conta.

O que continua verdadeiro: o recorte (`snowflake_export.py`) e o transporte
(`snowflake_load.py`) são cobertos offline, e é neles que moram os erros silenciosos —
agregado somado junto do detalhe, escopo esquecido, coluna casada por posição. O SQL do
warehouse falha alto quando falha.

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

**Sem contagem por módulo, de propósito.** A tabela que morava aqui trazia um inteiro por
módulo, copiado à mão, e apodreceu: dizia 131 testes na plataforma quando eram 389, listava
`verify.py` e `query.py` como se tivessem arquivo próprio (não têm — são exercitados de
dentro de `test_landing_roundtrip.py` e `test_config.py`), e afirmava 29 e 16 para
`snowflake_load.py` em dois parágrafos da **mesma seção**. Um número mantido à mão em dois
lugares é uma contradição esperando a data; a lista abaixo diz o que cada suíte prova, que é
a parte que não muda a cada teste novo. O total sai de `make test`, medido, não escrito.

| Arquivo | O que a suíte prova |
|---|---|
| `test_manifest.py` | Obrigações do contrato do consumidor, e as recusas |
| `test_land.py` · `test_landing_roundtrip.py` | Upload, idempotência, auto-correção, abortar antes de `_SUCCESS`, e a releitura que reconfere (`verify.py`) |
| `test_config.py` | Precedência de credencial, o `.env` não sobrepor o ambiente, a conversão de endpoint (`query.py`), e toda variável lida estar declarada |
| `test_prune_local.py` | Só apaga a cópia local depois de duas conferências independentes |
| `test_oltp_reference.py` | As queries do export contra fixtures DuckDB reais, a alocação por população e o share adulto medido antes do corte |
| `test_orders_reference.py` · `test_demand_profile.py` · `test_demand_check.py` | O calendário de preço, o IPF, os dois checksums da extração do MAPA e o reality check |
| `test_orders_oltp.py` · `test_orders_stream.py` · `test_orders_projection.py` | A fronteira da transação, a ordem entre escrita e commit de offset, a fusão monotônica |
| `test_snowflake_export.py` | O recorte: agregado `'Total'`, escopo AUF, dedup, DDL derivado do próprio recorte |
| `test_snowflake_load.py` | Stage qualificado, `OVERWRITE`, casamento por nome, reconferência, e os 4 defeitos de papel |
| `test_snowflake_evidence.py` · `test_stream_evidence.py` | Totais somados e não escritos, isolamento quebrado em destaque, e o que falhou não virar vazio |
| `test_silver_gate.py` | O portão do `dbt build` decidindo num lugar só |
| `test_dashboard_indicators.py` | O `CONTRACT.md` byte a byte igual ao que o gerador produz, e todo parâmetro ligado |
| `test_cli.py` | Os defaults da linha de comando, incluindo `--count` **não** ter um |

Fechado com um duplo de cliente S3 em memória (`platform/tests/fake_s3.py`), no espírito do
duplo de HTTP que a Source já usa. O duplo **valida o `ChecksumSHA256` declarado**, como o
servidor real faz: um erro na conversão hex→base64 falharia em teste, não em produção.

Confirmado não-vazio por mutação: desligar a comparação de sha256 em `verify.py` faz
`test_catches_a_tampered_object` falhar.

**O que ainda não é coberto**, e a lista cresceu com a Fase 2:

- **O caminho `dbt` do Silver** — os testes de dados exigem object storage de pé.
- **As DAGs** — nenhum teste importa o módulo do Airflow. As seis compilam via `DagBag`
  no container, o que pega erro de import mas não comportamento.
- **A árvore `models/warehouse/`** — os 24 modelos e seus testes só rodam **contra o
  Snowflake**. Não há equivalente offline, e não é oversight: um espelho em DuckDB seria
  uma segunda materialização da mesma verdade, e foi justamente a diferença entre os dois
  motores (`FILTER`, `WINDOW`) que os quebrou na primeira execução — um espelho DuckDB teria
  passado e escondido exatamente esses erros. **A consequência é real e fica registrada: sem
  conta Snowflake, `make warehouse` não roda e essa metade do projeto não é verificável.**
  O que atenua é que o recorte que a alimenta (`snowflake_export.py`) e o transporte
  (`snowflake_load.py`) são cobertos offline, e é neles que moram os erros silenciosos — o
  SQL do Gold falha alto quando falha.

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

Nenhum `.github/workflows`, e **nenhum remoto configurado** (`git remote -v` é vazio). A
fronteira depende de alguém rodar `make test` — e o alvo `source-test` existe exatamente
para ser um job que não instala nada. Dois jobs (Source sem dependência, plataforma com
venv) tornariam a fronteira verificada a cada push em vez de por disciplina.

Não está escrito porque **um workflow que nunca rodou é o oposto do que este repositório
faz com teste**: seria um arquivo afirmando uma verificação que ninguém viu acontecer, nem
verde nem vermelha. O gatilho é literal — no dia em que houver remoto, os dois jobs entram
e a primeira execução é a prova.

### A conta Snowflake é um trial — **aberta, por natureza**

Trial de 14 dias a partir de 2026-08-27. Quando expirar, `make warehouse` para de rodar e
com ele os 24 modelos e os testes do Gold/Mart. **O lakehouse não é afetado**: `make silver`
e as suítes Python continuam offline, sem credencial e sem custo — foi para isso que a
fronteira L2→L3 é física. Um trial anterior já expirou durante esta fase e o sintoma foi
`390913`, com o login autenticando e nenhum warehouse disponível.

### Aviso de depreciação do dbt — **aberta, cosmética**

`dbt build` emite 16 ocorrências de `CustomKeyInConfigDeprecation` por causa de
`+format: parquet` em `dbt_project.yml`. É config do `dbt-duckdb`, não do dbt-core, e mover
para `config.meta` como o aviso sugere pode quebrar a materialização `external`. Deixado
como está até o `dbt-duckdb` publicar a forma suportada; o aviso é ruído, não sintoma.

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
