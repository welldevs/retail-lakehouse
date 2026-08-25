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

**`TRAM` fica de fora, por decisão, não por lacuna.** Os downloads trazem 5 arquivos por
província; só 4 são incorporados. `TRAM` (trechos de rua com faixa de numeração, o mais
pesado — 14-28 MB por província — e o único que provavelmente dá granularidade de
número de porta) nunca foi inspecionado. Entra numa rodada futura só se a simulação de
Orders demonstrar necessidade dessa granularidade — decisão registrada, não uma omissão
silenciosa.

**`warehouse_province_map` não é gerado por esta source.** O seed
(`platform/dbt/seeds/warehouse_province_map_seed.csv`) foi derivado do Callejero
manualmente durante o desenvolvimento (`scripts/derive_warehouse_province_map.py`,
cruzando o nome do município em `UP` contra a existência de seções em `SECC`), e existe
independente da source em si. A source do Callejero não sabe que warehouses existem —
produz `province_code`/`municipality_code` como o INE os publica, sem nenhuma referência
a `mad1`/`bcn1`/`svq1`/`vlc1`. O vínculo é uma decisão desta plataforma, não uma
propriedade do INE, e só entra via `JOIN` no Silver/Gold.

## Dívida técnica

Resolvida em 2026-08-24, exceto o CI.

| Item | Situação |
|---|---|
| Cobertura de teste | **Fechada.** 19 → 44 testes, com duplo de S3 em memória |
| Caminho de extração em container | **Fechado.** `bcn1` extraído, validado, aterrissado e transformado dentro do container |
| Ambientes redundantes | **Removidos.** 265 MB (`venv/` quebrado e `orchestration/.venv`) |
| Credenciais de desenvolvimento | **Endurecidas.** Portas em loopback, chaves aleatórias, compose recusa subir sem elas |
| Divergência de `data/` | **Contida.** O padrão `data/` do `.gitignore` casa em qualquer nível |
| **CI** | **Em aberto**, deliberadamente adiado para quando o repositório subir |

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

Confirmado não-vazio por mutação: desligar a comparação de sha256 em `verify.py` faz
`test_catches_a_tampered_object` falhar.

**O que ainda não é coberto:** o caminho `dbt` (os 36 testes de dados exigem object storage
de pé) e o DAG (nenhum teste importa o módulo do Airflow). Os dois foram verificados
manualmente, em execução real.

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
