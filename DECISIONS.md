# Decisões, e o que cada uma custou

Este arquivo guarda a **história**: o que se decidiu, por quê, contra que evidência, e o que
se perdeu no caminho. O [ARCHITECTURE.md](ARCHITECTURE.md) guarda o **estado** — o que vale
hoje. A regra que separa os dois é simples e é verificada por teste:

> **Estado no ARCHITECTURE e no README. História só aqui.**

Ela existe porque os dois documentos vinham crescendo juntos, com o mesmo texto aparecendo em
lugares diferentes e envelhecendo em ritmos diferentes. Três revisões de documentação foram
gastas exatamente nisso.

## Como ler

O índice abaixo dá cada decisão em quatro linhas — **decisão, razão, evidência, trade-off** —
e aponta para a narrativa completa, que ficou **intacta**, com a data e os números que
valiam quando foi escrita. Um número datado não apodrece: ele era verdade naquele dia. O que
apodrece é o número apresentado como estado corrente, e por isso ele não mora aqui.

**Nem toda decisão tem trade-off confortável, e as que não têm são as que mais importam.**
Um "não custou nada" nesta lista é sinal de que a análise foi rasa, não de que a decisão foi
boa.

---

## Índice das decisões

### Cinco Sources como pacotes irmãos, com `dependencies = []`

- **Decisão.** Cada fonte é um pacote Python independente, sem dependência de terceiros, e a
  plataforma a consome pelo contrato físico em disco — nunca importando o código dela.
- **Razão.** Uma abstração compartilhada entre fontes que não se parecem (API viva, download
  manual, dado sintético) obrigaria a inventar o denominador comum antes de conhecer as
  diferenças.
- **Evidência.** `make source-test` roda no Python do sistema, fora do venv: se qualquer
  Source ganhar uma dependência, ele quebra. Um teste AST proíbe o import.
- **Trade-off.** Código repetido entre as cinco. Aceito: repetição clara custa menos que
  abstração prematura, e o retorno apareceu na imagem do Airflow — dependência zero significa
  que a Source roda em qualquer lugar.
- → [§ "Fronteira Source ↔ plataforma"](ARCHITECTURE.md)

### Segunda e terceira sources: INE e Callejero

- **Decisão.** População e endereços reais do INE entram como sources próprias, não como
  seeds.
- **Razão.** Uma base de clientes sintética precisa de geografia real para não ser um sorteio
  uniforme sobre um mapa que não existe.
- **Evidência.** A ambiguidade de homônimos municipais só apareceu contra o dado real, e
  produziu `ine_ambiguous_series_seed` — derivado por script, com alvo no Makefile.
- **Trade-off.** O Callejero é **download manual semestral**, e isso quebra a
  reprodutibilidade automática do RAW. É a razão de o `capture_id` existir.
- → [§ "Segunda source"](#segunda-source-população-do-ine) · [§ "Terceira source"](#terceira-source-callejero-do-ine)

### Snowflake recebe um recorte, não o Silver inteiro

- **Decisão.** O warehouse recebe as tabelas que respondem perguntas analíticas, e não uma
  segunda cópia do lakehouse.
- **Razão.** Copiar RAW para o Snowflake porque o Snowflake existe é o oposto de arquitetura.
- **Evidência.** A razão do recorte foi de 3,85% para 46,9% entre a Fase 2 e a Fase 6 **sem
  nenhuma regra mudar** — ela é função de quais sources cabem no escopo, e por isso não é
  propriedade do desenho.
- **Trade-off.** Perguntas fora do recorte exigem voltar ao lakehouse. Aceito, e declarado no
  painel como *Fora de alcance*.
- → [§ "Fase 2"](#fase-2-camada-analítica-no-snowflake)

### Pedidos nascem como LOG DE EVENTOS, não como fotografia

- **Decisão.** A quinta source entrega um log append-only; `silver_order` é um fold.
- **Razão.** Uma tabela com `status` diz onde o pedido está, nunca quanto tempo levou para
  chegar lá. As durações do warehouse são a resposta concreta ao que o log comprou.
- **Evidência.** Três folds independentes — window function, transacional e streaming —
  concordam pedido a pedido. Dois folds discordando acharam dois defeitos que nenhum teste
  pegava.
- **Trade-off.** O fold é caro e a reconstrução é O(n²) no volume atual — 55 min para 206 mil
  pedidos. Declarado no [BACKLOG.md](BACKLOG.md) com a correção.
- → [§ "Fase 3"](#fase-3-orders-como-eventos-quinta-source)

### Outbox transacional em vez de dual write

- **Decisão.** Estado de negócio e evento na **mesma transação**; o publicador lê a tabela.
- **Razão.** Escrever no banco e no broker em duas operações perde evento em toda falha
  entre as duas.
- **Evidência.** `prove_oltp_atomicity.py` injeta falha no banco e prova que os dois lados
  caem juntos.
- **Trade-off.** Duplicação **é possível** entre o ACK do broker e a marcação do outbox, e o
  projeto **não promete exactly-once**. A dedup vive no consumidor.
- → [§ "Fase 3, segunda metade"](#fase-3-segunda-metade-o-oltp-e-o-outbox-transacional)

### Kafka é transporte, nunca a fonte canônica

- **Decisão.** O log canônico nasce em disco; o broker carrega.
- **Razão.** Um broker com retenção de 7 dias não é sistema de registro.
- **Evidência.** Replay reproduz os 16 sha256; buraco de sequência é recusado; duplicata é
  descartada.
- **Trade-off.** Ninguém aqui precisa da latência que o Kafka compra, e isso está escrito.
- → [§ "Fase 3, terceira metade"](#fase-3-terceira-metade-transporte-replay-e-consumo-idempotente)

### Iceberg entra por CONCORRÊNCIA, não por volume

- **Decisão.** `live_order_state` em Iceberg, com dois escritores e um leitor concorrente.
- **Razão.** Neste volume um parquet com `os.replace` atômico serviria; o que o Iceberg
  compra é isolamento de snapshot entre escritores.
- **Evidência.** `make spike-iceberg` mediu catálogo, upsert, conflito e leitura **antes** de
  a projeção existir. O commit sobre snapshot velho é recusado, e o retry depois de recarregar
  passa.
- **Trade-off.** Metade da justificativa — *interop entre engines* — ficou **afirmada e não
  demonstrada** por quatro fases, porque os dois escritores eram Python. Só a Fase 7 pagou
  essa dívida.
- → [§ "Fase 3, quarta metade"](#fase-3-quarta-metade-a-projeção-concorrente-em-iceberg)

### Calibrar a demanda contra o MAPA 2025 — benchmark, nunca ground truth

- **Decisão.** O mix de categorias da cesta é calibrado contra o informe de consumo do MAPA.
- **Razão.** Sem isso, o mix espelhava o **tamanho do sortimento**: um catálogo com 475 SKUs
  de cuidado pessoal produzia uma cesta que ninguém compra.
- **Evidência.** A calibração encontrou um defeito de **preço** que a demanda anterior
  escondia — `unit_price` nem sempre é preço de unidade comprável.
- **Trade-off.** O MAPA mede **consumo domiciliar em volume**, não carrinho de e-commerce em
  valor. A conversão é heurística declarada, e o benchmark calibra volume; valor é
  consequência do preço observado.
- → [§ "Calibração da demanda"](#calibração-da-demanda-contra-o-mapa-2025-fase-4)

### Coorte só com atributo observado

- **Decisão.** A demanda varia por coorte de idade e região, e só por atributos que existem
  no dado.
- **Razão.** Um corte do benchmark só pode virar segmentação se o atributo estiver observado
  nos dois lados.
- **Evidência.** A Fase 5 encontrou 18% da base abaixo de 18 anos — titulares de conta
  recém-nascidos — porque a idade passou a governar a demanda.
- **Trade-off.** Grupos de índice neutro escondem defeito: um segmento sem diferença medida
  parece calibrado e apenas não foi testado.
- → [§ "Perfil de consumo do cliente"](#perfil-de-consumo-do-cliente-fase-5)

### A base de clientes é população servida, não compradores

- **Decisão.** `silver_customer` dimensiona a base pela população real das áreas atendidas.
- **Razão.** Uma base de 20 mil clientes distribuída uniformemente não tem densidade nenhuma
  para medir.
- **Evidência.** A Fase 6 mostrou que a proporção estava errada **sem quebrar nenhuma soma** —
  defeito de repartição, que só um teste que refaz a divisão a partir da fonte pega.
- **Trade-off.** Densidade de simulação parece penetração de mercado, e é o número mais fácil
  de citar fora de contexto de todo o painel. O rótulo viaja em cada coluna.
- → [§ "Densidade real da base de clientes"](#densidade-real-da-base-de-clientes-fase-6)

---

## Fase 7 — o fechamento

### Spark entra por interop e por forma; volume é NÃO-GATILHO declarado

- **Decisão.** Um job Spark calcula o ledger de estoque e escreve no mesmo catálogo Iceberg.
- **Razão.** Duas, e desempenho não é nenhuma. **Interop:** é o terceiro escritor do catálogo
  e o primeiro fora do Python — a propriedade que justificou o Iceberg estava afirmada desde a
  Fase 3 e nunca demonstrada. **Forma:** o saldo é uma soma corrida cujas *entradas são
  geradas por decisões tomadas a partir do próprio estado*, e window function não escreve de
  volta na partition que lê.
- **Evidência.** `make spike-spark-iceberg`, 18 perguntas, rodado **antes** de qualquer linha
  da fase, com os dois desfechos declarados de antemão. O gatilho de volume foi **medido e
  não disparou**: 37,9 M pares de cesta em 1,45 s e 2,31 GB num nó. E S7b roda a mesma entrada
  pela soma corrida em SQL: ela diverge em 14 de 30 dias e chega a −70 de saldo.
- **Trade-off.** Uma JVM, uma imagem e um perfil de compose a mais. Contido por construção: o
  Spark é **opcional** — `make silver` roda verde numa árvore onde ele nunca rodou, e isso é
  teste, não promessa. E `make spark-evidence` publica o tempo do Python puro ao lado, que
  neste volume ganha.

### Premissas contraditórias são defeito de modelo, e podem ser corrigidas

- **Decisão.** `slot_lead_hours_*` e `sla_minutes_picking` passaram a ser **derivados** das
  outras linhas do mesmo seed.
- **Razão.** A regra do projeto — recusar ajuste de premissa até a saída agradar — estava
  certa e faltava uma distinção: mexer numa premissa para melhorar um número é uma coisa;
  tornar duas premissas **mutuamente coerentes** é outra. Um alerta acima do teto aritmético e
  uma janela que abre depois da entrega não são resultados indesejados, são contradições.
- **Evidência.** 84% das entregas chegavam **antes** de a janela abrir (73.124 de 86.803);
  o limiar de SLA valia 90 contra um teto possível de 80.
- **Trade-off.** Fica trivialmente fácil continuar mexendo até o KPI agradar. Contido:
  `assert_order_premises_are_internally_coherent` afere a **derivação**, nunca o resultado —
  medido, trocar o limiar de 60 por 75 reprova, mesmo sendo um valor plausível.

### O RAW é selado, não reproduzido

- **Decisão.** `make freeze` sela a captura; `make freeze-check` confere.
- **Razão.** O RAW **não** é reproduzível — API viva, download manual, URL móvel — e prometer
  que fosse seria falso. Tudo a jusante é determinístico **dada a mesma RAW**.
- **Evidência.** Três revisões de documentação existiram porque uma regeração mudou números
  já escritos e nada avisou. A disciplina virou verificação.
- **Trade-off.** O selo cobre o **dado**, não a execução: `run_id` e timestamps ficam de fora
  de propósito, senão um re-land byte-idêntico quebraria o selo — e alarme que dispara sem
  causa treina quem revisa a ignorá-lo.

---

# A narrativa completa, por fase

As seções abaixo estão **como foram escritas**, com a data e os números que valiam
naquele dia. Um número datado não apodrece — ele era verdade quando foi medido. O que
apodrece é o número apresentado como estado corrente, e por isso ele não mora aqui.

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
de leitura que a Fase 3 desfez.** Ela é função de **quanto de cada source cai dentro do
escopo** — e como as sources crescem em ritmos diferentes, a razão se move sozinha, sem
ninguém tocar no recorte:

| medido em | Silver | atravessa | razão | o que mudou |
|---|---:|---:|---:|---|
| 2026-08-27 (Fase 2) | 3.796.213 | 146.240 | **3,85%** | só catálogo, população e clientes |
| 2026-08-29 (Fase 3) | 4.087.507 | 406.855 | **9,95%** | Orders entrou, e nasce dentro das AUFs |
| 2026-09-01 (Fase 6) | 7.098.881 | 3.327.809 | **46,9%** | clientes ×14 e pedidos ×14, ambos 100% no escopo |

O que continua fixo é o denominador que **não** atravessa: `silver_ine_population_series`
tem 3.094.992 linhas nacionais das quais 1,8% estão no escopo, e o Callejero tem 517 mil de
resolução de endereço que servem ao gerador, não ao analista. Sem Orders o recorte é 19,2%.

**Ler 46,9% como "o recorte afrouxou" é o mesmo erro que ler 5% como propriedade.** Nenhuma
regra mudou desde a Fase 2: escopo geográfico, última ingestão, dedup de grão. O que mudou
foi a proporção entre uma source nacional que quase não entra e duas sintéticas que entram
inteiras — e é exatamente por isso que o número sai de `make warehouse-evidence` e não deste
parágrafo.

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
dbt** no target `snowflake`, 0 erros; **215** no target `dev`, inalterados. O fechamento
cruza os três caminhos: soma de `MART_MARKET_COVERAGE.customers` = `DIM_CUSTOMER` vigente =
base do STAGE = **20.000**.

> **Fase 6 redimensionou a base pela população, e esse número é 286.826 hoje.** Os
> `102`/`215`/`146.240` acima também são desta fase e não da atual — ver "Escala real",
> no topo, para o estado corrente. O que esta seção estabelece não é o valor: é que **as
> três somas continuam iguais entre si**, e isso segue valendo em qualquer tamanho.

`DIM_PRODUCT` tem 4.962 versões para 4.959 produtos (3 com mais de uma
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
depois de o portão abrir. **A árvore `models/warehouse/` de Orders saiu no Marco 7.**
**Stock, Replenishment e Spark saíram na Fase 7**, e vale registrar *como*: o gatilho de
volume do Spark **não** disparou; ele entrou por interop e por forma, com a medição
desfavorável publicada. Restam **Delivery** (sem coordenada no Callejero — gatilho:
CartoCiudad/IGN ou OSM) e as três estruturas de dimensão, com os gatilhos intactos.

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

> **Superado na Fase 7.** Os dois achados abaixo deixaram de estar apenas registrados: as
> premissas foram conciliadas. O texto fica como estava porque a razão de eles terem
> sobrevivido três fases é o que importa — faltava a distinção entre *ajustar uma premissa
> até a saída agradar*, que se recusa, e *tornar duas premissas mutuamente coerentes*, que é
> correção de modelo. Ver a Fase 7 e `assert_order_premises_are_internally_coherent`.

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

> **Superado na Fase 7.** Os dois achados abaixo deixaram de estar apenas registrados: as
> premissas foram conciliadas. O texto fica como estava porque a razão de eles terem
> sobrevivido três fases é o que importa — faltava a distinção entre *ajustar uma premissa
> até a saída agradar*, que se recusa, e *tornar duas premissas mutuamente coerentes*, que é
> correção de modelo. Ver a Fase 7 e `assert_order_premises_are_internally_coherent`.

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

## Painel de conferência: o terceiro papel finalmente vestido

Streamlit sobre o `MART`, 22 indicadores em 7 grupos. O propósito declarado não é *mostrar
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
verdade via `AppTest` e exige zero exceção; é a única forma de as 25 consultas serem
exercitadas. Fica fora de `make test` porque exige conta viva.

## O portão do Silver morava em seis arquivos, e nenhum concordava com o outro

Achado em 2026-08-31 pela execução real: `mercadona_catalog_daily` reprovava **todo dia** na
última tarefa. Os quatro armazéns extraíam, validavam, aterrissavam e verificavam com
sucesso; o `silver` caía com *"no version-hint could be found"*.

**O dado estava certo o tempo inteiro. O portão é que morava no arquivo errado.**

### A decisão, e as seis cópias dela

Nem todos os 22 modelos do Silver podem ser construídos sempre, e os dois motivos são
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

> **Este parágrafo estava errado, e a Fase 6 o desfez.** "A correção mora onde a pergunta
> mora" pressupõe que a pergunta era *quem pode comprar*. Era também *quem pode existir*, e
> essa segunda pergunta ficou sem resposta: o titular de conta recém-nascido continuou no
> cadastro, só impedido de comprar. Um cadastro não é um censo. Ver
> [Densidade real da base de clientes (Fase 6)](#densidade-real-da-base-de-clientes-fase-6).

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

> Isto vale enquanto as bases dos armazéns são iguais. A Fase 6 as dimensionou pela população
> e a ordem se inverteu: com 128.771 clientes contra 95.498, mad1 passou a colocar mais
> pedidos que bcn1 apesar do índice menor. Os dois efeitos continuam existindo; o maior venceu.

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

## Densidade real da base de clientes (Fase 6)

A Fase 5 tratou os menores de idade **no domínio errado**. O achado era certo, a medição era
certa, e a correção respondia metade da pergunta: `min_buyer_age` impede que uma criança
*compre*, e não que ela *exista* como titular de conta. O argumento que sustentava a escolha —
"`silver_customer` é uma projeção fiel da população residente, e o contrato da Source diz
exatamente isso" — também era certo, e é justamente por isso que ele enganou: **a fidelidade
da projeção não é a propriedade em questão.** Uma base de clientes não é um censo.

### O segundo defeito, que ninguém tinha procurado

Ao abrir o cadastro, apareceu o que estava ao lado: **5.000 clientes por armazém**, um número
igual para AUFs que diferem por **4,6×** em população (mad1 tem 7,10 milhões de habitantes na
sua área de serviço; svq1 tem 1,59).

Nada reprovava. Os 216.591 endereços eram reais e verificados cliente a cliente contra o
Callejero; os totais fechavam; `assert_customer_reconciles_with_manifest` batia; o grão era
único. A única coisa errada era que a **densidade não existia** — e densidade, ao contrário de
soma, não aparece em nenhum total. Foi preciso um teste que refizesse a conta a partir do INE
para que ela virasse uma falha visível.

### O modelo

```
população municipal observada (INE 29005, year=2025, ref 2024-12-31)
  × share adulto da província do município (INE 31304, idades >= min_customer_age)
  × taxa de penetração
  = clientes daquele armazém
```

Três decisões, cada uma com um motivo que não é estético:

**A conta é por município, não por armazém.** Hoje cada armazém cai numa província só, então
as duas formas dão o mesmo inteiro. Multiplicar a população inteira do armazém por um único
share adulto *presumiria* isso; a forma por município continua certa se um armazém passar a
cruzar província, e a outra passa a estar errada em silêncio.

**O share adulto é medido ANTES da truncagem.** Medi-lo depois devolve 100% em toda província
— um número plausível, que não reprovaria nada, e cujo efeito seria dimensionar a base inteira
pela população total como se ela fosse adulta. As duas consultas compartilham o mesmo CTE da
pirâmide inteira para que o numerador de uma nunca deixe de ser o mesmo universo do
denominador da outra.

**O denominador é adulto, e não total.** A base é de adultos; distribuí-la por população total
daria peso a quem não pode ter cadastro. Medido: contra a alocação por população total, svq1
perderia 29 clientes e mad1 ganharia 16 — uma diferença de 0,15%, mas o denominador certo
custa o mesmo que o errado.

**O total é consequência, não cota.** Com uma cota de 20.000 repartida, acrescentar um
município à área de serviço *tiraria* clientes dos outros armazéns. Assim, ele acrescenta.

### A taxa, e as duas premissas que ela carrega

2,2 % é a participação do e-commerce no volume total de alimentação em 2025 (informe, seção 3).
É o **único número observado** disponível para dimensionar uma base de clientes neste
repositório, e ele já existia: `demand_profile_seed.channel_reference_pct`, rotulado
`observed` desde a Fase 4.

`customer_premises_seed` **aponta** para ele em vez de copiá-lo, e o export só sabe seguir esse
ponteiro — um ponteiro arbitrário faria a base ser dimensionada por qualquer número de qualquer
seed. O teste dbt confere o ponteiro e reprova se ele mudar de destino.

Duas premissas transformam um share de volume num share de gente, e **nenhuma é medida**:

1. **O comprador online consome como a média.** Sob ela, 2,2 % do volume ↔ 2,2 % das pessoas.
2. **Estes quatro armazéns modelam o canal online inteiro da AUF**, não um operador dentro
   dele. Aplicar participação de mercado de um operador exigiria uma fonte não ingerida aqui.

### Três camadas contra o menor de idade, e por que três

| camada | onde | o que pega |
|---|---|---|
| a distribuição entregue já é adulta | `oltp_reference._age_sql` | o gerador não *pode* sortear 7 anos |
| a referência é desconfiada | `reference_data._require_customer_scope` | referência de schema antigo, e truncagem pela metade — cabeçalho dizendo 18 com uma criança nas linhas |
| o dado pousado é reconferido | `validate._check_minimum_age` | qualquer coisa que tenha escapado às duas primeiras |

A do meio é a que existe por experiência: o cabeçalho é a **promessa**, e uma promessa sem
conferência é o que produziu os 18,01% na primeira vez.

### O que a fase mudou, medido

| | ANTES | DEPOIS |
|---|---:|---:|
| clientes | 20.000 | 286.826 |
| menores de idade | 3.602 (18,01%) | **0** |
| faixa de idade | 0 … 100 | 18 … 100 |
| base elegível a pedir | 16.398 | 286.826 |
| pedidos na janela de 4 dias | 5.248 | 91.788 |
| eventos | 36.596 | 636.848 |
| erro médio contra o alvo do MAPA | 0,075 pt | **0,070 pt** |

O erro contra o benchmark **melhorou** sem que a calibração fosse tocada: o IPF reconvergiu em
6 iterações sobre uma distribuição de coortes diferente — `LT35` deixou de conter crianças — e
o agregado ficou onde estava.

### A prova de fechamento que a taxa criou

Dimensionar a base como 2,2 % das pessoas *porque* 2,2 % do volume é online cria uma obrigação
que nenhuma fase anterior tinha: o modelo deveria então produzir 2,2 % do consumo doméstico
daquelas mesmas AUFs. Isso é conferível contra o próprio informe.

| escopo alimentar, janela de 4 dias | canal esperado | modelo | razão |
|---|---:|---:|---:|
| kg ou litro | 2.134.114 | 1.944.027 | 0,91× |
| receita (EUR) | 7.203.504 | 6.793.690 | 0,94× |

**Nada foi ajustado para isso fechar.** A taxa entrou nesta fase; `daily_order_rate`,
`basket_lines_*` e `quantity_max` entraram na Fase 3, escolhidos sem nenhuma relação com ela e
sem nenhuma fonte que os medisse. As duas metades se encontram nessa tabela pela primeira vez.

`NO_FOOD` fica fora do numerador: o per cápita do informe é de alimentação e bebidas e não
cobre drogaria — somá-lo compararia dois universos e inflaria a razão sem que nada estivesse
errado. `SIN_BENCHMARK` fica, porque são grupos alimentares que o informe não detalha mas que
pertencem ao mesmo universo que o per cápita mede.

A distância que sobra **não deve ser fechada** mexendo em `daily_order_rate`: nenhuma fonte
deste repositório mede cadência de compra nem cesta online, então não existe critério para
decidir qual dos dois lados está errado. Enquanto for assim, a razão é uma **observação**, não
um alvo. **Gatilho:** uma fonte que meça frequência de compra doméstica ou ticket médio por
canal transforma essa linha num teste.

### O efeito colateral que inverteu a fase anterior

| wh | clientes | pedidos | índice regional |
|---|---:|---:|---:|
| mad1 | 128.771 | 37.332 | 0,89 |
| bcn1 | 95.498 | 33.976 | 1,10 |
| vlc1 | 34.295 | 11.656 | 1,05 |
| svq1 | 28.262 | 8.824 | 0,96 |

A Fase 5 tinha bcn1 na frente pelo consumo per cápita da Cataluña. Agora a população de Madrid
domina, e a ordem se inverte. Os dois efeitos continuam existindo e o maior venceu — o que é
medição, não escolha, e é o tipo de coisa que só aparece quando as duas dimensões passam a ser
observadas ao mesmo tempo.

### Uma não-determinação medida, e o aviso que ela virou

Duas execuções de `export-oltp-reference` produzem `adult_share` diferentes **no último bit do
double** — 0,8224668719886548 contra 0,8224668719886545 — porque a agregação paralela do DuckDB
não fixa a ordem da soma de ponto flutuante. Não é defeito do módulo e **não propaga**: os
quatro alvos saem idênticos, e a base gerada a partir de dois exports diferentes tem o mesmo
`sha256`.

Mas só não propaga porque nenhum dos quatro produtos cai perto de um `.5`. A margem mais
apertada é a de mad1: **128.770,524404**, a 0,024 de um empate — cerca de 135 residentes. Se
Madrid crescer ou encolher esse tanto na próxima 29005, o alvo passa a alternar entre 128.770 e
128.771 de export para export, a base muda de tamanho sem que nada tenha sido decidido, e o
teste dbt (que tolera 1 cliente **exatamente por causa disto**) começa a passar por sorte.
`test_a_alocacao_nao_fica_na_beira_do_arredondamento` existe para avisar antes disso.

### O que ficou verificável

- `assert_no_customer_is_a_minor` varre **todas** as `ingestion_date`, e não só a corrente: os
  pedidos fixam `customer_ingestion_date` e o export resolve a versão mais recente de cada
  cliente, então filtrar por `is_latest_ingestion` deixaria a porta dos fundos aberta. Lê o
  limiar do seed **e** guarda um piso de 18 — medido: com o limiar em zero, a primeira metade
  passa.
- `assert_customer_base_follows_the_declared_population_allocation` refaz a alocação a partir
  de `silver_ine_population_by_municipality` × o share adulto de `silver_ine_population_series`
  e compara. É o teste que uma regeração uniforme reprova, e ele também confere o **ponteiro**
  da taxa.
- `--count` virou opcional e `count_source` entrou no manifesto. Sem isso, uma base gerada com
  override manual seria indistinguível de uma derivada da população.
- Quatro condições **não-aditivas** declaradas no CONTRACT, não três: seed, `ingestion_date`,
  `min_customer_age`, e a taxa/regra de alocação. As quatro trocam as pessoas por trás dos
  mesmos `customer_id`.
- Regenerar o cadastro **obriga** a regenerar os pedidos. Não é mudança de lógica de Orders —
  `assert_buyer_age_band_matches_the_customer_birth_year` reprova, e foi ele quem apontou isso
  durante a fase. Nenhum arquivo do domínio de pedidos foi alterado.
- **O plano de stream foi levado de volta à convergência**, e não só o lakehouse: o OLTP
  transacional foi resetado e reaplicado (636.848 eventos, uma transação cada), o outbox foi
  drenado para o Kafka e o sink Postgres consumiu o tópico inteiro — **39.751 duplicatas
  descartadas por `sequence_no`**, que são os eventos do universo anterior sendo corretamente
  rejeitados como mais velhos. `make orders-reconcile` fecha nos três caminhos, 91.788 pedidos.
- **O sink Iceberg foi deliberadamente NÃO drenado**, e o motivo está escrito na própria
  página de evidência. A tabela já estava no estado final: `orders-rebuild-projection` é o
  segundo escritor e escreve direto do RAW, sem passar pelo tópico. Drenar os 628.848 eventos
  restantes levaria ~9 horas de commits copy-on-write para descartar todos como
  iguais-ou-mais-velhos, sem mudar uma linha. O que prova a convergência é `orders-reconcile`,
  não o offset de um consumidor — e a página diz isso, em vez de deixar o lag parecer defeito.

---

## Change request — como mudar alguma coisa depois do freeze

O projeto está congelado. Isso não quer dizer imutável; quer dizer que mudança passa por um
registro, e não por um impulso. Copie o bloco abaixo e responda **todas** as linhas — se
alguma não puder ser respondida, a mudança não está pronta para ser feita.

```
## CR-NNN · <título>

Necessidade      Que problema concreto existe?
Evidência        Que teste, log ou medição demonstra que ele existe?
Insuficiência    Por que a solução atual não basta?
Componente       Quem deve ser responsável pela mudança?
Contratos        Que contratos de dados são afetados?
Regressão        Que testes podem quebrar?
Semântica        Algum campo muda de significado?
Proveniência     Continuaremos sabendo de onde veio o dado?
Reprodutibilidade O mesmo input continua produzindo o mesmo resultado?
Custo            A complexidade acrescentada se justifica?
Prova            Que teste demonstrará que a mudança melhorou o sistema?
Perda            O que deixa de ser verdade?
```

**O que não vale como necessidade**, e a lista é literal: *"é usado no mercado"*, *"fica mais
profissional"*, *"é uma best practice"*, *"empresas usam"*, *"pode ser útil no futuro"*,
*"fica bom no currículo"*.

Ideia sem CR vai para o [BACKLOG.md](BACKLOG.md).
