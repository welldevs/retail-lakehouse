# Evidência do plano de stream

Gerado por `make stream-evidence` contra o OLTP, o broker e a projeção **vivos** no
momento da captura. **Não é documentação escrita à mão** — todo número desta página
saiu de uma consulta a um dos três.

Existe porque a metade em streaming é dívida declarada: `make test` roda sem rede, e
os duplos em memória (`fake_kafka`, `fake_pg`, `fake_iceberg`) cobrem a **forma** do
código — a ordem da transação, o protocolo de dedup, a construção do SQL. O que eles
não podem cobrir é a **semântica** dos motores reais: que o Kafka preserva ordem por
chave, que o Postgres desfaz de verdade, que o Iceberg recusa um commit sobre
snapshot velho. Isto aqui é o registro de que ela foi exercida contra eles.

| capturado em | 2026-09-01 13:52:21 UTC |
|---|---|

## Plano transacional — OLTP e outbox

O evento nasce **dentro da mesma transação** que muda `orders` e `order_line`. Não é
o log sendo republicado: é a mudança de estado e o evento gravados atomicamente, que
é a única forma de os dois não divergirem. `outbox.event_id` é único, e o insert do
outbox vem **primeiro** — `rowcount = 0` significa evento já aplicado, e a transação
inteira é desfeita.

|  |  |
|---|---|
| pedidos em `orders` | 91,788 |
| linhas em `order_line` | 1,726,833 |
| eventos no `outbox` | 636,848 |
| ainda não publicados | 0 |
| pedidos distintos no outbox | 91,788 |
| primeira publicação | 2026-09-01 13:48:03.320663+00:00 |
| última publicação | 2026-09-01 13:49:00.746591+00:00 |

### Eventos no outbox, por tipo

| event_type | eventos |
|---|---|
| `order_cancelled` | 2,709 |
| `order_delivered` | 86,803 |
| `order_delivery_failed` | 878 |
| `order_dispatched` | 87,681 |
| `order_line_removed` | 32,830 |
| `order_line_substituted` | 66,307 |
| `order_payment_authorized` | 90,390 |
| `order_payment_failed` | 1,398 |
| `order_picked` | 87,681 |
| `order_picking_started` | 87,681 |
| `order_placed` | 91,788 |
| `order_returned` | 702 |

### Estado replicado, por fold do OLTP

| status do pedido | pedidos |
|---|---|
| `CANCELLED` | 2,709 |
| `DELIVERED` | 86,101 |
| `DELIVERY_FAILED` | 878 |
| `PAYMENT_FAILED` | 1,398 |
| `RETURNED` | 702 |

| status da linha | linhas |
|---|---|
| `fulfilled` | 1,550,082 |
| `not_picked` | 77,614 |
| `removed` | 32,830 |
| `substituted` | 66,307 |

## Transporte — Kafka

`key = order_id`, e a chave é **carregável**: o Kafka garante ordem dentro da
partição, e é isso que permite a dedup do consumidor ser limitada — comparar
`sequence_no` contra o `last_sequence_no` já gravado, sem conjunto de `event_id` que
cresce nem janela de expiração. Entrega é **at-least-once** do outbox para o broker
(publica → ack → marca, nunca marca → publica); o consumo é **effectively-once**
porque o offset só é commitado depois da escrita.

Tópico `retail.orders.events.v1` em `localhost:9092`.

### Marcas d'água por partição

| partição | low | high | mensagens |
|---|---|---|---|
| 0 | 0 | 170316 | 170,316 |
| 1 | 0 | 170458 | 170,458 |
| 2 | 0 | 170502 | 170,502 |
| 3 | 0 | 170528 | 170,528 |

Total no tópico: **681,804 mensagens**.

A soma pode exceder a contagem de eventos do log, e isso é **correto**: a
entrega do outbox para o broker é at-least-once por desenho, então uma queda
entre o ack e a marcação de `published_at` republica o lote. O que a torna
inofensiva é a dedup por `sequence_no` do outro lado.

### Lag por grupo de consumo

Dois grupos, e a diferença entre eles é o par lambda: cada sink consome o mesmo
tópico no seu próprio ritmo, com offset próprio. É o ponto de desacoplamento — o
sink Iceberg (~4min48s por passada, copy-on-write) não segura o sink Postgres
(~14s), e nenhum dos dois perde mensagem por causa do outro.

**Lag alto não é projeção atrasada quando a tabela foi reconstruída em lote.**
`orders-rebuild-projection` é o SEGUNDO escritor: ele escreve o estado final
direto do RAW, sem passar pelo tópico, e o offset do grupo de consumo não se
move com isso. Depois de uma regeração, drenar o tópico pelo sink Iceberg
reprocessaria centenas de milhares de eventos para descartar todos como
iguais-ou-mais-velhos — o merge é monotônico. O que prova a convergência dos
três caminhos é `make orders-reconcile`, e não o offset de um consumidor.

**`orders-projector`**

| partição | offset commitado | high | lag |
|---|---|---|---|
| 0 | 170316 | 170316 | 0 |
| 1 | 170458 | 170458 | 0 |
| 2 | 170502 | 170502 | 0 |
| 3 | 170528 | 170528 | 0 |

Lag total: **0**.

**`orders-projector-iceberg`**

| partição | offset commitado | high | lag |
|---|---|---|---|
| 0 | 19306 | 170316 | 151010 |
| 1 | 11164 | 170458 | 159294 |
| 2 | 11210 | 170502 | 159292 |
| 3 | 11276 | 170528 | 159252 |

Lag total: **628,848**.

## Projeção — Iceberg

O gatilho escrito para o Iceberg era *"um segundo engine precisar escrever a mesma
tabela"*. Ele disparou por **concorrência, não por volume**: neste volume um parquet
reescrito com `os.replace` atômico funcionaria. O que o Iceberg compra é isolamento
de snapshot entre dois escritores e um leitor concorrente, mais time travel.

`written_by` é a prova de que os dois escritores existem de fato, e é **consultável**
em vez de anedótica.

|  |  |
|---|---|
| tabela | `projection.live_order_state` |
| linhas | 91,788 |
| snapshots | 184 |
| snapshot corrente | 8579108754636027592 |
| metadado corrente | `s3://retail-lakehouse/iceberg/projection/live_order_state/metadata/00184-a1b142f7-6986-43e0-804a-1c9b42c5f16c.metadata.json` |

O caminho do metadado vem do **catálogo**, nunca de uma varredura do storage. O
DuckDB recusa adivinhar qual metadado é o corrente — *"globbing the filesystem…
could result in reading uncommitted data"* — e o atalho existe
(`unsafe_enable_version_guessing`), foi medido e foi **recusado**: ler metadado
não commitado é exatamente o que uma leitura concorrente não pode fazer.

### Proveniência: quem escreveu cada linha

| written_by | linhas |
|---|---|
| `rebuild` | 91,788 |

### Estado na projeção viva

| status | pedidos |
|---|---|
| `CANCELLED` | 2,709 |
| `DELIVERED` | 86,101 |
| `DELIVERY_FAILED` | 878 |
| `PAYMENT_FAILED` | 1,398 |
| `RETURNED` | 702 |

## Os três folds

O mesmo estado de pedido é calculado por três caminhos, e `make orders-reconcile`
compara os três coluna a coluna:

- **`silver_order`** — window functions em SQL sobre o log inteiro;
- **`orders`/`order_line` no OLTP** — máquina de estados transacional, evento a evento;
- **`live_order_state`** — fold em streaming sobre o tópico.

Só o primeiro **não compartilha código** com nenhum dos outros. É contra ele que a
comparação vale como verificação; o acordo entre a projeção e o OLTP vale como
evidência de transporte, não de correção — os dois compartilham o fold.

| fonte | pedidos |
|---|---|
| `iceberg` | 91,788 |
| `oltp` | 91,788 |
| `silver` | 91,788 |

Comparados: **91,788 pedidos**.

Resultado: **os três concordam em todos os pedidos comparados** — zero divergências, zero ausências.

---

Regenere com `make stream-evidence` depois de qualquer execução que valha registrar.
As provas que sustentam cada afirmação acima rodam em separado:
`make orders-prove-atomicity`, `make orders-prove-stream`, `make orders-prove-projection`.
