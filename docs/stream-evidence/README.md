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

| capturado em | 2026-08-29 13:32:28 UTC |
|---|---|

## Plano transacional — OLTP e outbox

O evento nasce **dentro da mesma transação** que muda `orders` e `order_line`. Não é
o log sendo republicado: é a mudança de estado e o evento gravados atomicamente, que
é a única forma de os dois não divergirem. `outbox.event_id` é único, e o insert do
outbox vem **primeiro** — `rowcount = 0` significa evento já aplicado, e a transação
inteira é desfeita.

|  |  |
|---|---|
| pedidos em `orders` | 6,400 |
| linhas em `order_line` | 120,693 |
| eventos no `outbox` | 44,456 |
| ainda não publicados | 0 |
| pedidos distintos no outbox | 6,400 |
| primeira publicação | 2026-08-28 18:38:48.728269+00:00 |
| última publicação | 2026-08-28 18:45:17.170144+00:00 |

### Eventos no outbox, por tipo

| event_type | eventos |
|---|---|
| `order_cancelled` | 196 |
| `order_delivered` | 6,046 |
| `order_delivery_failed` | 56 |
| `order_dispatched` | 6,102 |
| `order_line_removed` | 2,321 |
| `order_line_substituted` | 4,670 |
| `order_payment_authorized` | 6,298 |
| `order_payment_failed` | 102 |
| `order_picked` | 6,102 |
| `order_picking_started` | 6,102 |
| `order_placed` | 6,400 |
| `order_returned` | 61 |

### Estado replicado, por fold do OLTP

| status do pedido | pedidos |
|---|---|
| `CANCELLED` | 196 |
| `DELIVERED` | 5,985 |
| `DELIVERY_FAILED` | 56 |
| `PAYMENT_FAILED` | 102 |
| `RETURNED` | 61 |

| status da linha | linhas |
|---|---|
| `fulfilled` | 108,194 |
| `not_picked` | 5,508 |
| `removed` | 2,321 |
| `substituted` | 4,670 |

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
| 0 | 0 | 11306 | 11,306 |
| 1 | 0 | 11164 | 11,164 |
| 2 | 0 | 11210 | 11,210 |
| 3 | 0 | 11276 | 11,276 |

Total no tópico: **44,956 mensagens**.

A soma pode exceder a contagem de eventos do log, e isso é **correto**: a
entrega do outbox para o broker é at-least-once por desenho, então uma queda
entre o ack e a marcação de `published_at` republica o lote. O que a torna
inofensiva é a dedup por `sequence_no` do outro lado.

### Lag por grupo de consumo

Dois grupos, e a diferença entre eles é o par lambda: cada sink consome o mesmo
tópico no seu próprio ritmo, com offset próprio. É o ponto de desacoplamento — o
sink Iceberg (~4min48s por passada, copy-on-write) não segura o sink Postgres
(~14s), e nenhum dos dois perde mensagem por causa do outro.

**`orders-projector`**

| partição | offset commitado | high | lag |
|---|---|---|---|
| 0 | 11306 | 11306 | 0 |
| 1 | 11164 | 11164 | 0 |
| 2 | 11210 | 11210 | 0 |
| 3 | 11276 | 11276 | 0 |

Lag total: **0**.

**`orders-projector-iceberg`**

| partição | offset commitado | high | lag |
|---|---|---|---|
| 0 | 11306 | 11306 | 0 |
| 1 | 11164 | 11164 | 0 |
| 2 | 11210 | 11210 | 0 |
| 3 | 11276 | 11276 | 0 |

Lag total: **0**.

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
| linhas | 6,400 |
| snapshots | 105 |
| snapshot corrente | 304920774543672206 |
| metadado corrente | `s3://retail-lakehouse/iceberg/projection/live_order_state/metadata/00052-d31e1c3c-3de4-4394-8f8b-2006196c01e6.metadata.json` |

O caminho do metadado vem do **catálogo**, nunca de uma varredura do storage. O
DuckDB recusa adivinhar qual metadado é o corrente — *"globbing the filesystem…
could result in reading uncommitted data"* — e o atalho existe
(`unsafe_enable_version_guessing`), foi medido e foi **recusado**: ler metadado
não commitado é exatamente o que uma leitura concorrente não pode fazer.

### Proveniência: quem escreveu cada linha

| written_by | linhas |
|---|---|
| `rebuild` | 4,800 |
| `stream` | 1,600 |

### Estado na projeção viva

| status | pedidos |
|---|---|
| `CANCELLED` | 196 |
| `DELIVERED` | 5,985 |
| `DELIVERY_FAILED` | 56 |
| `PAYMENT_FAILED` | 102 |
| `RETURNED` | 61 |

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
| `iceberg` | 6,400 |
| `oltp` | 6,400 |
| `silver` | 6,400 |

Comparados: **6,400 pedidos**.

Resultado: **os três concordam em todos os pedidos comparados** — zero divergências, zero ausências.

---

Regenere com `make stream-evidence` depois de qualquer execução que valha registrar.
As provas que sustentam cada afirmação acima rodam em separado:
`make orders-prove-atomicity`, `make orders-prove-stream`, `make orders-prove-projection`.
