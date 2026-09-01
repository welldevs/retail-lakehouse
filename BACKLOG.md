# Backlog — o que não entrou, e o que faria entrar

Este arquivo existe por uma regra simples: **ideia nova vai para cá, não para o código.**
Depois da Fase 7 o projeto está congelado, e "seria interessante ter X" deixa de ser motivo
para abrir uma fase.

Cada item traz o **gatilho** — a condição concreta que o destravaria. Ausência sem gatilho é
desculpa; com gatilho é decisão. É a mesma disciplina da tabela de tecnologias não adotadas
do [ARCHITECTURE.md](ARCHITECTURE.md) e da lista *Fora de alcance* do painel.

**Nada aqui é dívida escondida.** O que é dívida está declarado como dívida no ARCHITECTURE,
com data. O que está aqui é escopo que nunca foi prometido.

---

## Precisa de fonte nova

Estes não dependem de esforço, e sim de dado que não existe neste repositório. Aproximá-los
produziria um número plausível e inventado — a única classe de erro que nenhum teste pega.

| Item | Por que não | Gatilho |
|---|---|---|
| **Rota, distância, tempo de deslocamento, otimização de entrega** | O Callejero do INE não tem coordenada nem adjacência. `minutes_to_delivered_*` é premissa declarada, e o próprio seed diz que **não** é tempo de rota. | Uma fonte com geometria: CartoCiudad/IGN, ou OSM. |
| **Margem, lucro, CMV** | A API da Mercadona expõe preço de **venda**, nunca custo de aquisição. Nenhuma outra fonte tem custo. | Uma fonte de custo por produto. |
| **Sazonalidade, YoY, comparativo mensal** | O catálogo observado cobre dias, não meses. Uma tendência sobre 9 dias mede ruído. | Uma janela de meses de catálogo — o que exige rodar a extração por meses, não uma decisão de modelagem. |
| **Estoque observado** | Nenhuma fonte deste repositório mede saldo. O ledger da Fase 7 é **calculado** a partir do consumo observado mais uma política declarada, e o rótulo `synthetic` viaja com cada linha. | Uma fonte de saldo ou movimento de estoque. |
| **Cadência por dia da semana** | `daily_order_rate` é fixo por construção, então a contagem diária de pedidos por armazém é constante. Não há efeito de segunda-feira nem de fim de semana, e inventá-lo seria escolher uma curva que ninguém mediu. | Uma fonte que meça cadência de compra semanal no varejo alimentar espanhol. |

---

## Não precisa de fonte — precisa de trabalho

Estes são acionáveis com o que já existe. Estão fora porque o projeto fechou, não porque
sejam impossíveis.

### RFM, LTV, recompra, coorte de cliente

**Por que não.** Nenhum mart junta cliente com pedido. `MART_CUSTOMER_BASE` tem cliente sem
pedido; `MART_ORDER_FUNNEL` e `MART_BASKET_DAILY` têm pedido agregado sem cliente. O elo
existe em `FACT_ORDER.customer_sk`, no GOLD — que `RETAIL_READER` não alcança, por desenho.

**Gatilho.** Um mart novo com grão de cliente e medidas de pedido (candidato:
`MART_CUSTOMER_ORDERS`). **É a lacuna mais acionável desta lista** e não exige fonte nova.

**Trade-off declarado.** Ela expõe uma armadilha: um mart de cliente × pedido convida a ler
LTV de uma base **sintética** como comportamento de mercado. O rótulo teria de viajar em cada
coluna, como em `MART_MARKET_COVERAGE`.

### O rebuild da projeção é O(n²)

**A medição, de 2026-09-01.** `orders-rebuild-projection` reconstruiu 206.523 pedidos em
**414 commits e ~55 minutos**. Cada commit é um `upsert` do pyiceberg contra a tabela
inteira, então o custo por lote cresce com o que já foi escrito. Na Fase 3, com 6.400
pedidos, isso levava segundos e era invisível.

**Este é o único lugar do projeto onde volume realmente doeu** — e vale registrar a ironia:
a justificativa do Spark diz que o gatilho de volume não disparou, e disparou aqui, no
caminho em Python.

**O gatilho, que é também a correção.** Quando `--reset` é usado, a tabela começa **vazia** e
não há escritor concorrente: não há o que fazer *upsert* contra. Um caminho de `append` em
lote único nesse caso troca 414 commits por um punhado, e o custo volta a ser linear. Não foi
feito porque a regeração é única e acontece antes do freeze — mas **numa máquina nova é um
imposto de 55 minutos**, e isso é razão suficiente para o item existir aqui em vez de sumir.

### Gerador de pedidos de segunda passada

**Por que.** A ruptura que o ledger calcula é **independente** das linhas removidas do
pedido. O gerador remove linha a uma taxa fixa sorteada, com motivo `unavailable` — nome
escolhido justamente porque não havia saldo quando ele foi escrito. Uma não causa a outra, e
cruzá-las como se causassem produziria uma correlação inventada.

**Gatilho.** Um gerador que releia o saldo do dia anterior antes de decidir a remoção.

**Trade-off, e é o que segura o item.** Isso **inverte a dependência do projeto**: hoje
pedido gera estoque; passaria a haver um ciclo entre os dois domínios. Não é barato e não é
inocente — por isso está declarado aqui em vez de aproximado.

### Estruturas de dimensão que nunca foram necessárias

`BRIDGE_PRODUCT_CATEGORY` (um produto aparece em mais de uma categoria; hoje o Gold usa
`primary_category_id`), `DIM_CENSUS_SECTION` e `DIM_ADDRESS`. **Gatilho:** uma pergunta de
negócio que exija o grão que elas oferecem. Construí-las antes disso seria modelar contra
hipótese.

---

## Operação e infraestrutura

| Item | Por que não | Gatilho |
|---|---|---|
| **CI** | Não existe remoto. `make test` roda offline em 15 s e é a mesma coisa que um CI rodaria. | O repositório ganhar um remoto. |
| **Debezium / Kafka Connect** | O outbox já entrega o evento na mesma transação do estado, e `wal_level=logical` já está ligado no `oltp-postgres` justamente para não exigir restart quando isso for plugado. | Precisar capturar mudanças de tabelas **fora** do desenho de outbox. |
| **Observabilidade (OpenTelemetry, Grafana, Prometheus)** | Desejável, e explicitamente **não pode atrasar o fechamento funcional**. Um dashboard existir para gerar captura de tela é o oposto do propósito. | Antes de instrumentar: provar que existem sinais úteis — latência, erro, throughput, falha, estado dos pipelines. Só então `application → OTel → Collector → backend`. |
| **Cluster Spark de verdade** | O job roda `local[*]`: driver e executor no mesmo JVM. Não há shuffle entre nós, e um cluster de mentira não provaria nem escala nem interoperabilidade — provaria que o compose sobe containers. | Um volume que não caiba num nó. `make spark-evidence` publica a medição que diz que ele ainda não chegou. |

---

## Como um item sai daqui

Pelo mesmo caminho que qualquer mudança depois do freeze: um **change request**, com o
template que fecha o [DECISIONS.md](DECISIONS.md) — necessidade, evidência, impacto nos
contratos, testes que podem quebrar, e a decisão registrada.

O que **não** vale como justificativa, e a lista é literal:

> "é usado no mercado" · "fica mais profissional" · "é uma best practice" · "empresas usam" ·
> "pode ser útil no futuro" · "fica bom no currículo"
