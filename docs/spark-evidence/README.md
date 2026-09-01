# Evidência do Spark — e a medição que joga contra ele

**Gerado por `make spark-evidence` em 2026-09-01 21:51:40 UTC.** Não editar à mão.

Esta página existe para sustentar uma afirmação **negativa**: o Spark não foi
adotado por desempenho. Dizer isso sem medir seria modéstia retórica — a mesma
doença do número copiado à mão, com o sinal trocado. Então o mesmo job roda nos
dois motores, sobre a mesma entrada, e os dois tempos ficam publicados.

## Por que o Spark está neste projeto

Duas razões, e desempenho não é nenhuma delas.

**1. É o terceiro escritor do catálogo Iceberg, e o primeiro fora do Python.** O
Iceberg foi justificado por *interop entre engines* desde a Fase 3, e essa metade da
justificativa estava **afirmada e nunca demonstrada**: os dois escritores eram
Python usando a mesma biblioteca. `make spike-spark-iceberg` foi o portão que testou
isso antes de qualquer linha desta fase existir — com os dois desfechos declarados
de antemão, incluindo o de apagar a cláusula de interop se ela não se sustentasse.

**2. A forma do job não é SQL.** O saldo de estoque é uma soma corrida cujas
*entradas são geradas por decisões tomadas a partir do próprio estado*: o saldo cai
abaixo do ponto, uma ordem é emitida, ela chega dias depois e muda o saldo seguinte,
que decide se há nova ordem. Window function lê a partition inteira mas não escreve
de volta nela.

## Os escritores do catálogo, hoje

`written_by` é o que torna "três escritores" um **fato consultável** em vez de
uma frase de documentação.

| Tabela | Linhas | Snapshots | Escritores |
|---|---|---|---|
| projection.live_order_state | 206523 | 414 | `rebuild` (206523) |
| operations.stock_consumption | 168010 | 7 | `platform` (168010) |
| operations.stock_ledger | 173970 | 2 | `spark` (173970) |

**Escritores distintos no catálogo: 3** — `platform`, `rebuild`, `spark`.

`platform`, `rebuild` são Python; `spark` é a JVM.

A propriedade que justificou o Iceberg desde a Fase 3 era *interop entre
engines*, e ela só deixa de ser afirmação quando esta lista tem um nome
que não é Python.

## O mesmo job, nos dois motores

A função do laço é **importada** de `jobs/spark/stock_ledger.py` pelos dois
caminhos — não é uma reimplementação aproximada. O que muda é só quem itera
sobre os grupos: um `for` num processo, ou o Spark distribuindo. Se fossem
duas implementações diferentes, a comparação mediria a habilidade de quem
escreveu cada uma.

|  | Python puro | Spark |
|---|---|---|
| linhas | 173970 | 173970 |
| séries | 17397 | 17397 |
| demanda | 6315644 | 6315644 |
| atendido | 6310606 | 6310606 |
| ruptura | 5038 | 5038 |
| ordens emitidas | 17397 | 17397 |
| chegadas | 17357 | 17357 |
| **segundos** | **16.6** | **54.1** (job) · 58.9 com a JVM e o container |

**Os dois resultados são IDÊNTICOS.** Sem isso a comparação de tempo não significaria nada, porque os dois mediriam coisas diferentes.

O tempo do Spark **inclui a subida da JVM e o `docker compose run`**, e
isso não é descontado de propósito: quem roda o job paga esse custo. A
coluna "job" é o que o próprio job cronometra, para que a diferença
entre as duas fique visível em vez de escondida numa nota de rodapé.

**Neste volume, Python puro é ~3.3x mais rápido.** Se o vencedor
for o Python — que é o esperado nesta escala — o número fica publicado do
mesmo jeito. Ele é a prova de que o Spark não está aqui por velocidade, e
uma página que só publicasse resultados favoráveis não provaria nada.

## O gatilho de volume, que NÃO disparou

O candidato natural a "volume que exige Spark" neste projeto é o self-join de
cesta — todo par de produtos comprados juntos, que é a base de qualquer análise
de afinidade. Medido no DuckDB, num nó:

| Pares | Pares distintos | Segundos | Pico de RSS (GB) |
|---|---|---|---|
| 37.899.395 | 8.789.258 | 1.49 | 2.38 |

**O gatilho de volume não disparou, e está medido.** O ARCHITECTURE
declara o gatilho do Spark como *"partição que o DuckDB não segura em
memória"*; este número é o que diz que ele continua fechado. Se um dia
virar minutos e dezenas de gigabytes, a justificativa do Spark muda — e
isso também é informação.

## O que esta página NÃO prova

- **Que o Spark escala aqui.** Ele roda `local[*]`: driver e executor no mesmo JVM.
  Não há shuffle entre nós, não há cluster, e um cluster de mentira não provaria
  nem escala nem interoperabilidade.
- **Que o job precisa de Spark hoje.** Precisa de um motor que expresse
  realimentação por série; o Python puro também expressa. O que o Spark acrescenta
  é ser o escritor fora do Python e paralelizar por série quando as séries crescerem.
- **Que o estoque é real.** O saldo é calculado a partir do consumo observado mais
  uma política declarada em seed. Nenhuma fonte deste repositório mede estoque.

