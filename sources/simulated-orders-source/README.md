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
quatro JSON planos — `customers.json`, `catalog.json`, `calendar.json`, `premises.json` — e
esta Source os consome só com a stdlib. Nenhum lado importa o código do outro.

`dependencies = []`, verificado por AST em `tests/test_dependencies.py`. Em particular **não**
importa `confluent-kafka`, `psycopg` nem `pyiceberg`: o broker, o OLTP e a projeção viva
existem um nível adiante, do lado da plataforma.

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
