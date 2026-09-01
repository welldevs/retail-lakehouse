# Source Contract — `simulated_orders`

Contrato entre esta Source e qualquer componente que consuma seus snapshots.
Tudo aqui é imposto por código e coberto por teste, salvo onde marcado como **[fonte]** —
característica do dado a montante, fora do controle desta Source.

**Diferença estrutural das outras quatro Sources: esta entrega um LOG, não uma fotografia.**
As outras quatro produzem o estado de alguma coisa num instante. Esta produz a sequência de
eventos que fez o estado mudar, e o estado não existe em lugar nenhum da partição — ele é o
**fold** dos eventos, e quem o materializa é o consumidor.

**É a segunda source DERIVADA**, como `simulated_oltp`: consome o Silver que as outras
produziram e devolve uma RAW nova. A regra de ouro da Fase 1 vale igual, deslocada um nível:

> O **pedido** é inventado; quem compra, o que se compra, quanto custa e onde mora não.

**Não há rede em nenhum passo.** `extract` aqui não faz nenhuma requisição: lê cinco
arquivos JSON planos (`customers`, `catalog`, `calendar`, `premises`, `demand_profile`) que a
plataforma pré-computou do Silver e dos seeds versionados com `retail-platform
export-orders-reference`, e gera os eventos a partir deles. O verbo é mantido por uniformidade
de interface — mesmo precedente do `extract --in <dir>` do `ine_callejero` e do `extract
--reference` do `simulated_oltp`.

## 1. Identidade do snapshot

| Chave | Valor | Onde vive |
|---|---|---|
| `source` | `simulated_orders` | `_manifest.json` → `source.name` |
| `ingestion_date` | `YYYY-MM-DD` (UTC) — **a data do PEDIDO** | diretório da partição e `partition.ingestion_date` |
| `wh` | armazém (`mad1`, `bcn1`, `svq1`, `vlc1`) | diretório da partição e `partition.warehouse` |

Uma partição **por armazém e por dia**. Os dois eixos não são escolha estética:
`platform/src/retail_platform/manifest.py` deriva `axis_name = "wh" if warehouse else None`,
e esses são os dois únicos formatos que o leitor de manifesto entende.

```
<root>/ingestion_date=YYYY-MM-DD/wh=<armazém>/
├── order_events.jsonl
├── _manifest.json
└── _SUCCESS          (só existe quando complete = true)
```

**`ingestion_date` é a data do PEDIDO, não a data do evento.** Todo evento de um pedido mora
na partição do dia em que o pedido foi *colocado*, mesmo quando `occurred_at` atravessa a
meia-noite — um pedido das 22h40 entregue às 9h do dia seguinte deixa **todos** os seus
eventos aqui.

A alternativa — particionar por data do evento — deixaria a partição de um dia impossível de
fechar: ela só estaria completa dois dias depois, quando o último pedido daquele dia
terminasse, e `_SUCCESS` perderia o significado. Ver obrigação 4.2.

Blocos de primeiro nível do `_manifest.json`: `manifest_version`, `run_id`,
`started_at_utc`, `finished_at_utc`, `duration_seconds`, `complete`, `source`,
`partition`, `config`, `reference`, `totals`, `schema_fingerprint`, `files[]`,
`failures[]`, `anomalies[]`, `history[]`.

## 2. Conteúdo garantido

`order_events.jsonl` é **NDJSON**: uma linha por evento, cada linha um objeto JSON completo e
independente. **Não é um array JSON**, e esse é o único desvio de forma canônica em relação às
outras quatro Sources — justificado na garantia 2.

### 2.1 Envelope — idêntico em todo evento

| Campo | Papel | Origem |
|---|---|---|
| `event_id` | identidade do evento | `sha256("<order_id>|<sequence_no>")[:32]` |
| `event_type` | um dos 12 do vocabulário | máquina de estados |
| `event_version` | versão do `payload` | `1` |
| `occurred_at` | instante do evento, ISO-8601 UTC | `ingestion_date` + offsets declarados |
| `order_id` | agregado **e** chave de partição do broker | `ord_<wh>_<YYYYMMDD>_<6 dígitos>` |
| `sequence_no` | posição no agregado, `1..N` sem buracos | ordem de emissão |
| `wh` | linhagem: qual armazém | argumento `--wh` |
| `producer` | quem emitiu | `simulated_orders` |
| `payload` | específico do tipo | ver 2.2 |

**`recorded_at` não existe aqui, de propósito.** Atraso e reordenação são propriedades do
*transporte*, e quem os mede é quem consome — do outro lado do broker, com o próprio relógio.
Carimbá-los aqui seria inventar um atraso que ninguém observou, e destruiria a
reprodutibilidade byte a byte do log.

### 2.2 Vocabulário de eventos e máquina de estados

| # | `event_type` | estado depois | exige estado | payload |
|---|---|---|---|---|
| 1 | `order_placed` | `PLACED` | — | `customer_id`, `customer_ingestion_date`, `buyer_age_band`, `province_code`, `municipality_code`, `postal_code`, `price_as_of`, `price_source`, `delivery_slot_start`, `delivery_slot_end`, `line_count`, `gross_amount`, `lines[]` |
| 2 | `order_payment_authorized` | `CONFIRMED` | `PLACED` | `payment_method`, `authorized_amount` |
| 3 | `order_payment_failed` | `PAYMENT_FAILED` ⏹ | `PLACED` | `decline_reason` |
| 4 | `order_cancelled` | `CANCELLED` ⏹ | `PLACED`, `CONFIRMED` | `cancelled_by`, `reason` |
| 5 | `order_picking_started` | `PICKING` | `CONFIRMED` | *(vazio)* |
| 6 | `order_line_substituted` | `PICKING` | `PICKING` | `line_no`, `source_product_id`, `substitute_source_product_id`, `substitute_unit_price`, `quantity` |
| 7 | `order_line_removed` | `PICKING` | `PICKING` | `line_no`, `source_product_id`, `quantity`, `reason` |
| 8 | `order_picked` | `PICKED` | `PICKING` | `picked_line_count`, `picked_amount` |
| 9 | `order_dispatched` | `IN_TRANSIT` | `PICKED` | *(vazio)* |
| 10 | `order_delivered` | `DELIVERED` | `IN_TRANSIT` | `delivered_within_slot`, `delivered_line_count`, `delivered_amount` |
| 11 | `order_delivery_failed` | `DELIVERY_FAILED` ⏹ | `IN_TRANSIT` | `reason` |
| 12 | `order_returned` | `RETURNED` ⏹ | `DELIVERED` | `returned_line_count`, `returned_line_no`, `returned_amount` |

⏹ = estado terminal: nenhum evento pode sair dele. **`DELIVERED` não é terminal** — uma
devolução ainda pode vir depois, e é por isso que ele não encerra o agregado.

Cada linha de `lines[]` traz `line_no`, `source_product_id`, `category_id`, `subgroup_id`,
`demand_group`, `quantity` e `unit_price`.

`demand_group` é o rótulo contra o qual a cesta foi calibrada (§2.6). Ele viaja **no evento**,
e não é redescoberto por join a jusante: o que importa é o grupo que valia no momento do
pedido, não o que o de-para diria hoje.

`unit_price` é o preço da **porção comprável**, que difere do `unit_price` cru da API nas 10
combinações produto×armazém vendidas a granel sem `unit_size` — ali a fonte devolve
`reference_price × 99`, o teto do seletor de peso (obrigação 5 do contrato da Mercadona).

### 2.3 O fold é não-trivial, e isso é a razão de a Source existir

Os eventos 6 e 7 alteram a cesta **depois** da colocação. Logo:

> `picked_amount` **não é derivável** de `gross_amount`. O valor do pedido só existe depois
> de dobrar o log.

Se fosse derivável, o log seria um carimbo de data e o modelo de eventos seria enfeite. Um
teste dbt invertido vigia exatamente isso do lado do consumidor.

### 2.4 O que é real e o que é inventado

| Dimensão | Natureza |
|---|---|
| Cliente, e o município/CEP dele | **observado** (via `silver_customer`, cujo endereço vem do Callejero) |
| Produto pedido | **observado** — existe no catálogo daquele armazém naquele dia |
| Preço pago | **observado** — é o `unit_price` daquele `(armazém, data, produto)` |
| Armazém e data | **observado** |
| Grupo de demanda de cada linha | **calibrado contra benchmark** — ver 2.6 |
| Idade e comunidade autónoma do comprador | **observado** (INE 31304 e Callejero, via `silver_customer`) |
| Propensão de cada coorte por grupo | **calibrado contra benchmark** — ver 2.7 |
| Cesta, cadência, horários, taxas de substituição/cancelamento/devolução | **sintético declarado** — ver 2.5 |

Três naturezas, e a distinção entre a segunda e a terceira é nova nesta fase:
**observado** vem de uma fonte que esta plataforma ingere; **benchmark** vem de uma fonte
oficial externa que ela *não* ingere e usa apenas como referência de distribuição;
**sintético** não tem âncora nenhuma. Um benchmark não é um dado nosso — chamá-lo assim seria
transformá-lo numa falsa representação da realidade.

### 2.5 Premissas — todas sintéticas, todas declaradas

Nenhuma fonte ingerida por esta plataforma mede venda, cesta, cadência de compra ou
disponibilidade de produto. Toda premissa vive em `platform/dbt/seeds/order_premises_seed.csv`,
é rotulada `synthetic` (nunca `observed`, nunca `proxy` — um rótulo diferente **reprova** o
export), e seu `sha256` viaja até `config.premises_sha256` no manifesto.

`daily_order_rate` é a única premissa **sem nenhuma âncora observacional**: é por isso que ela
mora num seed que qualquer um edita e reconstrói, e não numa constante escondida no gerador.
Não existe default para nenhuma premissa — uma chave ausente reprova a geração.

### 2.6 Modelo de demanda — calibrado contra o MAPA 2025

`demand_model_version` viaja em `config.demand_model_version` no manifesto. Versão em vigor:
**`mapa_2025_v2`**. A `v2` acrescenta a camada de coorte descrita em 2.7; tudo abaixo
descreve o agregado e continua valendo palavra por palavra.

**A cadeia, com preço fora do caminho da demanda:**

```
grupo de demanda   P(g)  <- alvo de VOLUME (kg/L) do MAPA, inclinado pelo canal e-commerce
       |
produto no grupo         <- UNIFORME (nenhuma fonte mede giro por SKU)
       |
quantidade               <- w(k)=1/2^(k-1), inalterado
       |
preço unitário           <- OBSERVADO (porção comprável da Mercadona)
       |
valor do pedido          <- consequência, nunca objetivo
```

Preço não aparece em nenhuma seta que aponta para demanda. Uma categoria cara pode ter volume
baixo e receita alta — no MAPA, mariscos são **0,81% do volume e 2,88% do valor** —, e isso é
resultado do modelo, não defeito.

**O que o benchmark calibra:** a probabilidade de um grupo de demanda aparecer numa linha.
**O que ele não calibra, e não pode:** `daily_order_rate`, `basket_lines_min/mode/max` e
`quantity_max`. O MAPA mede consumo doméstico do residente — não mede pedido de loja online,
nem cesta, nem cadência de compra. Essas continuam `synthetic` em `order_premises_seed.csv`.

**A ponte entre alvo em kg e sorteio de linhas** é o tamanho médio *observado* da embalagem,
jamais o preço: `P(g) ∝ alvo_volume_g / kg_médio_por_linha_g`. Um grupo cuja embalagem típica
pesa 1 kg precisa de menos linhas que um cujo produto típico pesa 100 g para entregar o mesmo
volume. Grupos com menos de 50% do sortimento convertível a kg/L (`reference_format` em `ud`,
`dz`) **não** recebem alvo de volume — medido: `HUEVOS` converte 18% e cai no fallback
declarado de share de linhas.

**Sazonalidade: neutra nos 12 meses, por ausência de evidência numérica.** Os gráficos mensais
do informe são imagens; só há cinco números mensais em prosa, todos do total da alimentação e
nunca por categoria. Além disso a janela cobre apenas agosto. O mecanismo existe, aplica-se à
**taxa de pedidos** (nunca ao mix — um fator global sobre o mix se normalizaria e não faria
nada) e tem teste que prova que um perfil não neutro muda a saída.

Os oito seeds versionados — `mapa_2025_benchmark_seed.csv`,
`demand_category_mapping_seed.csv`, `demand_profile_seed.csv`,
`demand_seasonality_seed.csv`, `demand_cohort_age_seed.csv`,
`demand_cohort_region_seed.csv`, `mapa_2025_region_seed.csv` e `ine_ccaa_map_seed.csv` —
têm `sha256` em `reference.demand_seeds_sha256`.

### 2.7 Propensão por coorte do comprador

O que a `v2` acrescenta é um andar acima do agregado: **P(grupo) vira P(grupo | coorte)**.

A coorte tem duas dimensões, e são as duas **únicas** em que um atributo observado do cliente
coincide com um corte publicado do informe:

| Dimensão | De onde vem no cliente | Corte do MAPA |
|---|---|---|
| `age_band` ∈ {`LT35`,`35_49`,`50_64`,`GE65`} | `birth_year`, da distribuição etária provincial do INE 31304 | idade do responsável de compra |
| `region` (comunidade autónoma) | `province_code`, do Callejero | comunidade autónoma |

**O que ficou de fora, e por quê.** Ciclo de vida do lar (9 tipos) e nível socioeconómico
(5 níveis) são os cortes mais ricos do informe, e vêm com volume e per cápita completos. O
bloqueio não é o dado — é o atributo: o cliente não tem composição familiar nem renda, e
atribuí-las seria **inventar o atributo**, a mesma proibição que a Fase 1 aplicou à densidade
por tramo. Sexo do comprador o cliente tem, mas o informe só o publica para consumo
*extradoméstico*. **Gatilho registrado:** se uma fase futura ingerir lares por província do
INE, o corte de ciclo de vida abre.

**O índice de afinidade** sai do mesmo gráfico para numerador e denominador, e por isso é
adimensional:

```
idx(g, faixa)         = volume_share(g, faixa) / populacao_share(faixa)
peso_bruto(g, coorte) = w_g  x  idx_idade(g, faixa)  x  idx_regiao(g, ccaa)
```

**Duas premissas novas, nomeadas e declaradas em `demand_profile_seed.csv`:**

1. `cohort_independence = multiplicative` — o informe publica as duas **marginais** e nunca o
   cruzamento idade × comunidade. Multiplicar assume independência condicional. É premissa,
   não medição.
2. `region_frequency_basis = per_capita_volume` — a intensidade regional manifesta-se como
   **frequência de pedido**, nunca como tamanho de cesta. O informe dá kg/ano e **não publica
   frequência de compra doméstica** (`actos de compra` só aparece no capítulo extradoméstico),
   então repartir a intensidade entre as duas seria inventar a repartição.

**Por que o índice não pode ser lido como share.** O `% Población` do MAPA é a população que
*vive em lares* cujo responsável de compra está naquela faixa — um lar com comprador de 40
anos carrega os filhos para dentro de `35_49`. O nosso cliente é um indivíduo. Por isso o
número entra como índice **relativo** entre faixas, e nunca como share absoluto.

**O agregado não se move, e esse é o critério de aceitação.** Um *iterative proportional
fitting* ajusta um fator por grupo até que a média dos pesos por coorte — ponderada pela
distribuição real de coortes **entre os pedidos** — reproduza os pesos agregados da `v1`. Sem
isso, o mix agregado sairia do alvo só porque a nossa pirâmide etária não é a do MAPA, e a
calibração da fase anterior seria desfeita de lado sem nada falhar.

**O IPF roda DENTRO de cada bloco**, e a restrição foi encontrada medindo. Normalizando a
coorte inteira de uma vez, `NO_FOOD` e `SIN_BENCHMARK` — que têm índice neutro por *ausência
de evidência* — saíam com 0,60× da fatia em 65+ contra menos de 35. O modelo passaria a
afirmar que quem tem mais de 65 anos compra 40% menos drogaria por linha de cesta: ninguém
mediu isso, era resíduo da normalização, e era **maior que a maioria dos efeitos medidos**.

**Idade mínima do comprador.** `min_buyer_age = 18`, em `order_premises_seed.csv`. Medido
antes desta fase: 18,01% dos clientes tinham menos de 18 anos — 3.602 de 20.000, com idades a
partir de zero. Isso **não é defeito da Source de OLTP**, cujo contrato declara que a idade
vem da distribuição *populacional* do INE e entrega exatamente isso; o que nunca fora
declarado era a diferença entre **residente** e **quem coloca um pedido**, e ela só passou a
importar quando a idade começou a governar a demanda. A base de clientes não foi tocada.

**A faixa é resolvida no dia do pedido**, não no export, e viaja carimbada em
`order_placed.buyer_age_band` — mesmo precedente de `demand_group`: é ela que escolheu o vetor
de pesos daquela cesta. Um cliente que faz aniversário dentro da janela tem dois pedidos em
faixas diferentes, e está certo.

## 3. Garantias

1. **Escrita atômica.** Nenhum arquivo é observável em estado parcial (temporário no mesmo
   diretório + `os.replace`, com `fsync`).
2. **Forma canônica, adaptada ao log.** Cada linha de `order_events.jsonl` é
   `json.dumps(ensure_ascii=False, sort_keys=True)` **sem indentação**, e o arquivo termina em
   newline; `_manifest.json` mantém a forma indentada das outras quatro Sources. O desvio é
   deliberado: um log é lido linha a linha e cresce por append, cada linha é um evento
   completo — o mesmo byte no disco e no tópico — e `read_json(format='newline_delimited')` do
   DuckDB o consome direto. A garantia que importa é preservada: mesmo conteúdo ⇒ mesmos bytes
   ⇒ mesmo SHA-256.
3. **Ordenação total e determinística.** As linhas são ordenadas por
   `(occurred_at, order_id, sequence_no)`. `occurred_at` sozinho empata; o desempate garante
   que os eventos de um mesmo pedido nunca saiam fora de ordem.
4. **Integridade verificável.** O manifesto declara `sha256`, `bytes` e `records`; `validate`
   recalcula os três a partir do disco.
5. **Reprodutibilidade por seed, com sub-seed por dia.** Cada `(wh, order_date)` deriva a
   própria `random.Random` de `sha256("<seed>|<wh>|<order_date>")`. `sha256` e não `hash()`,
   porque o hash de `str` em CPython é aleatorizado por processo. Verificado em subprocesso,
   com três valores de `PYTHONHASHSEED`.
6. **Acrescentar um dia é aditivo.** Como nenhum dia depende do sorteio de outro, gerar
   `D+1` deixa a partição de `D` **byte a byte idêntica**. É o análogo da propriedade de
   prefixo da Fase 1, num eixo diferente.

   **As quatro condições NÃO-aditivas**, que trocam os pedidos por trás dos mesmos ids:
   outra `seed`; outra referência (clientes ou catálogo reingeridos); outro
   `order_premises_seed.csv`; e — desde a Fase 4 — outro **`demand_model_version`**. As
   quatro são registradas em `config`, e regenerar sob qualquer uma delas exige
   `--overwrite`.

   `min_buyer_age`, introduzida na Fase 5, entra pela terceira: ela é uma premissa, e mudá-la
   muda **quem** está no conjunto elegível — logo, quem é sorteado. Não é uma quinta
   condição, é um caso da que já existia, e vale a pena dizer porque a intuição sugere o
   contrário: uma regra de *elegibilidade* parece filtro, e é sorteio.

   Uma consequência medida da quarta: a projeção Iceberg funde estado de forma **monotônica**,
   descartando linha com `last_sequence_no` menor. Essa fusão assume que um `order_id` sempre
   se refere ao mesmo pedido. Depois de trocar o modelo de demanda ele não se refere — e um
   rebuild sem reset descartou 4.028 linhas como "mais velhas" na Fase 4, deixando a projeção
   com dois universos misturados. Use `make orders-rebuild-projection PROJECTION_RESET=1`.
   Na Fase 5 o mesmo passo foi necessário de novo, e o reset devolveu 5.248 pedidos — 18%
   abaixo dos 6.400 anteriores, porque os menores de idade deixaram de comprar.
7. **Sem relógio.** Todo `occurred_at` deriva de `ingestion_date` mais offsets declarados,
   nunca de `datetime.now()`.
8. **`event_id` determinístico.** Deriva de `(order_id, sequence_no)`, nunca de `uuid4()` —
   sem isso o consumidor a jusante não conseguiria deduplicar entre replays.
9. **`sequence_no` contíguo.** `1..N` por pedido, sem buracos, verificado na releitura.
10. **Máquina de estados imposta na geração.** O gerador avança o estado pela mesma função que
    o `validate` usa para reconferir: uma transição impossível reprova na hora de emitir, não
    três camadas adiante.
11. **Inventário fechado.** Todo arquivo dentro da partição está declarado no manifesto.
12. **Imutabilidade.** Uma partição com `complete: true` não pode ser reescrita sem
    `--overwrite` explícito — inclusive quando o manifesto foi removido e só o `_SUCCESS`
    restou.
13. **Marcador de conclusão.** `_SUCCESS` existe se e somente se `complete: true`, e é sempre
    gravado **depois** do manifesto.
14. **Proveniência preservada.** `reference{}` registra as `ingestion_date` de cliente e de
    catálogo, o `price_as_of` e o `price_source` daquele dia, e o digest das premissas.
    `history[]` acumula cada execução anterior **com a seed e o digest de premissas que ela
    usou** — as duas entradas que trocam os pedidos por trás dos mesmos `order_id`.
15. **Coerência verificada contra a referência.** `validate` relê a referência e confere,
    pedido a pedido: o cliente pertence ao armazém da partição; todo produto existe no
    catálogo daquele `(armazém, data)`; o preço pago é o observado, comparado como `Decimal` e
    nunca como `float`; `gross_amount` fecha com as linhas colocadas; `picked_amount` fecha com
    as linhas cumpridas depois de substituições e remoções.
16. **`schema_fingerprint` estável por construção.** Diferente das outras quatro Sources, a
    impressão digital cobre o **vocabulário declarado inteiro** — envelope, campos de linha e
    payload dos 12 tipos — e não a união das chaves observadas. Um dia sem nenhuma devolução
    não teria `order_returned` no conjunto observado, e duas partições corretas divergiriam por
    sorteio. Ela muda quando o **código** muda, que é o que ela existe para detectar.
17. **Totais reconferidos.** Todos os campos de `totals` são recalculados da releitura, e
    `config.orders` tem de bater com `totals.order_rows`.
18. **Referência desconfiada.** Referência ausente, ilegível, com `rows` vazio, sem campo
    obrigatório, com preço não positivo ou com `price_source` fora do vocabulário reprova a
    geração — nunca produz pedidos enviesados em silêncio.
19. **Nenhum pedido nasce antes do cliente.** Um pedido cujo cliente só aparece numa geração
    posterior reprova a geração.

## 4. Obrigações do consumidor

1. **Itere `manifest.files[]`.** `files[].path` é relativo à raiz do snapshot, dois níveis
   acima da partição (`ingestion_date=…/wh=…/`).
2. **`ingestion_date` é a data do PEDIDO.** Para "eventos ocorridos no dia D", filtre por
   `occurred_at`, **não** por `ingestion_date`. O Silver expõe as duas colunas
   (`ingestion_date` e `event_date`) exatamente porque as duas perguntas são legítimas e
   diferentes.
3. **O estado é o fold, e materializá-lo é seu trabalho.** Não existe `orders.json` na
   partição, e isso não é omissão: duas representações da mesma verdade divergem. Dobre o log.
4. **Ordene por `sequence_no` dentro do pedido, nunca por `occurred_at`.** Dois eventos do
   mesmo pedido podem compartilhar o mesmo instante; só `sequence_no` é total.
5. **Deduplique por `event_id`.** Ele é determinístico de propósito: um replay entrega os
   mesmos eventos, e o consumidor tem de conseguir reconhecê-los.
6. **Trate a partição como imutável.** Não escreva dentro dela.
7. **`order_id` é único em toda a janela, não só na partição.** `wh` e o dia estão embutidos no
   próprio id, então `ord_mad1_20260824_000042` e `ord_mad1_20260825_000042` nunca colidem.
   A **estabilidade do referente**, porém, vale só dentro de `(wh, dia, seed, premissas,
   referência, modelo de demanda)`: um `--overwrite` com outra seed mantém os mesmos ids e troca os pedidos por
   trás deles. O manifesto registra as duas em `history`.
8. **Leia dinheiro como decimal, nunca como float.** Todo valor monetário viaja como string,
   pelo mesmo motivo da obrigação 4.4 do contrato da Mercadona.
9. **`price_source` qualifica o preço, e não é decorativo.** `carried_forward` significa que
   aquele dia não teve snapshot de catálogo para aquele armazém e o último preço conhecido foi
   carregado adiante. Um mart que compare receita entre dias sem olhar essa coluna compara
   preços de vintages diferentes.

## 5. O que a Source não faz

- **Não fala com o Lakehouse.** Sem `duckdb`, sem `boto3`, sem dbt, sem importar nada da
  plataforma. A referência chega como JSON plano.
- **Não busca rede.** Nenhum socket, em nenhum passo.
- **Não fala com broker, banco nem catálogo.** Sem `confluent-kafka`, sem `psycopg`, sem
  `pyiceberg`. O OLTP, o outbox, o tópico e a projeção viva existem **um nível adiante**, do
  lado da plataforma. Aqui só se escreve um log canônico em disco — e é justamente por o log
  ser byte-reprodutível que o replay pelo broker pode ser conferido contra ele.
- **Não materializa o estado do pedido.** Ver obrigação 4.3.
- **Não fornece identidade persistente de pedido entre execuções.** `order_id` é chave da
  janela; o referente é estável apenas sob a mesma `(seed, premissas, referência, modelo de
  demanda)`.
- **Não modela estoque, reposição, rota nem tempo de entrega.** Ver seção 6.
- **Não corrige o Silver.** Onde o dado a montante tem limite (seção 6), esta Source se defende
  de forma explícita e medida, mas não o conserta.

## 6. Escopo e limites **[fonte]**

Tudo medido contra o Lakehouse real em 2026-08-28 (catálogo `2026-08-24`…`2026-08-27`,
clientes `2026-08-24` e `2026-08-27`).

- **Não existe fato transacional em nenhuma fonte externa deste repo.** O contrato da
  Mercadona registra: *"Nenhum endpoint conhecido expõe venda, pedido ou estoque."* Todo pedido
  aqui é sintético, e isso está dito, não implícito.
- **Não existe fato de estoque.** Por isso `order_line_removed` carrega
  `reason = "unavailable"` e **não** `"out_of_stock"`: nomear como se houvesse estoque
  prometeria um dado que ninguém mediu. A taxa é declarada, não consequência de um saldo.
- **A escolha do produto é UNIFORME de propósito, e isso não é "cesta realista".** Nenhuma
  fonte mede venda, giro ou composição de cesta. Ponderar produto inventaria uma distribuição
  que ninguém mediu — a mesma proibição que a Fase 1 aplicou ao tramo. Consequência declarada:
  **o mix por categoria espelha o tamanho do sortimento**, e isso é consequência de uma
  premissa, não afirmação sobre o mercado.
- **A janela de entrega é uma promessa comercial numa grade fixa, nunca uma rota.** O Callejero
  não tem coordenada nem adjacência; calcular rota ou tempo de rota seria inventar geografia.
  `delivered_within_slot` compara dois instantes sintéticos — não é medida de pontualidade
  real. Gatilho para o real: uma sexta source com coordenada (CartoCiudad do IGN, ou OSM).
- **A janela de dias é limitada pelo catálogo, e o limite é medido.** Os 4 armazéns têm
  catálogo de `2026-08-24` a `2026-08-27`; `mad1` tem também `08-15` e `08-16`, com buraco de
  `08-17` a `08-23`. Um dia sem nenhum snapshot **anterior ou igual** para aquele armazém
  **reprova o export da referência** em vez de gerar preço inventado.
- **A taxa efetiva de substituição fica ligeiramente abaixo de `substitution_rate`.** Quando
  nenhum produto livre do mesmo subgrupo — nem, em segunda tentativa, da mesma categoria —
  está disponível, a linha segue cumprida e nenhum evento é emitido. Inventar um substituto
  fora do sortimento daquele armazém seria pior: seria inventar sortimento.
- **Um cliente faz no máximo um pedido por dia.** Simplificação declarada; nenhuma fonte deste
  repo mede cadência de compra.
- **Sexo, idade e endereço do cliente não influenciam a cesta.** Cruzar demografia com consumo
  exigiria um dado de consumo que não existe. A correlação é descartada por construção, como a
  Fase 1 descartou a correlação real entre sexo e idade.

## 7. Códigos de saída

| Código | `extract` | `validate` |
|---|---|---|
| 0 | partição completa | validação passou |
| 1 | — | validação reprovou |
| 2 | falha fatal: nada foi gerado, partição inutilizável | — |
| 3 | exceção não tratada (nenhum traceback escapa) | idem |

`validate` **exige `--reference`**, como a Source de clientes e pelo mesmo motivo: as garantias
centrais — o produto existe naquele catálogo, o preço é o observado, o cliente é daquele
armazém — só são verificáveis relendo a mesma referência que gerou a partição. Um validador que
só confere checksum provaria integridade, não coerência.

`--date` é **obrigatório** no `extract`, diferente das outras quatro Sources. Nelas a partição
é "o snapshot de hoje" e o default de hoje faz sentido; aqui `ingestion_date` é a data do
pedido, e cair no dia corrente por omissão geraria pedidos num dia que talvez nem tenha
catálogo, ou fora da janela exportada.

## 8. Versão do contrato

`manifest_version: 1`. Cada Source tem seu próprio contador — são contratos independentes. A
plataforma declara a versão que sabe ler em
`retail_platform.SUPPORTED_MANIFEST_VERSIONS["simulated_orders"]` e recusa qualquer outra, em
vez de interpretar por adivinhação.
