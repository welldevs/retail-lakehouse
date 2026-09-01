# Evidência do warehouse analítico

Gerado por `make warehouse-evidence` contra a conta Snowflake viva no momento da
captura. **Não é documentação escrita à mão** — todo número desta página saiu de uma
consulta ao destino.

Existe porque a conta é um trial e a metade Snowflake do projeto não é reproduzível
offline como a metade Lakehouse. Quando a conta expirar, isto continua sendo a prova
datada de que os modelos rodaram; `make silver` e as suítes seguem rodando sem ela.

## Identidade da captura

| | |
|---|---|
| conta | `WD97271` |
| usuário | `WELLTONALMEIDA` |
| papel da sessão | `ACCOUNTADMIN` |
| warehouse | `COMPUTE_WH` |
| database | `RETAIL` |
| capturado em | 2026-09-01 15:04:30 -07:00 |

O papel acima é o da **captura**, não o do pipeline. Ler as três camadas de uma vez é
justamente o que nenhum papel do projeto pode fazer — é essa a separação. O pipeline
roda como `RETAIL_LOADER` (carga) e `RETAIL_TRANSFORMER` (dbt); a posse na tabela
abaixo é a prova disso, porque cada objeto pertence a quem o escreveu.

## Objetos, posse e volume

A coluna **posse** é o ponto: o carregador é dono do STAGE, o transformador é dono de
GOLD e MART, e nada é do administrador. Enquanto o pipeline rodava como
`ACCOUNTADMIN` esta tabela mostraria um único dono em toda parte — os três papéis
existiam, estavam verificados, e nenhuma execução passava por eles.

| schema | tabela | posse | linhas |
|---|---|---|---|
| GOLD | `DIM_CATEGORY` | `RETAIL_TRANSFORMER` | 151 |
| GOLD | `DIM_CUSTOMER` | `RETAIL_TRANSFORMER` | 573,652 |
| GOLD | `DIM_DATE` | `RETAIL_TRANSFORMER` | 4,018 |
| GOLD | `DIM_GEOGRAPHY` | `RETAIL_TRANSFORMER` | 699 |
| GOLD | `DIM_PRODUCT` | `RETAIL_TRANSFORMER` | 5,002 |
| GOLD | `DIM_WAREHOUSE` | `RETAIL_TRANSFORMER` | 4 |
| GOLD | `FACT_INGESTION_RUN` | `RETAIL_TRANSFORMER` | 78 |
| GOLD | `FACT_ORDER` | `RETAIL_TRANSFORMER` | 206,523 |
| GOLD | `FACT_ORDER_EVENT` | `RETAIL_TRANSFORMER` | 1,433,723 |
| GOLD | `FACT_ORDER_ITEM` | `RETAIL_TRANSFORMER` | 3,892,062 |
| GOLD | `FACT_ORDER_PREMISE` | `RETAIL_TRANSFORMER` | 32 |
| GOLD | `FACT_POPULATION_MUNICIPALITY` | `RETAIL_TRANSFORMER` | 21,410 |
| GOLD | `FACT_PRICE_CHANGE` | `RETAIL_TRANSFORMER` | 129,483 |
| GOLD | `FACT_PRICE_SNAPSHOT` | `RETAIL_TRANSFORMER` | 146,482 |
| GOLD | `FACT_STOCK_LEDGER` | `RETAIL_TRANSFORMER` | 173,970 |
| MART | `MART_ASSORTMENT_DAILY` | `RETAIL_TRANSFORMER` | 5,060 |
| MART | `MART_BASKET_DAILY` | `RETAIL_TRANSFORMER` | 5,360 |
| MART | `MART_CUSTOMER_BASE` | `RETAIL_TRANSFORMER` | 286,826 |
| MART | `MART_DEMAND_COHORT` | `RETAIL_TRANSFORMER` | 5,902 |
| MART | `MART_FULFILLMENT_SLA` | `RETAIL_TRANSFORMER` | 36 |
| MART | `MART_MARKET_COVERAGE` | `RETAIL_TRANSFORMER` | 370 |
| MART | `MART_ORDER_FUNNEL` | `RETAIL_TRANSFORMER` | 36 |
| MART | `MART_PRICE_EVOLUTION` | `RETAIL_TRANSFORMER` | 146,482 |
| MART | `MART_STOCK_HEALTH` | `RETAIL_TRANSFORMER` | 5,988 |
| STAGE | `STG_CATEGORY` | `RETAIL_LOADER` | 151 |
| STAGE | `STG_CUSTOMER` | `RETAIL_LOADER` | 573,652 |
| STAGE | `STG_GEOGRAPHY` | `RETAIL_LOADER` | 699 |
| STAGE | `STG_INGESTION_RUN` | `RETAIL_LOADER` | 78 |
| STAGE | `STG_ORDER` | `RETAIL_LOADER` | 206,523 |
| STAGE | `STG_ORDER_EVENT` | `RETAIL_LOADER` | 1,433,723 |
| STAGE | `STG_ORDER_LINE` | `RETAIL_LOADER` | 3,892,062 |
| STAGE | `STG_ORDER_PREMISE` | `RETAIL_LOADER` | 32 |
| STAGE | `STG_POPULATION_MUNICIPALITY` | `RETAIL_LOADER` | 21,410 |
| STAGE | `STG_PRICE_CHANGE` | `RETAIL_LOADER` | 129,483 |
| STAGE | `STG_PRODUCT_PRICE` | `RETAIL_LOADER` | 146,482 |
| STAGE | `STG_SERVICE_AREA` | `RETAIL_LOADER` | 370 |
| STAGE | `STG_STOCK_LEDGER` | `RETAIL_LOADER` | 173,970 |
| STAGE | `STG_STOCK_PREMISE` | `RETAIL_LOADER` | 4 |
| STAGE | `STG_WAREHOUSE` | `RETAIL_LOADER` | 4 |

Total no destino: **13,621,992 linhas**.

## Isolamento verificado papel a papel

Executado com `use secondary roles none`. Sem isso a verificação passaria por engano:
contas Snowflake modernas nascem com `DEFAULT_SECONDARY_ROLES = ('ALL')` e ativam
todos os papéis do usuário além do primário — medido nesta conta antes da correção.

- **RETAIL_READER** — So leitura de MART. E o papel de BI e de terceiros.
  - lê `RETAIL.MART.MART_CUSTOMER_BASE`
  - **não** lê `RETAIL.GOLD.DIM_CUSTOMER`
  - **não** lê `RETAIL.STAGE.STG_CUSTOMER`
- **RETAIL_TRANSFORMER** — Le o STAGE e escreve GOLD e MART. E o papel do dbt.
  - lê `RETAIL.STAGE.STG_CUSTOMER`
  - lê `RETAIL.GOLD.DIM_CUSTOMER`
- **RETAIL_LOADER** — Carrega o recorte do Silver no STAGE. Nao le GOLD nem MART.
  - lê `RETAIL.STAGE.STG_CUSTOMER`
  - **não** lê `RETAIL.MART.MART_CUSTOMER_BASE`
  - **não** lê `RETAIL.GOLD.DIM_CUSTOMER`

Resultado: **a matriz confere inteira** — nenhuma violação.

## Papéis em execução, vistos pelo verbo

A matriz acima prova o que cada papel **pode** ler. Esta tabela prova o que cada um
**fez** — e é a diferença entre governança verificada e governança adotada. Os três
papéis existiam desde a Fase 2, com os grants certos, enquanto todas as execuções
passavam por `ACCOUNTADMIN`; nada nesta página teria mostrado isso, porque a posse
só muda quando alguém escreve de fato.

Janela de 167 horas: `information_schema.query_history` não recupera nada além de
sete dias. Uma semana sem execução esvazia a seção, e ela **declara a ausência** em
vez de imprimir uma tabela vazia, que se leria como "não há separação".

| papel | tipo de query | queries | última |
|---|---|---|---|
| `RETAIL_LOADER` | `SELECT` | 412 | 2026-09-01 14:49 |
| `RETAIL_LOADER` | `PUT_FILES` | 166 | 2026-09-01 14:49 |
| `RETAIL_LOADER` | `CREATE_TABLE` | 166 | 2026-09-01 14:49 |
| `RETAIL_LOADER` | `COPY` | 165 | 2026-09-01 14:49 |
| `RETAIL_LOADER` | `USE` | 17 | 2026-09-01 09:43 |
| `RETAIL_READER` | `SELECT` | 810 | 2026-09-01 14:55 |
| `RETAIL_READER` | `USE` | 37 | 2026-09-01 14:55 |
| `RETAIL_READER` | `SHOW` | 3 | 2026-08-31 06:41 |
| `RETAIL_TRANSFORMER` | `SELECT` | 3,040 | 2026-09-01 14:49 |
| `RETAIL_TRANSFORMER` | `CREATE_TABLE_AS_SELECT` | 416 | 2026-09-01 14:49 |
| `RETAIL_TRANSFORMER` | `SHOW` | 209 | 2026-09-01 14:49 |
| `RETAIL_TRANSFORMER` | `USE` | 17 | 2026-09-01 09:43 |

## Amostras

Poucas linhas por mart, só para que o conteúdo seja inspecionável depois que o
destino não existir mais. Não substituem o warehouse enquanto ele viver.

### `MART_MARKET_COVERAGE`

| WH | MUNICIPALITY_NAME | CUSTOMERS | MUNICIPALITY_POPULATION | CUSTOMERS_PER_10K_INHABITANTS | HAS_NO_CUSTOMERS |
|---|---|---|---|---|---|
| vlc1 | Domeño | 18 | 723.0 | 248.963 | False |
| mad1 | Redueña | 7 | 288.0 | 243.056 | False |
| mad1 | Batres | 48 | 1976.0 | 242.915 | False |
| mad1 | Tielmes | 70 | 2964.0 | 236.167 | False |
| bcn1 | Òrrius | 19 | 812.0 | 233.99 | False |

### `MART_PRICE_EVOLUTION`

| SNAPSHOT_DATE | WH | DISPLAY_NAME | UNIT_PRICE | PRICE_DELTA | DAYS_SINCE_PREVIOUS_SNAPSHOT |
|---|---|---|---|---|---|
| 2026-08-28 | vlc1 | Merluza abierta en libro sin cabeza y sin espina | 17.25 | 4.14 | 1 |
| 2026-08-28 | vlc1 | Merluza sin aletas y sin escamas | 17.25 | 4.14 | 1 |
| 2026-08-28 | vlc1 | Merluza a rodajas | 17.25 | 4.14 | 1 |
| 2026-08-24 | mad1 | Maquinilla de afeitar recargable Gillette Labs Body + Intimate 2 hojas | 12.90 | -4.00 | 8 |
| 2026-09-01 | vlc1 | Rodajas de emperador pequeñas Hacendado ultracongeladas | 9.95 | -3.00 | 1 |

### `MART_ASSORTMENT_DAILY`

| SNAPSHOT_DATE | WH | CATEGORY_NAME | PRODUCTS | PRODUCTS_EXCLUSIVE_HERE | AVG_UNIT_PRICE |
|---|---|---|---|---|---|
| 2026-08-27 | bcn1 | Leche y bebidas vegetales | 120 | 13 | 3.7956 |
| 2026-08-26 | bcn1 | Leche y bebidas vegetales | 120 | 13 | 3.7956 |
| 2026-08-25 | bcn1 | Leche y bebidas vegetales | 120 | 15 | 3.7956 |
| 2026-08-28 | bcn1 | Leche y bebidas vegetales | 120 | 13 | 3.7956 |
| 2026-08-24 | bcn1 | Leche y bebidas vegetales | 120 | 15 | 3.7956 |

### `MART_DEMAND_COHORT`

| DEMAND_GROUP | PCT_LT35 | PCT_GE65 |
|---|---|---|
| VINO | 0.53 | 2.31 |
| CARNE_OVINO_CAPRINO | 0.11 | 0.31 |
| CARNE_CONEJO | 0.03 | 0.09 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0.28 | 0.73 |
| PESCADO_FRESCO | 0.35 | 0.81 |

### `MART_CUSTOMER_BASE`

| CUSTOMER_ID | WH | MUNICIPALITY_NAME | POSTAL_CODE | AGE_BAND | SEX_LABEL |
|---|---|---|---|---|---|
| cust_bcn1_000000 | bcn1 | Hospitalet de Llobregat, L' | 08906 | 45-64 | Hombres |
| cust_bcn1_000001 | bcn1 | Hospitalet de Llobregat, L' | 08904 | 18-29 | Hombres |
| cust_bcn1_000002 | bcn1 | Badalona | 08913 | 45-64 | Mujeres |
| cust_bcn1_000003 | bcn1 | Hospitalet de Llobregat, L' | 08902 | 45-64 | Mujeres |
| cust_bcn1_000004 | bcn1 | Terrassa | 08224 | 65+ | Hombres |

### `MART_ORDER_FUNNEL`

| ORDER_DATE | WH | ORDERS_PLACED | ORDERS_CONFIRMED | ORDERS_PICKED | ORDERS_DELIVERED | ORDERS_CANCELLED | ORDERS_RETURNED | DELIVERY_RATE |
|---|---|---|---|---|---|---|---|---|
| 2026-08-24 | bcn1 | 8494 | 8369 | 8144 | 8068 | 225 | 68 | 0.9498 |
| 2026-08-24 | mad1 | 9333 | 9181 | 8900 | 8823 | 281 | 80 | 0.9454 |
| 2026-08-24 | svq1 | 2206 | 2177 | 2117 | 2092 | 60 | 17 | 0.9483 |
| 2026-08-24 | vlc1 | 2914 | 2871 | 2777 | 2759 | 94 | 22 | 0.9468 |
| 2026-08-25 | bcn1 | 8494 | 8355 | 8120 | 8039 | 235 | 61 | 0.9464 |
| 2026-08-25 | mad1 | 9333 | 9193 | 8940 | 8840 | 253 | 79 | 0.9472 |
| 2026-08-25 | svq1 | 2206 | 2175 | 2116 | 2089 | 59 | 13 | 0.9470 |
| 2026-08-25 | vlc1 | 2914 | 2869 | 2770 | 2745 | 99 | 24 | 0.9420 |

### `MART_FULFILLMENT_SLA`

| ORDER_DATE | WH | SLA_MINUTES | MAX_PICKING_MINUTES | ORDERS_BREACHING_SLA | P50_MINUTES_TO_PICK | P90_MINUTES_TO_DELIVER | ORDERS_DELIVERED_BEFORE_SLOT | ORDERS_DELIVERED_WITHIN_SLOT | ORDERS_DELIVERED_AFTER_SLOT |
|---|---|---|---|---|---|---|---|---|---|
| 2026-08-24 | bcn1 | 60.000000 | 80 | 814 | 36.000 | 110.000 | 3316 | 2043 | 2709 |
| 2026-08-24 | mad1 | 60.000000 | 80 | 918 | 36.000 | 110.000 | 3792 | 2173 | 2858 |
| 2026-08-24 | svq1 | 60.000000 | 80 | 190 | 34.000 | 111.000 | 873 | 516 | 703 |
| 2026-08-24 | vlc1 | 60.000000 | 80 | 283 | 36.000 | 109.000 | 1203 | 694 | 862 |
| 2026-08-25 | bcn1 | 60.000000 | 80 | 783 | 36.000 | 110.000 | 3477 | 1995 | 2567 |
| 2026-08-25 | mad1 | 60.000000 | 80 | 863 | 36.000 | 109.000 | 3761 | 2159 | 2920 |
| 2026-08-25 | svq1 | 60.000000 | 80 | 190 | 36.000 | 110.000 | 890 | 504 | 695 |
| 2026-08-25 | vlc1 | 60.000000 | 80 | 258 | 36.000 | 110.000 | 1158 | 666 | 921 |

### `MART_BASKET_DAILY`

| ORDER_DATE | WH | CATEGORY_NAME | ORDERS_TOUCHING_CATEGORY | LINES_PLACED | LINES_SUBSTITUTED | REVENUE_PLACED | REVENUE_FULFILLED | SUBSTITUTION_RATE |
|---|---|---|---|---|---|---|---|---|
| 2026-08-24 | mad1 | Leche y bebidas vegetales | 5024 | 7614 | 285 | 53905.36 | 50718.49 | 0.0374 |
| 2026-08-26 | mad1 | Leche y bebidas vegetales | 4903 | 7390 | 297 | 53106.82 | 49791.10 | 0.0402 |
| 2026-08-28 | mad1 | Leche y bebidas vegetales | 4960 | 7484 | 265 | 52846.44 | 49200.28 | 0.0354 |
| 2026-08-25 | mad1 | Leche y bebidas vegetales | 4941 | 7403 | 269 | 51776.95 | 48977.62 | 0.0363 |
| 2026-08-27 | mad1 | Leche y bebidas vegetales | 4944 | 7358 | 275 | 52543.64 | 48830.37 | 0.0374 |

### `MART_STOCK_HEALTH`

| STOCK_DATE | WH | CATEGORY_NAME | CLOSING_UNITS | UNITS_DEMANDED | UNITS_SHORT | SERIES_WITH_SHORTFALL | FILL_RATE | DAYS_OF_COVER | DAYS_OF_COVER_TYPICAL_PRODUCT | REPLENISHMENT_ORDERS | STOCK_LABEL |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-08-29 | svq1 | Pizzas | 5 | 27 | 0 | 0 | 1.0000 | 0.29 | 0.29 | 0 | synthetic |
| 2026-08-29 | svq1 | Pescado | 6 | 4 | 0 | 0 | 1.0000 | 0.67 | 0.67 | 0 | synthetic |
| 2026-08-28 | vlc1 | Pizzas | 14 | 20 | 0 | 0 | 1.0000 | 0.87 | 0.87 | 0 | synthetic |
| 2026-08-30 | svq1 | Carne | 4 | 6 | 0 | 0 | 1.0000 | 1.03 | 1.03 | 0 | synthetic |
| 2026-08-28 | svq1 | Postres de soja | 72 | 104 | 2 | 1 | 0.9808 | 1.03 | 1.01 | 3 | synthetic |
| 2026-08-28 | mad1 | Pescado | 41 | 30 | 0 | 0 | 1.0000 | 1.08 | 1.08 | 0 | synthetic |
| 2026-08-28 | vlc1 | Vino rosado | 75 | 90 | 0 | 0 | 1.0000 | 1.10 | 1.14 | 1 | synthetic |
| 2026-08-28 | svq1 | Pescado | 10 | 21 | 0 | 0 | 1.0000 | 1.11 | 1.11 | 1 | synthetic |

