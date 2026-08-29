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
| capturado em | 2026-08-29 06:29:46 -07:00 |

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
| GOLD | `DIM_CUSTOMER` | `RETAIL_TRANSFORMER` | 40,000 |
| GOLD | `DIM_DATE` | `RETAIL_TRANSFORMER` | 4,018 |
| GOLD | `DIM_GEOGRAPHY` | `RETAIL_TRANSFORMER` | 699 |
| GOLD | `DIM_PRODUCT` | `RETAIL_TRANSFORMER` | 4,978 |
| GOLD | `DIM_WAREHOUSE` | `RETAIL_TRANSFORMER` | 4 |
| GOLD | `FACT_INGESTION_RUN` | `RETAIL_TRANSFORMER` | 46 |
| GOLD | `FACT_ORDER` | `RETAIL_TRANSFORMER` | 6,400 |
| GOLD | `FACT_ORDER_EVENT` | `RETAIL_TRANSFORMER` | 44,456 |
| GOLD | `FACT_ORDER_ITEM` | `RETAIL_TRANSFORMER` | 120,693 |
| GOLD | `FACT_ORDER_PREMISE` | `RETAIL_TRANSFORMER` | 30 |
| GOLD | `FACT_POPULATION_MUNICIPALITY` | `RETAIL_TRANSFORMER` | 21,410 |
| GOLD | `FACT_PRICE_CHANGE` | `RETAIL_TRANSFORMER` | 77,733 |
| GOLD | `FACT_PRICE_SNAPSHOT` | `RETAIL_TRANSFORMER` | 94,863 |
| MART | `MART_ASSORTMENT_DAILY` | `RETAIL_TRANSFORMER` | 3,272 |
| MART | `MART_BASKET_DAILY` | `RETAIL_TRANSFORMER` | 2,375 |
| MART | `MART_CUSTOMER_BASE` | `RETAIL_TRANSFORMER` | 20,000 |
| MART | `MART_FULFILLMENT_SLA` | `RETAIL_TRANSFORMER` | 16 |
| MART | `MART_MARKET_COVERAGE` | `RETAIL_TRANSFORMER` | 370 |
| MART | `MART_ORDER_FUNNEL` | `RETAIL_TRANSFORMER` | 16 |
| MART | `MART_PRICE_EVOLUTION` | `RETAIL_TRANSFORMER` | 94,863 |
| STAGE | `STG_CATEGORY` | `RETAIL_LOADER` | 151 |
| STAGE | `STG_CUSTOMER` | `RETAIL_LOADER` | 40,000 |
| STAGE | `STG_GEOGRAPHY` | `RETAIL_LOADER` | 699 |
| STAGE | `STG_INGESTION_RUN` | `RETAIL_LOADER` | 46 |
| STAGE | `STG_ORDER` | `RETAIL_LOADER` | 6,400 |
| STAGE | `STG_ORDER_EVENT` | `RETAIL_LOADER` | 44,456 |
| STAGE | `STG_ORDER_LINE` | `RETAIL_LOADER` | 120,693 |
| STAGE | `STG_ORDER_PREMISE` | `RETAIL_LOADER` | 30 |
| STAGE | `STG_POPULATION_MUNICIPALITY` | `RETAIL_LOADER` | 21,410 |
| STAGE | `STG_PRICE_CHANGE` | `RETAIL_LOADER` | 77,733 |
| STAGE | `STG_PRODUCT_PRICE` | `RETAIL_LOADER` | 94,863 |
| STAGE | `STG_SERVICE_AREA` | `RETAIL_LOADER` | 370 |
| STAGE | `STG_WAREHOUSE` | `RETAIL_LOADER` | 4 |

Total no destino: **943,248 linhas**.

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

## Amostras

Poucas linhas por mart, só para que o conteúdo seja inspecionável depois que o
destino não existir mais. Não substituem o warehouse enquanto ele viver.

### `MART_MARKET_COVERAGE`

| WH | MUNICIPALITY_NAME | CUSTOMERS | MUNICIPALITY_POPULATION | CUSTOMERS_PER_10K_INHABITANTS | HAS_NO_CUSTOMERS |
|---|---|---|---|---|---|
| vlc1 | Emperador | 4 | 700.0 | 57.143 | False |
| svq1 | Molares, Los | 20 | 3659.0 | 54.66 | False |
| svq1 | Burguillos | 35 | 7448.0 | 46.992 | False |
| svq1 | Villanueva del Río y Minas | 23 | 5073.0 | 45.338 | False |
| svq1 | Santiponce | 35 | 8634.0 | 40.537 | False |

### `MART_PRICE_EVOLUTION`

| SNAPSHOT_DATE | WH | DISPLAY_NAME | UNIT_PRICE | PRICE_DELTA | DAYS_SINCE_PREVIOUS_SNAPSHOT |
|---|---|---|---|---|---|
| 2026-08-28 | vlc1 | Merluza abierta en libro sin cabeza y sin espina | 17.25 | 4.14 | 1 |
| 2026-08-28 | vlc1 | Merluza a rodajas | 17.25 | 4.14 | 1 |
| 2026-08-28 | vlc1 | Merluza sin aletas y sin escamas | 17.25 | 4.14 | 1 |
| 2026-08-24 | mad1 | Maquinilla de afeitar recargable Gillette Labs Body + Intimate 2 hojas | 12.90 | -4.00 | 8 |
| 2026-08-24 | mad1 | Leche de continuación en polvo 2 Nidina Nestlé | 14.95 | -3.00 | 8 |

### `MART_ASSORTMENT_DAILY`

| SNAPSHOT_DATE | WH | CATEGORY_NAME | PRODUCTS | PRODUCTS_EXCLUSIVE_HERE | AVG_UNIT_PRICE |
|---|---|---|---|---|---|
| 2026-08-24 | bcn1 | Leche y bebidas vegetales | 120 | 15 | 3.7956 |
| 2026-08-25 | bcn1 | Leche y bebidas vegetales | 120 | 15 | 3.7956 |
| 2026-08-26 | bcn1 | Leche y bebidas vegetales | 120 | 13 | 3.7956 |
| 2026-08-27 | bcn1 | Leche y bebidas vegetales | 120 | 13 | 3.7956 |
| 2026-08-28 | bcn1 | Leche y bebidas vegetales | 120 | 13 | 3.7956 |

### `MART_CUSTOMER_BASE`

| CUSTOMER_ID | WH | MUNICIPALITY_NAME | POSTAL_CODE | AGE_BAND | SEX_LABEL |
|---|---|---|---|---|---|
| cust_bcn1_000000 | bcn1 | Hospitalet de Llobregat, L' | 08906 | 30-44 | Hombres |
| cust_bcn1_000001 | bcn1 | Hospitalet de Llobregat, L' | 08904 | 00-17 | Hombres |
| cust_bcn1_000002 | bcn1 | Badalona | 08913 | 45-64 | Mujeres |
| cust_bcn1_000003 | bcn1 | Hospitalet de Llobregat, L' | 08902 | 45-64 | Mujeres |
| cust_bcn1_000004 | bcn1 | Terrassa | 08224 | 65+ | Hombres |

### `MART_ORDER_FUNNEL`

| ORDER_DATE | WH | ORDERS_PLACED | ORDERS_CONFIRMED | ORDERS_PICKED | ORDERS_DELIVERED | ORDERS_CANCELLED | ORDERS_RETURNED | DELIVERY_RATE |
|---|---|---|---|---|---|---|---|---|
| 2026-08-24 | bcn1 | 400 | 396 | 381 | 377 | 15 | 5 | 0.9425 |
| 2026-08-24 | mad1 | 400 | 394 | 383 | 377 | 11 | 1 | 0.9425 |
| 2026-08-24 | svq1 | 400 | 396 | 382 | 378 | 14 | 2 | 0.9450 |
| 2026-08-24 | vlc1 | 400 | 391 | 381 | 379 | 10 | 6 | 0.9475 |
| 2026-08-25 | bcn1 | 400 | 396 | 387 | 385 | 9 | 3 | 0.9625 |
| 2026-08-25 | mad1 | 400 | 396 | 384 | 383 | 12 | 2 | 0.9575 |
| 2026-08-25 | svq1 | 400 | 391 | 378 | 377 | 13 | 2 | 0.9425 |
| 2026-08-25 | vlc1 | 400 | 391 | 378 | 375 | 13 | 4 | 0.9375 |

### `MART_FULFILLMENT_SLA`

| ORDER_DATE | WH | SLA_MINUTES | MAX_PICKING_MINUTES | ORDERS_BREACHING_SLA | P50_MINUTES_TO_PICK | P90_MINUTES_TO_DELIVER | ORDERS_DELIVERED_BEFORE_SLOT | ORDERS_DELIVERED_WITHIN_SLOT | ORDERS_DELIVERED_AFTER_SLOT |
|---|---|---|---|---|---|---|---|---|---|
| 2026-08-24 | bcn1 | 90.000000 | 80 | 0 | 36.000 | 107.000 | 319 | 33 | 25 |
| 2026-08-24 | mad1 | 90.000000 | 78 | 0 | 36.000 | 108.000 | 331 | 24 | 22 |
| 2026-08-24 | svq1 | 90.000000 | 80 | 0 | 36.000 | 107.000 | 326 | 19 | 33 |
| 2026-08-24 | vlc1 | 90.000000 | 78 | 0 | 36.000 | 109.000 | 320 | 31 | 28 |
| 2026-08-25 | bcn1 | 90.000000 | 78 | 0 | 36.000 | 108.600 | 326 | 31 | 28 |
| 2026-08-25 | mad1 | 90.000000 | 76 | 0 | 36.000 | 111.000 | 316 | 34 | 33 |
| 2026-08-25 | svq1 | 90.000000 | 80 | 0 | 36.000 | 110.000 | 318 | 30 | 29 |
| 2026-08-25 | vlc1 | 90.000000 | 76 | 0 | 35.000 | 110.600 | 327 | 28 | 20 |

### `MART_BASKET_DAILY`

| ORDER_DATE | WH | CATEGORY_NAME | ORDERS_TOUCHING_CATEGORY | LINES_PLACED | LINES_SUBSTITUTED | REVENUE_PLACED | REVENUE_FULFILLED | SUBSTITUTION_RATE |
|---|---|---|---|---|---|---|---|---|
| 2026-08-25 | bcn1 | Marisco | 52 | 56 | 3 | 21017.23 | 24654.60 | 0.0536 |
| 2026-08-24 | vlc1 | Marisco | 60 | 66 | 0 | 18038.74 | 18022.71 | 0.0000 |
| 2026-08-24 | bcn1 | Marisco | 62 | 69 | 0 | 27523.45 | 16437.51 | 0.0000 |
| 2026-08-26 | vlc1 | Marisco | 51 | 54 | 1 | 15654.76 | 15576.39 | 0.0185 |
| 2026-08-25 | vlc1 | Marisco | 50 | 54 | 3 | 11958.40 | 13206.95 | 0.0556 |

