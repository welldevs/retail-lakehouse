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
| capturado em | 2026-09-01 06:26:01 -07:00 |

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
| GOLD | `FACT_INGESTION_RUN` | `RETAIL_TRANSFORMER` | 58 |
| GOLD | `FACT_ORDER` | `RETAIL_TRANSFORMER` | 91,788 |
| GOLD | `FACT_ORDER_EVENT` | `RETAIL_TRANSFORMER` | 636,848 |
| GOLD | `FACT_ORDER_ITEM` | `RETAIL_TRANSFORMER` | 1,726,833 |
| GOLD | `FACT_ORDER_PREMISE` | `RETAIL_TRANSFORMER` | 31 |
| GOLD | `FACT_POPULATION_MUNICIPALITY` | `RETAIL_TRANSFORMER` | 21,410 |
| GOLD | `FACT_PRICE_CHANGE` | `RETAIL_TRANSFORMER` | 129,483 |
| GOLD | `FACT_PRICE_SNAPSHOT` | `RETAIL_TRANSFORMER` | 146,482 |
| MART | `MART_ASSORTMENT_DAILY` | `RETAIL_TRANSFORMER` | 5,060 |
| MART | `MART_BASKET_DAILY` | `RETAIL_TRANSFORMER` | 2,380 |
| MART | `MART_CUSTOMER_BASE` | `RETAIL_TRANSFORMER` | 286,826 |
| MART | `MART_DEMAND_COHORT` | `RETAIL_TRANSFORMER` | 2,622 |
| MART | `MART_FULFILLMENT_SLA` | `RETAIL_TRANSFORMER` | 16 |
| MART | `MART_MARKET_COVERAGE` | `RETAIL_TRANSFORMER` | 370 |
| MART | `MART_ORDER_FUNNEL` | `RETAIL_TRANSFORMER` | 16 |
| MART | `MART_PRICE_EVOLUTION` | `RETAIL_TRANSFORMER` | 146,482 |
| STAGE | `STG_CATEGORY` | `RETAIL_LOADER` | 151 |
| STAGE | `STG_CUSTOMER` | `RETAIL_LOADER` | 573,652 |
| STAGE | `STG_GEOGRAPHY` | `RETAIL_LOADER` | 699 |
| STAGE | `STG_INGESTION_RUN` | `RETAIL_LOADER` | 58 |
| STAGE | `STG_ORDER` | `RETAIL_LOADER` | 91,788 |
| STAGE | `STG_ORDER_EVENT` | `RETAIL_LOADER` | 636,848 |
| STAGE | `STG_ORDER_LINE` | `RETAIL_LOADER` | 1,726,833 |
| STAGE | `STG_ORDER_PREMISE` | `RETAIL_LOADER` | 31 |
| STAGE | `STG_POPULATION_MUNICIPALITY` | `RETAIL_LOADER` | 21,410 |
| STAGE | `STG_PRICE_CHANGE` | `RETAIL_LOADER` | 129,483 |
| STAGE | `STG_PRODUCT_PRICE` | `RETAIL_LOADER` | 146,482 |
| STAGE | `STG_SERVICE_AREA` | `RETAIL_LOADER` | 370 |
| STAGE | `STG_WAREHOUSE` | `RETAIL_LOADER` | 4 |

Total no destino: **7,108,040 linhas**.

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
| vlc1 | Domeño | 18 | 723.0 | 248.963 | False |
| mad1 | Redueña | 7 | 288.0 | 243.056 | False |
| mad1 | Batres | 48 | 1976.0 | 242.915 | False |
| mad1 | Tielmes | 70 | 2964.0 | 236.167 | False |
| bcn1 | Òrrius | 19 | 812.0 | 233.99 | False |

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
| 2026-08-28 | bcn1 | Leche y bebidas vegetales | 120 | 13 | 3.7956 |
| 2026-08-25 | bcn1 | Leche y bebidas vegetales | 120 | 15 | 3.7956 |
| 2026-08-24 | bcn1 | Leche y bebidas vegetales | 120 | 15 | 3.7956 |
| 2026-08-27 | bcn1 | Leche y bebidas vegetales | 120 | 13 | 3.7956 |
| 2026-08-26 | bcn1 | Leche y bebidas vegetales | 120 | 13 | 3.7956 |

### `MART_DEMAND_COHORT`

| DEMAND_GROUP | PCT_LT35 | PCT_GE65 |
|---|---|---|
| VINO | 0.52 | 2.30 |
| CARNE_CONEJO | 0.03 | 0.10 |
| CARNE_OVINO_CAPRINO | 0.10 | 0.33 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0.29 | 0.74 |
| BEBIDAS_ESPIRITUOSAS | 0.10 | 0.23 |

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
| 2026-08-24 | bcn1 | 8494 | 8342 | 8092 | 8010 | 250 | 63 | 0.9430 |
| 2026-08-24 | mad1 | 9333 | 9187 | 8892 | 8798 | 295 | 79 | 0.9427 |
| 2026-08-24 | svq1 | 2206 | 2181 | 2123 | 2101 | 58 | 12 | 0.9524 |
| 2026-08-24 | vlc1 | 2914 | 2879 | 2790 | 2760 | 89 | 18 | 0.9472 |
| 2026-08-25 | bcn1 | 8494 | 8371 | 8110 | 8025 | 261 | 64 | 0.9448 |
| 2026-08-25 | mad1 | 9333 | 9187 | 8920 | 8832 | 267 | 68 | 0.9463 |
| 2026-08-25 | svq1 | 2206 | 2173 | 2115 | 2095 | 58 | 19 | 0.9497 |
| 2026-08-25 | vlc1 | 2914 | 2863 | 2780 | 2756 | 83 | 25 | 0.9458 |

### `MART_FULFILLMENT_SLA`

| ORDER_DATE | WH | SLA_MINUTES | MAX_PICKING_MINUTES | ORDERS_BREACHING_SLA | P50_MINUTES_TO_PICK | P90_MINUTES_TO_DELIVER | ORDERS_DELIVERED_BEFORE_SLOT | ORDERS_DELIVERED_WITHIN_SLOT | ORDERS_DELIVERED_AFTER_SLOT |
|---|---|---|---|---|---|---|---|---|---|
| 2026-08-24 | bcn1 | 90.000000 | 80 | 0 | 36.000 | 110.000 | 6764 | 677 | 569 |
| 2026-08-24 | mad1 | 90.000000 | 80 | 0 | 36.000 | 110.000 | 7391 | 749 | 658 |
| 2026-08-24 | svq1 | 90.000000 | 80 | 0 | 36.000 | 110.000 | 1763 | 195 | 143 |
| 2026-08-24 | vlc1 | 90.000000 | 80 | 0 | 34.000 | 109.000 | 2337 | 231 | 192 |
| 2026-08-25 | bcn1 | 90.000000 | 80 | 0 | 36.000 | 110.000 | 6777 | 678 | 570 |
| 2026-08-25 | mad1 | 90.000000 | 80 | 0 | 34.000 | 110.000 | 7412 | 782 | 638 |
| 2026-08-25 | svq1 | 90.000000 | 80 | 0 | 36.000 | 111.000 | 1733 | 186 | 176 |
| 2026-08-25 | vlc1 | 90.000000 | 80 | 0 | 36.000 | 109.000 | 2329 | 229 | 198 |

### `MART_BASKET_DAILY`

| ORDER_DATE | WH | CATEGORY_NAME | ORDERS_TOUCHING_CATEGORY | LINES_PLACED | LINES_SUBSTITUTED | REVENUE_PLACED | REVENUE_FULFILLED | SUBSTITUTION_RATE |
|---|---|---|---|---|---|---|---|---|
| 2026-08-24 | mad1 | Leche y bebidas vegetales | 5038 | 7505 | 305 | 54485.86 | 50718.44 | 0.0406 |
| 2026-08-26 | mad1 | Leche y bebidas vegetales | 4925 | 7449 | 266 | 54128.21 | 50619.94 | 0.0357 |
| 2026-08-25 | mad1 | Leche y bebidas vegetales | 4908 | 7376 | 273 | 53069.40 | 49792.89 | 0.0370 |
| 2026-08-27 | mad1 | Leche y bebidas vegetales | 4992 | 7457 | 269 | 52938.63 | 49639.88 | 0.0361 |
| 2026-08-26 | mad1 | Jamón serrano | 497 | 522 | 18 | 47565.86 | 45670.00 | 0.0345 |

