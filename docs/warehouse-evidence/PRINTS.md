# Prints do Snowflake — o que capturar e o que cada um prova

Seis capturas, escolhidas pelo mesmo critério: cada uma prova algo que **o repositório
sozinho não prova**. Tudo o que é verificável offline já tem teste (645 no Python, 215 no
dbt contra o DuckDB) e não precisa de print — um print de código passando é decoração.

O que só existe na conta viva é: os papéis sendo de fato vestidos, a posse dos objetos, e
a negação de acesso acontecendo. É isso que vale capturar antes de o trial expirar.

Salve os arquivos nesta pasta com o nome indicado. O [README.md](README.md) desta pasta é
gerado por `make warehouse-evidence` e cobre a parte textual.

---

### 1. `01-papeis-por-tipo-de-query.png` — **o print mais importante**

**Onde:** worksheet, logo depois de uma execução (`make warehouse-refresh` ou a DAG
`warehouse_load`):

```sql
select role_name, query_type, count(*) as queries, max(start_time) as ultima
from table(information_schema.query_history(
     end_time_range_start => dateadd('hour', -2, current_timestamp()),
     result_limit => 10000))
where role_name like 'RETAIL%'
group by 1, 2 order by 1, 3 desc;
```

**O que prova:** que as execuções passam pelos papéis do projeto, e não por
`ACCOUNTADMIN`. Melhor que rolar o Query History porque a separação aparece no **tipo de
query**, não só no nome do papel — cada papel só executa o verbo que lhe cabe:

| papel | tipos observados |
|---|---|
| `RETAIL_LOADER` | `PUT_FILES`, `COPY`, `CREATE_TABLE` — o transporte |
| `RETAIL_TRANSFORMER` | `CREATE_TABLE_AS_SELECT` — os modelos do dbt |
| `RETAIL_READER` | só `SELECT` |

Sem este print, criar três papéis e verificá-los seria indistinguível de rodar tudo como
administrador — que foi o estado do projeto até esta rodada.

> Se preferir a tela nativa: Monitoring → Query History com a coluna *Role* visível serve,
> mas exige enquadrar linhas dos três papéis juntas.

### 2. `02-arvore-retail.png`

**Onde:** Data → Databases → `RETAIL`, com `STAGE`, `GOLD` e `MART` expandidos.

**O que prova:** as três camadas com nomes e conteúdo reais — 9 tabelas `STG_*`, 6 `DIM_*`
+ 4 `FACT_*`, 4 `MART_*`. É a arquitetura descrita no [ARCHITECTURE.md](../../ARCHITECTURE.md)
existindo, não desenhada.

### 3. `03-posse-e-grants.png`

**Onde:** uma tabela de `GOLD` → aba *Privileges* (ou o resultado de
`show grants on table RETAIL.GOLD.DIM_CUSTOMER`).

**O que prova:** que a posse é de `RETAIL_TRANSFORMER`. Este foi o detalhe que reprovou 8
modelos de uma vez: `grant all` concede os privilégios aplicáveis e **posse não é um
deles**, então `create or replace table` falhava. O print mostra a correção no lugar.

### 4. `04-acesso-negado-reader.png`

**Onde:** worksheet com `use role RETAIL_READER;` seguido de
`select * from RETAIL.GOLD.DIM_CUSTOMER limit 1;` — e o erro na tela.

**O que prova:** o isolamento acontecendo, não declarado. É o complemento do
`check_isolation`: o teste diz que a matriz confere, o print mostra a recusa.

> Se o erro **não** aparecer, a sessão está com papéis secundários ativos. Rode
> `select current_secondary_roles();` — tem de vir vazio. `make warehouse-bootstrap`
> desliga isso no usuário; foi assim que a verificação de RBAC passava por engano antes.

### 5. `05-mart-market-coverage.png`

**Onde:** worksheet, resultado de:

```sql
select wh, municipality_name, customers, municipality_population,
       customers_per_10k_inhabitants
from RETAIL.MART.MART_MARKET_COVERAGE
where not has_no_customers
order by customers_per_10k_inhabitants desc limit 20;
```

**O que prova:** o mart onde o sintético encontra o observado — clientes gerados contra
população real do INE, com o grão declarado. Mostra também onde a simulação **não** tem
densidade, que é o ponto: o mart expõe a lacuna em vez de escondê-la.

### 6. `06-warehouse-config.png`

**Onde:** Admin → Warehouses → `COMPUTE_WH`.

**O que prova:** X-Small com auto-suspend de 60 s. Proporcionalidade: 146 mil linhas e um
`dbt build` de ~12 s não pedem mais que isso, e o default de 300 s cobraria cinco minutos
de compute ocioso por execução.

---

## O que deliberadamente não entra

- **Print de teste verde.** Os 645 testes Python e 215 do dbt rodam offline; quem clonar o
  repositório reproduz. Print de algo reproduzível é enfeite.
- **Print do dbt docs / lineage.** O grafo sai do próprio repositório com
  `dbt docs generate`; não depende da conta.
- **Print de dado em volume.** Amostras já estão no [README.md](README.md), geradas por
  consulta e datadas.
