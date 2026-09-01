# Source Contract — `simulated_oltp`

Contrato entre esta Source e qualquer componente que consuma seus snapshots.
Tudo aqui é imposto por código e coberto por teste, salvo onde marcado como **[fonte]** —
característica do dado do INE, fora do controle desta Source.

**Diferença estrutural das outras três Sources: esta é DERIVADA.** As outras três são
upstream de dado externo (API da Mercadona, API do INE, arquivos do Callejero); esta
consome o Silver que elas produziram e devolve uma RAW nova. A inversão de direção é
deliberada, e é o que permite gerar clientes sintéticos sem inventar geografia.

**Não há rede em nenhum passo.** `extract` aqui não faz nenhuma requisição: lê três
arquivos JSON planos que a plataforma pré-computou do Silver com `retail-platform
export-oltp-reference`, e gera os clientes a partir deles. O verbo é mantido por
uniformidade de interface; o significado muda — mesmo precedente do `extract --in <dir>`
do `ine_callejero`, um nível antes na cadeia.

**Por que a referência existe:** toda Source deste repo é FROZEN (`dependencies = []`,
verificado por AST em `tests/test_dependencies.py`), e `duckdb`/`boto3` são dependências
exclusivas da plataforma por design. Uma Source não pode abrir conexão com o Lakehouse
sem quebrar essa fronteira. A referência é um contrato **físico** entre os dois lados:
nenhum importa o código do outro.

## 1. Identidade do snapshot

| Chave | Valor | Onde vive |
|---|---|---|
| `source` | `simulated_oltp` | `_manifest.json` → `source.name` |
| `ingestion_date` | `YYYY-MM-DD` (UTC) | diretório da partição e `partition.ingestion_date` |
| `wh` | armazém (`mad1`, `bcn1`, `svq1`, `vlc1`) | diretório da partição e `partition.warehouse` |

Uma partição **por warehouse**, como na source da Mercadona. Não é escolha estética:
`platform/src/retail_platform/manifest.py` deriva `axis_name = "wh" if warehouse else
None`, e esses são os dois únicos formatos que o leitor de manifesto entende. Também dá
isolamento de falha — se `vlc1` falhar, os outros três não são afetados.

```
<root>/ingestion_date=YYYY-MM-DD/wh=<armazém>/
├── customers.json
├── _manifest.json
└── _SUCCESS          (só existe quando complete = true)
```

Blocos de primeiro nível do `_manifest.json`: `manifest_version`, `run_id`,
`started_at_utc`, `finished_at_utc`, `duration_seconds`, `complete`, `source`,
`partition`, `config`, `reference`, `totals`, `schema_fingerprint`, `files[]`,
`failures[]`, `anomalies[]`, `history[]`.

## 2. Conteúdo garantido

`customers.json` é um **array JSON** de objetos, um por cliente. Todo objeto tem
exatamente estes campos, sempre presentes:

| Campo | Papel | Origem |
|---|---|---|
| `customer_id` | identidade do cliente sintético | gerado: `cust_<wh>_<índice de 6 dígitos>` |
| `wh` | linhagem: qual armazém gerou | argumento `--wh` |
| `province_code`, `province_name` | linhagem: província | `warehouse_province_map_seed` |
| `municipality_code`, `municipality_name` | linhagem: município | INE 29005 |
| `candidate_index` | linhagem: **qual tramo** | índice em `address_candidates.json` |
| `street_name` | via ou pseudovia | Callejero (`VIAS`/`PSEU`) |
| `postal_code` | CEP | Callejero (`TRAM`, campo `CPOS`) |
| `numbering_type` | `"0"`/`"1"`/`"2"`, verbatim | Callejero (`TRAM`, campo `TINUM`) |
| `house_number` | inteiro **ou `null`** | sorteado na faixa real do tramo |
| `first_name`, `last_name` | nome | lista curada embutida no pacote |
| `sex_label` | `"Hombres"` / `"Mujeres"` | amostrado pela proporção municipal |
| `birth_year` | ano de nascimento | `ano(ingestion_date) − idade amostrada`, com a idade sempre `>= min_customer_age` |

**O cliente é inventado; o lugar onde ele mora não.** Município, via, CEP e faixa de
numeração vêm sempre de uma linha real do Callejero. Nada é construído a partir de partes
soltas, e nenhum CEP, município ou via fictício é gerado.

### Quantos clientes cada armazém tem

**Não é mais um argumento.** Até a Fase 5 o número vinha da linha de comando, e o Makefile
passava 5.000 para os quatro armazéns — o mesmo número para AUFs que diferem por **4,6× em
população**. Nada reprovava: os endereços eram reais, os totais fechavam, o manifesto batia.
A única coisa errada era que a densidade não existia, e densidade não aparece em nenhum total.

Omitindo `--count`, o alvo vem de `customer_allocation`, no cabeçalho de
`municipality_population_weights.json`:

```
população municipal observada (INE 29005)
  × share adulto da província daquele município (INE 31304, idades >= min_customer_age)
  × taxa de penetração
  = clientes daquele armazém
```

| wh | pop. servida | share adulto | pop. adulta | clientes |
|---|---:|---:|---:|---:|
| mad1 | 7.104.034 | 82,393 % | 5.853.206 | 128.771 |
| bcn1 | 5.277.804 | 82,247 % | 4.340.819 | 95.498 |
| vlc1 | 1.885.230 | 82,689 % | 1.558.871 | 34.295 |
| svq1 | 1.585.157 | 81,041 % | 1.284.630 | 28.262 |
| | **15.852.225** | | **13.037.526** | **286.826** |

**O total é consequência, não cota.** Cada armazém sai da própria população; acrescentar um
município à área de serviço acrescenta clientes, em vez de tirá-los dos outros.

`--count` continua funcionando e vira **override explícito**, registrado no manifesto como
`config.count_source: "cli"` — sem isso, uma base digitada à mão seria indistinguível de uma
derivada da população.

### A taxa de penetração, e as duas premissas que ela carrega

A taxa é **2,2 %**: a participação do e-commerce no volume total de alimentação em 2025
(informe do MAPA, seção 3). É um número **observado**, e ele **não** é declarado no domínio
do cadastro — `customer_premises.customer_penetration_source` aponta para
`demand_profile.channel_reference_pct`, onde ele foi medido. Copiá-lo criaria dois lugares
para mudá-lo.

Duas premissas, **nenhuma medida**, transformam um share de volume num share de gente:

1. **O comprador online consome como a média.** Sob ela, 2,2 % do volume corresponde a 2,2 %
   das pessoas. Nenhuma fonte deste repositório mede penetração de cliente.
2. **Estes quatro armazéns modelam o canal online inteiro da AUF**, não um operador dentro
   dele. Aplicar participação de mercado de um operador exigiria uma fonte que este
   repositório não ingere.

### Cadeia de amostragem

```
warehouse → município ponderado pela população municipal observada (INE 29005)
          → tramo UNIFORME entre os candidatos válidos daquele município
          → número da casa dentro da faixa real, respeitando a paridade
          → sexo pela proporção municipal observada (INE 29005)
          → idade pela distribuição PROVINCIAL ADULTA (INE 31304) — proxy, truncada
          → nome de lista curada embutida
```

A escolha do tramo é **uniforme de propósito, e isso não é densidade populacional**: não
existe população por rua nem por tramo em nenhuma fonte que esta plataforma ingere.
Ponderar tramos inventaria uma distribuição que ninguém mediu. O peso populacional entra
uma única vez, na escolha do município.

### Composição demográfica, explicitamente

| Dimensão | Como é obtida | Natureza |
|---|---|---|
| Município | população municipal **observada** (29005, `year=2025`, `reference_date` 2024-12-31, cifras oficiales, inteiros) | observado |
| Sexo | distribuição municipal **observada** (mesma tabela) | observado |
| Idade | distribuição **provincial ADULTA** (31304, `year=2022`, `reference_date` 2022-06-30, `fk_periodo=27`, `fk_tipo_dato=2`), truncada em `min_customer_age` e renormalizada | **proxy** |
| Tamanho da base | população adulta do armazém × taxa de penetração | derivado |

#### O cadastro não é a população residente

Até a Fase 5 era, e por isso **18,01 % dos clientes tinham menos de 18 anos** — 3.602 de
20.000, com `age_at_ingestion` de **0 a 100**. Havia titular de conta recém-nascido.

Isso não era defeito desta Source: o contrato declarava que a idade vinha da distribuição
**populacional** provincial do INE, e era exatamente isso que ela entregava. O que nunca fora
declarado é que **um cadastro não é um censo**. Enquanto a idade não fazia nada, um titular de
três anos era inofensivo; a partir do momento em que ela governa a demanda (Fase 5), 18 % da
base entra na faixa mais jovem do benchmark sendo criança e **nada quebra**.

A idade mínima é premissa da **plataforma** (`customer_premises.min_customer_age`), não desta
Source, e chega já aplicada: `province_age_distribution.json` traz só as idades `>= 18`,
renormalizadas para somar 1 por província, com `min_customer_age` declarado no cabeçalho ao
lado dos dois rótulos agregados que já eram excluídos. Esta Source **desconfia da promessa**:
recusa referência sem o cabeçalho, recusa referência cujas linhas contradigam o cabeçalho, e
`validate` recalcula a idade de cada cliente pousado.

Quatro aproximações declaradas, nenhuma escondida:

1. **A idade é provincial, não municipal.** Não existe faixa etária por município nas
   tabelas ingeridas por esta plataforma — a 29005 não tem coluna de idade e a 31304 só
   tem província. Todo município de uma província recebe a mesma forma de pirâmide.
2. **Vintages diferentes.** A defasagem real entre os dois insumos é de **2 anos e 6
   meses** (2024-12-31 contra 2022-06-30), medida por `reference_date` e não por `year`.
3. **Naturezas diferentes.** A 31304 com `fk_tipo_dato=2` traz **estimativas** (valores
   não inteiros, ex. Madrid 2022 = 6.825.005,307136); a 29005 traz cifras oficiales.
   Misturar as duas é uma segunda aproximação.
4. **O cliente é um indivíduo adulto, não um lar.** Uma conta de supermercado é, na prática,
   de um domicílio, e a Espanha tem cerca de 2,5 pessoas por lar. Modelar isso exigiria
   composição familiar por província, que esta plataforma **não ingere** — a mesma proibição
   que impediu o corte de ciclo de vida do lar na Fase 5. Consequência declarada: a base é uma
   fração dos **adultos**, e não dos **lares**.

Além disso, **sexo e idade são amostrados independentemente**. A 31304 tem idade cruzada
com sexo, e essa correlação real (mulheres dominam as faixas altas) é descartada por
construção. A truncagem em 18 anos acentua isso de leve — as faixas altas, onde a
diferença é maior, passam a pesar mais na base — e continua não sendo modelada.

## 3. Garantias

1. **Escrita atômica.** Nenhum arquivo é observável em estado parcial (temporário no
   mesmo diretório + `os.replace`, com `fsync`).
2. **Forma canônica.** `customers.json` e `_manifest.json` são serializados com
   `ensure_ascii=False, sort_keys=True, indent=2` + quebra final, de modo que o SHA-256
   dependa apenas do conteúdo.
3. **Integridade verificável.** O manifesto declara `sha256`, `bytes` e `records`;
   `validate` recalcula os três a partir do disco.
4. **Reprodutibilidade por seed.** A mesma `(referência, wh, count, seed,
   ingestion_date)` produz `customers.json` **byte a byte idêntico**. Uma única
   instância `random.Random(seed)` é consumida sempre na mesma ordem; nada itera `set`
   nem `dict` reconstruído, então a saída não depende de `PYTHONHASHSEED` (verificado em
   subprocesso, com três valores diferentes).
5. **Sem relógio.** `birth_year` deriva de `ingestion_date`, nunca de `datetime.now()`.
6. **Inventário fechado.** Todo arquivo dentro da partição está declarado no manifesto.
7. **Imutabilidade.** Uma partição com `complete: true` não pode ser reescrita sem
   `--overwrite` explícito.
8. **Marcador de conclusão.** `_SUCCESS` existe se e somente se `complete: true`, e é
   sempre gravado **depois** do manifesto.
9. **Proveniência preservada.** `reference{}` registra a `ingestion_date` de cada insumo
   do Silver; `history[]` acumula cada execução anterior **com a seed que ela usou** —
   sem isso, um `--overwrite` trocaria as pessoas por trás dos mesmos `customer_id` sem
   deixar rastro.
10. **Coerência geoespacial verificada.** `validate` relê a referência e confere, cliente
    a cliente, que o endereço bate campo a campo com a linha de origem e que o município
    pertence à Área Urbana Funcional daquele warehouse.
11. **Nenhum endereço fabricado.** `house_number` é `null` — nunca um número inventado —
    sempre que a fonte não sustenta um número real (ver seção 6).
12. **Totais reconferidos.** `totals.customer_rows`, `municipalities_used`,
    `house_number_null` e `bytes` são recalculados da releitura, e `config.count` tem de
    bater com a contagem real.
13. **`schema_fingerprint` estável.** É a união das chaves observadas. Por isso
    `house_number` está **sempre presente**, mesmo nulo: uma chave opcional faria a
    impressão digital da partição depender da seed.
14. **Referência desconfiada.** Referência ausente, ilegível, com `rows` vazio ou sem
    campo obrigatório reprova a geração — nunca produz clientes enviesados em silêncio.
15. **Nenhum titular de conta abaixo da idade mínima.** Em três camadas independentes: a
    plataforma entrega a distribuição já truncada, `reference_data` recusa uma referência
    cujo cabeçalho falte ou cujas linhas o contradigam, e `validate` recalcula
    `ano(ingestion_date) − birth_year` de **cada cliente pousado**. A idade mínima vem sempre
    da referência, nunca de uma constante do pacote.
16. **O tamanho da base é derivado, não digitado.** Sem `--count`, ele vem de
    `customer_allocation`; com `--count`, o manifesto registra `count_source: "cli"`. Não há
    default: uma referência sem alvo **reprova** em vez de escolher um número.

### Condições não-aditivas

Crescer a base com a **mesma** seed, data e escopo é aditivo — os primeiros N clientes saem
byte a byte iguais. Estas quatro coisas **não** são: elas trocam as pessoas por trás dos
mesmos `customer_id`, exigem `--overwrite`, e obrigam a regerar os pedidos que apontam para
aqueles clientes.

| condição | onde mora |
|---|---|
| `seed` | `config.seed` do manifesto |
| `ingestion_date` | `birth_year = ano(ingestion_date) − idade` |
| `min_customer_age` | `customer_premises_seed`, refletida em `reference.min_customer_age` |
| taxa de penetração ou regra de alocação | `demand_profile_seed` / `customer_premises_seed`, refletidas em `reference.penetration_pct` e `reference.allocation_rule` |

As quatro ficam registradas no manifesto justamente para que duas partições com a mesma
contagem e o mesmo aspecto sejam distinguíveis.

## 4. Obrigações do consumidor

1. **Itere `manifest.files[]`.** `files[].path` é relativo à raiz do snapshot, dois
   níveis acima da partição (`ingestion_date=…/wh=…/`).
2. **Trate a partição como imutável.** Não escreva dentro dela.
3. **`customer_id` é chave da partição, não identidade persistente.** São duas garantias
   diferentes:
   - **Unicidade:** vale em toda a `ingestion_date` — `wh` está embutido no próprio id,
     então `cust_mad1_000042` e `cust_vlc1_000042` nunca colidem.
   - **Estabilidade do referente:** vale só dentro de `(wh, ingestion_date, seed)`. Um
     `--overwrite` com outra seed mantém os mesmos ids e troca as pessoas por trás deles.
     Prova concreta: `birth_year` deriva de `ingestion_date`, logo **a mesma seed em
     outro dia produz outra pessoa sob o mesmo id**.

   Identidade de cliente que atravessa dias não é fornecida nem derivável daqui. Ver
   seção 5.
4. **O endereço é atribuição, não identidade.** Os campos geográficos são uma
   *referência* a uma linha real do Callejero, não parte da identidade do cliente. Duas
   consequências que **não são bug**:
   - `customer_id` nunca deriva de nenhum campo geográfico;
   - dois clientes podem receber o mesmo tramo **e o mesmo `house_number`**, isto é,
     endereço idêntico. Espere repetição; ela não é removida aqui.
5. **Rastreie o tramo por `candidate_index`, não por CEP + rua.** Medido: no escopo das 4
   AUFs (216.594 tramos), `(street_code, postal_code)` produz 105.112 combinações, das
   quais só 28.886 (**13,3%**) são unitárias — a maior cobre 70 tramos. O grão real do
   tramo tem 11 colunas. `candidate_index` aponta a linha exata da referência declarada
   em `manifest.reference`, e é o único caminho de auditoria exato.
6. **`house_number: null` significa "não existe", não "desconhecido".** É qualificado
   pelo campo companheiro `numbering_type` ao lado. Ver seção 6.

## 5. O que a Source não faz

- **Não fala com o Lakehouse.** Sem `duckdb`, sem `boto3`, sem dbt, sem importar nada da
  plataforma. A referência chega como JSON plano.
- **Não busca rede.** Nenhum socket, em nenhum passo.
- **Não fornece identidade persistente de cliente.** Não há merge entre dias, e esta Source
  não produz modelo Silver — quem versiona a identidade é o consumidor. Desde a Fase 2 isso
  existe do lado da plataforma (`silver_customer` e a dimensão SCD2 `DIM_CUSTOMER`), e
  continua sendo responsabilidade dele, não desta Source. A garantia aqui é a mesma de
  sempre: `customer_id` é chave da partição.
- **Não gera Orders, estoque nem entrega.** Fase 1 é estritamente Customers.
- **Não normaliza vocabulário da fonte.** `numbering_type` e `sex_label` carregam os
  rótulos do INE verbatim.
- **Não corrige o Silver.** Onde o dado a montante tem defeito (ver seção 6), esta Source
  se defende de forma explícita e medida, mas não o conserta.

## 6. Escopo e limites da fonte **[fonte]**

Tudo medido contra o Lakehouse real (Callejero `ingestion_date=2026-08-25`, população
`2026-08-26`), no escopo das 4 AUFs.

- **216.594 tramos com CEP** nos 370 municípios das AUFs (`mad1`=128, `bcn1`=133,
  `svq1`=46, `vlc1`=63 municípios). Nenhum município pertence a dois warehouses.
- **3 tramos órfãos** não casam nem com via nem com pseudovia. São excluídos dos
  candidatos e a contagem fica em `excluded_rows.no_street_or_pseudo_match`. Restam
  **216.591 candidatos**.
- **838 tramos são pseudovia** (`street_id = "00000"`), onde o nome vem de `PSEU`.
- **9.024 tramos (~4,2%) têm `numbering_type = "0"`**: o INE declara que a via não tem
  esquema de numeração. Sortear um número aqui fabricaria geografia — a mesma proibição
  do CEP, um nível abaixo. `house_number` fica `null`.
- **14 tramos têm `numbering_type = "2"` e faixa `0000..0000`.** Filtrar só por
  `numbering_type` **não** pega este caso: sortear em `[0,0]` daria número de casa 0, que
  não existe. O 0 é excluído do conjunto amostrável, e faixa sem número da paridade
  declarada também cai para `null`.
- **Qualificadores de número não são carregados.** 4.444 e 4.473 linhas do dataset têm
  qualificador (`A`/`B`/`Z`…) nos extremos da faixa. Um `house_number` "12" para um tramo
  cujo intervalo real é `0012-A..0012-Z` é aproximação.
- **Os dois produtos do INE grafam o mesmo município de forma diferente em 370 de 370
  casos**: o Callejero usa caixa alta com artigo entre parênteses (`"BRUC (EL)"`), a
  tabela 29005 usa caixa mista com artigo posposto (`"Bruc, El"`). Os dois convivem na
  referência com rótulos distintos (`municipality_name` e `municipality_name_callejero`);
  o cliente carrega o da 29005. **A chave comparável é sempre o código, nunca o nome.**
- **Homônimos nacionais: resolvidos a montante, reconferidos aqui.** A tabela RAW 29005 é
  nacional (~8.200 municípios) e traz séries com `Nombre` idêntico para municípios
  homônimos de províncias diferentes. Até 2026-08-27 o join por nome de
  `silver_ine_population_by_municipality` casava as duas, e três municípios das AUFs
  ficavam com duas linhas divergentes:

  | Município | Série correta | Série intrusa |
  |---|---|---|
  | Arroyomolinos (28/015, `mad1`) | `DPOP12967` = 38.075 | `DPOP4729` = 816 (Cáceres) |
  | Molar, El (28/086, `mad1`) | `DPOP13174` = 9.999 | `DPOP19423` = 295 (Tarragona) |
  | Torrent (46/244, `vlc1`) | `DPOP21778` = 90.928 | `DPOP7960` = 182 (Girona) |

  Corrigido no modelo com o **código oficial do INE** (`ine_ambiguous_series_seed`,
  derivado de `VALORES_SERIE/{COD}`), não por heurística de valor. O export apenas
  **verifica** o invariante — uma série por `(município, sexo, ano)` — e **recusa** se ele
  cair, em vez de escolher um valor por conta própria.
- **Os 103 `age_label` da 31304 não formam uma partição.** Junto das 101 idades simples
  convivem dois agregados sobrepostos, `"Total"` e `"85 y más años"`. Somar os 103
  ingenuamente dá **2,03×** o valor correto (medido em Madrid/2022: 27.724.421 contra
  13.650.010, que é exatamente o rótulo `"Total"`), e `"85 años"` colidiria com
  `"85 y más años"` no mesmo balde ao virar inteiro. Ambos são excluídos, e
  `"100 y más años"` — balde terminal legítimo — permanece.
- **`silver_ine_population_series` está duplicada em duas `ingestion_date`** com linhas
  idênticas. O export fixa `max(ingestion_date)`.

## 7. Códigos de saída

| Código | `extract` | `validate` |
|---|---|---|
| 0 | partição completa | validação passou |
| 1 | — | validação reprovou |
| 2 | falha fatal: nada foi gerado, partição inutilizável | — |
| 3 | exceção não tratada (nenhum traceback escapa) | idem |

`validate` **exige `--reference`**, diferente das outras três Sources: a garantia central
— todo cliente mora num endereço real da AUF do seu warehouse — só é verificável relendo
a mesma referência que gerou a partição. Um validador que só confere checksum provaria
integridade, não coerência.

## 8. Versão do contrato

`manifest_version: 1`. Cada Source tem seu próprio contador — são contratos
independentes. A plataforma declara a versão que sabe ler em
`retail_platform.SUPPORTED_MANIFEST_VERSIONS["simulated_oltp"]` e recusa qualquer outra,
em vez de interpretar por adivinhação.
