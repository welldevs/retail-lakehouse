# Source Contract — `ine_population_api`

Contrato entre esta Source e qualquer componente que consuma seus snapshots.
Tudo aqui é imposto por código e coberto por teste, salvo onde marcado como **[fonte]** —
característica da API Tempus3 do INE, fora do controle desta Source.

Mais enxuto que o contrato da Mercadona Catalog Source: sem eixo de armazém, sem
identidade de produto/preço, sem premissa de moeda — nada disso existe aqui. O que os
dois compartilham (escrita atômica, forma canônica, inventário fechado, imutabilidade,
retomada segura) é hygiene genérica de partição, mantida verbatim.

## 1. Identidade do snapshot

Cada execução completa produz um snapshot identificado por duas chaves — **sem** eixo de
armazém, ao contrário da Mercadona: a API Tempus3 devolve todas as províncias e todos os
anos num único payload por `table_id`, então não há um eixo de fetch para promover a
diretório.

| Chave | Valor | Onde vive |
|---|---|---|
| `source` | `ine_population_api` | `_manifest.json` → `source.name` |
| `ingestion_date` | `YYYY-MM-DD` (UTC) | diretório da partição e `partition.ingestion_date` |

```
<root>/ingestion_date=YYYY-MM-DD/
├── tables/table_id=<id>.json
├── _manifest.json
├── _SUCCESS          (só existe quando complete = true)
└── _run.log
```

Blocos de primeiro nível do `_manifest.json`: `manifest_version`, `run_id`,
`started_at_utc`, `finished_at_utc`, `duration_seconds`, `complete`, `source`,
`partition`, `config`, `totals`, `schema_fingerprint`, `files[]`, `failures[]`,
`anomalies[]`, `history[]`.

**Compatibilidade dentro de uma versão de manifesto:** uma lista ausente equivale à lista
vazia. Consumidores devem usar `manifest.get("anomalies", [])`, nunca indexação direta.

## 2. Conteúdo garantido

O snapshot preserva **série temporal de população por província**, tal como a API a
devolve — sem interpretação.

| Campo | Caminho |
|---|---|
| Código da série | `tables/table_id=<id>.json` → `[].COD` |
| Descrição da série (inclui província) | `tables/table_id=<id>.json` → `[].Nombre` |
| Pontos da série (ano, valor) | `tables/table_id=<id>.json` → `[].Data[]` |

**`table_id` é 31304** ("Población por fecha, sexo, edad y provincia"), com dimensões
`Sexo`, `Edad`, `Provincias` — confirmado consultando `GRUPOS_TABLA/31304` e
`DATOS_TABLA/31304` diretamente na API. **A província vem como texto**, não como código
numérico: `Nombre` concatena idade, território e sexo separados por `". "` — ex.:
`"Total. Albacete. Ambos sexos. Población. Número."`.

**Medido contra o payload real** (16.377 séries, 1.547.496 pontos, table_id=31304):

- **A ordem dos três primeiros segmentos NÃO é fixa.** 97,2% das séries seguem
  `"<Idade>. <Território>. <Sexo>."`, mas o bucket etário `"85 y más años"` inteiro
  (156 séries) vem como `"<Sexo>. <Idade>. <Território>."`, e as 309 séries do total
  nacional vêm como `"Total Nacional. <Idade>. <Sexo>."` (ou, só para
  `"100 y más años"`, `"<Idade>. Total Nacional. <Sexo>."`). Extrair por posição fixa
  (ex.: `split_part(..., 2)`) erra o território em ~2,8% das séries. O consumidor deve
  identificar território e sexo por **vocabulário conhecido** (lista fechada de 52
  províncias + o valor `"Total Nacional"`; 3 valores de sexo), não por índice.
- **`"Total Nacional"` não é uma província** — é a agregação do país inteiro, presente
  na mesma tabela junto com as 52 províncias (50 + Ceuta e Melilla).
- **Cada série tem 2 pontos por ano**, não 1: um por `FK_Periodo` (`27` = referência
  30/06, `26` = referência 31/12 — o INE publica duas estimativas de população por
  ano). `(série, Anyo, FK_Periodo)` é único; `(série, Anyo)` sozinho não é.

O Silver (`silver_ine_population_series`) já implementa a extração por vocabulário e o
grão com `fk_periodo`.

**`table_id` 29005** ("Cifras oficiales del padrón por municipio") também confirmado,
consultando `DATOS_TABLA/29005` diretamente na API. Dimensão diferente de 31304: só
**município + sexo**, sem idade. **O município vem como texto, sem código** — nem sequer
código de província, ao contrário do que se poderia supor por analogia com 31304.

**Medido contra uma amostra grande do payload real** (4.509 séries completas de um
payload maior que a rede truncou durante a investigação — ver histórico do projeto):

- **A ordem dos segmentos É fixa aqui**, ao contrário de 31304: 100% da amostra segue
  `"<Município>. <Sexo>. Total habitantes. Personas."` — 4 segmentos sempre, município e
  sexo podem ser extraídos por posição, não por vocabulário.
- **Vocabulário de sexo é diferente do de 31304**: `"Total"` / `"Hombres"` /
  `"Mujeres"`, não `"Ambos sexos"` / `"Hombres"` / `"Mujeres"`.
- **`Secreto: true` não apareceu na amostra** (0 ocorrências) — mas a amostra não cobre
  os ~8.200 municípios inteiros da Espanha, então isso não está provado para o dataset
  completo. Não há exemplo observado do que a fonte coloca em `Valor` quando
  `Secreto: true` — o consumidor não deve presumir que vira `0` ou `null`.
- **Payload sem filtro é grande** (série histórica desde 1996, ~8.200 municípios × 3
  séries de sexo × ~30 anos): truncou no meio da própria investigação desta extensão sob
  a rede daquele ambiente, apesar de `HTTP 200`. O `Fetcher` desta Source já trata corpo
  JSON inválido como erro retryable (`http_client.py`), então uma resposta truncada é
  reexecutada com backoff — mas o `--timeout` default (30s) é curto para um payload deste
  tamanho; quem rodar `--tables 29005` num link lento deve considerar `--timeout` maior.
- **Não há código de município no payload** — mesma limitação de 31304 com província,
  só que mais séria aqui porque nome de município não é chave segura nacionalmente (nomes
  duplicados entre províncias diferentes). Resolver o código é responsabilidade do
  consumidor (ver `scripts/derive_municipality_codes.py` na raiz do projeto, fora desta
  Source).

O Silver (`silver_ine_population_by_municipality`) implementa a extração por posição e
o cruzamento de código.

**Medido contra o payload real e completo** (extração de produção, 40.791 séries, 2.253.624
pontos): 6 séries de `table_id=29005` — 2 municípios (`"Gatova"`, província de Castellón,
e `"Palmerola"`, província de Girona), 3 séries cada (Total/Hombres/Mujeres) — não têm
**nenhum** ponto de dado (`Data: []`), não só `Valor` nulo. Nenhum dos dois municípios cai
dentro do escopo desta plataforma (08/28/41/46). A plataforma (Makefile raiz, DAG) roda
`validate` **sem `--strict`** por causa disso — reprovar toda extração por 2 municípios
fora de escopo não protegeria nada real; a contagem de séries sem valor continua
reportada no output do `validate` de qualquer forma, só deixou de ser fatal.

**Essa mesma extração de produção mediu o payload grande de `table_id=31304` sendo
genuinamente frágil em trânsito.** A primeira tentativa (rede da máquina que rodou esta
extensão, 2026-08-26) baixou ~264 MB e falhou perto do fim —
`JSONDecodeError: Expecting ':' delimiter` no byte 154.057.166 — depois de ~33 min. O
retry automático do `Fetcher` (`http_client.py`, já tratava corpo JSON inválido como erro
retentável antes desta extensão) refez a busca e fechou íntegro na 2ª tentativa (`retry
1/3`, backoff 5s). Total pousado no MinIO: `table_id=31304` (276.503.711 bytes) +
`table_id=29005` (125.350.984 bytes) ≈ 402 MB em 2 arquivos de tabela, 5 objetos no total
com manifesto/log/`_SUCCESS`. Registrado aqui porque o `--timeout` default (30 s, ver README § "extract") é curto demais
pra um payload deste tamanho nessa rede — quem repetir esta
extração num link lento deve considerar `--timeout` maior, não só mais `--max-retries`.

**O JSON é o contrato físico.** Não há schema relacional: a estrutura da resposta da API
é preservada como recebida.

## 3. Garantias

1. **Escrita atômica.** Nenhum arquivo de dados é observável em estado parcial.
2. **Forma canônica.** Todo arquivo é `json.dumps(ensure_ascii=False, sort_keys=True,
   indent=2)` + newline final.
3. **Integridade verificável.** `_manifest.json` declara `sha256`, `bytes` e `records`
   (contagem de séries) de cada arquivo. `validate` recalcula os três a partir do disco.
4. **Inventário fechado.** Todo arquivo dentro da partição está declarado no manifesto.
5. **Imutabilidade.** Uma partição com `complete: true` não pode ser reescrita sem
   `--overwrite` explícito. Um arquivo reaproveitado cujo conteúdo divergiu do manifesto
   anterior é rebaixado da fonte e registrado em `anomalies[]`.
6. **Retomada segura.** Uma partição incompleta pode ser completada reexecutando o mesmo
   comando com o mesmo `--tables`.
7. **Proveniência preservada.** `history[]` acumula o resumo de cada execução anterior.
8. **Detecção de mudança de schema.** `schema_fingerprint` registra o conjunto ordenado
   de chaves de série e de ponto de dado, mais um hash. `validate` recalcula e compara.
9. **Marcador de conclusão.** `_SUCCESS` existe se e somente se `complete: true`.
10. **Identidade persistida antes do fan-out.** Um manifesto preliminar (`complete:
    false`) é gravado assim que a partição é criada, antes de qualquer `table_id` ser
    buscado — não depois de uma etapa de descoberta, porque não há uma aqui.
11. **Forma canônica verificada no reuso.** Um arquivo já presente só é reaproveitado se
    seus bytes forem exatamente a serialização canônica do seu próprio conteúdo.
12. **Anomalias registradas.** `anomalies[]` lista eventos tratados durante a extração.
    `validate` os reporta como aviso, sem reprovar uma partição já corrigida.
13. **Cobertura verificada.** `validate` confere que todo `table_id` em
    `manifest.config.tables` tem arquivo — cobertura é config-driven, não descoberta
    contra uma árvore externa como na Mercadona.
14. **Totais reconferidos.** `totals.series_count`, `totals.data_point_count`,
    `totals.table_files` e `totals.bytes` são recalculados da releitura.
15. **Falha registrada é fatal na validação.** Qualquer entrada em `failures[]` reprova a
    partição.

**Deliberadamente sem equivalente aqui** (existem na Mercadona, não fazem sentido nesta
fonte): `lang` fixo por partição — não há eixo de idioma; identidade de produto/preço;
premissa de moeda.

## 4. Obrigações do consumidor

1. **Itere `manifest.files[]`.** `files[].path` é relativo à raiz do snapshot, não à
   partição — começa em `ingestion_date=…/`. Resolva com
   `os.path.join(partition, "..", entry["path"])` — um nível a menos que a Mercadona,
   porque não há eixo de armazém entre a raiz e a partição.
2. **Trate a partição como imutável.** Não escreva dentro dela.
3. **Cada snapshot já contém a série histórica inteira.** `Data[]` cobre muitos anos por
   série. Um novo `ingestion_date` é um **pull atualizado**, não incremental — não assuma
   que só "linhas novas" aparecem.
4. **Não assuma cadência de atualização.** O INE publica Cifras de Población de forma
   irregular (nominalmente ~1-2x/ano, historicamente às vezes meses entre publicações). Um
   novo `ingestion_date` sem nenhum `Valor` diferente do snapshot anterior é o caso
   esperado, não um sinal de falha.
5. **Extraia a província de `Nombre`, com cuidado.** Não há código de província no
   payload — só o nome por extenso, dentro de uma string com outros campos concatenados
   (ver §2). Não há mapeamento pronto para código INE de província aqui; se precisar de
   um, ele vive na camada que cruza esta fonte com outra (fora do escopo desta Source).
6. **Verifique `schema_fingerprint` entre snapshots** se depender da forma dos campos.

## 5. O que a Source não faz

Deduplicação · modelagem dimensional · normalização · fatos e dimensões · transformação
Silver · Gold/Marts · mapeamento província → outro sistema geográfico · cálculo de
métricas · ingestão em object storage · Spark · dbt · Airflow.

Cada execução é uma **fotografia** da tabela pedida. Interpretação (extrair província,
filtrar por sexo/idade, cruzar com outra fonte) é da camada posterior.

## 6. Escopo e limites da fonte **[fonte]**

- A API Tempus3 (`servicios.ine.es/wstempus`) é pública e não exige autenticação.
- Comportamento de rate-limit **não foi medido** contra esta fonte — ao contrário da
  Mercadona, onde 403 intermitente sob concorrência foi medido e documentado. Esta Source
  não tem fan-out concorrente (uma execução busca um punhado de `table_id`,
  sequencialmente), então não há cenário local que exigiria medir isso ainda.
- `table_id: 31304` (província+idade+sexo) e `table_id: 29005` (município+sexo, sem
  idade) são os confirmados nesta versão do contrato — ver §2 para o formato medido de
  cada um. Outras tabelas do INE podem ser passadas via `--tables`, mas a forma de
  `Nombre` para elas não foi verificada.
- Dado abaixo do nível de município (ex.: seção censitária) não foi localizado nesta
  API (`DATOS_TABLA`/Tempus3) durante a investigação que levou a `table_id: 29005` —
  o que existe (Censo/SDC21) parece ser uma API estruturalmente diferente, não
  verificada, fora do escopo desta Source por ora.
- Dado de município com idade (não só sexo) existe no INE, mas fragmentado numa família
  de dezenas de `table_id` — um por província — não mapeados para as 4 províncias desta
  plataforma; não perseguido nesta versão do contrato (ver ARCHITECTURE.md).

## 7. Códigos de saída

| Código | `extract` | `validate` |
|---|---|---|
| 0 | nenhum `table_id` alvo falhou | aprovado |
| 1 | falha parcial, registrada em `failures[]` | reprovado |
| 2 | falha fatal: partição inutilizável ou uso inválido | — |
| 3 | exceção não tratada | exceção não tratada |

## 8. Versão do contrato

`manifest_version: 1`. Contador independente do da Mercadona — são contratos separados.
`validate` recusa manifesto de versão diferente da suportada pelo código, em vez de
interpretá-lo por adivinhação.
