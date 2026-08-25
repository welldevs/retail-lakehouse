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
- `table_id: 31304` é o único confirmado nesta versão do contrato. Outras tabelas do INE
  (ex.: 9688–9691, com outras dimensões) podem ser passadas via `--tables`, mas a forma de
  `Nombre` para elas não foi verificada.
- Nenhum endpoint conhecido desta fonte expõe dado abaixo do nível de província (ex.:
  município) na tabela usada aqui.

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
