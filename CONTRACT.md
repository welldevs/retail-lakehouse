# Source Contract — `mercadona_catalog_api`

Contrato entre esta Source e qualquer componente que consuma seus snapshots.
Tudo aqui é imposto por código e coberto por teste, salvo onde marcado como **[fonte]** —
característica da API de origem, fora do controle desta Source.

## 1. Identidade do snapshot

Cada execução completa produz um snapshot identificado por três chaves:

| Chave | Valor | Onde vive |
|---|---|---|
| `source` | `mercadona_catalog_api` | `_manifest.json` → `source.name` |
| `ingestion_date` | `YYYY-MM-DD` (UTC) | diretório da partição e `partition.ingestion_date` |
| `warehouse` | ex.: `mad1` | diretório da partição e `partition.warehouse` |

```
<root>/ingestion_date=YYYY-MM-DD/wh=<warehouse>/
├── categories/categories.json
├── catalog/category_id=<id>.json
├── _manifest.json
├── _SUCCESS          (só existe quando complete = true)
└── _run.log
```

Blocos de primeiro nível do `_manifest.json`: `manifest_version`, `run_id`,
`started_at_utc`, `finished_at_utc`, `duration_seconds`, `complete`, `source`,
`partition`, `config`, `totals`, `schema_fingerprint`, `files[]`, `failures[]`,
`anomalies[]`, `history[]`.

**Compatibilidade dentro de uma versão de manifesto:** uma lista ausente equivale à lista
vazia. Snapshots gravados antes da introdução de `anomalies[]` não têm o campo, e devem ser
lidos como "nenhuma anomalia registrada". Consumidores devem usar
`manifest.get("anomalies", [])`, nunca indexação direta.

## 2. Conteúdo garantido

O snapshot preserva **category**, **product**, **price**, **source metadata** e
**ingestion metadata**.

| Campo | Caminho |
|---|---|
| Categoria nível 1 (árvore) | `categories/categories.json` → `results[].name` |
| Categoria nível 2 (árvore) | `categories/categories.json` → `results[].categories[].name` |
| Categoria nível 2 (catálogo) | `catalog/category_id=<id>.json` → `name` |
| Categoria nível 1 (catálogo) | `.categories[].products[].categories[0].name` |
| Subgrupo | `.categories[].name` |
| Produto | `.categories[].products[].display_name` |
| Preço | `.categories[].products[].price_instructions.unit_price` |

**O JSON é o contrato físico.** Não há schema relacional: a estrutura da resposta da API é
preservada como recebida, e a Source não define tipos, chaves primárias ou normalização.

## 3. Garantias

1. **Escrita atômica.** Nenhum arquivo de dados é observável em estado parcial. Toda
   gravação usa arquivo temporário + `os.replace`, com `fsync` antes da troca.
2. **Forma canônica.** Todo arquivo é `json.dumps(ensure_ascii=False, sort_keys=True,
   indent=2)` + newline final. O mesmo conteúdo produz sempre os mesmos bytes e o mesmo
   SHA-256, independentemente da ordem em que a API devolveu as chaves.
3. **Integridade verificável.** `_manifest.json` declara `sha256`, `bytes` e `records` de
   cada arquivo. `validate` recalcula os três a partir do disco.
4. **Inventário fechado.** Todo arquivo dentro da partição, em qualquer profundidade,
   está declarado no manifesto. A extração varre o diretório ao final e declara arquivos
   de catálogo válidos que a execução não visitou; `validate` varre recursivamente e trata
   arquivo não declarado como erro.
5. **Imutabilidade.** Uma partição com `complete: true` não pode ser reescrita sem
   `--overwrite` explícito — inclusive quando o `_manifest.json` foi removido e só o
   `_SUCCESS` restou. Um arquivo reaproveitado cujo conteúdo divergiu do manifesto anterior
   é rebaixado da fonte e registrado em `anomalies[]`, nunca promovido a nova verdade.
6. **`lang` fixo por partição.** Reabrir uma partição com outro idioma é recusado (salvo
   `--overwrite`, que reescreve a partição inteira), o que torna impossível uma partição
   de idioma misto.
7. **Retomada segura.** Uma partição incompleta pode ser completada reexecutando o mesmo
   comando. Arquivo ilegível, com forma inesperada ou fora da forma canônica é rebaixado da
   fonte, não propagado.
8. **Proveniência preservada.** `history[]` acumula o resumo de cada execução anterior
   sobre a partição: `run_id`, janela, requisições, retries e totais.
9. **Detecção de mudança de schema.** `schema_fingerprint` registra o conjunto ordenado de
   chaves de `product` e de `price_instructions` mais um hash. `validate` recalcula e
   compara.
10. **Marcador de conclusão.** `_SUCCESS` existe se e somente se `complete: true`. A
    incoerência entre os dois é erro de validação.
11. **Identidade persistida antes do fan-out.** Um manifesto preliminar
    (`complete: false`) é gravado logo após a árvore de categorias. Uma interrupção no meio
    da extração deixa a partição com identidade em disco — `source.lang`, `partition`,
    `run_id` — de modo que as garantias 5 e 6 continuam valendo na retomada.
12. **Forma canônica verificada no reuso.** Um arquivo já presente só é reaproveitado se
    seus bytes forem exatamente a serialização canônica do seu próprio conteúdo. Um arquivo
    com o mesmo valor JSON mas outra grafia é rebaixado da fonte, para que o SHA-256
    declarado seja sempre o digest canônico.
13. **Anomalias registradas.** `anomalies[]` lista eventos tratados durante a extração —
    arquivo não canônico, conteúdo divergente do manifesto anterior, arquivo ilegível não
    visitado. `validate` os reporta como aviso, sem reprovar uma partição já corrigida.
14. **Cobertura verificada.** `validate` confere que toda categoria de nível 2 da árvore
    tem arquivo de catálogo, exceto em partição parcial (`--limit`), onde vira aviso.
15. **Totais reconferidos.** `totals.product_rows`, `totals.unique_product_ids`,
    `totals.catalog_files` e `totals.bytes` são recalculados da releitura, e `records` e
    `bytes` são reconferidos arquivo a arquivo — inclusive o `records` da árvore.
16. **Falha registrada é fatal na validação.** Qualquer entrada em `failures[]` reprova a
    partição.

## 4. Obrigações do consumidor

1. **Itere `manifest.files[]`.** Não faça `glob` no diretório: o manifesto é o inventário
   autoritativo. **`files[].path` é relativo à raiz do snapshot, não à partição** — ou
   seja, começa em `ingestion_date=…/wh=…/`. Para resolvê-lo:
   `os.path.join(partition, "..", "..", entry["path"])`. A garantia 4 torna `glob` e
   manifesto equivalentes numa partição validada; a regra existe para partições que você
   não validou.
2. **Trate a partição como imutável.** Não escreva dentro dela.
3. **Carregue `ingestion_date` e `warehouse` como colunas.** Nenhum payload contém o
   armazém: essa identidade existe apenas no caminho e no manifesto. Ingerir o conteúdo
   sem o caminho perde o armazém de forma irrecuperável.
4. **Leia `unit_price` como string.** É string em 100% dos registros. Converter para
   `float` perde precisão decimal em moeda.
5. **Distinga `source_product_id` de identidade de negócio.** São duas coisas, e a fonte
   só entrega a primeira.
   - **`source_product_id`** é `product.id` (string). É a chave **da fonte** e a única
     disponível para join — use-a. Dentro de um snapshot ela é consistente: as aparições
     repetidas do mesmo id diferem apenas no array `categories[]`, que reflete o arquivo de
     onde a linha foi lida, e nunca em preço ou nome.
   - **Identidade de negócio** — "o mesmo item comercial ao longo do tempo" — **não é
     fornecida nem derivável** desta fonte. Sem `ean`/`gtin`/`barcode` (§6), dois fatos a
     tornam indecidível: o mesmo `display_name` aparece sob ids diferentes no mesmo
     snapshot, com preços diferentes; e um id pode ser substituído por outro entre
     snapshots consecutivos com nome, categoria e subgrupo idênticos **[fonte]**.
     Resolvê-la é do consumidor, com chave externa ou regra de casamento declarada como
     premissa.
6. **Não assuma moeda a partir do payload** — a API não a declara **[fonte]**.
7. **Verifique `schema_fingerprint` entre snapshots** se depender da forma dos campos.
8. **Espere repetição de produto.** Um produto pode aparecer em mais de uma categoria e em
   mais de um subgrupo. É semântica da fonte **[fonte]** e não é removida aqui.

## 5. O que a Source não faz

Deduplicação · modelagem dimensional · normalização · fatos e dimensões · transformação
Silver · Gold/Marts · enriquecimento · histórico analítico · cálculo de métricas · SCD ·
detecção de variação de preço · ingestão em Snowflake · envio para S3 · Spark · dbt ·
Airflow · Kafka.

Cada execução é uma **fotografia**. Interpretação temporal é da camada posterior. Os campos
`unit_price`, `reference_price`, `bulk_price`, `previous_unit_price`, `price_decreased` e
`tax_percentage` são preservados no payload sem interpretação.

## 6. Escopo e limites da fonte **[fonte]**

- Um snapshot cobre **um armazém**. `complete: true` significa "todas as categorias de
  nível 2 deste `(ingestion_date, wh)` foram gravadas", não "catálogo completo da rede".
- Apenas ids de categoria de **nível 2** são acessíveis em `/api/categories/{id}/`; ids de
  nível 1 respondem `404`/`410`.
- Não há chave cross-source: `ean`, `gtin`, `barcode`, `brand` e `origin` não existem nos
  payloads deste endpoint. `product.id` é a **chave da fonte**: identifica o registro dentro
  do snapshot, e **não** carrega garantia de estabilidade temporal — a fonte pode reemitir o
  mesmo item comercial sob outro id, e não sinaliza quando o faz. `price_decreased` e
  `previous_unit_price` não cobrem esse caso: são atributos do id novo, que não conhece o
  antigo. Ver §4.5.
- Nenhum endpoint conhecido expõe venda, pedido ou estoque. Esta Source não tem fato
  transacional.
- `robots.txt` de `tienda.mercadona.es` declara `Disallow: /api`, e a Source envia um
  User-Agent de navegador. Ver README, seção "Restrições da fonte".

## 7. Códigos de saída

| Código | `extract` | `validate` |
|---|---|---|
| 0 | nenhuma categoria alvo falhou | aprovado |
| 1 | falha parcial, registrada em `failures[]` | reprovado |
| 2 | falha fatal: partição inutilizável ou uso inválido | — |
| 3 | exceção não tratada | exceção não tratada |

## 8. Versão do contrato

`manifest_version: 2`. `validate` recusa manifesto de versão diferente da suportada pelo
código, em vez de interpretá-lo por adivinhação.
