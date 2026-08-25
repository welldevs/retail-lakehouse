# Source Contract — `ine_callejero`

Contrato entre esta Source e qualquer componente que consuma seus snapshots.
Tudo aqui é imposto por código e coberto por teste, salvo onde marcado como **[fonte]** —
característica do dataset do INE, fora do controle desta Source.

**Diferença estrutural das outras duas Sources: não há API.** O Callejero só é
distribuído para download manual no site do INE. `extract` aqui não faz nenhuma
requisição de rede — incorpora arquivos que já foram baixados e colocados num
diretório local (`--in`). O verbo é mantido por uniformidade de interface com as
outras Sources; o significado muda. O que as três compartilham (escrita atômica,
inventário fechado, imutabilidade, retomada segura) é hygiene genérica de partição,
mantida verbatim.

## 1. Identidade do snapshot

| Chave | Valor | Onde vive |
|---|---|---|
| `source` | `ine_callejero` | `_manifest.json` → `source.name` |
| `ingestion_date` | `YYYY-MM-DD` (UTC) | diretório da partição e `partition.ingestion_date` |

Sem segundo eixo de partição — nem por warehouse, nem por província: o artefato é um
intake do dataset oficial inteiro, não um recorte.

```
<root>/ingestion_date=YYYY-MM-DD/
├── provinces/province=<código>/<nome ORIGINAL do arquivo baixado>
├── _manifest.json
├── _SUCCESS          (só existe quando complete = true)
└── _run.log
```

`provinces/province=<código>/` é estrutura de DIRETÓRIO dentro da partição —
equivalente ao `tables/` da source de população — não um eixo formal do lado da
plataforma. O nome do arquivo landado é o mesmo do download (ex.:
`VIAS.P28.D260630.G260702`), não um nome genérico: preserva proveniência (data e
geração do INE) sem precisar de outro mecanismo.

Blocos de primeiro nível do `_manifest.json`: `manifest_version`, `run_id`,
`started_at_utc`, `finished_at_utc`, `duration_seconds`, `complete`, `source`,
`partition`, `config`, `totals`, `files[]`, `failures[]`, `history[]`.

## 2. Conteúdo garantido

**Os downloads do Callejero trazem 5 arquivos por província (`SECC`, `UP`, `VIAS`,
`TRAM`, `PSEU`); esta Source incorpora apenas 4: `SECC`, `UP`, `VIAS` e `PSEU`.**
`TRAM` existe no download e é ignorado deliberadamente — nem chega a ser candidato em
`discover_input_files` (ver `partition.INPUT_FILE`, que só reconhece os outros 4). Fica
para uma versão futura, se a granularidade de número/trecho de endereço vier a ser
necessária.

**Encoding real: ISO-8859-1 (Latin-1)**, linhas terminadas em CRLF — medido com `file`
contra os arquivos reais, não presumido. **RAW preserva os bytes originais**: esta
Source não decodifica, não converte encoding, não normaliza fim de linha. O SHA-256
registrado é sobre o arquivo exatamente como o INE o distribui.

**Nenhum dos 4 arquivos tem código postal (CEP) nem um conceito oficial de "bairro"
urbano** — medido inspecionando o conteúdo real, não assumido por ausência de busca. O
mais próximo de "bairro" é o nome de unidade populacional (núcleo) do `UP`, que é
sub-municipal mas não é uma divisão administrativa oficial de bairro.

**Layout medido de cada arquivo** (posições 0-indexadas, `[início:fim)`), contra os
arquivos reais das 4 províncias dos warehouses — não a documentação oficial do INE, que
não acompanhou o download:

| Dataset | Largura | Campos |
|---|---|---|
| `SECC` | 11 chars (10 de código + 1 espaço à direita) | `[0:2]` província · `[2:5]` município · `[5:7]` distrito · `[7:10]` seção |
| `VIAS` | 132 chars | `[0:10]` código (província+município+id via) · `[10:38]` nome curto · `[38:46]` data · `[47:52]` id via · `[52:57]` tipo de via (CALLE/PLAZA/CMNO/PRAJE...) · `[57:107]` nome completo · `[107:132]` nome curto alternativo |
| `PSEU` | 127 chars | `[0:10]` código (província+município+id) · `[10:63]` nome · `[63:71]` data · `[72:77]` id · `[77:127]` nome completo — sem campo de tipo (pseudovias não são classificadas por tipo de via) |
| `UP` | 604 chars | `[0:12]` código (província+município(3)+sufixo(7), sufixo `0000000` = linha agregada do MUNICÍPIO) · `[15:23]` data · `[94:314]` nome do MUNICÍPIO · `[459:529]` nome da UNIDADE POPULACIONAL/núcleo (ex. `*DISEMINADO*` para população dispersa). Campos redundantes em outras larguras (70/50/25 chars, mais duas repetições) existem mas não são extraídos nesta versão — são formas abreviadas das mesmas duas informações, confirmado comparando o conteúdo, não assumido. |

Validado contra endereços reais: "Alcalá" no município 079 (Madrid), "Gran Vía" em 3
municípios diferentes, "Abrera"/"Can Vilalba"/"Sant Miquel" (Barcelona), municípios
079/019/091/250 = Madrid/Barcelona/Sevilla/València — os mesmos códigos já usados em
`warehouse_province_map` (seed da plataforma, fonte independente).

**O texto de largura fixa é o contrato físico.** Não há schema relacional; a Source não
parseia campos — só copia o arquivo e registra fatos estruturais (contagem de linhas,
largura dominante) para a revalidação. A tabela acima documenta o que foi medido para
orientar o consumidor (Silver); não é imposta nem verificada pelo código desta Source.

## 3. Garantias

1. **Escrita atômica.** Nenhum arquivo é observável em estado parcial.
2. **Bytes preservados.** O arquivo landado é idêntico, byte a byte, ao arquivo em
   `--in`. Nenhuma reserialização.
3. **Integridade verificável.** `_manifest.json` declara `sha256`, `bytes`,
   `line_count` e `dominant_line_width` de cada arquivo. `validate` recalcula os
   quatro a partir do disco.
4. **Inventário fechado.** Todo arquivo dentro da partição está declarado no manifesto.
5. **Imutabilidade.** Uma partição com `complete: true` não pode ser reescrita sem
   `--overwrite` explícito.
6. **Divergência exige decisão explícita.** Um arquivo já landado (numa partição
   incompleta, sendo retomada) cujo conteúdo diverge do arquivo atual em `--in` não é
   silenciosamente sobrescrito nem silenciosamente mantido — vira falha registrada,
   resolvida só com `--overwrite`.
7. **Retomada segura.** Uma partição incompleta pode ser completada reexecutando o
   mesmo comando, com `--in` contendo os arquivos que faltavam.
8. **Proveniência preservada.** `history[]` acumula o resumo de cada execução anterior;
   o nome original do arquivo (data e geração do INE) é preservado no path landado.
9. **Marcador de conclusão.** `_SUCCESS` existe se e somente se `complete: true`.
10. **Identidade persistida antes do fan-out.** Um manifesto preliminar (`complete:
    false`) é gravado assim que a partição é criada, antes de qualquer arquivo ser
    copiado.
11. **Cobertura verificada.** `validate` confere que toda combinação (província,
    dataset) em `manifest.config` tem arquivo landado.
12. **Totais reconferidos.** `totals.files_landed` e `totals.bytes` são recalculados da
    releitura.
13. **Falha registrada é fatal na validação.** Qualquer entrada em `failures[]` reprova
    a partição.
14. **Mudança de layout é aviso, não é corrigida.** Uma largura de linha dominante
    diferente da registrada no manifesto vira aviso em `validate` (fatal só com
    `--strict`) — sinal para o consumidor de que o INE pode ter mudado o formato; esta
    Source não tenta adivinhar nem se adaptar.

**Deliberadamente sem equivalente aqui** (existem nas outras Sources, não fazem
sentido nesta): forma canônica de reserialização (não há JSON aqui para reserializar);
`schema_fingerprint` de chaves nomeadas (texto de largura fixa não tem chaves);
detecção de "arquivo fora da forma canônica" (não há forma além da original).

## 4. Obrigações do consumidor

1. **Itere `manifest.files[]`.** `files[].path` é relativo à raiz do snapshot,
   começando em `ingestion_date=…/`.
2. **Trate a partição como imutável.** Não escreva dentro dela.
3. **Parseie os campos você mesmo, usando a tabela da seção 2 como ponto de partida —
   e revalide contra uma amostra real antes de depender de um offset.** Esta Source não
   garante que o layout medido aqui continua válido para uma `ingestion_date` futura;
   ela só garante os bytes que copiou.
4. **Não assuma cadência de atualização.** O Callejero é publicado semestralmente pelo
   INE, sem garantia de conteúdo diferente entre publicações.
5. **`warehouse → província/município` não vem desta Source.** Essa relação é decisão
   da plataforma, não do INE — vive em `warehouse_province_map` (seed dbt), consumida
   via JOIN no Silver/Gold. Esta Source não sabe que warehouses existem.

## 5. O que a Source não faz

Parsing de campo · deduplicação · modelagem dimensional · normalização · fatos e
dimensões · transformação Silver · Gold/Marts · mapeamento warehouse → província ·
cálculo de área de atendimento/logística · ingestão de `TRAM` · ingestão em object
storage · Spark · dbt · Airflow.

Cada execução incorpora uma **cópia fiel** dos arquivos pedidos. Interpretação
(extrair rua, seção, cruzar com warehouse) é da camada posterior.

## 6. Escopo e limites da fonte **[fonte]**

- O Callejero é publicado pelo INE sem API — só download manual, semestral.
- Layout de arquivo **não documentado oficialmente no download** (sem "Diseño de
  Registro" anexado); a tabela da seção 2 foi reconstruída por evidência.
- `TRAM` não foi inspecionado por esta Source — nem a largura, nem o conteúdo. Nenhuma
  afirmação é feita sobre ele além de que existe no download.
- Nenhum dos 4 arquivos incorporados contém código postal nem densidade populacional.

## 7. Códigos de saída

| Código | `extract` | `validate` |
|---|---|---|
| 0 | toda combinação (província, dataset) alvo foi landada | aprovado |
| 1 | falha parcial, registrada em `failures[]` | reprovado |
| 2 | falha fatal: partição inutilizável ou uso inválido | — |
| 3 | exceção não tratada | exceção não tratada |

## 8. Versão do contrato

`manifest_version: 1`. Contador independente das outras duas Sources — são contratos
separados. `validate` recusa manifesto de versão diferente da suportada pelo código.
