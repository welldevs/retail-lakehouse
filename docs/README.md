# Documentos de referência

## Fontes externas usadas como **benchmark**, não ingeridas

Um benchmark não é uma fonte desta plataforma. Não tem Source, não tem RAW, não tem
partição, e nenhum número dele entra num fato. Ele é usado **apenas como alvo de
distribuição**, e o CONTRACT da Source de Orders trata `benchmark` como uma terceira
natureza ao lado de `observed` e `synthetic`.

### Informe del Consumo Alimentario en España 2025 — MAPA

| | |
|---|---|
| arquivo | `docs/Informe comsumo 2025_.pdf` — **não versionado** (`docs/*.pdf` no `.gitignore`) |
| origem | Ministerio de Agricultura, Pesca y Alimentación (gob.es) |
| URL | https://www.mapa.gob.es/es/alimentacion/temas/consumo-tendencias/panel-de-consumo-alimentario/ultimos-datos |
| sha256 | `b8c8abb6230ceda48db36f9fd59f8f99f268d332d49af3a3cdd73b430c4f0856` |
| tamanho | 30.086.039 bytes · 645 páginas |
| baixado em | 2026-08-31 |

**Por que não está versionado.** São 29 MB de binário, e o histórico do git é permanente. O
que o repositório precisa preservar é a **rastreabilidade**, não o arquivo: cada uma das 64
linhas de `platform/dbt/seeds/mapa_2025_benchmark_seed.csv` cita a seção do informe de onde
o número veio, e `provenance` distingue `informe_table` (tabela-cabeçalho da seção) de
`informe_prose` (número citado no texto) e de `derived` (calculado a partir de dois números
publicados, com a derivação escrita na própria linha).

**A URL aponta para "últimos datos" e vai mudar.** Quando o MAPA publicar o informe de 2026,
este link passará a servir o arquivo novo. O `sha256` acima é o que identifica a edição
usada; se ele deixar de bater, o benchmark em vigor não é o que os seeds descrevem, e a
versão do modelo (`mapa_2025_v1`) precisa mudar junto.

**Achado de proveniência.** A folha de rosto do PDF diz *"Informe del consumo alimentario en
España 2024"*, enquanto o corpo inteiro reporta o ano **2025** ("A cierre del año 2025…",
"frente a los 26.823,4 millones del año 2024"). É resíduo de copiar-colar da edição anterior
na página de créditos. Os seeds citam o **corpo**. A discrepância fica registrada aqui e no
CONTRACT em vez de ser silenciosamente resolvida.

**Como extrair de novo.** `pdftotext -layout` preserva o alinhamento das tabelas-cabeçalho de
cada seção, que é de onde saem `Parte de mercado volumen (%)`, `Parte de mercado valor (%)` e
`Precio medio (€/kg)`. Os gráficos mensais e de canal são **imagens**: só os rótulos dos
eixos saem no texto, e é por isso que não há perfil sazonal por categoria.

## Evidências geradas

Nenhum número destas páginas é escrito à mão. Refaça-as em vez de editá-las.

| diretório | gerado por |
|---|---|
| `docs/demand-evidence/` | `make demand-reality-check` |
| `docs/warehouse-evidence/` | `make warehouse-evidence` |
| `docs/stream-evidence/` | `make stream-evidence` |

`docs/demand-evidence/before_mapa_2025_v1.json` é a exceção: não é gerado a cada execução, é
um **snapshot congelado** do mix antes da calibração. Depois de `orders-refresh-all
--overwrite`, o estado anterior não existe mais em lugar nenhum — sem este arquivo, o reality
check só consegue dizer "é assim hoje", que é metade da pergunta.
