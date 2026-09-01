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
`informe_prose` (número citado no texto), `informe_chart` (rótulo impresso num gráfico) e
`derived` (calculado a partir de dois números publicados, com a derivação escrita na
própria linha). Os seeds de coorte citam a **página do PDF**, e não a seção, porque é a
página que se abre para reconferir um rótulo de gráfico.

**A URL aponta para "últimos datos" e vai mudar.** Quando o MAPA publicar o informe de 2026,
este link passará a servir o arquivo novo. O `sha256` acima é o que identifica a edição
usada; se ele deixar de bater, o benchmark em vigor não é o que os seeds descrevem, e a
versão do modelo (`mapa_2025_v2`) precisa mudar junto.

**Achado de proveniência.** A folha de rosto do PDF diz *"Informe del consumo alimentario en
España 2024"*, enquanto o corpo inteiro reporta o ano **2025** ("A cierre del año 2025…",
"frente a los 26.823,4 millones del año 2024"). É resíduo de copiar-colar da edição anterior
na página de créditos. Os seeds citam o **corpo**. A discrepância fica registrada aqui e no
CONTRACT em vez de ser silenciosamente resolvida.

**Como extrair de novo.** `pdftotext -layout` preserva o alinhamento das tabelas-cabeçalho de
cada seção, que é de onde saem `Parte de mercado volumen (%)`, `Parte de mercado valor (%)` e
`Precio medio (€/kg)`. Os gráficos mensais e de canal são **imagens**: só os rótulos dos
eixos saem no texto, e é por isso que não há perfil sazonal por categoria.

**Os blocos `Demográficos` estão em dois formatos, e o segundo exige ler a página.** Dezessete
seções trazem uma tabela compacta que o `pdftotext` recupera inteira; as demais trazem
gráficos de barras. Esses gráficos **carregam rótulo numérico impresso** — a página 158 mostra
`8,89 / 2,63 · 30,33 / 18,19 · 31,34 / 34,55 · 29,44 / 44,63` — então lê-los é extração, e não
estimativa. Foram lidas ~45 páginas para cobrir os 39 grupos pesáveis nas duas dimensões de
coorte.

**Dois checksums independentes conferem cada leitura**, e são a razão de a extração manual ser
aceitável:

1. as quatro faixas etárias de **volume** somam 100,00;
2. as de **população** somam `8,89 + 30,33 + 31,34 + 29,44 = 100,00`, e esses quatro números
   se repetem em **toda** seção, porque são o universo e não uma medição da categoria.

Um dígito mal lido quebra uma das duas somas, e `demand_profile.load_cohort_age` reprova. O
seed de região não tem soma para fechar — traz 4 das 17 comunidades — e por isso a conferência
dele é a constância do share de população, verificada em
`test_share_de_populacao_e_o_mesmo_em_todo_grupo`.

**Duas discrepâncias da própria fonte**, registradas em vez de aparadas: a página 206 publica
`30,5 / 31,7 / 29,0` de população onde todas as outras publicam `30,3 / 31,3 / 29,4`, e a
página 84 rotula a Comunidad de Madrid com `13,78` onde as demais rotulam `13,86`. As duas
estão na coluna `note` da linha correspondente, e a tolerância dos checksums é larga o
bastante para admiti-las e estreita o bastante para pegar um dígito trocado.

## Evidências geradas

Nenhum número destas páginas é escrito à mão. Refaça-as em vez de editá-las.

| diretório | gerado por |
|---|---|
| `docs/demand-evidence/` | `make demand-reality-check` |
| `docs/warehouse-evidence/` | `make warehouse-evidence` |
| `docs/stream-evidence/` | `make stream-evidence` |

Os `before_*.json` de `docs/demand-evidence/` são a exceção: não são gerados a cada
execução, são **snapshots congelados**. Depois de `orders-refresh-all --overwrite` o estado
anterior não existe mais em lugar nenhum — sem eles, o reality check só consegue dizer "é
assim hoje", que é metade da pergunta.

| snapshot | o que congela |
|---|---|
| `before_mapa_2025_v1.json` | o mix **uniforme**, antes de qualquer calibração |
| `before_mapa_2025_v2.json` | o mix **calibrado no agregado**, antes da camada de coorte |

O padrão de `--before` é o **v2**, o estado imediatamente anterior. Usar o v1 como padrão
faria a queda de receita da correção de preço da Fase 4 ser lida como se fosse da Fase 5:
duas fases somadas numa coluna só. O v1 continua no disco e é citado no texto da página.

O snapshot v2 registra `cohorts: null` — a janela que ele congela é anterior ao carimbo
`buyer_age_band` existir. `null`, e não um objeto vazio: vazio seria indistinguível de "medi
e não havia nada".
