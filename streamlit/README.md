# Painel estratégico sobre o MART

Bancada de conferência dos indicadores **antes** de reconstruí-los no Power BI.

```bash
make dashboard            # sobe o painel em http://localhost:8501
make dashboard-contract   # regenera CONTRACT.md a partir de indicators.py
```

Se for a primeira vez: `make dashboard-venv` instala `streamlit`, `pandas`, `pyarrow` e
`altair` no venv da plataforma. Eles ficam em `[project.optional-dependencies]` de
`platform/pyproject.toml`, **fora** de `dependencies` — o `infra/Dockerfile.airflow` instala
exatamente aquela lista, e ~150 MB de UI não têm o que fazer numa imagem que não renderiza
dashboard nenhum.

## O que tem aqui

| Arquivo | Papel |
|---|---|
| `indicators.py` | **A fonte única.** SQL e explicação de cada indicador, no mesmo lugar |
| `CONTRACT.md` | **Gerado** de `indicators.py`. É o documento de conferência |
| `contract.py` | O gerador. Não conecta em nada — o CONTRACT é revisável sem credencial |
| `connection.py` | Sessão `RETAIL_READER`, com `use secondary roles none` |
| `app.py` | A interface |

## Três decisões que o painel toma

**Veste `RETAIL_READER`, e prova isso na tela.** O papel de BI lê MART e mais nada. O painel
roda uma sonda ao vivo que confirma a recusa em `GOLD` e `STAGE` — um painel que afirma
respeitar um limite sem demonstrar está pedindo confiança. É a primeira vez que este papel é
vestido por um consumidor de verdade; a carga e o dbt já vestiam os outros dois.

**Lê ao vivo, com o relógio à mostra.** O cache tem TTL de 60 s e há um botão que o limpa. A
barra lateral mostra a hora da leitura e a contagem de linhas e a janela de **cada** mart,
para que uma carga nova apareça como *mudança de base* e não como número diferente sem
explicação. Rode `make warehouse-refresh` com o painel aberto e clique em **Reler o destino
agora**.

**As armadilhas não ficam em rodapé.** Cada indicador carrega as suas, vindas do mesmo módulo
que carrega o SQL — então o aviso não pode envelhecer em relação à consulta.

## O que o painel não exibe

Está na aba *Fora de alcance* e no `CONTRACT.md`, com o gatilho de cada item: margem,
estoque, recompra/LTV/coorte, rota, penetração de mercado, tendência.

A lacuna mais acionável: **nenhum mart junta cliente com pedido.** O elo existe em
`FACT_ORDER.customer_sk`, no GOLD, que `RETAIL_READER` não alcança por desenho. Fechar isso
não exige fonte nova — exige um mart novo com grão de cliente e medidas de pedido.
