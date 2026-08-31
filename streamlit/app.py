"""Painel estrategico sobre o MART do Snowflake — para avaliar os indicadores antes do Power BI.

O QUE ESTE PAINEL E, e o que ele nao e.

E uma BANCADA DE CONFERENCIA. Cada indicador aparece com a pergunta que responde, o grao da
fonte, se o dado e observado ou sintetico, e — sobretudo — as ARMADILHAS de reconstrui-lo em
outra ferramenta. O objetivo declarado e que quem for montar isto no Power BI ja saiba onde a
medida obvia produz um numero plausivel e errado, que e a unica classe de erro que nenhum
teste pega.

Nao e camada de aplicacao, nao guarda estado e nao escreve nada. Le o MART e mostra.

TRES DECISOES QUE MERECEM ESTAR NO TOPO DO ARQUIVO:

  1. VESTE `RETAIL_READER`, e prova isso na tela. O papel de BI le MART e mais nada; o painel
     roda uma sonda ao vivo que confirma a recusa em GOLD e STAGE. Um painel que afirma
     respeitar um limite sem demonstrar esta pedindo confianca.

  2. LE AO VIVO, com o relogio a mostra. O cache tem TTL curto e ha um botao que o limpa. O
     painel mostra a hora da leitura e a contagem de linhas de cada mart, para que uma carga
     nova apareca como MUDANCA DE BASE e nao como numero diferente sem explicacao.

  3. AS ARMADILHAS NAO FICAM EM RODAPE. Cada indicador carrega as suas, vindas do mesmo
     modulo que carrega o SQL — entao nao ha como o aviso envelhecer em relacao a consulta.
"""

from __future__ import annotations

import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import indicators as I  # noqa: E402
from connection import (  # noqa: E402
    DATABASE,
    DashboardError,
    identity,
    open_session,
    probe_isolation,
    run,
)

TTL = int(os.environ.get("RETAIL_DASHBOARD_TTL", "60"))

st.set_page_config(page_title="Retail Lakehouse — MART", page_icon="📦", layout="wide")


# ------------------------------------------------------------------------------------
# Leitura
# ------------------------------------------------------------------------------------
# `st.cache_resource` para a CONEXAO (uma por sessao) e `st.cache_data` para o DADO (TTL
# curto). Sao coisas diferentes: reusar a conexao e economia; reusar o dado por muito tempo
# seria mentir sobre o que esta no destino agora.

@st.cache_resource(show_spinner=False)
def _sessao():
    return open_session()


@st.cache_data(ttl=TTL, show_spinner=False)
def consulta(sql: str, params: dict | None = None) -> pd.DataFrame:
    return run(_sessao(), sql, params)


def numerico(df: pd.DataFrame, colunas) -> pd.DataFrame:
    """Decimal do Snowflake chega como object; Altair precisa de float."""
    saida = df.copy()
    for coluna in colunas:
        if coluna in saida.columns:
            saida[coluna] = pd.to_numeric(saida[coluna], errors="coerce")
    return saida


def armadilhas(ind: I.Indicador) -> None:
    """As ressalvas do indicador, vindas do MESMO modulo que carrega o SQL."""
    with st.expander(f"Grao, tipo e armadilhas — {ind.titulo}", expanded=False):
        st.markdown(
            f"**Pergunta que responde.** {ind.pergunta}\n\n"
            f"**Grao da fonte.** `{ind.grao}`\n\n"
            f"**Tipo do dado.** {ind.tipo}\n\n"
            f"**Marts.** {', '.join(f'`{m}`' for m in ind.marts)}"
        )
        if ind.armadilhas:
            st.markdown("**Ao reconstruir no Power BI:**")
            for item in ind.armadilhas:
                st.markdown(f"- {item}")
        st.code(ind.sql.strip(), language="sql")


# ------------------------------------------------------------------------------------
# Conexao
# ------------------------------------------------------------------------------------
try:
    ident = identity(_sessao())
except DashboardError as exc:
    st.error(str(exc))
    st.stop()

# ------------------------------------------------------------------------------------
# Barra lateral: filtros, frescor e a sonda de isolamento
# ------------------------------------------------------------------------------------
with st.sidebar:
    st.markdown(f"### `{DATABASE}.MART`")
    st.caption(f"papel **{ident['papel']}** · lido em {ident['lido_em']}")

    if st.button("Reler o destino agora", type="primary", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"Cache de {TTL}s. O botao o limpa e forca uma leitura nova.")

    st.divider()

    janela = consulta(I.JANELA)
    inicio_min = pd.to_datetime(janela.iloc[0, 0]).date()
    fim_max = pd.to_datetime(janela.iloc[0, 1]).date()

    periodo = st.date_input(
        "Periodo", value=(inicio_min, fim_max),
        min_value=inicio_min, max_value=fim_max, format="YYYY-MM-DD",
    )
    if isinstance(periodo, tuple) and len(periodo) == 2:
        inicio, fim = periodo
    else:
        inicio, fim = inicio_min, fim_max

    lista_wh = consulta(I.ARMAZENS).iloc[:, 0].tolist()
    armazens = st.multiselect("Armazens", lista_wh, default=lista_wh)
    if not armazens:
        st.warning("Selecione ao menos um armazem.")
        st.stop()

    P = {"inicio": str(inicio), "fim": str(fim), "armazens": ",".join(armazens)}

    st.divider()
    st.markdown("**Frescor da base**")
    frescor = consulta(I.FRESCOR)
    st.dataframe(frescor, hide_index=True, width="stretch")
    st.caption(
        "Contagem e janela de cada mart, lidas agora. Uma carga nova muda estes numeros — "
        "e por isso que eles ficam a vista, e nao escondidos num rodape."
    )

    st.divider()
    with st.expander("Isolamento do papel, ao vivo"):
        for alvo, ok, resultado in probe_isolation(_sessao()):
            st.markdown(f"{'✅' if ok else '❌'} `{alvo}` — {resultado}")
        st.caption(
            "`RETAIL_READER` le MART e mais nada. A recusa em GOLD e STAGE e do motor, com "
            "`use secondary roles none` — sem isso a verificacao passaria por engano."
        )

# ------------------------------------------------------------------------------------
# Cabecalho e KPIs
# ------------------------------------------------------------------------------------
st.title("Indicadores estrategicos — camada MART")
st.caption(
    f"{inicio} a {fim} · {len(armazens)} armazem(ns) · conta `{ident['conta']}` "
    f"({ident['regiao']}) · papel `{ident['papel']}`"
)

st.info(
    "**O pedido e sintetico; o cliente, o produto, o preco e o endereco nao.** Cada indicador "
    "declara o proprio tipo no bloco de armadilhas. O que NAO da para exibir esta na aba "
    "*Fora de alcance*, com o gatilho que destravaria cada item.",
    icon="ℹ️",
)

ind = {i.chave: i for i in I.INDICADORES}
resumo = consulta(ind["resumo_comercial"].sql, P)
if resumo.empty or pd.isna(resumo.iloc[0]["PEDIDOS_COLOCADOS"]):
    st.warning("Nenhum pedido no periodo selecionado. Amplie o intervalo.")
    st.stop()
r = resumo.iloc[0]
moeda = r["MOEDA"] or ""

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Pedidos colocados", f"{int(r['PEDIDOS_COLOCADOS']):,}".replace(",", "."))
c2.metric("Entregues", f"{int(r['PEDIDOS_ENTREGUES']):,}".replace(",", "."),
          f"{float(r['TAXA_ENTREGA'])*100:.2f}% do colocado")
c3.metric(f"Receita apurada ({moeda})",
          f"{float(r['RECEITA_APURADA']):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
c4.metric(f"Ticket medio ({moeda})",
          f"{float(r['TICKET_MEDIO']):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
          help="Receita apurada / pedidos SEPARADOS. Sobre pedidos colocados daria outro "
               "numero, e ele mede uma coisa que nao existe — ver as armadilhas.")
c5.metric("Nao apurado",
          f"{float(r['VALOR_COLOCADO']) - float(r['RECEITA_APURADA']):,.2f}"
          .replace(",", "X").replace(".", ",").replace("X", "."),
          help="Colocado menos apurado. A decomposicao por CAUSA esta na aba Comercial — "
               "um numero unico mistura duas perdas de areas diferentes.")
armadilhas(ind["resumo_comercial"])

# ------------------------------------------------------------------------------------
# Abas
# ------------------------------------------------------------------------------------
abas = st.tabs([
    "A. Comercial", "B. Operacao", "C. Cesta", "D. Sortimento e preco",
    "E. Oferta x demanda", "F. Base e cobertura", "Fora de alcance",
])

# ---------------------------------------------------------------- A. Comercial
with abas[0]:
    esq, dir_ = st.columns([3, 2])

    with esq:
        st.subheader("Funil, por marco alcancado")
        funil = numerico(consulta(ind["funil"].sql, P), ["PEDIDOS", "TAXA"])
        funil["rotulo"] = funil.apply(
            lambda x: f"{int(x['PEDIDOS']):,}".replace(",", ".") + f"  ({x['TAXA']*100:.1f}%)",
            axis=1,
        )
        st.dataframe(
            funil[["ETAPA", "PEDIDOS", "TAXA"]].rename(
                columns={"ETAPA": "Etapa", "PEDIDOS": "Pedidos", "TAXA": "Sobre o colocado"}),
            hide_index=True, width="stretch",
            column_config={"Sobre o colocado": st.column_config.ProgressColumn(
                format="percent", min_value=0.0, max_value=1.0)},
        )
        armadilhas(ind["funil"])

    with dir_:
        st.subheader("Por onde escapa")
        vaz = numerico(consulta(ind["vazamento"].sql, P), ["PEDIDOS", "SOBRE_COLOCADOS"])
        st.dataframe(
            vaz.rename(columns={"MOTIVO": "Motivo", "PEDIDOS": "Pedidos",
                                "SOBRE_COLOCADOS": "Sobre o colocado"}),
            hide_index=True, width="stretch",
        )
        st.caption("Estas contagens NAO somam com as etapas do funil: o devolvido atravessou "
                   "o funil inteiro antes de sair.")
        armadilhas(ind["vazamento"])

    st.divider()
    st.subheader("Perda de valor, decomposta por causa")
    perda = numerico(consulta(ind["decomposicao_perda"].sql, P),
                     ["VALOR", "PEDIDOS_AFETADOS", "POR_PEDIDO"])
    st.dataframe(
        perda[["CAUSA", "VALOR", "PEDIDOS_AFETADOS", "POR_PEDIDO"]].rename(columns={
            "CAUSA": "Causa", "VALOR": f"Valor ({moeda})",
            "PEDIDOS_AFETADOS": "Pedidos", "POR_PEDIDO": f"Por pedido ({moeda})"}),
        hide_index=True, width="stretch",
    )
    st.warning(
        "**`SUM(gross) - SUM(net)` mistura duas perdas com causas opostas.** Uma e operacao "
        "de loja (a cesta encolheu na separacao); a outra e pagamento e cancelamento (o "
        "pedido morreu antes de alguem toca-lo). Um numero unico esconde qual esta "
        "acontecendo — e a soma das duas fecha exatamente com o total.",
        icon="⚠️",
    )
    armadilhas(ind["decomposicao_perda"])

    st.divider()
    st.subheader("Serie diaria")
    serie = numerico(consulta(ind["serie_diaria"].sql, P),
                     ["ORDERS_PLACED", "ORDERS_DELIVERED", "NET_AMOUNT_PICKED",
                      "AMOUNT_DELTA", "TICKET_MEDIO", "DELIVERY_RATE"])
    serie["ORDER_DATE"] = pd.to_datetime(serie["ORDER_DATE"])
    e, d = st.columns(2)
    e.caption(f"Receita apurada por dia ({moeda})")
    e.bar_chart(serie, x="ORDER_DATE", y="NET_AMOUNT_PICKED", color="WH", stack=False)
    d.caption("Pedidos entregues por dia")
    d.bar_chart(serie, x="ORDER_DATE", y="ORDERS_DELIVERED", color="WH", stack=False)
    st.dataframe(serie, hide_index=True, width="stretch")
    armadilhas(ind["serie_diaria"])

# ---------------------------------------------------------------- B. Operacao
with abas[1]:
    st.subheader("SLA de separacao")
    sla = numerico(consulta(ind["sla_separacao"].sql, P),
                   ["LIMIAR_DECLARADO_MIN", "MAXIMO_OBSERVADO_MIN", "VIOLACOES",
                    "PEDIDOS_COM_SEPARACAO"])
    s = sla.iloc[0]
    a, b, cc, dd = st.columns(4)
    a.metric("Limiar declarado", f"{s['LIMIAR_DECLARADO_MIN']:.0f} min",
             help="Vem de FACT_ORDER_PREMISE — o mesmo seed que o gerador leu.")
    b.metric("Maximo observado", f"{s['MAXIMO_OBSERVADO_MIN']:.0f} min")
    cc.metric("Violacoes", f"{int(s['VIOLACOES']):,}".replace(",", "."))
    dd.metric("Pedidos com separacao", f"{int(s['PEDIDOS_COM_SEPARACAO']):,}".replace(",", "."))
    st.warning(
        "**Zero violacoes, e nao porque a operacao seja boa.** O limiar declarado e 90 min e "
        "o teto ARITMETICO da separacao e 80 (`basket_lines_max` 40 x "
        "`minutes_per_line_picked` 2). As tres premissas nao se cruzam. Ler o zero sem o "
        "limiar e o maximo ao lado e ler uma operacao impecavel que nao existe.",
        icon="⚠️",
    )
    armadilhas(ind["sla_separacao"])

    st.divider()
    e, d = st.columns([3, 2])
    with e:
        st.subheader("Tempo por etapa")
        pct = numerico(consulta(ind["percentis_etapa"].sql, P),
                       ["MEDIA_P50", "MEDIA_P90", "PEDIDOS"])
        st.dataframe(
            pct[["ETAPA", "MEDIA_P50", "MEDIA_P90", "PEDIDOS"]].rename(columns={
                "ETAPA": "Etapa", "MEDIA_P50": "media p50 (min)",
                "MEDIA_P90": "media p90 (min)", "PEDIDOS": "Pedidos"}),
            hide_index=True, width="stretch",
        )
        st.caption("`media p90`, e nao `p90`: percentil nao soma nem tira media. O nome da "
                   "coluna diz o que a coluna e.")
        armadilhas(ind["percentis_etapa"])

    with d:
        st.subheader("Janela de entrega")
        jan = numerico(consulta(ind["janela_entrega"].sql, P), ["ENTREGAS"])
        total = jan["ENTREGAS"].sum()
        jan["Sobre as entregas"] = jan["ENTREGAS"] / total if total else 0
        st.dataframe(
            jan[["RESULTADO", "ENTREGAS", "Sobre as entregas"]].rename(
                columns={"RESULTADO": "Resultado", "ENTREGAS": "Entregas"}),
            hide_index=True, width="stretch",
            column_config={"Sobre as entregas": st.column_config.ProgressColumn(
                format="percent", min_value=0.0, max_value=1.0)},
        )
        st.warning(
            "**O desvio e para CEDO, nao para tarde.** A maioria das entregas chega ANTES de "
            "a janela abrir. Uma taxa unica de aderencia convida a concluir o inverso do "
            "fato.",
            icon="⚠️",
        )
        armadilhas(ind["janela_entrega"])

# ---------------------------------------------------------------- C. Cesta
with abas[2]:
    st.subheader("Receita por categoria")
    cat = numerico(consulta(ind["receita_categoria"].sql, P),
                   ["RECEITA_APURADA", "VALOR_PEDIDO", "VALOR_PERDIDO",
                    "LINHAS_PEDIDAS", "UNIDADES_ENTREGUES"])
    topo = cat.head(20)
    st.caption(f"20 maiores por receita apurada ({moeda}) — de {len(cat)} categorias")
    st.bar_chart(topo, x="CATEGORIA", y="RECEITA_APURADA", horizontal=True)
    st.dataframe(cat, hide_index=True, width="stretch")
    st.info("`orders_touching_category` NAO e aditivo entre categorias e por isso nao esta "
            "nesta consulta. Contagem de pedidos vem de MART_ORDER_FUNNEL.", icon="ℹ️")
    armadilhas(ind["receita_categoria"])

    st.divider()
    st.subheader("Substituicao e remocao")
    sub = numerico(consulta(ind["substituicao_categoria"].sql, P),
                   ["LINHAS_PEDIDAS", "LINHAS_SUBSTITUIDAS", "LINHAS_REMOVIDAS",
                    "LINHAS_NUNCA_SEPARADAS", "TAXA_SUBSTITUICAO", "TAXA_REMOCAO"])
    st.dataframe(sub, hide_index=True, width="stretch")
    st.warning(
        "**Estas taxas sao PREMISSA declarada, nao observacao.** Nenhuma fonte deste "
        "repositorio mede disponibilidade. A variacao entre categorias e ruido sobre uma taxa "
        "constante — ranquear categorias por 'risco de ruptura' com este dado e inventar.",
        icon="⚠️",
    )
    armadilhas(ind["substituicao_categoria"])

# ---------------------------------------------------------------- D. Sortimento
with abas[3]:
    st.subheader("Sortimento por armazem")
    sort = numerico(consulta(ind["sortimento_armazem"].sql, P),
                    ["DIAS_OBSERVADOS", "PRODUTOS_MEDIA_DIA", "PRODUTOS_MAXIMO_DIA",
                     "EXCLUSIVOS_MEDIA_DIA", "PRECO_MEDIO", "NOVIDADES_PERIODO"])
    st.dataframe(sort, hide_index=True, width="stretch")
    armadilhas(ind["sortimento_armazem"])

    st.divider()
    e, d = st.columns([2, 3])
    with e:
        st.subheader("Movimento do catalogo")
        mov = numerico(consulta(ind["movimento_catalogo"].sql, P),
                       ["PRODUTOS", "PRODUTOS_DISTINTOS", "DIAS"])
        st.dataframe(mov, hide_index=True, width="stretch")
        armadilhas(ind["movimento_catalogo"])
    with d:
        st.subheader("Maiores variacoes de preco")
        var = numerico(consulta(ind["variacao_preco"].sql, P),
                       ["PRECO_ANTERIOR", "PRECO", "VARIACAO", "VARIACAO_PCT",
                        "DIAS_DESDE_O_ANTERIOR"])
        st.dataframe(var.head(50), hide_index=True, width="stretch")
        st.caption("`dias_desde_o_anterior` e obrigatorio na leitura: ha lacunas de ate 8 "
                   "dias, e sem essa coluna uma variacao de 8 dias se passa por uma de 1.")
        armadilhas(ind["variacao_preco"])

# ---------------------------------------------------------------- E. Oferta x demanda
with abas[4]:
    st.subheader("Oferta x demanda, no mesmo grao")
    od = numerico(consulta(ind["oferta_demanda"].sql, P),
                  ["PRODUTOS_OFERTADOS", "PRODUTOS_PEDIDOS", "COBERTURA_DEMANDA",
                   "LINHAS_PEDIDAS", "RECEITA_APURADA"])
    st.caption("Os dois marts tem grao (data, wh, category_id) exatamente para permitir isto "
               "sem reagregacao.")
    st.scatter_chart(od.head(60), x="PRODUTOS_OFERTADOS", y="RECEITA_APURADA",
                     size="LINHAS_PEDIDAS")
    st.dataframe(od, hide_index=True, width="stretch")
    st.warning(
        "**A demanda e sintetica e a escolha de produto e UNIFORME.** Consequencia declarada: "
        "o mix por categoria espelha o TAMANHO DO SORTIMENTO. Ler cobertura de demanda como "
        "preferencia de cliente e ler a premissa de volta.",
        icon="⚠️",
    )
    armadilhas(ind["oferta_demanda"])

# ---------------------------------------------------------------- F. Base
with abas[5]:
    st.info("Os dois indicadores desta aba NAO tem eixo de data: trazem a versao vigente. Os "
            "filtros de periodo da barra lateral nao se aplicam a eles.", icon="ℹ️")
    e, d = st.columns(2)
    with e:
        st.subheader("Base de clientes")
        base = numerico(consulta(ind["base_clientes"].sql), ["CLIENTES", "IDADE_MEDIA",
                                                            "MUNICIPIOS", "CEPS"])
        st.bar_chart(base, x="FAIXA_ETARIA", y="CLIENTES", color="ARMAZEM", stack=True)
        st.dataframe(base, hide_index=True, width="stretch")
        armadilhas(ind["base_clientes"])
    with d:
        st.subheader("Cobertura municipal")
        cob = numerico(consulta(ind["cobertura_municipal"].sql),
                       ["MUNICIPIOS_NA_AUF", "MUNICIPIOS_SEM_CLIENTE", "CLIENTES",
                        "POPULACAO_AUF", "CLIENTES_POR_10K"])
        st.dataframe(cob, hide_index=True, width="stretch")
        st.warning(
            "**`clientes_por_10k` tem numerador SINTETICO e denominador OBSERVADO.** Mede "
            "densidade da simulacao, nunca penetracao de mercado. E o numero mais facil de "
            "citar fora de contexto de todo o painel.",
            icon="⚠️",
        )
        armadilhas(ind["cobertura_municipal"])

# ---------------------------------------------------------------- Fora de alcance
with abas[6]:
    st.subheader("O que este painel NAO exibe, e por que")
    st.markdown(
        "Uma lista de ausencias declaradas vale mais que um indicador inventado. Cada item "
        "traz o **gatilho** que o destravaria, para que a conversa seja sobre o que falta e "
        "nao sobre o que poderia ser aproximado."
    )
    for titulo, motivo, gatilho in I.FORA_DE_ALCANCE:
        with st.container(border=True):
            st.markdown(f"**{titulo}**")
            st.markdown(motivo)
            st.caption(f"Gatilho: {gatilho}")

    st.divider()
    st.markdown(
        "**A lacuna mais acionavel** e a terceira: nenhum mart junta cliente com pedido. O "
        "elo existe em `FACT_ORDER.customer_sk`, no GOLD, que `RETAIL_READER` nao alcanca por "
        "desenho. Fechar isso nao exige fonte nova — exige um mart novo com grao de cliente e "
        "medidas de pedido. Sem ele, nao ha recompra, LTV, coorte nem receita por cliente."
    )

# ------------------------------------------------------------------------------------
st.divider()
st.caption(
    f"`{ident['database']}.{ident['schema']}` · conta `{ident['conta']}` · "
    f"usuario `{ident['usuario']}` · papel `{ident['papel']}` · "
    f"warehouse `{ident['warehouse']}` · regiao `{ident['regiao']}` · "
    f"lido em {ident['lido_em']} · cache {TTL}s · "
    f"{len(I.INDICADORES)} indicadores. Consultas e ressalvas em `streamlit/CONTRACT.md`, "
    f"gerado de `indicators.py`."
)
