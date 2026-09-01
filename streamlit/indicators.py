"""Os indicadores do painel: a consulta E a explicacao, no MESMO lugar.

POR QUE ISTO E UM MODULO E NAO UM DOCUMENTO. O `CONTRACT.md` desta pasta e GERADO daqui
(`make dashboard-contract`). Se a explicacao morasse num markdown escrito a mao, ela seria um
segundo lugar onde o indicador vive, e os dois divergiriam no primeiro ajuste de SQL — com o
detalhe cruel de que a conferencia continuaria "passando", porque ninguem le um SQL e um texto
lado a lado procurando desacordo. E o mesmo motivo pelo qual o DDL do STAGE e derivado do
recorte em vez de escrito a mao, e pelo qual `docs/warehouse-evidence/` e gerado.

O QUE CADA CAMPO CARREGA, e por que nenhum e opcional:

  `pergunta`  — o que o indicador responde. Se nao couber numa frase, o indicador esta
                fazendo duas coisas.
  `grao`      — o grao da fonte. E o que diz se uma soma e legitima: somar
                `orders_touching_category` entre categorias conta o mesmo pedido varias vezes.
  `tipo`      — `observado` (veio de fonte real), `sintetico` (foi gerado), `misto` ou
                `derivado`. Um painel que nao distingue os dois convida a ler densidade de
                simulacao como penetracao de mercado.
  `armadilhas`— o que da errado ao reconstruir isto no Power BI. Nao e ressalva de rodape: sao
                os casos em que a medida obvia produz um numero PLAUSIVEL e errado, que e a
                unica classe de erro que nenhum teste pega.

PARAMETROS. Toda consulta com eixo de data aceita `%(inicio)s`, `%(fim)s` e `%(armazens)s`
(lista separada por virgula). O filtro de armazem usa
`array_contains(wh::variant, split(%(armazens)s, ','))` — uma linha, sem SQL montado por
concatenacao, entao nao ha como injetar nada pelo seletor da interface.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DB = "RETAIL"
FILTRO_DATA = "order_date between %(inicio)s and %(fim)s"
FILTRO_DATA_SNAP = "snapshot_date between %(inicio)s and %(fim)s"
FILTRO_WH = "array_contains(wh::variant, split(%(armazens)s, ','))"


@dataclass(frozen=True)
class Indicador:
    chave: str
    titulo: str
    grupo: str
    pergunta: str
    grao: str
    tipo: str
    marts: tuple[str, ...]
    sql: str
    armadilhas: tuple[str, ...] = field(default_factory=tuple)
    datado: bool = True


INDICADORES: tuple[Indicador, ...] = (

    # ================================================================= A. COMERCIAL
    Indicador(
        chave="resumo_comercial",
        titulo="Resumo comercial",
        grupo="A. Comercial",
        pergunta="Quantos pedidos entraram, quantos chegaram ao cliente, e quanto foi apurado?",
        grao="(order_date, wh) agregado para o total do periodo",
        tipo="sintetico (o pedido) sobre observado (cliente, produto, preco)",
        marts=("MART_ORDER_FUNNEL",),
        armadilhas=(
            "TICKET MEDIO tem dois denominadores possiveis e eles NAO sao equivalentes: "
            "receita/pedidos_separados = 95,21 e receita/pedidos_colocados = 90,95. O "
            "segundo divide a receita de quem foi separado pelo total incluindo quem nunca "
            "chegou a separacao — mede uma coisa que nao existe. Use `orders_picked`.",
            "`net_amount_picked` e NULO para pedido que morreu antes da separacao, e `sum()` "
            "ignora nulo. Isso e correto e proposital: quem nunca foi separado nao contribui "
            "com zero, contribui com nada. No Power BI, um `SUM` sobre coluna nula faz o "
            "mesmo; um `COALESCE(...,0)` inventaria uma apuracao que nao houve.",
        ),
        sql=f"""
            select
                sum(orders_placed)                                  as pedidos_colocados,
                sum(orders_confirmed)                               as pedidos_confirmados,
                sum(orders_picked)                                  as pedidos_separados,
                sum(orders_delivered)                               as pedidos_entregues,
                sum(gross_amount_placed)                            as valor_colocado,
                sum(net_amount_picked)                              as receita_apurada,
                -- Denominador = separados. Ver as armadilhas.
                round(sum(net_amount_picked)
                      / nullif(sum(orders_picked), 0), 2)           as ticket_medio,
                round(sum(orders_delivered)
                      / nullif(sum(orders_placed), 0), 4)           as taxa_entrega,
                max(currency)                                       as moeda
            from {DB}.MART.MART_ORDER_FUNNEL
            where {FILTRO_DATA} and {FILTRO_WH}
        """,
    ),

    Indicador(
        chave="funil",
        titulo="Funil de conversao, por marco alcancado",
        grupo="A. Comercial",
        pergunta="De cada 100 pedidos colocados, quantos atravessaram cada etapa?",
        grao="(order_date, wh) agregado; uma linha por ETAPA",
        tipo="derivado de contagem de marco",
        marts=("MART_ORDER_FUNNEL",),
        armadilhas=(
            "AS ETAPAS SAO CONTADAS POR MARCO ALCANCADO, nunca por status. `order_status` "
            "guarda o estado do ULTIMO evento: um pedido devolvido tem status RETURNED e FOI "
            "entregue. Medido nesta base: contar status='DELIVERED' da 5.985; contar "
            "delivered_at is not null da 6.046 — os 61 devolvidos. Um funil sobre status "
            "publica uma taxa de entrega 1% menor que a real e nada reprova.",
            "Marco e monotonico (uma vez alcancado, nao volta atras); status nao e. O mart "
            "ja resolve isso — as colunas `orders_*` sao contagens de marco. No Power BI, NAO "
            "reconstrua o funil a partir de um campo de status.",
        ),
        sql=f"""
            with total as (
                select
                    sum(orders_placed)          as colocado,
                    sum(orders_confirmed)       as confirmado,
                    sum(orders_picking_started) as separacao_iniciada,
                    sum(orders_picked)          as separado,
                    sum(orders_dispatched)      as despachado,
                    sum(orders_delivered)       as entregue
                from {DB}.MART.MART_ORDER_FUNNEL
                where {FILTRO_DATA} and {FILTRO_WH}
            )
            select 1 as ordem, 'Colocado'            as etapa, colocado            as pedidos, 1.0 as taxa from total
            union all select 2, 'Pagamento aprovado', confirmado,         round(confirmado/nullif(colocado,0),4)         from total
            union all select 3, 'Separacao iniciada', separacao_iniciada, round(separacao_iniciada/nullif(colocado,0),4) from total
            union all select 4, 'Separado',           separado,           round(separado/nullif(colocado,0),4)           from total
            union all select 5, 'Despachado',         despachado,         round(despachado/nullif(colocado,0),4)         from total
            union all select 6, 'Entregue',           entregue,           round(entregue/nullif(colocado,0),4)           from total
            order by ordem
        """,
    ),

    Indicador(
        chave="vazamento",
        titulo="Por onde o pedido escapa",
        grupo="A. Comercial",
        pergunta="Quantos pedidos sairam do funil, e por qual motivo?",
        grao="(order_date, wh) agregado; uma linha por motivo de saida",
        tipo="derivado de contagem de marco",
        marts=("MART_ORDER_FUNNEL",),
        armadilhas=(
            "ESTAS CONTAGENS NAO SOMAM COM AS ETAPAS DO FUNIL. `orders_returned` conta quem "
            "saiu DEPOIS de atravessar o funil inteiro; somar as saidas as etapas contaria os "
            "devolvidos duas vezes. No Power BI, mantenha os dois blocos separados e nunca "
            "monte um 'total de pedidos' somando etapas com saidas.",
        ),
        sql=f"""
            select 'Pagamento recusado' as motivo, sum(orders_payment_failed)  as pedidos,
                   round(sum(orders_payment_failed)/nullif(sum(orders_placed),0),4) as sobre_colocados
              from {DB}.MART.MART_ORDER_FUNNEL where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 'Cancelado', sum(orders_cancelled),
                   round(sum(orders_cancelled)/nullif(sum(orders_placed),0),4)
              from {DB}.MART.MART_ORDER_FUNNEL where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 'Entrega falhou', sum(orders_delivery_failed),
                   round(sum(orders_delivery_failed)/nullif(sum(orders_placed),0),4)
              from {DB}.MART.MART_ORDER_FUNNEL where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 'Devolvido (apos entrega)', sum(orders_returned),
                   round(sum(orders_returned)/nullif(sum(orders_placed),0),4)
              from {DB}.MART.MART_ORDER_FUNNEL where {FILTRO_DATA} and {FILTRO_WH}
            order by pedidos desc
        """,
    ),

    Indicador(
        chave="decomposicao_perda",
        titulo="Perda de valor, decomposta por CAUSA",
        grupo="A. Comercial",
        pergunta="O que foi colocado e nao foi apurado — e por que nao foi?",
        grao="(order_date, wh) agregado",
        tipo="derivado",
        marts=("MART_ORDER_FUNNEL",),
        armadilhas=(
            "ESTE E O INDICADOR MAIS FACIL DE ERRAR DO PAINEL, e o erro produz um numero "
            "plausivel. `SUM(gross) - SUM(net)` da 58.327,81 e MISTURA duas perdas com causas "
            "opostas. `SUM(amount_delta)` da 4.834,73 — e nao e a mesma coisa, nem esta "
            "errado: `amount_delta` so existe para pedido SEPARADO, e `sum()` ignora o nulo "
            "dos outros 298.",
            "A decomposicao correta, verificada aritmeticamente (4.834,73 + 53.493,08 = "
            "58.327,81): perda na SEPARACAO = sum(amount_delta), a cesta encolheu com "
            "remocao e substituicao; perda por PEDIDO MORTO = o resto, o valor integral de "
            "quem nunca chegou a separacao. Sao problemas de areas diferentes — uma e "
            "operacao de loja, a outra e pagamento e cancelamento — e um numero unico esconde "
            "qual esta acontecendo.",
            "OBSERVACAO NAO EXPLICADA, registrada em vez de omitida: o pedido morto vale em "
            "media 179,51 contra 135,36 do separado (valor colocado nos dois casos), sobre "
            "298 pedidos. Uma hipotese compativel e que cesta maior leva mais tempo para "
            "separar (`minutes_per_line_picked` x linhas) e portanto oferece uma janela maior "
            "para o cancelamento chegar antes. NAO da para confirmar pelo MART — nao ha grao "
            "de pedido aqui — entao fica como pergunta, nao como conclusao.",
        ),
        sql=f"""
            with t as (
                select sum(gross_amount_placed) as colocado,
                       sum(net_amount_picked)   as apurado,
                       sum(amount_delta)        as delta_separacao,
                       sum(orders_placed)       as pedidos,
                       sum(orders_picked)       as separados
                from {DB}.MART.MART_ORDER_FUNNEL
                where {FILTRO_DATA} and {FILTRO_WH}
            )
            select 1 as ordem, 'Perda na separacao (cesta encolheu)' as causa,
                   delta_separacao as valor, separados as pedidos_afetados,
                   round(delta_separacao/nullif(separados,0),2) as por_pedido from t
            union all
            select 2, 'Perda por pedido morto (nunca separado)',
                   colocado - apurado - delta_separacao, pedidos - separados,
                   round((colocado - apurado - delta_separacao)
                         /nullif(pedidos - separados,0),2) from t
            union all
            select 3, 'TOTAL nao apurado', colocado - apurado, pedidos - separados, null from t
            order by ordem
        """,
    ),

    Indicador(
        chave="serie_diaria",
        titulo="Serie diaria: pedidos e receita",
        grupo="A. Comercial",
        pergunta="Como pedidos e receita se movem dia a dia, por armazem?",
        grao="(order_date, wh) — o grao nativo do mart",
        tipo="misto",
        marts=("MART_ORDER_FUNNEL",),
        armadilhas=(
            "A JANELA ATUAL TEM 4 DIAS. Nao ha tendencia, sazonalidade nem comparativo "
            "semanal a extrair disso — qualquer linha de tendencia sobre 4 pontos e "
            "decoracao. O eixo existe para que a serie CRESCA, e cresce a cada dia que a "
            "DAG rodar.",
        ),
        sql=f"""
            select order_date, wh,
                   orders_placed, orders_delivered, net_amount_picked, amount_delta,
                   round(net_amount_picked/nullif(orders_picked,0),2) as ticket_medio,
                   delivery_rate
            from {DB}.MART.MART_ORDER_FUNNEL
            where {FILTRO_DATA} and {FILTRO_WH}
            order by order_date, wh
        """,
    ),

    # ================================================================= B. OPERACAO
    Indicador(
        chave="sla_separacao",
        titulo="SLA de separacao: limiar, maximo e violacoes",
        grupo="B. Operacao",
        pergunta="Quantos pedidos estouraram o limiar declarado de separacao?",
        grao="(order_date, wh) agregado",
        tipo="derivado sobre premissa declarada (sintetica)",
        marts=("MART_FULFILLMENT_SLA",),
        armadilhas=(
            "AS TRES COLUNAS SO SIGNIFICAM ALGO JUNTAS, e publica-las separadas e o erro. "
            "`orders_breaching_sla` = 0, e nao porque a operacao seja boa: o limiar declarado "
            "e 90 minutos e o teto ARITMETICO da separacao e 80 "
            "(`basket_lines_max` 40 x `minutes_per_line_picked` 2). O maximo observado e "
            "exatamente 80. As tres premissas nao se cruzam, e o zero e consequencia disso.",
            "O limiar vem de `FACT_ORDER_PREMISE`, que veio do seed que o GERADOR leu, cujo "
            "sha256 esta no manifesto de cada particao do RAW. No Power BI, NAO cravar 90 num "
            "measure: leia a coluna `sla_minutes`. Cravar cria a segunda copia do numero, e "
            "no dia em que o seed mudar o painel passa a medir contra um limiar que nenhum "
            "pedido conheceu — sem reprovar nada, porque zero contra o limiar errado tem a "
            "mesma aparencia de zero contra o certo.",
        ),
        sql=f"""
            select
                max(sla_minutes)            as limiar_declarado_min,
                max(max_picking_minutes)    as maximo_observado_min,
                sum(orders_breaching_sla)   as violacoes,
                sum(orders_with_pick)       as pedidos_com_separacao
            from {DB}.MART.MART_FULFILLMENT_SLA
            where {FILTRO_DATA} and {FILTRO_WH}
        """,
    ),

    Indicador(
        chave="percentis_etapa",
        titulo="Tempo por etapa (p50 / p90)",
        grupo="B. Operacao",
        pergunta="Quanto tempo cada etapa leva, no meio e na cauda?",
        grao="(order_date, wh); mediana das medianas quando agregado — ver armadilhas",
        tipo="derivado",
        marts=("MART_FULFILLMENT_SLA",),
        armadilhas=(
            "PERCENTIL NAO SOMA E NAO TIRA MEDIA. O mart guarda p50/p90 por (dia, armazem); "
            "o painel mostra a MEDIA desses percentis quando ha mais de uma linha, e isso e "
            "uma aproximacao, nao o percentil do conjunto. Para o percentil verdadeiro do "
            "periodo seria preciso o grao de pedido, que vive em `FACT_ORDER` — fora do "
            "alcance de `RETAIL_READER`, por desenho. O rotulo da coluna diz `media_p90` "
            "justamente para nao se passar pelo p90.",
            "Os percentis sao calculados sobre os pedidos que ALCANCARAM cada marco "
            "(`percentile_cont` ignora nulo). Um p90 de entrega que contasse os cancelados "
            "como zero mediria a operacao de outra empresa. As colunas `orders_with_*` dizem "
            "sobre quantos pedidos cada percentil foi calculado.",
        ),
        sql=f"""
            select 1 as ordem, 'Colocado -> pagamento' as etapa,
                   round(avg(p50_minutes_to_confirm),1) as media_p50,
                   round(avg(p90_minutes_to_confirm),1) as media_p90,
                   sum(orders_with_confirm)             as pedidos
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 2, 'Separacao (inicio -> fim)', round(avg(p50_minutes_to_pick),1),
                   round(avg(p90_minutes_to_pick),1), sum(orders_with_pick)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 3, 'Separado -> despachado', round(avg(p50_minutes_to_dispatch),1),
                   round(avg(p90_minutes_to_dispatch),1), sum(orders_with_pick)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 4, 'Despachado -> entregue', round(avg(p50_minutes_to_deliver),1),
                   round(avg(p90_minutes_to_deliver),1), sum(orders_with_deliver)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 5, 'Ciclo total (colocado -> entregue)',
                   round(avg(p50_minutes_placed_to_delivered),1),
                   round(avg(p90_minutes_placed_to_delivered),1), sum(orders_with_deliver)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            order by ordem
        """,
    ),

    Indicador(
        chave="janela_entrega",
        titulo="Janela de entrega: antes, dentro, depois",
        grupo="B. Operacao",
        pergunta="A entrega aconteceu dentro da janela prometida ao cliente?",
        grao="(order_date, wh) agregado; uma linha por resultado",
        tipo="derivado",
        marts=("MART_FULFILLMENT_SLA",),
        armadilhas=(
            "CHEGAR CEDO E CHEGAR TARDE SAO PROBLEMAS OPOSTOS, e uma taxa unica de "
            "'aderencia' apaga qual deles esta acontecendo. Medido: das 6.046 entregas, "
            "5.166 chegam ANTES de a janela abrir, 471 dentro, 409 depois. A taxa de "
            "aderencia de 8% convida a concluir 'a operacao atrasa', que e o inverso do fato.",
            "A causa e aritmetica e esta nas premissas: `slot_lead_hours` sorteia o inicio da "
            "janela entre 2h e 24h depois da colocacao, enquanto a soma dos marcos entrega em "
            "~4,6h. As duas premissas foram declaradas separadamente e nunca conciliadas. "
            "Registrado, nao corrigido — mexer no seed para a taxa melhorar seria ajustar a "
            "entrada ate a saida agradar.",
            "No Power BI, publique as TRES contagens. Se um unico indicador for exigido, use "
            "'entregas fora da janela' com o detalhe de direcao ao lado.",
        ),
        sql=f"""
            select 1 as ordem, 'Antes de a janela abrir' as resultado,
                   sum(orders_delivered_before_slot) as entregas
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 2, 'Dentro da janela', sum(orders_delivered_within_slot)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            union all
            select 3, 'Depois de a janela fechar', sum(orders_delivered_after_slot)
              from {DB}.MART.MART_FULFILLMENT_SLA where {FILTRO_DATA} and {FILTRO_WH}
            order by ordem
        """,
    ),

    # ================================================================= C. CESTA
    Indicador(
        chave="receita_categoria",
        titulo="Receita por categoria",
        grupo="C. Cesta e categoria",
        pergunta="Quais categorias respondem pela receita apurada?",
        grao="(order_date, wh, category_id) agregado por categoria",
        tipo="misto",
        marts=("MART_BASKET_DAILY",),
        armadilhas=(
            "`orders_touching_category` NAO E ADITIVO entre categorias: um pedido com leite e "
            "pao conta uma vez em cada. Somar as 151 categorias de um dia da muito mais que "
            "os 1.600 pedidos daquele dia. Linhas, unidades e valor SAO aditivos, porque cada "
            "linha pertence a exatamente uma categoria. Para contagem de pedidos use "
            "MART_ORDER_FUNNEL, que tem o grao certo.",
            "`revenue_fulfilled` e o que foi ENTREGUE; `revenue_placed` e o que foi pedido. A "
            "diferenca (`revenue_lost`) e remocao mais pedido nunca separado. Nao troque um "
            "pelo outro num grafico de 'receita' sem dizer qual.",
            "A CATEGORIA E A DO MOMENTO DO PEDIDO, gravada no proprio evento, e nao a que o "
            "produto tem hoje. E o comportamento correto para receita historica, e difere de "
            "um join contra a dimensao corrente.",
        ),
        sql=f"""
            select
                parent_category_name                as categoria_nivel1,
                category_name                       as categoria,
                sum(revenue_fulfilled)              as receita_apurada,
                sum(revenue_placed)                 as valor_pedido,
                sum(revenue_lost)                   as valor_perdido,
                sum(lines_placed)                   as linhas_pedidas,
                sum(units_fulfilled)                as unidades_entregues,
                count(distinct order_date)          as dias,
                max(currency)                       as moeda
            from {DB}.MART.MART_BASKET_DAILY
            where {FILTRO_DATA} and {FILTRO_WH}
            group by 1, 2
            order by receita_apurada desc nulls last
        """,
    ),

    Indicador(
        chave="substituicao_categoria",
        titulo="Substituicao e remocao, por categoria",
        grupo="C. Cesta e categoria",
        pergunta="Onde a cesta muda mais entre o pedido e a entrega?",
        grao="(order_date, wh, category_id) agregado por categoria",
        tipo="derivado sobre premissa declarada (sintetica)",
        marts=("MART_BASKET_DAILY",),
        armadilhas=(
            "AS TAXAS SAO PREMISSA, NAO OBSERVACAO. `substitution_rate` (0,04) e "
            "`removal_rate` (0,02) foram DECLARADAS no seed do gerador; nenhuma fonte deste "
            "repositorio mede disponibilidade. A variacao entre categorias e ruido de "
            "amostragem sobre uma taxa constante, nao um sinal de sortimento. Um painel que "
            "ranqueia categorias por 'risco de ruptura' com este dado esta inventando.",
            "`unavailable` e o motivo registrado, e NAO `out_of_stock`: nao existe fato de "
            "estoque nesta plataforma. O vocabulario e deliberado — nomear como estoque "
            "prometeria um dado que ninguem mediu.",
            "Recalcule a taxa a partir das SOMAS (linhas_substituidas / linhas_pedidas). "
            "Tirar media das taxas por dia-armazem-categoria pondera cada celula igualmente, "
            "independente do tamanho — o classico paradoxo de Simpson num painel.",
        ),
        sql=f"""
            select
                category_name                                       as categoria,
                sum(lines_placed)                                   as linhas_pedidas,
                sum(lines_substituted)                              as linhas_substituidas,
                sum(lines_removed)                                  as linhas_removidas,
                sum(lines_never_picked)                             as linhas_nunca_separadas,
                round(sum(lines_substituted)/nullif(sum(lines_placed),0), 4) as taxa_substituicao,
                round(sum(lines_removed)    /nullif(sum(lines_placed),0), 4) as taxa_remocao
            from {DB}.MART.MART_BASKET_DAILY
            where {FILTRO_DATA} and {FILTRO_WH}
            group by 1
            having sum(lines_placed) >= 100
            order by taxa_substituicao desc
        """,
    ),

    Indicador(
        chave="perfil_por_faixa",
        titulo="Perfil de consumo por faixa etaria do comprador",
        grupo="C. Cesta e categoria",
        pergunta="O que cada faixa etaria leva, e onde ela difere mais das outras?",
        grao="(order_date, wh, buyer_age_band, demand_group) agregado por faixa e grupo",
        tipo="sintetico calibrado contra benchmark (MAPA 2025)",
        marts=("MART_DEMAND_COHORT",),
        armadilhas=(
            "O AGREGADO NAO MUDA ENTRE FAIXAS, DE PROPOSITO. A calibracao por coorte e "
            "neutra no total — um IPF garante que a media ponderada dos pesos por coorte "
            "reproduz o mix agregado. Procurar o efeito desta camada num total nao encontra "
            "nada; ele esta inteiro na comparacao ENTRE faixas da mesma linha.",
            "COMPARE FATIA, NUNCA CONTAGEM. As quatro faixas tem tamanhos diferentes na base "
            "(35_49 e a maior, LT35 a menor), entao 'linhas por faixa' mede o tamanho da "
            "coorte e nao a propensao dela. `share_within_band` ja tem a propria coorte no "
            "denominador; e ela que isola as duas coisas.",
            "A PROPENSAO E BENCHMARK, NAO OBSERVACAO DESTA LOJA. Os indices vem do consumo "
            "domestico espanhol medido pelo MAPA, e o `% Poblacion` de la e a populacao que "
            "VIVE EM LARES com responsavel naquela faixa — nao a populacao daquela idade. "
            "Por isso o numero entra como indice relativo, e nunca como share absoluto.",
            "NO_FOOD e SIN_BENCHMARK aparecem com razao proxima de 1 por CONSTRUCAO: o "
            "informe nao mede drogaria nem limpeza, o indice deles e neutro e a fatia de "
            "cada bloco e mantida constante entre coortes. Ler isso como 'todas as idades "
            "compram xampu igual' seria transformar ausencia de medicao em medicao.",
        ),
        sql=f"""
            with por_faixa as (
                select
                    buyer_age_band,
                    demand_group,
                    sum(lines_placed)                               as linhas
                from {DB}.MART.MART_DEMAND_COHORT
                where {FILTRO_DATA} and {FILTRO_WH}
                group by 1, 2
            ),
            total as (
                select buyer_age_band, sum(linhas) as linhas_faixa
                from por_faixa group by 1
            )
            select
                p.demand_group                                      as grupo,
                max(case when p.buyer_age_band = 'LT35'
                         then round(100 * p.linhas / t.linhas_faixa, 2) end)  as pct_lt35,
                max(case when p.buyer_age_band = '35_49'
                         then round(100 * p.linhas / t.linhas_faixa, 2) end)  as pct_35_49,
                max(case when p.buyer_age_band = '50_64'
                         then round(100 * p.linhas / t.linhas_faixa, 2) end)  as pct_50_64,
                max(case when p.buyer_age_band = 'GE65'
                         then round(100 * p.linhas / t.linhas_faixa, 2) end)  as pct_ge65,
                sum(p.linhas)                                       as linhas_total
            from por_faixa p
            join total t on t.buyer_age_band = p.buyer_age_band
            group by 1
            having sum(p.linhas) >= 100
            order by div0(
                max(case when p.buyer_age_band = 'GE65'
                         then p.linhas / t.linhas_faixa end),
                max(case when p.buyer_age_band = 'LT35'
                         then p.linhas / t.linhas_faixa end)
            ) desc
        """,
    ),

    Indicador(
        chave="pedidos_por_regiao",
        titulo="Pedidos por armazem, e a intensidade regional que os separa",
        grupo="C. Cesta e categoria",
        pergunta="Por que bcn1 coloca mais pedidos que mad1, se as bases tem o mesmo tamanho?",
        grao="(order_date, wh) agregado por armazem",
        tipo="sintetico inclinado por consumo per capita observado (MAPA, secao 3)",
        marts=("MART_DEMAND_COHORT",),
        armadilhas=(
            "A DIFERENCA E DELIBERADA E OBSERVADA. Ate a fase anterior os quatro armazens "
            "tinham a mesma contagem por construcao. O informe mede consumo per capita por "
            "comunidade autonoma — Cataluna 620,82 kg-L por pessoa e ano contra 505,86 de "
            "Madrid — e essa razao passou a pesar QUANTOS clientes pedem.",
            "A INTENSIDADE VIRA FREQUENCIA, E ISSO E ESCOLHA DECLARADA. O informe da kg por "
            "ano e NAO publica frequencia de compra domestica; repartir a intensidade entre "
            "frequencia e tamanho de cesta seria inventar a reparticao. Ler estes numeros "
            "como 'catalao compra mais vezes' e ler a premissa, nao uma medicao.",
            "O TOTAL DA JANELA NAO MUDA por causa desta inclinacao: o indice e renormalizado "
            "sobre as quatro comunidades servidas. O que ela move e a REPARTICAO entre "
            "armazens, nunca a soma.",
        ),
        sql=f"""
            select
                wh                                                  as armazem,
                count(distinct order_date)                          as dias,
                sum(lines_placed)                                   as linhas,
                sum(units_placed)                                   as unidades,
                round(sum(revenue_fulfilled), 2)                    as receita,
                max(currency)                                       as moeda
            from {DB}.MART.MART_DEMAND_COHORT
            where {FILTRO_DATA} and {FILTRO_WH}
            group by 1
            order by linhas desc
        """,
    ),

    # ================================================= D. SORTIMENTO E PRECO
    Indicador(
        chave="sortimento_armazem",
        titulo="Sortimento por armazem",
        grupo="D. Sortimento e preco",
        pergunta="Quanto catalogo cada armazem tem, e quanto disso e exclusivo dele?",
        grao="(snapshot_date, wh, category_id) agregado por armazem",
        tipo="observado",
        marts=("MART_ASSORTMENT_DAILY",),
        armadilhas=(
            "SORTIMENTO E A PRESENCA DA LINHA no fato de preco, nao uma tabela propria. Uma "
            "linha afirma 'este produto estava no catalogo deste armazem neste dia'. Cuidado "
            "ao ler ausencia: pode ser produto fora do catalogo OU dia nao observado — os "
            "dias 2026-08-17 a 08-23 nao existem e nao podem ser recuperados, porque a API "
            "so serve o preco de hoje.",
            "`products_in_all_warehouses` e o denominador honesto de qualquer comparacao "
            "entre armazens: comparar preco medio de catalogos diferentes mede a diferenca de "
            "CATALOGO, nao de preco.",
            "A JANELA DESTE MART E MAIOR que a de pedidos (catalogo desde 2026-08-15, pedidos "
            "desde 08-24). Cruzar os dois sem alinhar a data compara periodos diferentes.",
        ),
        sql=f"""
            select
                wh                                                  as armazem,
                count(distinct snapshot_date)                       as dias_observados,
                round(avg(produtos_no_dia), 0)                      as produtos_media_dia,
                max(produtos_no_dia)                                as produtos_maximo_dia,
                round(avg(exclusivos_no_dia), 0)                    as exclusivos_media_dia,
                round(avg(preco_medio_dia), 4)                      as preco_medio,
                sum(novidades)                                      as novidades_periodo
            from (
                select snapshot_date, wh,
                       sum(products)                as produtos_no_dia,
                       sum(products_exclusive_here) as exclusivos_no_dia,
                       avg(avg_unit_price)          as preco_medio_dia,
                       sum(new_arrivals)            as novidades
                from {DB}.MART.MART_ASSORTMENT_DAILY
                where {FILTRO_DATA_SNAP} and {FILTRO_WH}
                group by 1, 2
            )
            group by 1
            order by 1
        """,
    ),

    Indicador(
        chave="variacao_preco",
        titulo="Maiores variacoes de preco",
        grupo="D. Sortimento e preco",
        pergunta="Que produtos mudaram de preco, e entre quais dias observados?",
        grao="(snapshot_date, wh, source_product_id)",
        tipo="observado",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "`days_since_previous_snapshot` E OBRIGATORIO NA LEITURA. Ha lacunas medidas de "
            "ate 8 dias no catalogo; comparar uma variacao de 8 dias com uma de 1 dia sem "
            "essa coluna trata as duas como o mesmo fato. Ela viaja na consulta de proposito.",
            "`identity_ambiguous` marca id novo cujo nome ja existia na particao anterior. A "
            "fonte nao diz se e o mesmo item rechaveado ou um item retirado e outro lancado — "
            "a dimensao carrega a marca para que a escolha seja visivel em vez de herdada.",
            "Este mart tem 112 mil linhas e e o unico do painel que precisa de filtro no SQL "
            "e nao em memoria.",
        ),
        sql=f"""
            select
                snapshot_date, wh, display_name as produto, category_name as categoria,
                previous_unit_price as preco_anterior, unit_price as preco,
                price_delta as variacao, price_delta_pct as variacao_pct,
                days_since_previous_snapshot as dias_desde_o_anterior,
                change_type as tipo_de_mudanca,
                identity_ambiguous as identidade_ambigua
            from {DB}.MART.MART_PRICE_EVOLUTION
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
              and price_delta is not null and price_delta <> 0
            order by abs(price_delta) desc
            limit 200
        """,
    ),

    Indicador(
        chave="movimento_catalogo",
        titulo="Movimento do catalogo",
        grupo="D. Sortimento e preco",
        pergunta="Quantos produtos entraram, sairam, mudaram de preco ou ficaram estaveis?",
        grao="(snapshot_date, wh, source_product_id) agregado por tipo",
        tipo="observado",
        marts=("MART_PRICE_EVOLUTION",),
        armadilhas=(
            "'Saiu' significa AUSENTE DO PROXIMO SNAPSHOT OBSERVADO, nao descontinuado. Com "
            "lacuna de 7 dias na serie, a distincao importa: o produto pode ter voltado num "
            "dia que ninguem olhou.",
        ),
        sql=f"""
            select change_type as tipo_de_mudanca, count(*) as produtos,
                   count(distinct source_product_id) as produtos_distintos,
                   count(distinct snapshot_date) as dias
            from {DB}.MART.MART_PRICE_EVOLUTION
            where {FILTRO_DATA_SNAP} and {FILTRO_WH}
            group by 1 order by produtos desc
        """,
    ),

    # ================================================= E. OFERTA x DEMANDA
    Indicador(
        chave="oferta_demanda",
        titulo="Oferta x demanda, no mesmo grao",
        grupo="E. Oferta x demanda",
        pergunta="Do catalogo disponivel em cada categoria, quanto foi efetivamente pedido?",
        grao="(order_date, wh, category_id) — o grao COMUM aos dois marts",
        tipo="misto: oferta observada, demanda sintetica",
        marts=("MART_ASSORTMENT_DAILY", "MART_BASKET_DAILY"),
        armadilhas=(
            "ESTE E O UNICO CRUZAMENTO QUE OS DOIS MARTS PERMITEM SEM REAGREGACAO, e e por "
            "isso que ambos tem grao (data, wh, category_id). O join e por igualdade nas tres "
            "colunas; qualquer outro nivel exige agregar antes e o resultado passa a depender "
            "da ordem das operacoes.",
            "A DEMANDA E SINTETICA E A ESCOLHA DE PRODUTO E UNIFORME. Nenhuma fonte deste "
            "repositorio mede venda, giro ou composicao de cesta. Consequencia declarada: o "
            "mix por categoria ESPELHA O TAMANHO DO SORTIMENTO. Ler 'cobertura de demanda' "
            "como preferencia de cliente e ler a premissa de volta.",
            "Um `inner join` esconde a categoria com oferta e sem demanda, que e justamente o "
            "caso interessante. O `left join` a partir da oferta preserva o zero.",
        ),
        sql=f"""
            select
                o.category_name                                     as categoria,
                sum(o.products)                                     as produtos_ofertados,
                sum(coalesce(d.distinct_products_ordered, 0))        as produtos_pedidos,
                round(sum(coalesce(d.distinct_products_ordered, 0))
                      / nullif(sum(o.products), 0), 4)              as cobertura_demanda,
                sum(coalesce(d.lines_placed, 0))                    as linhas_pedidas,
                sum(coalesce(d.revenue_fulfilled, 0))               as receita_apurada
            from {DB}.MART.MART_ASSORTMENT_DAILY o
            left join {DB}.MART.MART_BASKET_DAILY d
                   on  d.order_date  = o.snapshot_date
                  and  d.wh          = o.wh
                  and  d.category_id = o.category_id
            where o.snapshot_date between %(inicio)s and %(fim)s
              and array_contains(o.wh::variant, split(%(armazens)s, ','))
            group by 1
            order by produtos_ofertados desc
        """,
    ),

    # ================================================= F. BASE E COBERTURA
    Indicador(
        chave="base_clientes",
        titulo="Base de clientes",
        grupo="F. Base e cobertura",
        pergunta="Como a base esta distribuida por armazem, faixa etaria e sexo?",
        grao="customer_id (versao vigente)",
        tipo="SINTETICO (a pessoa) sobre observado (o endereco)",
        marts=("MART_CUSTOMER_BASE",),
        datado=False,
        armadilhas=(
            "A PESSOA E INVENTADA; O LUGAR ONDE ELA MORA NAO. Municipio, via, CEP e faixa de "
            "numeracao vem sempre de uma linha real do Callejero. Distribuicao por idade e "
            "sexo e premissa do gerador, nao demografia — nao ha nada a concluir dela sobre "
            "o mercado espanhol.",
            "`age_at_ingestion` e a idade que o gerador sorteou, nao idade calculada contra "
            "hoje. Calcula-la contra a data corrente faria o indicador mudar sozinho a cada "
            "aniversario, sem nenhuma observacao nova.",
            "SEM EIXO DE DATA: este mart tem a versao VIGENTE de cada cliente "
            "(`where is_current`). Os filtros de periodo do painel nao se aplicam a ele.",
        ),
        sql=f"""
            select wh as armazem, age_band as faixa_etaria, sex_label as sexo,
                   count(*) as clientes,
                   round(avg(age_at_ingestion), 1) as idade_media,
                   count(distinct municipality_code) as municipios,
                   count(distinct postal_code) as ceps
            from {DB}.MART.MART_CUSTOMER_BASE
            group by 1, 2, 3
            order by 1, 2, 3
        """,
    ),

    Indicador(
        chave="cobertura_municipal",
        titulo="Cobertura municipal",
        grupo="F. Base e cobertura",
        pergunta="Quais municipios da area de atendimento tem cliente, e quais nao tem?",
        grao="(wh, province_code, municipality_code) — todos os 370 da AUF",
        tipo="misto: numerador sintetico, denominador observado",
        marts=("MART_MARKET_COVERAGE",),
        datado=False,
        armadilhas=(
            "`customers_per_10k_inhabitants` TEM NUMERADOR SINTETICO E DENOMINADOR OBSERVADO. "
            "Serve para comparar a DENSIDADE DA SIMULACAO entre municipios — nunca como "
            "estimativa de penetracao de mercado. E o indicador mais facil de citar fora de "
            "contexto de todo o painel.",
            "O mart parte dos 370 municipios da AUF, nao dos clientes, e e isso que o faz "
            "valer: partir dos clientes mostraria 100% de cobertura por construcao, sempre. "
            "`has_no_customers = true` e um resultado legitimo e informativo.",
        ),
        sql=f"""
            select wh as armazem, province_name as provincia,
                   count(*) as municipios_na_auf,
                   count_if(has_no_customers) as municipios_sem_cliente,
                   sum(customers) as clientes,
                   sum(municipality_population) as populacao_auf,
                   round(sum(customers) / nullif(sum(municipality_population), 0) * 10000, 2)
                       as clientes_por_10k
            from {DB}.MART.MART_MARKET_COVERAGE
            group by 1, 2
            order by 1
        """,
    ),
)

# ------------------------------------------------------------------------------------
# Frescor: nao e indicador de negocio, e o que torna a atualizacao VISIVEL.
# ------------------------------------------------------------------------------------
# Sem isto, uma carga nova entra e o painel muda os numeros sem dizer que mudou de base.
# Com isto, contagem e janela de cada mart aparecem na tela e a comparacao antes/depois de
# uma carga e direta.
FRESCOR = f"""
    select 'MART_ORDER_FUNNEL' as mart, count(*) as linhas,
           min(order_date)::varchar as inicio, max(order_date)::varchar as fim
      from {DB}.MART.MART_ORDER_FUNNEL
    union all select 'MART_FULFILLMENT_SLA', count(*), min(order_date)::varchar, max(order_date)::varchar
      from {DB}.MART.MART_FULFILLMENT_SLA
    union all select 'MART_BASKET_DAILY', count(*), min(order_date)::varchar, max(order_date)::varchar
      from {DB}.MART.MART_BASKET_DAILY
    union all select 'MART_ASSORTMENT_DAILY', count(*), min(snapshot_date)::varchar, max(snapshot_date)::varchar
      from {DB}.MART.MART_ASSORTMENT_DAILY
    union all select 'MART_PRICE_EVOLUTION', count(*), min(snapshot_date)::varchar, max(snapshot_date)::varchar
      from {DB}.MART.MART_PRICE_EVOLUTION
    union all select 'MART_CUSTOMER_BASE', count(*), null, null
      from {DB}.MART.MART_CUSTOMER_BASE
    union all select 'MART_MARKET_COVERAGE', count(*), null, null
      from {DB}.MART.MART_MARKET_COVERAGE
    order by mart
"""

# Limites do eixo de data, para o seletor nao oferecer periodo vazio.
JANELA = f"""
    select min(inicio)::varchar as inicio, max(fim)::varchar as fim from (
        select min(order_date) as inicio, max(order_date) as fim from {DB}.MART.MART_ORDER_FUNNEL
        union all
        select min(snapshot_date), max(snapshot_date) from {DB}.MART.MART_ASSORTMENT_DAILY
    )
"""

ARMAZENS = f"select distinct wh from {DB}.MART.MART_ORDER_FUNNEL order by wh"


# ------------------------------------------------------------------------------------
# O que NAO da para exibir, e por que. Vai para o CONTRACT e para a interface.
# ------------------------------------------------------------------------------------
# Uma lista de ausencias declaradas vale mais que um indicador inventado. Cada item diz o
# GATILHO que o destravaria, para que a conversa seja sobre o que falta e nao sobre o que
# poderia ser aproximado.
FORA_DE_ALCANCE: tuple[tuple[str, str, str], ...] = (
    ("Margem, lucro, CMV",
     "Nenhuma fonte deste repositorio tem custo. A API da Mercadona expoe preco de venda, "
     "nunca custo de aquisicao.",
     "Uma fonte de custo por produto. Sem ela, qualquer margem e inventada."),

    ("Estoque, ruptura, giro, cobertura de estoque",
     "Nao existe fato de estoque na plataforma — esta escrito no CONTRACT da source da "
     "Mercadona. As taxas de substituicao e remocao sao PREMISSA declarada, nao consequencia "
     "de um saldo. E por isso que o motivo registrado e `unavailable` e nao `out_of_stock`.",
     "Uma fonte de saldo ou movimento de estoque."),

    ("Recompra, LTV, coorte, receita por cliente, RFM",
     "NENHUM MART JUNTA CLIENTE COM PEDIDO. `MART_CUSTOMER_BASE` tem cliente sem pedido; "
     "`MART_ORDER_FUNNEL` e `MART_BASKET_DAILY` tem pedido agregado sem cliente. O elo existe "
     "em `FACT_ORDER.customer_sk`, no GOLD — que `RETAIL_READER` nao alcanca, por desenho.",
     "Um mart novo com grao de cliente e medidas de pedido (candidato: MART_CUSTOMER_ORDERS). "
     "E a lacuna mais acionavel desta lista, e nao exige fonte nova — so modelagem."),

    ("Rota, tempo de deslocamento, distancia, otimizacao de entrega",
     "Bloqueio duro e ja registrado: o Callejero nao tem coordenada nem adjacencia. O que se "
     "modela e JANELA de entrega — uma promessa comercial numa grade fixa — nunca rota.",
     "Geocodificacao. Foi recusada de proposito: inventaria posicao."),

    ("Penetracao de mercado, share, potencial por municipio",
     "Os clientes sao SINTETICOS. `customers_per_10k_inhabitants` tem numerador sintetico "
     "sobre denominador observado do INE: mede densidade da SIMULACAO, e citada fora de "
     "contexto parece market share.",
     "Uma base de clientes real. Fora de escopo declarado do projeto."),

    ("Tendencia, sazonalidade, comparativo semanal ou mensal, YoY",
     "A janela de pedidos tem 4 dias (2026-08-24 a 08-27) e a de catalogo 8 dias observados, "
     "com uma lacuna de 7 dias que NAO pode ser recuperada — a API so serve o preco de hoje. "
     "Linha de tendencia sobre 4 pontos e decoracao.",
     "Tempo. A serie cresce sozinha a cada dia que a DAG rodar."),

    ("Percentil verdadeiro do periodo",
     "O mart guarda p50/p90 por (dia, armazem). Percentil nao soma nem tira media; a media "
     "dos percentis e aproximacao e o painel a rotula como tal.",
     "Grao de pedido para o consumidor de BI — hoje so em `FACT_ORDER`, fora do alcance de "
     "`RETAIL_READER`."),
)
