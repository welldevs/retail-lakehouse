# Contrato dos indicadores do painel

**Gerado por `make dashboard-contract`.** Não editar à mão: este arquivo é derivado
de [`indicators.py`](indicators.py), que é onde a consulta e a explicação moram
juntas. Editar aqui cria o segundo lugar onde o indicador vive, e os dois divergem
no primeiro ajuste de SQL — com o detalhe cruel de que a conferência continuaria
passando, porque ninguém lê um SQL e um texto lado a lado procurando desacordo.

Deriva de `indicators.py` sha256 `889338bb5ccab785d64951fe3eebb4eee0482c465ddfcc43237cba1b181e9c36`. O cabeçalho traz o hash da origem e
**não** a data da geração: assim regerar um contrato em dia não muda um byte, e
`git diff --exit-code streamlit/CONTRACT.md` depois de `make dashboard-contract`
é a conferência de que os dois não divergiram.

## Para que serve

Conferir cada indicador **antes** de reconstruí-lo no Power BI. Para cada um:
a pergunta que responde, o grão da fonte, se o dado é observado ou sintético, o SQL
exato que o painel executa, e as **armadilhas** — os casos em que a medida óbvia
produz um número plausível e errado, que é a única classe de erro que nenhum teste
pega.

## Fronteira e credencial

| | |
|---|---|
| Destino | `RETAIL.MART` — e **somente** MART |
| Papel | `RETAIL_READER`, o papel de BI. Lê MART; recusado em GOLD e STAGE |
| Sessão | abre com `use secondary roles none` — sem isso a restrição passaria por engano |
| Autenticação | par de chaves RSA, de `~/.snowflake/config.toml`. Nenhum segredo no repo |
| Escrita | nenhuma. O painel não cria, não altera e não apaga nada |

O painel roda uma sonda ao vivo que confirma a recusa em GOLD e STAGE, porque um
painel que afirma respeitar um limite sem demonstrar está pedindo confiança.

## Parâmetros

Toda consulta com eixo de data aceita três parâmetros ligados (*bound*), nunca
concatenados — então não há como injetar nada pelo seletor da interface:

| Parâmetro | Tipo | Uso |
|---|---|---|
| `%(inicio)s` | data ISO | limite inferior, inclusivo |
| `%(fim)s` | data ISO | limite superior, inclusivo |
| `%(armazens)s` | lista por vírgula | `array_contains(wh::variant, split(%(armazens)s, ','))` |

Os indicadores marcados **sem eixo de data** ignoram `inicio`/`fim`: trazem a versão
vigente.

## Índice

- **A. Comercial**
  - [Resumo comercial](#resumo_comercial)
  - [Funil de conversao, por marco alcancado](#funil)
  - [Por onde o pedido escapa](#vazamento)
  - [Perda de valor, decomposta por CAUSA](#decomposicao_perda)
  - [Serie diaria: pedidos e receita](#serie_diaria)
- **B. Operacao**
  - [SLA de separacao: limiar, maximo e violacoes](#sla_separacao)
  - [Tempo por etapa (p50 / p90)](#percentis_etapa)
  - [Janela de entrega: antes, dentro, depois](#janela_entrega)
- **C. Cesta e categoria**
  - [Receita por categoria](#receita_categoria)
  - [Substituicao e remocao, por categoria](#substituicao_categoria)
  - [Perfil de consumo por faixa etaria do comprador](#perfil_por_faixa)
  - [Pedidos por armazem, e a intensidade regional que os separa](#pedidos_por_regiao)
- **D. Sortimento e preco**
  - [Sortimento por armazem](#sortimento_armazem)
  - [Maiores variacoes de preco](#variacao_preco)
  - [Movimento do catalogo](#movimento_catalogo)
- **E. Oferta x demanda**
  - [Oferta x demanda, no mesmo grao](#oferta_demanda)
- **F. Base e cobertura**
  - [Base de clientes](#base_clientes)
  - [Cobertura municipal](#cobertura_municipal)
- **G. Estoque e reposicao**
  - [Cobertura de estoque por categoria](#cobertura_estoque)
  - [Ruptura: unidades e series afetadas](#ruptura_estoque)
  - [Reposicao: ordens disparadas](#reposicao_estoque)
  - [Giro diario por categoria](#giro_estoque)
- [O que o painel NÃO exibe](#o-que-o-painel-não-exibe)

## A. Comercial

<a id="resumo_comercial"></a>
### Resumo comercial

| | |
|---|---|
| Chave | `resumo_comercial` |
| Pergunta | Quantos pedidos entraram, quantos chegaram ao cliente, e quanto foi apurado? |
| Grão da fonte | `(order_date, wh) agregado para o total do periodo` |
| Tipo do dado | sintetico (o pedido) sobre observado (cliente, produto, preco) |
| Marts | `MART_ORDER_FUNNEL` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. TICKET MEDIO tem dois denominadores possiveis e eles NAO sao equivalentes: receita/pedidos_separados = 95,21 e receita/pedidos_colocados = 90,95. O segundo divide a receita de quem foi separado pelo total incluindo quem nunca chegou a separacao — mede uma coisa que nao existe. Use `orders_picked`.
2. `net_amount_picked` e NULO para pedido que morreu antes da separacao, e `sum()` ignora nulo. Isso e correto e proposital: quem nunca foi separado nao contribui com zero, contribui com nada. No Power BI, um `SUM` sobre coluna nula faz o mesmo; um `COALESCE(...,0)` inventaria uma apuracao que nao houve.

```sql
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
            from RETAIL.MART.MART_ORDER_FUNNEL
            where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
```

<a id="funil"></a>
### Funil de conversao, por marco alcancado

| | |
|---|---|
| Chave | `funil` |
| Pergunta | De cada 100 pedidos colocados, quantos atravessaram cada etapa? |
| Grão da fonte | `(order_date, wh) agregado; uma linha por ETAPA` |
| Tipo do dado | derivado de contagem de marco |
| Marts | `MART_ORDER_FUNNEL` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. AS ETAPAS SAO CONTADAS POR MARCO ALCANCADO, nunca por status. `order_status` guarda o estado do ULTIMO evento: um pedido devolvido tem status RETURNED e FOI entregue. Medido nesta base: contar status='DELIVERED' da 5.985; contar delivered_at is not null da 6.046 — os 61 devolvidos. Um funil sobre status publica uma taxa de entrega 1% menor que a real e nada reprova.
2. Marco e monotonico (uma vez alcancado, nao volta atras); status nao e. O mart ja resolve isso — as colunas `orders_*` sao contagens de marco. No Power BI, NAO reconstrua o funil a partir de um campo de status.

```sql
with total as (
                select
                    sum(orders_placed)          as colocado,
                    sum(orders_confirmed)       as confirmado,
                    sum(orders_picking_started) as separacao_iniciada,
                    sum(orders_picked)          as separado,
                    sum(orders_dispatched)      as despachado,
                    sum(orders_delivered)       as entregue
                from RETAIL.MART.MART_ORDER_FUNNEL
                where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            )
            select 1 as ordem, 'Colocado'            as etapa, colocado            as pedidos, 1.0 as taxa from total
            union all select 2, 'Pagamento aprovado', confirmado,         round(confirmado/nullif(colocado,0),4)         from total
            union all select 3, 'Separacao iniciada', separacao_iniciada, round(separacao_iniciada/nullif(colocado,0),4) from total
            union all select 4, 'Separado',           separado,           round(separado/nullif(colocado,0),4)           from total
            union all select 5, 'Despachado',         despachado,         round(despachado/nullif(colocado,0),4)         from total
            union all select 6, 'Entregue',           entregue,           round(entregue/nullif(colocado,0),4)           from total
            order by ordem
```

<a id="vazamento"></a>
### Por onde o pedido escapa

| | |
|---|---|
| Chave | `vazamento` |
| Pergunta | Quantos pedidos sairam do funil, e por qual motivo? |
| Grão da fonte | `(order_date, wh) agregado; uma linha por motivo de saida` |
| Tipo do dado | derivado de contagem de marco |
| Marts | `MART_ORDER_FUNNEL` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. ESTAS CONTAGENS NAO SOMAM COM AS ETAPAS DO FUNIL. `orders_returned` conta quem saiu DEPOIS de atravessar o funil inteiro; somar as saidas as etapas contaria os devolvidos duas vezes. No Power BI, mantenha os dois blocos separados e nunca monte um 'total de pedidos' somando etapas com saidas.

```sql
select 'Pagamento recusado' as motivo, sum(orders_payment_failed)  as pedidos,
                   round(sum(orders_payment_failed)/nullif(sum(orders_placed),0),4) as sobre_colocados
              from RETAIL.MART.MART_ORDER_FUNNEL where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            union all
            select 'Cancelado', sum(orders_cancelled),
                   round(sum(orders_cancelled)/nullif(sum(orders_placed),0),4)
              from RETAIL.MART.MART_ORDER_FUNNEL where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            union all
            select 'Entrega falhou', sum(orders_delivery_failed),
                   round(sum(orders_delivery_failed)/nullif(sum(orders_placed),0),4)
              from RETAIL.MART.MART_ORDER_FUNNEL where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            union all
            select 'Devolvido (apos entrega)', sum(orders_returned),
                   round(sum(orders_returned)/nullif(sum(orders_placed),0),4)
              from RETAIL.MART.MART_ORDER_FUNNEL where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            order by pedidos desc
```

<a id="decomposicao_perda"></a>
### Perda de valor, decomposta por CAUSA

| | |
|---|---|
| Chave | `decomposicao_perda` |
| Pergunta | O que foi colocado e nao foi apurado — e por que nao foi? |
| Grão da fonte | `(order_date, wh) agregado` |
| Tipo do dado | derivado |
| Marts | `MART_ORDER_FUNNEL` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. ESTE E O INDICADOR MAIS FACIL DE ERRAR DO PAINEL, e o erro produz um numero plausivel. `SUM(gross) - SUM(net)` da 58.327,81 e MISTURA duas perdas com causas opostas. `SUM(amount_delta)` da 4.834,73 — e nao e a mesma coisa, nem esta errado: `amount_delta` so existe para pedido SEPARADO, e `sum()` ignora o nulo dos outros 298.
2. A decomposicao correta, verificada aritmeticamente (4.834,73 + 53.493,08 = 58.327,81): perda na SEPARACAO = sum(amount_delta), a cesta encolheu com remocao e substituicao; perda por PEDIDO MORTO = o resto, o valor integral de quem nunca chegou a separacao. Sao problemas de areas diferentes — uma e operacao de loja, a outra e pagamento e cancelamento — e um numero unico esconde qual esta acontecendo.
3. OBSERVACAO NAO EXPLICADA, registrada em vez de omitida: o pedido morto vale em media 179,51 contra 135,36 do separado (valor colocado nos dois casos), sobre 298 pedidos. Uma hipotese compativel e que cesta maior leva mais tempo para separar (`minutes_per_line_picked` x linhas) e portanto oferece uma janela maior para o cancelamento chegar antes. NAO da para confirmar pelo MART — nao ha grao de pedido aqui — entao fica como pergunta, nao como conclusao.

```sql
with t as (
                select sum(gross_amount_placed) as colocado,
                       sum(net_amount_picked)   as apurado,
                       sum(amount_delta)        as delta_separacao,
                       sum(orders_placed)       as pedidos,
                       sum(orders_picked)       as separados
                from RETAIL.MART.MART_ORDER_FUNNEL
                where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
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
```

<a id="serie_diaria"></a>
### Serie diaria: pedidos e receita

| | |
|---|---|
| Chave | `serie_diaria` |
| Pergunta | Como pedidos e receita se movem dia a dia, por armazem? |
| Grão da fonte | `(order_date, wh) — o grao nativo do mart` |
| Tipo do dado | misto |
| Marts | `MART_ORDER_FUNNEL` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. A JANELA ATUAL TEM 4 DIAS. Nao ha tendencia, sazonalidade nem comparativo semanal a extrair disso — qualquer linha de tendencia sobre 4 pontos e decoracao. O eixo existe para que a serie CRESCA, e cresce a cada dia que a DAG rodar.

```sql
select order_date, wh,
                   orders_placed, orders_delivered, net_amount_picked, amount_delta,
                   round(net_amount_picked/nullif(orders_picked,0),2) as ticket_medio,
                   delivery_rate
            from RETAIL.MART.MART_ORDER_FUNNEL
            where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            order by order_date, wh
```

## B. Operacao

<a id="sla_separacao"></a>
### SLA de separacao: limiar, maximo e violacoes

| | |
|---|---|
| Chave | `sla_separacao` |
| Pergunta | Quantos pedidos estouraram o limiar declarado de separacao? |
| Grão da fonte | `(order_date, wh) agregado` |
| Tipo do dado | derivado sobre premissa declarada (sintetica) |
| Marts | `MART_FULFILLMENT_SLA` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. AS TRES COLUNAS SO SIGNIFICAM ALGO JUNTAS, e publica-las separadas e o erro. `orders_breaching_sla` = 0, e nao porque a operacao seja boa: o limiar declarado e 90 minutos e o teto ARITMETICO da separacao e 80 (`basket_lines_max` 40 x `minutes_per_line_picked` 2). O maximo observado e exatamente 80. As tres premissas nao se cruzam, e o zero e consequencia disso.
2. O limiar vem de `FACT_ORDER_PREMISE`, que veio do seed que o GERADOR leu, cujo sha256 esta no manifesto de cada particao do RAW. No Power BI, NAO cravar 90 num measure: leia a coluna `sla_minutes`. Cravar cria a segunda copia do numero, e no dia em que o seed mudar o painel passa a medir contra um limiar que nenhum pedido conheceu — sem reprovar nada, porque zero contra o limiar errado tem a mesma aparencia de zero contra o certo.

```sql
select
                max(sla_minutes)            as limiar_declarado_min,
                max(max_picking_minutes)    as maximo_observado_min,
                sum(orders_breaching_sla)   as violacoes,
                sum(orders_with_pick)       as pedidos_com_separacao
            from RETAIL.MART.MART_FULFILLMENT_SLA
            where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
```

<a id="percentis_etapa"></a>
### Tempo por etapa (p50 / p90)

| | |
|---|---|
| Chave | `percentis_etapa` |
| Pergunta | Quanto tempo cada etapa leva, no meio e na cauda? |
| Grão da fonte | `(order_date, wh); mediana das medianas quando agregado — ver armadilhas` |
| Tipo do dado | derivado |
| Marts | `MART_FULFILLMENT_SLA` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. PERCENTIL NAO SOMA E NAO TIRA MEDIA. O mart guarda p50/p90 por (dia, armazem); o painel mostra a MEDIA desses percentis quando ha mais de uma linha, e isso e uma aproximacao, nao o percentil do conjunto. Para o percentil verdadeiro do periodo seria preciso o grao de pedido, que vive em `FACT_ORDER` — fora do alcance de `RETAIL_READER`, por desenho. O rotulo da coluna diz `media_p90` justamente para nao se passar pelo p90.
2. Os percentis sao calculados sobre os pedidos que ALCANCARAM cada marco (`percentile_cont` ignora nulo). Um p90 de entrega que contasse os cancelados como zero mediria a operacao de outra empresa. As colunas `orders_with_*` dizem sobre quantos pedidos cada percentil foi calculado.

```sql
select 1 as ordem, 'Colocado -> pagamento' as etapa,
                   round(avg(p50_minutes_to_confirm),1) as media_p50,
                   round(avg(p90_minutes_to_confirm),1) as media_p90,
                   sum(orders_with_confirm)             as pedidos
              from RETAIL.MART.MART_FULFILLMENT_SLA where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            union all
            select 2, 'Separacao (inicio -> fim)', round(avg(p50_minutes_to_pick),1),
                   round(avg(p90_minutes_to_pick),1), sum(orders_with_pick)
              from RETAIL.MART.MART_FULFILLMENT_SLA where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            union all
            select 3, 'Separado -> despachado', round(avg(p50_minutes_to_dispatch),1),
                   round(avg(p90_minutes_to_dispatch),1), sum(orders_with_pick)
              from RETAIL.MART.MART_FULFILLMENT_SLA where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            union all
            select 4, 'Despachado -> entregue', round(avg(p50_minutes_to_deliver),1),
                   round(avg(p90_minutes_to_deliver),1), sum(orders_with_deliver)
              from RETAIL.MART.MART_FULFILLMENT_SLA where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            union all
            select 5, 'Ciclo total (colocado -> entregue)',
                   round(avg(p50_minutes_placed_to_delivered),1),
                   round(avg(p90_minutes_placed_to_delivered),1), sum(orders_with_deliver)
              from RETAIL.MART.MART_FULFILLMENT_SLA where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            order by ordem
```

<a id="janela_entrega"></a>
### Janela de entrega: antes, dentro, depois

| | |
|---|---|
| Chave | `janela_entrega` |
| Pergunta | A entrega aconteceu dentro da janela prometida ao cliente? |
| Grão da fonte | `(order_date, wh) agregado; uma linha por resultado` |
| Tipo do dado | derivado |
| Marts | `MART_FULFILLMENT_SLA` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. CHEGAR CEDO E CHEGAR TARDE SAO PROBLEMAS OPOSTOS, e uma taxa unica de 'aderencia' apaga qual deles esta acontecendo. Publique sempre a direcao: uma adesao baixa convida a concluir 'a operacao atrasa', e ja foi medido neste projeto o caso oposto — em 2026-09-01, 84% das entregas chegavam ANTES de a janela abrir.
2. ESSA MEDICAO DE 2026-09-01 ERA DEFEITO DE MODELO, e nao da operacao: `slot_lead_hours` sorteava o inicio da janela entre 2h e 24h depois da colocacao, enquanto a soma dos marcos entregava em no maximo 8,5h. As duas premissas eram DECLARADAS SEPARADAMENTE e nunca conciliadas. Corrigido na Fase 7: `slot_lead_hours_*` passou a ser DERIVADO do ciclo declarado no mesmo seed (1h a 8h, contra 2h a 24h), e o teste `assert_order_premises_are_internally_coherent` afere a DERIVACAO — nunca a adesao, para que ninguem ajuste o numero ate o KPI agradar.
3. No Power BI, publique as TRES contagens. Se um unico indicador for exigido, use 'entregas fora da janela' com o detalhe de direcao ao lado.

```sql
select 1 as ordem, 'Antes de a janela abrir' as resultado,
                   sum(orders_delivered_before_slot) as entregas
              from RETAIL.MART.MART_FULFILLMENT_SLA where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            union all
            select 2, 'Dentro da janela', sum(orders_delivered_within_slot)
              from RETAIL.MART.MART_FULFILLMENT_SLA where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            union all
            select 3, 'Depois de a janela fechar', sum(orders_delivered_after_slot)
              from RETAIL.MART.MART_FULFILLMENT_SLA where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            order by ordem
```

## C. Cesta e categoria

<a id="receita_categoria"></a>
### Receita por categoria

| | |
|---|---|
| Chave | `receita_categoria` |
| Pergunta | Quais categorias respondem pela receita apurada? |
| Grão da fonte | `(order_date, wh, category_id) agregado por categoria` |
| Tipo do dado | misto |
| Marts | `MART_BASKET_DAILY` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. `orders_touching_category` NAO E ADITIVO entre categorias: um pedido com leite e pao conta uma vez em cada. Somar as 151 categorias de um dia da muito mais que os 1.600 pedidos daquele dia. Linhas, unidades e valor SAO aditivos, porque cada linha pertence a exatamente uma categoria. Para contagem de pedidos use MART_ORDER_FUNNEL, que tem o grao certo.
2. `revenue_fulfilled` e o que foi ENTREGUE; `revenue_placed` e o que foi pedido. A diferenca (`revenue_lost`) e remocao mais pedido nunca separado. Nao troque um pelo outro num grafico de 'receita' sem dizer qual.
3. A CATEGORIA E A DO MOMENTO DO PEDIDO, gravada no proprio evento, e nao a que o produto tem hoje. E o comportamento correto para receita historica, e difere de um join contra a dimensao corrente.

```sql
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
            from RETAIL.MART.MART_BASKET_DAILY
            where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            group by 1, 2
            order by receita_apurada desc nulls last
```

<a id="substituicao_categoria"></a>
### Substituicao e remocao, por categoria

| | |
|---|---|
| Chave | `substituicao_categoria` |
| Pergunta | Onde a cesta muda mais entre o pedido e a entrega? |
| Grão da fonte | `(order_date, wh, category_id) agregado por categoria` |
| Tipo do dado | derivado sobre premissa declarada (sintetica) |
| Marts | `MART_BASKET_DAILY` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. AS TAXAS SAO PREMISSA, NAO OBSERVACAO. `substitution_rate` (0,04) e `removal_rate` (0,02) foram DECLARADAS no seed do gerador; nenhuma fonte deste repositorio mede disponibilidade. A variacao entre categorias e ruido de amostragem sobre uma taxa constante, nao um sinal de sortimento. Um painel que ranqueia categorias por 'risco de ruptura' com este dado esta inventando.
2. `unavailable` e o motivo registrado, e NAO `out_of_stock`: nao existe fato de estoque nesta plataforma. O vocabulario e deliberado — nomear como estoque prometeria um dado que ninguem mediu.
3. Recalcule a taxa a partir das SOMAS (linhas_substituidas / linhas_pedidas). Tirar media das taxas por dia-armazem-categoria pondera cada celula igualmente, independente do tamanho — o classico paradoxo de Simpson num painel.

```sql
select
                category_name                                       as categoria,
                sum(lines_placed)                                   as linhas_pedidas,
                sum(lines_substituted)                              as linhas_substituidas,
                sum(lines_removed)                                  as linhas_removidas,
                sum(lines_never_picked)                             as linhas_nunca_separadas,
                round(sum(lines_substituted)/nullif(sum(lines_placed),0), 4) as taxa_substituicao,
                round(sum(lines_removed)    /nullif(sum(lines_placed),0), 4) as taxa_remocao
            from RETAIL.MART.MART_BASKET_DAILY
            where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            group by 1
            having sum(lines_placed) >= 100
            order by taxa_substituicao desc
```

<a id="perfil_por_faixa"></a>
### Perfil de consumo por faixa etaria do comprador

| | |
|---|---|
| Chave | `perfil_por_faixa` |
| Pergunta | O que cada faixa etaria leva, e onde ela difere mais das outras? |
| Grão da fonte | `(order_date, wh, buyer_age_band, demand_group) agregado por faixa e grupo` |
| Tipo do dado | sintetico calibrado contra benchmark (MAPA 2025) |
| Marts | `MART_DEMAND_COHORT` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. O AGREGADO NAO MUDA ENTRE FAIXAS, DE PROPOSITO. A calibracao por coorte e neutra no total — um IPF garante que a media ponderada dos pesos por coorte reproduz o mix agregado. Procurar o efeito desta camada num total nao encontra nada; ele esta inteiro na comparacao ENTRE faixas da mesma linha.
2. COMPARE FATIA, NUNCA CONTAGEM. As quatro faixas tem tamanhos diferentes na base (35_49 e a maior, LT35 a menor), entao 'linhas por faixa' mede o tamanho da coorte e nao a propensao dela. `share_within_band` ja tem a propria coorte no denominador; e ela que isola as duas coisas.
3. A PROPENSAO E BENCHMARK, NAO OBSERVACAO DESTA LOJA. Os indices vem do consumo domestico espanhol medido pelo MAPA, e o `% Poblacion` de la e a populacao que VIVE EM LARES com responsavel naquela faixa — nao a populacao daquela idade. Por isso o numero entra como indice relativo, e nunca como share absoluto.
4. NO_FOOD e SIN_BENCHMARK aparecem com razao proxima de 1 por CONSTRUCAO: o informe nao mede drogaria nem limpeza, o indice deles e neutro e a fatia de cada bloco e mantida constante entre coortes. Ler isso como 'todas as idades compram xampu igual' seria transformar ausencia de medicao em medicao.

```sql
with por_faixa as (
                select
                    buyer_age_band,
                    demand_group,
                    sum(lines_placed)                               as linhas
                from RETAIL.MART.MART_DEMAND_COHORT
                where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
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
```

<a id="pedidos_por_regiao"></a>
### Pedidos por armazem, e a intensidade regional que os separa

| | |
|---|---|
| Chave | `pedidos_por_regiao` |
| Pergunta | Por que bcn1 coloca mais pedidos que mad1, se as bases tem o mesmo tamanho? |
| Grão da fonte | `(order_date, wh) agregado por armazem` |
| Tipo do dado | sintetico inclinado por consumo per capita observado (MAPA, secao 3) |
| Marts | `MART_DEMAND_COHORT` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. A DIFERENCA E DELIBERADA E OBSERVADA. Ate a fase anterior os quatro armazens tinham a mesma contagem por construcao. O informe mede consumo per capita por comunidade autonoma — Cataluna 620,82 kg-L por pessoa e ano contra 505,86 de Madrid — e essa razao passou a pesar QUANTOS clientes pedem.
2. A INTENSIDADE VIRA FREQUENCIA, E ISSO E ESCOLHA DECLARADA. O informe da kg por ano e NAO publica frequencia de compra domestica; repartir a intensidade entre frequencia e tamanho de cesta seria inventar a reparticao. Ler estes numeros como 'catalao compra mais vezes' e ler a premissa, nao uma medicao.
3. O TOTAL DA JANELA NAO MUDA por causa desta inclinacao: o indice e renormalizado sobre as quatro comunidades servidas. O que ela move e a REPARTICAO entre armazens, nunca a soma.

```sql
select
                wh                                                  as armazem,
                count(distinct order_date)                          as dias,
                sum(lines_placed)                                   as linhas,
                sum(units_placed)                                   as unidades,
                round(sum(revenue_fulfilled), 2)                    as receita,
                max(currency)                                       as moeda
            from RETAIL.MART.MART_DEMAND_COHORT
            where order_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            group by 1
            order by linhas desc
```

## D. Sortimento e preco

<a id="sortimento_armazem"></a>
### Sortimento por armazem

| | |
|---|---|
| Chave | `sortimento_armazem` |
| Pergunta | Quanto catalogo cada armazem tem, e quanto disso e exclusivo dele? |
| Grão da fonte | `(snapshot_date, wh, category_id) agregado por armazem` |
| Tipo do dado | observado |
| Marts | `MART_ASSORTMENT_DAILY` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. SORTIMENTO E A PRESENCA DA LINHA no fato de preco, nao uma tabela propria. Uma linha afirma 'este produto estava no catalogo deste armazem neste dia'. Cuidado ao ler ausencia: pode ser produto fora do catalogo OU dia nao observado — os dias 2026-08-17 a 08-23 nao existem e nao podem ser recuperados, porque a API so serve o preco de hoje.
2. `products_in_all_warehouses` e o denominador honesto de qualquer comparacao entre armazens: comparar preco medio de catalogos diferentes mede a diferenca de CATALOGO, nao de preco.
3. A JANELA DESTE MART E MAIOR que a de pedidos (catalogo desde 2026-08-15, pedidos desde 08-24). Cruzar os dois sem alinhar a data compara periodos diferentes.

```sql
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
                from RETAIL.MART.MART_ASSORTMENT_DAILY
                where snapshot_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
                group by 1, 2
            )
            group by 1
            order by 1
```

<a id="variacao_preco"></a>
### Maiores variacoes de preco

| | |
|---|---|
| Chave | `variacao_preco` |
| Pergunta | Que produtos mudaram de preco, e entre quais dias observados? |
| Grão da fonte | `(snapshot_date, wh, source_product_id)` |
| Tipo do dado | observado |
| Marts | `MART_PRICE_EVOLUTION` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. `days_since_previous_snapshot` E OBRIGATORIO NA LEITURA. Ha lacunas medidas de ate 8 dias no catalogo; comparar uma variacao de 8 dias com uma de 1 dia sem essa coluna trata as duas como o mesmo fato. Ela viaja na consulta de proposito.
2. `identity_ambiguous` marca id novo cujo nome ja existia na particao anterior. A fonte nao diz se e o mesmo item rechaveado ou um item retirado e outro lancado — a dimensao carrega a marca para que a escolha seja visivel em vez de herdada.
3. Este mart tem 112 mil linhas e e o unico do painel que precisa de filtro no SQL e nao em memoria.

```sql
select
                snapshot_date, wh, display_name as produto, category_name as categoria,
                previous_unit_price as preco_anterior, unit_price as preco,
                price_delta as variacao, price_delta_pct as variacao_pct,
                days_since_previous_snapshot as dias_desde_o_anterior,
                change_type as tipo_de_mudanca,
                identity_ambiguous as identidade_ambigua
            from RETAIL.MART.MART_PRICE_EVOLUTION
            where snapshot_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
              and price_delta is not null and price_delta <> 0
            order by abs(price_delta) desc
            limit 200
```

<a id="movimento_catalogo"></a>
### Movimento do catalogo

| | |
|---|---|
| Chave | `movimento_catalogo` |
| Pergunta | Quantos produtos entraram, sairam, mudaram de preco ou ficaram estaveis? |
| Grão da fonte | `(snapshot_date, wh, source_product_id) agregado por tipo` |
| Tipo do dado | observado |
| Marts | `MART_PRICE_EVOLUTION` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. 'Saiu' significa AUSENTE DO PROXIMO SNAPSHOT OBSERVADO, nao descontinuado. Com lacuna de 7 dias na serie, a distincao importa: o produto pode ter voltado num dia que ninguem olhou.

```sql
select change_type as tipo_de_mudanca, count(*) as produtos,
                   count(distinct source_product_id) as produtos_distintos,
                   count(distinct snapshot_date) as dias
            from RETAIL.MART.MART_PRICE_EVOLUTION
            where snapshot_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            group by 1 order by produtos desc
```

## E. Oferta x demanda

<a id="oferta_demanda"></a>
### Oferta x demanda, no mesmo grao

| | |
|---|---|
| Chave | `oferta_demanda` |
| Pergunta | Do catalogo disponivel em cada categoria, quanto foi efetivamente pedido? |
| Grão da fonte | `(order_date, wh, category_id) — o grao COMUM aos dois marts` |
| Tipo do dado | misto: oferta observada, demanda sintetica |
| Marts | `MART_ASSORTMENT_DAILY`, `MART_BASKET_DAILY` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. ESTE E O UNICO CRUZAMENTO QUE OS DOIS MARTS PERMITEM SEM REAGREGACAO, e e por isso que ambos tem grao (data, wh, category_id). O join e por igualdade nas tres colunas; qualquer outro nivel exige agregar antes e o resultado passa a depender da ordem das operacoes.
2. A DEMANDA E SINTETICA E A ESCOLHA DE PRODUTO E UNIFORME. Nenhuma fonte deste repositorio mede venda, giro ou composicao de cesta. Consequencia declarada: o mix por categoria ESPELHA O TAMANHO DO SORTIMENTO. Ler 'cobertura de demanda' como preferencia de cliente e ler a premissa de volta.
3. Um `inner join` esconde a categoria com oferta e sem demanda, que e justamente o caso interessante. O `left join` a partir da oferta preserva o zero.

```sql
select
                o.category_name                                     as categoria,
                sum(o.products)                                     as produtos_ofertados,
                sum(coalesce(d.distinct_products_ordered, 0))        as produtos_pedidos,
                round(sum(coalesce(d.distinct_products_ordered, 0))
                      / nullif(sum(o.products), 0), 4)              as cobertura_demanda,
                sum(coalesce(d.lines_placed, 0))                    as linhas_pedidas,
                sum(coalesce(d.revenue_fulfilled, 0))               as receita_apurada
            from RETAIL.MART.MART_ASSORTMENT_DAILY o
            left join RETAIL.MART.MART_BASKET_DAILY d
                   on  d.order_date  = o.snapshot_date
                  and  d.wh          = o.wh
                  and  d.category_id = o.category_id
            where o.snapshot_date between %(inicio)s and %(fim)s
              and array_contains(o.wh::variant, split(%(armazens)s, ','))
            group by 1
            order by produtos_ofertados desc
```

## F. Base e cobertura

<a id="base_clientes"></a>
### Base de clientes

| | |
|---|---|
| Chave | `base_clientes` |
| Pergunta | Como a base esta distribuida por armazem, faixa etaria e sexo? |
| Grão da fonte | `customer_id (versao vigente)` |
| Tipo do dado | SINTETICO (a pessoa) sobre observado (o endereco) |
| Marts | `MART_CUSTOMER_BASE` |
| Eixo de data | **não** — versão vigente |

**Armadilhas ao reconstruir no Power BI**

1. A PESSOA E INVENTADA; O LUGAR ONDE ELA MORA NAO. Municipio, via, CEP e faixa de numeracao vem sempre de uma linha real do Callejero. Distribuicao por idade e sexo e premissa do gerador, nao demografia — nao ha nada a concluir dela sobre o mercado espanhol.
2. `age_at_ingestion` e a idade que o gerador sorteou, nao idade calculada contra hoje. Calcula-la contra a data corrente faria o indicador mudar sozinho a cada aniversario, sem nenhuma observacao nova.
3. SEM EIXO DE DATA: este mart tem a versao VIGENTE de cada cliente (`where is_current`). Os filtros de periodo do painel nao se aplicam a ele.

```sql
select wh as armazem, age_band as faixa_etaria, sex_label as sexo,
                   count(*) as clientes,
                   round(avg(age_at_ingestion), 1) as idade_media,
                   count(distinct municipality_code) as municipios,
                   count(distinct postal_code) as ceps
            from RETAIL.MART.MART_CUSTOMER_BASE
            group by 1, 2, 3
            order by 1, 2, 3
```

<a id="cobertura_municipal"></a>
### Cobertura municipal

| | |
|---|---|
| Chave | `cobertura_municipal` |
| Pergunta | Quais municipios da area de atendimento tem cliente, e quais nao tem? |
| Grão da fonte | `(wh, province_code, municipality_code) — todos os 370 da AUF` |
| Tipo do dado | misto: numerador sintetico, denominador observado |
| Marts | `MART_MARKET_COVERAGE` |
| Eixo de data | **não** — versão vigente |

**Armadilhas ao reconstruir no Power BI**

1. `customers_per_10k_inhabitants` TEM NUMERADOR SINTETICO E DENOMINADOR OBSERVADO. Serve para comparar a DENSIDADE DA SIMULACAO entre municipios — nunca como estimativa de penetracao de mercado. E o indicador mais facil de citar fora de contexto de todo o painel.
2. O mart parte dos 370 municipios da AUF, nao dos clientes, e e isso que o faz valer: partir dos clientes mostraria 100% de cobertura por construcao, sempre. `has_no_customers = true` e um resultado legitimo e informativo.

```sql
select wh as armazem, province_name as provincia,
                   count(*) as municipios_na_auf,
                   count_if(has_no_customers) as municipios_sem_cliente,
                   sum(customers) as clientes,
                   sum(municipality_population) as populacao_auf,
                   round(sum(customers) / nullif(sum(municipality_population), 0) * 10000, 2)
                       as clientes_por_10k
            from RETAIL.MART.MART_MARKET_COVERAGE
            group by 1, 2
            order by 1
```

## G. Estoque e reposicao

<a id="cobertura_estoque"></a>
### Cobertura de estoque por categoria

| | |
|---|---|
| Chave | `cobertura_estoque` |
| Pergunta | Quantos dias de demanda o estoque de cada categoria ainda cobre? |
| Grão da fonte | `(stock_date, wh, category_id) — uma linha por categoria de nivel 2, por dia` |
| Tipo do dado | derivado: consumo observado, politica sintetica |
| Marts | `MART_STOCK_HEALTH` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. SALDO NAO SOMA ENTRE DIAS. `closing_units` de segunda mais o de terca nao e o estoque da semana — e o mesmo estoque contado duas vezes. Somar entre PRODUTOS dentro do dia esta certo; somar num eixo de tempo nunca esta. E a primeira coisa que um BI vai tentar.
2. AS DUAS COBERTURAS RESPONDEM PERGUNTAS DIFERENTES e o painel publica as duas de proposito. `days_of_cover` e a razao das somas: quantos dias o estoque DA CATEGORIA cobre a demanda dela. `days_of_cover_typical_product` e a media das razoes: quantos dias o produto TIPICO cobre. A segunda e sempre maior, porque produto de giro baixo tem cobertura enorme e domina a media.
3. COBERTURA E RAZAO, ENTAO NAO SE MEDIA de novo. Tirar `avg(days_of_cover)` sobre categorias produz a media de uma media e nao corresponde a nenhum estoque real.

```sql
select stock_date as dia, wh as armazem, category_name as categoria,
                   sum(closing_units) as unidades_em_estoque,
                   round(sum(closing_units) / nullif(sum(units_demanded), 0), 2)
                       as dias_de_cobertura,
                   round(avg(days_of_cover_typical_product), 2)
                       as cobertura_do_produto_tipico,
                   sum(product_days) as pares_produto_dia
            from RETAIL.MART.MART_STOCK_HEALTH
            where stock_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            group by 1, 2, 3
            order by dias_de_cobertura asc nulls last
```

<a id="ruptura_estoque"></a>
### Ruptura: unidades e series afetadas

| | |
|---|---|
| Chave | `ruptura_estoque` |
| Pergunta | Quanto a demanda pediu que a prateleira nao tinha, e em quantos produtos? |
| Grão da fonte | `(stock_date, wh, category_id)` |
| Tipo do dado | derivado: consumo observado, politica sintetica |
| Marts | `MART_STOCK_HEALTH` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. AS DUAS UNIDADES NAO SE SUBSTITUEM. `units_short` diz QUANTO faltou; `series_with_shortfall` diz em quantos (produto, dia) faltou alguma coisa. Um produto popular faltando 500 unidades e 500 produtos faltando 1 sao problemas operacionais diferentes com o mesmo `units_short`. Publique os dois.
2. `fill_rate` E NULO QUANDO NAO HOUVE DEMANDA, e nao 1. Uma categoria sem pedido no dia nao teve 100% de atendimento — nao teve pedido. Um BI que converta esse nulo em 1 sobe a media de atendimento com dias em que nada aconteceu.
3. ESTA RUPTURA E INDEPENDENTE DAS LINHAS `unavailable` DO PEDIDO. O gerador remove linhas a uma taxa FIXA sorteada, sem olhar saldo; este ledger calcula falta a partir do saldo. Uma nao causa a outra, e cruza-las como se causassem produziria uma correlacao inventada. Gatilho para unificar: um gerador de segunda passada que releia o saldo do dia anterior.

```sql
select stock_date as dia, wh as armazem, category_name as categoria,
                   sum(units_demanded) as unidades_pedidas,
                   sum(units_fulfilled) as unidades_atendidas,
                   sum(units_short) as unidades_em_falta,
                   sum(series_with_shortfall) as produtos_com_falta,
                   sum(product_days) as produtos_no_dia,
                   round(sum(units_fulfilled) / nullif(sum(units_demanded), 0), 4)
                       as taxa_de_atendimento
            from RETAIL.MART.MART_STOCK_HEALTH
            where stock_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            group by 1, 2, 3
            having sum(units_demanded) > 0
            order by unidades_em_falta desc
```

<a id="reposicao_estoque"></a>
### Reposicao: ordens disparadas

| | |
|---|---|
| Chave | `reposicao_estoque` |
| Pergunta | Quantas ordens de compra a politica disparou, e de quantas unidades? |
| Grão da fonte | `(stock_date, wh, category_id)` |
| Tipo do dado | derivado: consumo observado, politica sintetica |
| Marts | `MART_STOCK_HEALTH` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. ORDEM EMITIDA NAO E ORDEM CHEGADA. Ela chega `supplier_lead_days` dias depois, e uma ordem emitida perto do fim da janela NUNCA aparece como chegada — o pedido em transito no fim do periodo e propriedade real de qualquer ledger. Comparar ordens com chegadas no mesmo periodo e a leitura errada mais provavel deste indicador.
2. A POLITICA E SINTETICA E ESTA DECLARADA. `reorder_point_days`, `reorder_target_days` e `supplier_lead_days` vem de um seed, nao de negociacao com fornecedor nenhum. O numero de ordens e consequencia direta deles.
3. UMA ORDEM EM ABERTO POR VEZ, por politica min-max classica. Sem essa trava um produto em ruptura emitiria uma ordem por dia enquanto a primeira ainda estivesse a caminho, e a chegada em cascata produziria um pico de estoque que nenhuma operacao real teria.

```sql
select stock_date as dia, wh as armazem, category_name as categoria,
                   sum(replenishment_orders) as ordens_emitidas,
                   sum(reorder_units) as unidades_pedidas_ao_fornecedor,
                   sum(product_days) as produtos_no_dia,
                   round(sum(replenishment_orders) / nullif(sum(product_days), 0), 4)
                       as fracao_de_produtos_repondo
            from RETAIL.MART.MART_STOCK_HEALTH
            where stock_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            group by 1, 2, 3
            having sum(replenishment_orders) > 0
            order by ordens_emitidas desc
```

<a id="giro_estoque"></a>
### Giro diario por categoria

| | |
|---|---|
| Chave | `giro_estoque` |
| Pergunta | Quantas vezes por dia o estoque de cada categoria se renova? |
| Grão da fonte | `(stock_date, wh, category_id)` |
| Tipo do dado | derivado: consumo observado, politica sintetica |
| Marts | `MART_STOCK_HEALTH` |
| Eixo de data | sim |

**Armadilhas ao reconstruir no Power BI**

1. NAO E GIRO ANUALIZADO, e nao multiplique por 365. A janela tem poucos dias, e anualizar projetaria um comportamento sazonal que ninguem observou — o proprio gerador nao tem efeito de dia da semana, porque `daily_order_rate` e fixo.
2. O DENOMINADOR E O SALDO MEDIO DO DIA (abertura + fechamento) / 2, e nao o fechamento. Usar o fechamento faz o giro explodir para infinito no dia em que a prateleira zera — que e justamente o dia mais interessante.
3. GIRO ALTO NAO E BOM POR SI. Ele sobe tanto quando a demanda cresce quanto quando o estoque encolhe; leia-o ao lado da cobertura e da ruptura, senao uma prateleira quase vazia parece eficiencia.

```sql
select stock_date as dia, wh as armazem, category_name as categoria,
                   round(avg(turnover_daily), 4) as giro_diario,
                   sum(units_fulfilled) as unidades_vendidas,
                   sum(opening_units) as saldo_abertura,
                   sum(closing_units) as saldo_fechamento,
                   sum(units_short) as unidades_em_falta
            from RETAIL.MART.MART_STOCK_HEALTH
            where stock_date between %(inicio)s and %(fim)s and array_contains(wh::variant, split(%(armazens)s, ','))
            group by 1, 2, 3
            order by giro_diario desc nulls last
```

## O que o painel NÃO exibe

Uma lista de ausências declaradas vale mais que um indicador inventado. Cada item
traz o **gatilho** que o destravaria, para que a conversa seja sobre o que falta e
não sobre o que poderia ser aproximado.

### Margem, lucro, CMV

Nenhuma fonte deste repositorio tem custo. A API da Mercadona expoe preco de venda, nunca custo de aquisicao.

**Gatilho:** Uma fonte de custo por produto. Sem ela, qualquer margem e inventada.

### Estoque OBSERVADO, e a ligacao entre ruptura e linha indisponivel

O grupo G publica saldo, ruptura, giro e cobertura desde a Fase 7 — mas eles sao CALCULADOS, nunca observados: um job Spark deriva o saldo do consumo medido nos pedidos mais uma politica declarada em seed. Continua nao existindo fonte de estoque neste repositorio, e esta escrito no CONTRACT da source da Mercadona.

A consequencia concreta, que importa ao ler o grupo G: a ruptura do ledger e INDEPENDENTE das linhas removidas do pedido. O gerador remove linha a uma taxa FIXA sorteada, com motivo `unavailable` e nao `out_of_stock` justamente porque nao havia saldo quando ele foi escrito. Uma nao causa a outra, e cruzar as duas produziria uma correlacao inventada.

**Gatilho:** Para saldo real: uma fonte de estoque ou movimento. Para ligar as duas rupturas sem fonte nova: um gerador de SEGUNDA PASSADA, que releia o saldo do dia anterior antes de decidir a remocao. Isso inverteria a dependencia atual (hoje pedido gera estoque) e criaria um ciclo entre os dois dominios — nao e barato, e por isso esta declarado aqui em vez de aproximado.

### Recompra, LTV, coorte, receita por cliente, RFM

NENHUM MART JUNTA CLIENTE COM PEDIDO. `MART_CUSTOMER_BASE` tem cliente sem pedido; `MART_ORDER_FUNNEL` e `MART_BASKET_DAILY` tem pedido agregado sem cliente. O elo existe em `FACT_ORDER.customer_sk`, no GOLD — que `RETAIL_READER` nao alcanca, por desenho.

**Gatilho:** Um mart novo com grao de cliente e medidas de pedido (candidato: MART_CUSTOMER_ORDERS). E a lacuna mais acionavel desta lista, e nao exige fonte nova — so modelagem.

### Rota, tempo de deslocamento, distancia, otimizacao de entrega

Bloqueio duro e ja registrado: o Callejero nao tem coordenada nem adjacencia. O que se modela e JANELA de entrega — uma promessa comercial numa grade fixa — nunca rota.

**Gatilho:** Geocodificacao. Foi recusada de proposito: inventaria posicao.

### Penetracao de mercado, share, potencial por municipio

Os clientes sao SINTETICOS. `customers_per_10k_inhabitants` tem numerador sintetico sobre denominador observado do INE: mede densidade da SIMULACAO, e citada fora de contexto parece market share.

**Gatilho:** Uma base de clientes real. Fora de escopo declarado do projeto.

### Tendencia, sazonalidade, comparativo semanal ou mensal, YoY

A janela de pedidos tem 4 dias (2026-08-24 a 08-27) e a de catalogo 8 dias observados, com uma lacuna de 7 dias que NAO pode ser recuperada — a API so serve o preco de hoje. Linha de tendencia sobre 4 pontos e decoracao.

**Gatilho:** Tempo. A serie cresce sozinha a cada dia que a DAG rodar.

### Percentil verdadeiro do periodo

O mart guarda p50/p90 por (dia, armazem). Percentil nao soma nem tira media; a media dos percentis e aproximacao e o painel a rotula como tal.

**Gatilho:** Grao de pedido para o consumidor de BI — hoje so em `FACT_ORDER`, fora do alcance de `RETAIL_READER`.

---

## Consultas auxiliares

Não são indicadores de negócio. `FRESCOR` é o que torna uma carga nova **visível**:
sem ele, os números mudam e ninguém sabe que a base mudou.

```sql
select 'MART_ORDER_FUNNEL' as mart, count(*) as linhas,
           min(order_date)::varchar as inicio, max(order_date)::varchar as fim
      from RETAIL.MART.MART_ORDER_FUNNEL
    union all select 'MART_FULFILLMENT_SLA', count(*), min(order_date)::varchar, max(order_date)::varchar
      from RETAIL.MART.MART_FULFILLMENT_SLA
    union all select 'MART_BASKET_DAILY', count(*), min(order_date)::varchar, max(order_date)::varchar
      from RETAIL.MART.MART_BASKET_DAILY
    union all select 'MART_ASSORTMENT_DAILY', count(*), min(snapshot_date)::varchar, max(snapshot_date)::varchar
      from RETAIL.MART.MART_ASSORTMENT_DAILY
    union all select 'MART_PRICE_EVOLUTION', count(*), min(snapshot_date)::varchar, max(snapshot_date)::varchar
      from RETAIL.MART.MART_PRICE_EVOLUTION
    union all select 'MART_CUSTOMER_BASE', count(*), null, null
      from RETAIL.MART.MART_CUSTOMER_BASE
    union all select 'MART_MARKET_COVERAGE', count(*), null, null
      from RETAIL.MART.MART_MARKET_COVERAGE
    order by mart
```

```sql
select min(inicio)::varchar as inicio, max(fim)::varchar as fim from (
        select min(order_date) as inicio, max(order_date) as fim from RETAIL.MART.MART_ORDER_FUNNEL
        union all
        select min(snapshot_date), max(snapshot_date) from RETAIL.MART.MART_ASSORTMENT_DAILY
    )
```

```sql
select distinct wh from RETAIL.MART.MART_ORDER_FUNNEL order by wh
```

