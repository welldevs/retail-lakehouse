-- O RAW SELADO NAO PODE TER MUDADO — a parte que o dbt consegue ver.
--
-- DIVISAO DE TRABALHO, declarada porque a metade que falta importa. `make freeze-check`
-- rele o object storage e compara sha256 por sha256: essa e a verificacao COMPLETA, e ela
-- exige MinIO de pe. Este teste faz a parte que roda em `make silver`, sem rede extra e
-- sem ninguem precisar lembrar: confere se as particoes que o Silver ENXERGA sao as mesmas
-- que foram seladas, com as mesmas contagens declaradas.
--
-- O QUE ISSO PEGA, e e o risco real desta fase: dado aterrissado DEPOIS do fechamento. Uma
-- particao nova nao quebra soma nenhuma — ela apenas torna errado todo numero ja publicado
-- sobre a janela, em silencio. E a mesma classe do defeito de reparticao da Fase 6.
--
-- O QUE ELE NAO PEGA: bytes alterados dentro de uma particao cujo manifesto continua
-- declarando as mesmas contagens. Para isso e preciso reler o objeto — `make freeze-check`.
--
-- SELO VAZIO E ESTADO LEGITIMO. Um repositorio antes de `make freeze`, ou um clone novo,
-- tem zero linhas em `frozen_capture` e este teste passa. Nao ha terceiro estado.
--
-- SO O DOMINIO DE PEDIDOS, de proposito: e o unico que este projeto REGERA. As outras quatro
-- sources vem de extracao contra fonte externa e nunca sao reescritas no lugar; quem as
-- cobre e o `freeze-check`. Alargar este teste para elas exigiria um modelo de manifesto
-- para cada, que hoje nao existe — e criar quatro modelos para um risco que nao se
-- materializa seria construir contra hipotese.
with selo as (

    -- CAST EXPLICITO, e nao confianca na inferencia. O selo nasce VAZIO, e um parquet sem
    -- linha nao carrega tipo confiavel entre a escrita e a leitura: a primeira execucao
    -- deste teste quebrou com `replace(INTEGER, ...)`, um erro sobre TIPO tres passos longe
    -- da causa, que era a AUSENCIA de dado. Declarar o tipo no seed resolve a carga; o cast
    -- aqui resolve a leitura, e os dois juntos fazem o estado nao-selado se comportar como
    -- o selado, so que sem linhas.
    select
        replace(cast(partition_key as varchar), 'ingestion_date=', '')  as chave,
        cast(records as bigint)                                         as records
    from {{ ref('frozen_capture') }}
    where cast(source as varchar) = 'simulated_orders'

),

vivo as (

    -- `declared_event_rows`, E NAO A SOMA DAS TRES CONTAGENS. A primeira versao deste teste
    -- somava pedidos + linhas + eventos e reprovou nas 36 particoes — nao por defeito, por
    -- comparar grandezas diferentes. O selo agrega `records` dos ARQUIVOS declarados no
    -- manifesto, e a particao de pedidos tem UM arquivo: `order_events.jsonl`. Logo o numero
    -- do selo e a contagem de EVENTOS, e so ela. Medido: as 36 batem exatamente.
    --
    -- Se um dia a Source passar a aterrissar mais de um arquivo por particao, este mapeamento
    -- deixa de valer — e reprovar e o comportamento certo, porque o selo passou a medir outra
    -- coisa.
    select
        strftime(ingestion_date, '%Y-%m-%d') || '/wh=' || partition_wh  as chave,
        declared_event_rows                                             as records
    from {{ ref('silver_orders_manifest') }}

)

-- Selo vazio: nada a guardar ainda.
select
    coalesce(s.chave, v.chave)                    as chave,
    s.records                                     as registros_selados,
    v.records                                     as registros_no_silver,
    case
        when s.chave is null then 'particao NOVA, aterrissada depois do selo'
        when v.chave is null then 'particao SELADA que sumiu do Silver'
        else 'contagem declarada MUDOU dentro de uma particao selada'
    end                                           as violacao
from selo s
full outer join vivo v on v.chave = s.chave
where (select count(*) from selo) > 0
  and (s.chave is null or v.chave is null or s.records <> v.records)
