-- TODA PARTICAO DE PEDIDO NASCEU DAS MESMAS PREMISSAS. UMA JANELA, UM CONTRATO.
--
-- A LACUNA QUE ISTO FECHA, declarada no Marco B e medida na hora. O manifesto de cada
-- particao guarda `premises_sha256` — o hash do pacote de referencia que o gerador leu — e a
-- Source valida esse hash NA GERACAO. Mas nada comparava as particoes ENTRE SI depois de
-- aterrissadas.
--
-- O ESTADO PERIGOSO QUE ELE PEGA e concreto e ja aconteceu neste repositorio: no Marco B o
-- seed mudou (`sla_minutes_picking` 90 -> 60, `slot_lead_hours` 2-24h -> 1-8h) e o RAW
-- continuou sendo o antigo. `make silver` passou inteiro. Se a regeracao seguinte fosse
-- INTERROMPIDA no meio, metade da janela teria as premissas novas e metade as velhas — e
-- nada reprovaria, porque cada particao continua internamente consistente e todos os totais
-- continuam fechando. O warehouse mediria adesao de janela contra duas politicas ao mesmo
-- tempo, publicando um numero que nao descreve nenhuma das duas.
--
-- E A MESMA CLASSE DA DENSIDADE DA BASE DE CLIENTES NA FASE 6: defeito de REPARTICAO, que
-- soma nenhuma denuncia.
--
-- `generator_version` viaja junto pelo mesmo motivo: duas versoes do gerador na mesma janela
-- e a mesma doenca com outro nome.
--
-- O QUE ELE AINDA NAO PEGA, e fica declarado: se TODAS as particoes forem velhas de forma
-- uniforme, elas concordam entre si e este teste passa. Comparar o RAW contra o seed ATUAL
-- do repositorio exige recomputar o hash do pacote de referencia, que e trabalho de fora do
-- dbt — e e o que `make freeze` sela.
with particoes as (

    select distinct
        premises_sha256,
        generator_version
    from {{ ref('silver_orders_manifest') }}

),

contagem as (

    select
        count(distinct premises_sha256)  as conjuntos_de_premissa,
        count(distinct generator_version) as versoes_do_gerador
    from particoes

)

select
    c.conjuntos_de_premissa,
    c.versoes_do_gerador,
    (select string_agg(distinct premises_sha256, ' | ') from particoes) as premissas,
    (select string_agg(distinct generator_version, ' | ') from particoes) as geradores,
    'a janela mistura mais de um contrato: metade dela responde a outra politica' as violacao
from contagem c
where c.conjuntos_de_premissa <> 1
   or c.versoes_do_gerador <> 1
