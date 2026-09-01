-- A CESTA QUE O GERADOR SORTEIA E A CESTA QUE O SEED DECLARA.
--
-- LEIA ISTO ANTES DE ACHAR QUE E UM TESTE DE KPI, porque parece um e nao e. O projeto recusa
-- testes que afiram RESULTADO — foi por isso que `assert_order_premises_are_internally_coherent`
-- afere a derivacao e nao a adesao. Este aqui nao afere se a operacao e boa: ele afere se a
-- DISTRIBUICAO OBSERVADA e a DISTRIBUICAO DECLARADA sao a mesma.
--
-- A CADEIA, que so fecha se as duas pontas concordarem:
--
--   o seed declara      basket_lines ~ triangular(min, max + 1, moda), truncada
--   o seed declara      sla_picking_percentile = 0.90
--   dai DERIVA          sla_minutes_picking = floor(p90) x minutes_per_line_picked
--   e o gerador produz  picking_minutes = minutes_per_line_picked x linhas da cesta
--
--   logo, se a cesta observada seguir mesmo a triangular declarada, a fracao de pedidos que
--   estoura o limiar tem de ser 1 - sla_picking_percentile.
--
-- O QUE ELE PEGA QUE NADA MAIS PEGA. `premises_sha256` garante que o gerador leu ESTES
-- valores; nenhum teste verificava que ele os usou com a FORMA declarada. Trocar
-- `rng.triangular` por `rng.uniform` mantendo os mesmos tres numeros passaria em todos os
-- outros testes: os limites continuariam certos, os totais continuariam fechando, a moda
-- sumiria em silencio — e todo indicador de cesta media viraria plausivel e errado.
--
-- A PROBABILIDADE E CALCULADA, NAO CRAVADA. Para a triangular no ramo acima da moda,
-- P(X >= x) = (b - x)^2 / ((b - a)(b - m)). Aqui `b` e `basket_lines_max + 1` porque e assim
-- que o gerador sorteia, e o corte e `limiar/por_linha + 1` porque estourar exige a linha
-- SEGUINTE. Medido em 2026-09-01: teorico 0,09747, observado 0,09694 sobre 197.402 pedidos
-- separados — 5 decimos de milesimo de diferenca.
--
-- A TOLERANCIA E 1 PONTO PERCENTUAL, e e folgada de proposito: com quase 200 mil amostras o
-- erro de amostragem e da ordem de 0,07 pp, entao 1 pp so dispara se a FORMA mudar. Apertar
-- mais transformaria um teste de distribuicao num teste de sorte.
with premissa as (

    select
        max(case when premise_key = 'basket_lines_min'        then cast(value as double) end) as a,
        max(case when premise_key = 'basket_lines_max'        then cast(value as double) end) + 1 as b,
        max(case when premise_key = 'basket_lines_mode'       then cast(value as double) end) as m,
        max(case when premise_key = 'minutes_per_line_picked' then cast(value as double) end) as por_linha,
        max(case when premise_key = 'sla_minutes_picking'     then cast(value as double) end) as limiar,
        max(case when premise_key = 'sla_picking_percentile'  then cast(value as double) end) as percentil
    from {{ ref('order_premises') }}

),

teorico as (

    select
        *,
        -- corte em LINHAS: estourar o limiar exige a primeira linha acima dele
        (limiar / por_linha) + 1                             as corte,
        power(b - ((limiar / por_linha) + 1), 2)
            / ((b - a) * (b - m))                            as p_estouro
    from premissa

),

observado as (

    select
        count(*)                                                          as separados,
        count(*) filter (
            where date_diff('minute', picking_started_at, picked_at) > (select limiar from premissa)
        )                                                                 as estouros
    from {{ ref('silver_order') }}
    where picked_at is not null

)

select
    o.separados,
    o.estouros,
    round(o.estouros::double / o.separados, 5) as taxa_observada,
    round(t.p_estouro, 5)                      as taxa_teorica,
    round(1 - t.percentil, 5)                  as taxa_declarada,
    'a cesta observada nao segue a triangular declarada no seed' as violacao
from observado o
cross join teorico t
where o.separados > 0
  and abs(o.estouros::double / o.separados - t.p_estouro) > 0.01
