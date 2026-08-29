-- sequence_no tem de ser 1..N contiguo dentro de cada pedido.
--
-- POR QUE ISTO E O TESTE MAIS BASICO DE UM LOG. Um evento perdido entre a Source e o parquet
-- nao muda nenhuma contagem que o olho perceba: o pedido continua tendo eventos, o fold
-- continua produzindo um estado, e o valor continua plausivel. O que muda e o SIGNIFICADO —
-- um pedido que perdeu `order_line_removed` passa a declarar receita que nao aconteceu.
--
-- Conferir min=1, max=N e count=N pega as tres formas do defeito de uma vez: buraco no meio,
-- comeco perdido e duplicata. Comparar so `count(*) = max(sequence_no)` deixaria passar um
-- log em que uma duplicata compensa um buraco.
select
    order_id,
    count(*)                as eventos,
    min(sequence_no)        as menor,
    max(sequence_no)        as maior,
    count(distinct sequence_no) as distintos
from {{ ref('silver_order_event') }}
group by order_id
having min(sequence_no) <> 1
    or max(sequence_no) <> count(*)
    or count(distinct sequence_no) <> count(*)
