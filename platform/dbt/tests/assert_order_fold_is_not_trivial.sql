-- TESTE INVERTIDO: reprova quando NADA deu errado.
--
-- Irmao de assert_geography_postal_code_is_not_a_key, e pelo mesmo motivo. Aquele reprova
-- quando nenhum CEP cruza municipio, ou seja, quando o grao composto de DIM_GEOGRAPHY perde a
-- justificativa. Este reprova quando nenhuma cesta muda entre a colocacao e a separacao — ou
-- seja, quando `net_amount` passa a ser derivavel de `gross_amount_placed`.
--
-- POR QUE ISSO E UM DEFEITO E NAO UM DIA CALMO. Se o fold e trivial, o log de eventos nao
-- carrega nenhuma informacao que uma tabela de pedidos nao carregaria, e o modelo inteiro —
-- a Source que grava log em vez de estado, o Silver que dobra, o broker que transporta —
-- deixa de se justificar. O ARCHITECTURE.md ja recusou explicitamente a versao decorativa
-- disto: "publicar o proprio output batch num topico e consumir de volta adicionaria um
-- broker para manter e zero informacao".
--
-- Medido na janela de 2026-08-24 a 08-27: 4.670 linhas substituidas e 2.321 removidas em
-- 120.693, e 5.166 pedidos com valor separado diferente do colocado.
select
    count(*)                                                        as pedidos_com_separacao,
    sum(case when substituted_lines > 0 or removed_lines > 0 then 1 else 0 end)
                                                                    as pedidos_com_cesta_alterada,
    sum(case when net_amount is distinct from gross_amount_placed then 1 else 0 end)
                                                                    as pedidos_com_valor_diferente
from {{ ref('silver_order') }}
where net_amount is not null
having sum(case when substituted_lines > 0 or removed_lines > 0 then 1 else 0 end) = 0
    or sum(case when net_amount is distinct from gross_amount_placed then 1 else 0 end) = 0
