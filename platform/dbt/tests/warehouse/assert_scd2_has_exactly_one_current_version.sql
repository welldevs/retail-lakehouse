-- Toda chave natural das duas SCD2 tem EXATAMENTE uma versao corrente, e os intervalos
-- nao se sobrepoem nem deixam buraco.
--
-- Sao dois defeitos classicos de SCD2, e nenhum dos dois falha sozinho:
--   * DUAS versoes correntes: um join com a dimensao passa a duplicar todo fato ligado
--     aquela chave, e o total dobra em silencio;
--   * ZERO versoes correntes: o mesmo join simplesmente perde as linhas, e o total
--     encolhe sem erro nenhum.
--
-- Ambos sao consequencia tipica de um `lead()` com particao errada, que continua
-- compilando e produzindo uma tabela de aparencia normal.
--
-- Falha com uma linha por chave natural com contagem diferente de 1.
{% set dimensoes = [
    ('dim_product',  'source_product_id'),
    ('dim_customer', 'customer_id'),
] %}

{% for modelo, chave in dimensoes %}
select
    '{{ modelo }}'                                          as modelo,
    cast({{ chave }} as varchar)                            as chave_natural,
    count_if(is_current)                                    as versoes_correntes,
    count(*)                                                as versoes_totais
from {{ ref(modelo) }}
group by {{ chave }}
having count_if(is_current) <> 1
{% if not loop.last %}union all{% endif %}
{% endfor %}
