-- Todo (municipio, ano) tem EXATAMENTE os dois rotulos de sexo.
--
-- E o teste que torna seguro ter excluido o agregado. O recorte tira `sex_label = 'Total'`
-- porque somar os tres rotulos daria o dobro da populacao — mas tirar o agregado so e
-- legitimo se o detalhe o reconstroi. Se um municipio aparecer com um unico rotulo (sigilo
-- estatistico do INE suprimindo um dos sexos, ou uma linha perdida no caminho), a soma
-- daquele municipio fica pela metade e o denominador de todo per-capita fica errado — sem
-- que nada mais no modelo perceba.
--
-- Por que isto e teste e nao premissa: agregado misturado com detalhe nao FALHA, produz um
-- numero plausivel. Foi assim que a piramide etaria da Fase 1 saiu 2,03x inflada com
-- age_label, e nenhum teste de entao pegava.
--
-- Falha com uma linha por (municipio, ano) incompleto.
select
    province_code,
    municipality_code,
    municipality_name,
    year,
    count(distinct sex_label)                   as rotulos,
    listagg(distinct sex_label, ', ')           as quais
from {{ ref('fact_population_municipality') }}
group by 1, 2, 3, 4
having count(distinct sex_label) <> 2
