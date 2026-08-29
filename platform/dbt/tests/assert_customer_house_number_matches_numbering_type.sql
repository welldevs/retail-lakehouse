-- NUNCA FABRICAR NUMERO DE CASA. O Callejero declara em TINUM (aqui, numbering_type) se a
-- via tem numeracao e qual a paridade; o gerador respeita, e este teste reconfere olhando
-- so o Silver — sem precisar da referencia, que e insumo efemero.
--
-- Tres condicoes ESTRITAS, todas em uma direcao so:
--   1. numero presente  =>  numbering_type <> "0"  (a fonte diz que nao ha numeracao)
--   2. numero presente  =>  paridade bate ("1" impar, "2" par)
--   3. numero presente  =>  >= 1  (0 nao e numero de casa; 14 tramos reais tem faixa
--      0000..0000 e sortear neles daria exatamente isso)
--
-- A reciproca NAO e testavel aqui, e de proposito: numero ausente com numbering_type "1"
-- ou "2" e legitimo quando a faixa do tramo nao contem nenhum numero da paridade
-- declarada (ex.: pares entre 3 e 3). Checar essa direcao exigiria a faixa, que so existe
-- em address_candidates.json. Afirmar um "se e somente se" que o dado nao sustenta seria
-- pior do que nao testar.
--
-- Falha com uma linha por cliente inconsistente.
select
    ingestion_date,
    wh,
    customer_id,
    numbering_type,
    house_number,
    case
        when numbering_type = '0'                       then 'numero em via sem numeracao'
        when house_number < 1                           then 'numero menor que 1'
        when numbering_type = '1' and house_number % 2 = 0 then 'par em via impar'
        when numbering_type = '2' and house_number % 2 = 1 then 'impar em via par'
    end                                                 as motivo
from {{ ref('silver_customer') }}
where house_number is not null
  and (
        numbering_type = '0'
     or house_number < 1
     or (numbering_type = '1' and house_number % 2 = 0)
     or (numbering_type = '2' and house_number % 2 = 1)
  )
