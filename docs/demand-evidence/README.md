# Reality check da demanda sintetica

GERADO por `make demand-reality-check`. Nenhum numero desta pagina foi escrito a mao; refaca-a em vez de edita-la.

- modelo de demanda: **mapa_2025_v2**
- benchmark: MAPA, Informe del Consumo Alimentario en Espana 2025
- medido em: 2026-09-01T21:55:35Z
- ANTES congelado em: 2026-09-01T11:32:54Z (`before_mapa_2025_v2`) — o estado IMEDIATAMENTE anterior a esta versao do modelo, e nao o mais antigo que existe. `before_mapa_2025_v1` guarda o mix uniforme de antes da calibracao agregada e continua no disco; misturar os dois numa coluna so faria os efeitos de duas fases serem lidos como um.

## Como ler as colunas

**MAPA %** e o consumo domestico bruto. **ALVO %** e o mesmo numero inclinado pela participacao do e-commerce e renormalizado — e contra ele que o modelo deve ser julgado. Os dois diferem de proposito: o informe mede o que o residente consome, e a cesta online nao tem a mesma composicao (1,1% do volume fresco chega por e-commerce contra 2,8% do resto). Fruta fresca DEVE aparecer abaixo dos 14,13% domesticos numa loja online; se aparecesse em 14,13% o modelo estaria confundindo consumo total com canal.

Receita sozinha nao serve como indicador de realismo: ela mistura QUANTO se compra com QUANTO CUSTA. Foi assim que 10 linhas de catalogo com preco de teto de API responderam por 16,6% de toda a receita simulada sem que nenhum total fechasse errado.

## Totais

| dimensao | ANTES | DEPOIS | variacao |
|---|---:|---:|---:|
| unidades | 204 824 | 6 613 349 | +3128,8 % |
| kg ou litro | 144 425,1 | 4 646 078,6 | +3116,9 % |
| receita (EUR) | 583 154,43 | 18 775 796,48 | +3119,7 % |
| EUR por kg | 4,04 | 4,04 | +0,1 % |

O EUR/kg do MAPA para o total da alimentacao domestica em 2025 e **3,25**. O nosso cobre tambem o terco nao alimentar do catalogo, que o informe nao mede, entao os dois nao sao diretamente comparaveis — a comparacao util e grupo a grupo, mais abaixo.

## Os tres blocos

O share alimentar e premissa declarada; a divisao entre calibrado e nao calibrado sai da cobertura do proprio benchmark, e nao de um numero novo.

| bloco | peso declarado | % do volume observado | origem do peso |
|---|---:|---:|---|
| calibrado pelo MAPA | 73,20 % (linhas) | 81,60 % (kg) | food_line_share x cobertura do benchmark (86.12%) |
| alimentar sem benchmark | 11,80 % (linhas) | 11,76 % (kg) | complemento, dividido por tamanho de sortimento |
| nao alimentar | 15,00 % (linhas) | 6,64 % (kg) | 1 - food_line_share; fora do universo do MAPA |

Peso declarado e share de LINHAS; a coluna observada e share de VOLUME. Nao devem coincidir: um pacote de agua pesa 3,5 kg e um sache de tempero pesa 30 g.

## Volume (kg ou litro) — dentro do bloco calibrado

Todas as colunas somam 100 sobre os mesmos grupos. E a dimensao que o MAPA publica e onde a calibracao age.

| grupo | ANTES % | MAPA % | ALVO % | DEPOIS % | erro | canal |
|---|---:|---:|---:|---:|---:|---|
| AGUA | 18,26 | 10,90 | 18,01 | 18,45 | +0,44 | coarse |
| FRUTAS_FRESCAS | 9,51 | 14,13 | 9,17 | 9,32 | +0,15 | coarse |
| LECHE_SEMIDESNATADA | 8,82 | 4,94 | 8,16 | 8,35 | +0,19 | coarse |
| HORTALIZAS_FRESCAS | 5,62 | 8,50 | 6,02 | 5,56 | -0,46 | fine |
| LECHE_ENTERA | 5,30 | 3,24 | 5,35 | 5,47 | +0,12 | coarse |
| CERVEZA | 5,38 | 3,10 | 5,12 | 5,25 | +0,13 | coarse |
| PATATAS | 4,67 | 4,52 | 4,53 | 4,69 | +0,15 | fine |
| LECHE_DESNATADA | 3,57 | 2,36 | 3,90 | 3,96 | +0,06 | coarse |
| LECHES_FERMENTADAS | 3,88 | 2,30 | 3,80 | 3,92 | +0,12 | coarse |
| PLATOS_PREPARADOS | 3,23 | 3,14 | 3,52 | 3,27 | -0,25 | fine |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 3,39 | 2,03 | 3,35 | 3,43 | +0,08 | coarse |
| QUESOS | 2,47 | 1,42 | 2,35 | 2,42 | +0,08 | coarse |
| PAN | 2,24 | 4,76 | 2,25 | 2,32 | +0,08 | fine |
| ACEITES_OLIVA | 1,78 | 1,18 | 1,95 | 2,00 | +0,05 | coarse |
| VINO | 2,11 | 1,18 | 1,95 | 2,00 | +0,05 | coarse |
| CARNE_POLLO | 1,95 | 2,29 | 1,89 | 1,94 | +0,05 | group |
| BOLLERIA_PASTELERIA | 1,80 | 1,01 | 1,73 | 1,77 | +0,04 | fine |
| ZUMOS | 1,59 | 0,94 | 1,55 | 1,60 | +0,05 | coarse |
| CARNE_TRANSFORMADA | 1,58 | 1,84 | 1,52 | 1,52 | +0,00 | group |
| CARNE_CERDO | 1,56 | 1,73 | 1,43 | 1,48 | +0,05 | group |
| HUEVOS | 0,19 | 1,60 | 1,42 | 0,18 | -1,24 | fine |
| PASTAS | 1,25 | 0,74 | 1,22 | 1,25 | +0,03 | coarse |
| LEGUMBRES | 1,06 | 0,60 | 1,06 | 1,12 | +0,05 | fine |
| FRUTOS_SECOS | 1,06 | 0,62 | 1,02 | 1,05 | +0,03 | coarse |
| ARROZ | 1,04 | 0,68 | 1,00 | 1,02 | +0,02 | fine |
| CONSERVAS_PESCADO_MARISCO | 0,95 | 0,71 | 0,92 | 0,94 | +0,02 | fine |
| ACEITE_GIRASOL | 0,86 | 0,55 | 0,91 | 0,95 | +0,04 | coarse |
| CHOCOLATES_CACAOS | 0,90 | 0,49 | 0,81 | 0,83 | +0,02 | coarse |
| PESCADOS_CONGELADOS | 0,62 | 0,35 | 0,58 | 0,59 | +0,01 | coarse |
| PESCADO_FRESCO | 0,56 | 1,21 | 0,57 | 0,50 | -0,07 | fine |
| CARNE_VACUNO | 0,55 | 0,65 | 0,54 | 0,55 | +0,01 | group |
| HARINAS_SEMOLAS | 0,54 | 0,35 | 0,54 | 0,55 | +0,01 | fine |
| CAFES_INFUSIONES | 0,37 | 0,31 | 0,51 | 0,37 | -0,14 | coarse |
| CARNE_FRESCA_OTRAS | 0,45 | 0,55 | 0,45 | 0,47 | +0,01 | group |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0,43 | 0,81 | 0,43 | 0,43 | +0,00 | fine |
| BEBIDAS_ESPIRITUOSAS | 0,19 | 0,12 | 0,20 | 0,20 | +0,00 | coarse |
| MIEL | 0,08 | 0,06 | 0,10 | 0,11 | +0,01 | coarse |
| CARNE_OVINO_CAPRINO | 0,08 | 0,12 | 0,10 | 0,10 | +0,00 | group |
| CARNE_CONEJO | 0,08 | 0,09 | 0,07 | 0,08 | +0,00 | group |

Erro absoluto medio contra o ALVO: **0,111** pontos; maior desvio: **1,238** pontos.

O objetivo NAO e minimizar este numero. Ele esta aqui para que um desvio grande seja explicavel, e nao para ser perseguido — um erro de zero significaria que o sortimento do catalogo casa perfeitamente com a cesta espanhola, o que seria suspeito, nao bom.

Grupos com linhas sem conversao para kg (o desvio deles contra o ALVO nao mede a calibracao, mede a cobertura da conversao): `HORTALIZAS_FRESCAS` (36612 linhas), `PLATOS_PREPARADOS` (16458 linhas), `CARNE_TRANSFORMADA` (1860 linhas), `HUEVOS` (25948 linhas), `CAFES_INFUSIONES` (18093 linhas)

## Unidades — sobre o total, inclusive nao alimentar

Nao e comparavel ao MAPA: o informe mede peso e volume, nunca peca. Esta tabela existe para mostrar quantas LINHAS cada grupo ocupa na cesta.

| grupo | ANTES % | DEPOIS % |
|---|---:|---:|
| AGUA | 2,97 | 3,02 |
| SIN_BENCHMARK | 11,83 | 11,80 |
| FRUTAS_FRESCAS | 5,86 | 6,02 |
| LECHE_SEMIDESNATADA | 1,33 | 1,31 |
| NO_FOOD | 15,00 | 15,00 |
| HORTALIZAS_FRESCAS | 9,09 | 9,00 |
| LECHE_ENTERA | 0,86 | 0,86 |
| CERVEZA | 1,86 | 1,88 |
| PATATAS | 1,94 | 1,90 |
| LECHE_DESNATADA | 0,62 | 0,65 |
| LECHES_FERMENTADAS | 3,33 | 3,38 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 5,03 | 5,05 |
| PLATOS_PREPARADOS | 4,61 | 4,57 |
| QUESOS | 4,03 | 4,08 |
| PAN | 3,43 | 3,52 |
| ACEITES_OLIVA | 0,49 | 0,52 |
| VINO | 1,33 | 1,31 |
| CARNE_POLLO | 1,79 | 1,75 |
| BOLLERIA_PASTELERIA | 2,69 | 2,62 |
| ZUMOS | 0,95 | 0,94 |
| CARNE_TRANSFORMADA | 1,96 | 1,90 |
| CARNE_CERDO | 1,61 | 1,54 |
| PASTAS | 1,68 | 1,66 |
| LEGUMBRES | 1,06 | 1,05 |
| FRUTOS_SECOS | 2,78 | 2,77 |
| ARROZ | 0,81 | 0,80 |
| ACEITE_GIRASOL | 0,23 | 0,25 |
| CONSERVAS_PESCADO_MARISCO | 3,02 | 3,03 |
| CHOCOLATES_CACAOS | 1,48 | 1,42 |
| PESCADOS_CONGELADOS | 0,77 | 0,72 |
| CARNE_VACUNO | 0,84 | 0,86 |
| HARINAS_SEMOLAS | 0,25 | 0,27 |
| PESCADO_FRESCO | 0,53 | 0,56 |
| CARNE_FRESCA_OTRAS | 0,56 | 0,58 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0,50 | 0,49 |
| CAFES_INFUSIONES | 1,61 | 1,57 |
| BEBIDAS_ESPIRITUOSAS | 0,16 | 0,16 |
| HUEVOS | 0,85 | 0,84 |
| MIEL | 0,08 | 0,10 |
| CARNE_OVINO_CAPRINO | 0,17 | 0,19 |
| CARNE_CONEJO | 0,06 | 0,06 |

## Receita — sobre o total

A coluna do MAPA e o share de VALOR domestico, sem inclinacao de canal. Ela NAO e um alvo: a receita e consequencia do volume e do preco observado da Mercadona, e converge com o MAPA so na medida em que os precos espanhois e os da Mercadona se parecem. Volume e valor devem DIVERGIR entre si — mariscos sao 0,81% do volume e 2,88% do valor domestico, e um simulador que os igualasse estaria errado.

| grupo | ANTES % | MAPA valor % | DEPOIS % |
|---|---:|---:|---:|
| AGUA | 1,84 | 0,80 | 1,88 |
| SIN_BENCHMARK | 8,75 | — | 8,78 |
| FRUTAS_FRESCAS | 4,22 | 9,94 | 4,34 |
| LECHE_SEMIDESNATADA | 1,95 | 1,46 | 1,89 |
| NO_FOOD | 18,80 | — | 18,65 |
| HORTALIZAS_FRESCAS | 5,62 | 6,60 | 5,60 |
| LECHE_ENTERA | 1,20 | 1,02 | 1,21 |
| CERVEZA | 1,98 | 1,57 | 1,99 |
| PATATAS | 1,71 | 1,85 | 1,69 |
| LECHE_DESNATADA | 0,85 | 0,68 | 0,96 |
| LECHES_FERMENTADAS | 1,87 | 1,85 | 1,90 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 3,11 | 1,83 | 3,16 |
| PLATOS_PREPARADOS | 4,72 | 5,51 | 4,68 |
| QUESOS | 5,09 | 4,34 | 5,06 |
| PAN | 1,61 | 3,96 | 1,62 |
| ACEITES_OLIVA | 1,48 | 1,74 | 1,60 |
| VINO | 1,52 | 1,29 | 1,50 |
| CARNE_POLLO | 2,46 | 3,90 | 2,43 |
| BOLLERIA_PASTELERIA | 2,95 | 2,18 | 2,87 |
| ZUMOS | 0,51 | 0,44 | 0,51 |
| CARNE_TRANSFORMADA | 6,06 | 6,79 | 5,98 |
| CARNE_CERDO | 2,28 | 4,05 | 2,19 |
| PASTAS | 0,88 | 0,60 | 0,87 |
| LEGUMBRES | 0,58 | 0,40 | 0,58 |
| FRUTOS_SECOS | 1,95 | 1,72 | 1,95 |
| ARROZ | 0,52 | 0,51 | 0,50 |
| ACEITE_GIRASOL | 0,39 | 0,30 | 0,44 |
| CONSERVAS_PESCADO_MARISCO | 3,14 | 2,87 | 3,15 |
| CHOCOLATES_CACAOS | 1,63 | 1,81 | 1,51 |
| PESCADOS_CONGELADOS | 1,41 | 1,17 | 1,33 |
| CARNE_VACUNO | 2,20 | 2,87 | 2,25 |
| HARINAS_SEMOLAS | 0,13 | 0,16 | 0,13 |
| PESCADO_FRESCO | 1,41 | 4,10 | 1,37 |
| CARNE_FRESCA_OTRAS | 0,70 | 1,30 | 0,77 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0,99 | 2,88 | 1,05 |
| CAFES_INFUSIONES | 1,69 | 2,00 | 1,69 |
| BEBIDAS_ESPIRITUOSAS | 0,44 | 0,41 | 0,45 |
| HUEVOS | 0,79 | 1,90 | 0,80 |
| MIEL | 0,11 | 0,13 | 0,14 |
| CARNE_OVINO_CAPRINO | 0,32 | 0,61 | 0,40 |
| CARNE_CONEJO | 0,15 | 0,25 | 0,15 |

## Preco medio por kg — a coluna comparavel sem conversao nenhuma

Share exige converter volume em linhas e inclinar por canal; EUR/kg nao exige nada. Um grupo cujo EUR/kg bate com o informe tem preco observado saudavel, independentemente de quantas linhas dele saem — e foi esta coluna que denunciou o defeito de preco de granel antes da calibracao existir.

| grupo | ANTES EUR/kg | MAPA EUR/kg | DEPOIS EUR/kg | linhas sem kg |
|---|---:|---:|---:|---:|
| AGUA | 0,50 | 0,24 | 0,50 | 0 |
| SIN_BENCHMARK | 2,93 | — | 3,02 | 542 |
| FRUTAS_FRESCAS | 2,20 | 2,28 | 2,31 | 0 |
| LECHE_SEMIDESNATADA | 1,10 | 0,96 | 1,12 | 0 |
| NO_FOOD | 11,59 | — | 11,36 | 288675 |
| HORTALIZAS_FRESCAS | 4,96 | 2,52 | 4,99 | 36612 |
| LECHE_ENTERA | 1,12 | 1,02 | 1,10 | 0 |
| CERVEZA | 1,83 | 1,64 | 1,87 | 0 |
| PATATAS | 1,82 | 1,33 | 1,79 | 0 |
| LECHE_DESNATADA | 1,18 | 0,94 | 1,20 | 0 |
| LECHES_FERMENTADAS | 2,39 | 2,61 | 2,40 | 0 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 4,54 | 2,92 | 4,56 | 0 |
| PLATOS_PREPARADOS | 7,25 | 5,69 | 7,10 | 16458 |
| QUESOS | 10,19 | 9,96 | 10,34 | 0 |
| PAN | 3,56 | 2,70 | 3,46 | 0 |
| ACEITES_OLIVA | 4,13 | 4,79 | 3,96 | 0 |
| VINO | 3,57 | 3,55 | 3,73 | 0 |
| CARNE_POLLO | 6,28 | 5,53 | 6,19 | 0 |
| BOLLERIA_PASTELERIA | 8,15 | 6,99 | 8,04 | 0 |
| ZUMOS | 1,60 | 1,51 | 1,57 | 0 |
| CARNE_TRANSFORMADA | 19,03 | 11,99 | 19,45 | 1860 |
| CARNE_CERDO | 7,23 | 7,59 | 7,34 | 0 |
| PASTAS | 3,49 | 2,65 | 3,43 | 0 |
| LEGUMBRES | 2,71 | 2,18 | 2,56 | 0 |
| FRUTOS_SECOS | 9,10 | 8,98 | 9,15 | 0 |
| ARROZ | 2,47 | 2,41 | 2,44 | 0 |
| ACEITE_GIRASOL | 2,27 | 1,77 | 2,31 | 0 |
| CONSERVAS_PESCADO_MARISCO | 16,37 | 13,16 | 16,63 | 0 |
| CHOCOLATES_CACAOS | 8,99 | 11,93 | 9,01 | 0 |
| PESCADOS_CONGELADOS | 11,19 | — | 11,09 | 0 |
| CARNE_VACUNO | 19,76 | 14,27 | 20,26 | 0 |
| HARINAS_SEMOLAS | 1,16 | 1,47 | 1,21 | 0 |
| PESCADO_FRESCO | 12,53 | 10,98 | 13,62 | 0 |
| CARNE_FRESCA_OTRAS | 7,80 | — | 8,19 | 0 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 11,39 | 11,50 | 11,92 | 0 |
| CAFES_INFUSIONES | 22,66 | 21,19 | 22,58 | 18093 |
| BEBIDAS_ESPIRITUOSAS | 11,25 | 11,58 | 11,30 | 0 |
| HUEVOS | 20,81 | 3,85 | 22,24 | 25948 |
| MIEL | 6,34 | 7,55 | 6,28 | 0 |
| CARNE_OVINO_CAPRINO | 18,99 | 17,04 | 19,76 | 0 |
| CARNE_CONEJO | 9,87 | 9,00 | 9,72 | 0 |

## O que o benchmark nao alcanca

- **NO_FOOD** e **SIN_BENCHMARK** nao tem alvo do MAPA e nunca sao somados ao bloco calibrado. O informe mede alimentos e bebidas; drogaria, limpeza, maquiagem e mascotas estao fora do universo dele.
- share alimentar declarado: **0.85** — premissa `synthetic`, sem benchmark. Nenhuma fonte deste repo mede a composicao alimentar de uma cesta online, e o MAPA nao mede drogaria.
- cobertura do benchmark: **86.12%** do volume domestico espanhol, somando as folhas pesaveis. O complemento vai para `SIN_BENCHMARK`, dividido por tamanho de sortimento.
- `ACEITUNAS` (Aceitunas): sem share publicado — secao 4.2
- `CEREALES` (Cereales de desayuno): sem share publicado — secao 4.5.2
- `GALLETAS` (Galletas): sem share publicado — secao 4.5.3
- `GASEOSAS_REFRESCOS` (Gaseosas y bebidas refrescantes): sem share publicado — secao 4.4.4
- `PRODUCTOS_NAVIDENOS` (Productos navidenos): sem share publicado — secao 4.5.4

### Sazonalidade

Perfil NEUTRO nos 12 meses, por ausencia de evidencia numerica: os graficos mensais do informe sao imagens, e so ha cinco numeros mensais em prosa — todos do TOTAL da alimentacao, nunca por categoria. Alem disso a janela do simulador cobre apenas agosto, entao nao existe eixo mensal para exercer. O mecanismo existe, aplica-se a taxa de pedidos e tem teste que prova que um perfil nao neutro muda a saida; o gatilho para propor um perfil e a janela cobrir novembro e dezembro.

## Propensao por coorte do comprador

A coorte tem duas dimensoes, e sao as duas UNICAS em que um atributo observado do cliente coincide com um corte publicado do informe: **idade** (`birth_year`, da distribuicao provincial do INE) e **comunidade autonoma** (`province_code`, do Callejero). Ciclo de vida do lar e nivel socioeconomico sao os cortes mais ricos do MAPA e ficaram de fora: o cliente nao tem composicao familiar nem renda, e atribui-las seria inventar o atributo.

**O agregado nao se move, e isso e o criterio de aceitacao.** Os pesos por coorte, ponderados pela distribuicao real de coortes entre os pedidos, reproduzem os pesos da calibracao agregada — o IPF existe para isso. Quem procurar o efeito desta camada num total nao vai encontrar: ele esta inteiro nas colunas abaixo.

### Fatia de cada grupo DENTRO da coorte (% das linhas)

Denominador e a propria coorte, e nao o total: e assim que a comparacao entre faixas isola a propensao do tamanho da coorte. A coluna `x` e a razao entre a faixa mais velha e a mais nova — o resumo de uma linha inteira.

| grupo | LT35 % | 35_49 % | 50_64 % | GE65 % | x GE65/LT35 |
|---|---:|---:|---:|---:|---:|
| VINO | 0,53 | 0,86 | 1,62 | 2,31 | 4,36 |
| CARNE_OVINO_CAPRINO | 0,10 | 0,16 | 0,21 | 0,31 | 3,01 |
| CARNE_CONEJO | 0,03 | 0,04 | 0,06 | 0,09 | 2,98 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0,28 | 0,39 | 0,59 | 0,73 | 2,64 |
| PESCADO_FRESCO | 0,36 | 0,44 | 0,66 | 0,82 | 2,29 |
| BEBIDAS_ESPIRITUOSAS | 0,11 | 0,12 | 0,18 | 0,23 | 2,15 |
| ACEITES_OLIVA | 0,36 | 0,41 | 0,60 | 0,72 | 2,02 |
| FRUTAS_FRESCAS | 4,57 | 5,33 | 6,24 | 8,04 | 1,76 |
| PESCADOS_CONGELADOS | 0,53 | 0,62 | 0,83 | 0,92 | 1,76 |
| PAN | 2,65 | 3,49 | 4,02 | 3,92 | 1,48 |
| MIEL | 0,09 | 0,07 | 0,11 | 0,12 | 1,43 |
| CERVEZA | 1,41 | 1,86 | 2,22 | 2,00 | 1,42 |
| HORTALIZAS_FRESCAS | 8,28 | 7,81 | 9,24 | 10,85 | 1,31 |
| LECHE_DESNATADA | 0,56 | 0,64 | 0,70 | 0,73 | 1,31 |
| FRUTOS_SECOS | 2,29 | 2,77 | 3,04 | 2,98 | 1,30 |
| PATATAS | 1,82 | 1,87 | 1,97 | 1,96 | 1,08 |
| CAFES_INFUSIONES | 1,59 | 1,47 | 1,59 | 1,63 | 1,02 |
| CARNE_CERDO | 1,43 | 1,64 | 1,65 | 1,45 | 1,01 |
| CONSERVAS_PESCADO_MARISCO | 2,91 | 3,01 | 3,23 | 2,93 | 1,01 |
| SIN_BENCHMARK | 11,80 | 11,88 | 11,76 | 11,81 | 1,00 |
| CARNE_VACUNO | 0,90 | 0,78 | 0,90 | 0,89 | 0,99 |
| NO_FOOD | 15,07 | 14,94 | 14,96 | 14,99 | 0,99 |
| LECHE_SEMIDESNATADA | 1,32 | 1,40 | 1,26 | 1,28 | 0,97 |
| CARNE_FRESCA_OTRAS | 0,58 | 0,59 | 0,60 | 0,53 | 0,92 |
| LECHES_FERMENTADAS | 3,50 | 3,69 | 3,24 | 3,03 | 0,87 |
| LEGUMBRES | 1,25 | 0,95 | 0,99 | 1,02 | 0,81 |
| BOLLERIA_PASTELERIA | 2,87 | 2,87 | 2,47 | 2,29 | 0,80 |
| CARNE_TRANSFORMADA | 1,89 | 2,17 | 1,99 | 1,51 | 0,80 |
| AGUA | 3,23 | 3,39 | 2,83 | 2,55 | 0,79 |
| HUEVOS | 1,02 | 0,78 | 0,77 | 0,78 | 0,77 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 5,93 | 5,17 | 4,71 | 4,43 | 0,75 |
| CARNE_POLLO | 1,89 | 1,88 | 1,82 | 1,38 | 0,73 |
| ACEITE_GIRASOL | 0,32 | 0,24 | 0,23 | 0,23 | 0,70 |
| CHOCOLATES_CACAOS | 1,62 | 1,64 | 1,26 | 1,12 | 0,69 |
| QUESOS | 4,86 | 4,47 | 3,74 | 3,19 | 0,66 |
| PLATOS_PREPARADOS | 5,63 | 5,24 | 4,07 | 3,18 | 0,56 |
| ZUMOS | 1,26 | 1,01 | 0,81 | 0,69 | 0,55 |
| LECHE_ENTERA | 1,11 | 1,08 | 0,63 | 0,60 | 0,54 |
| HARINAS_SEMOLAS | 0,40 | 0,25 | 0,21 | 0,21 | 0,54 |
| PASTAS | 2,35 | 1,82 | 1,42 | 1,04 | 0,44 |
| ARROZ | 1,33 | 0,79 | 0,59 | 0,51 | 0,38 |

Uma razao de 1,00 significa que a faixa etaria nao move aquele grupo. `NO_FOOD` e `SIN_BENCHMARK` ficam perto de 1 por construcao: o informe nao os mede, o indice deles e neutro, e a fatia de cada BLOCO e mantida constante entre coortes de proposito. Sem essa fronteira eles absorviam o residuo da normalizacao e o modelo passava a afirmar que idoso compra 40% menos drogaria — numero que nenhuma fonte deste repo mede, e maior que a maioria dos efeitos que sao medidos.

### Frequencia por comunidade autonoma

Antes desta fase os quatro armazens tinham a MESMA contagem de pedidos por construcao. O informe mede consumo per capita por comunidade, e essa diferenca passa a valer — como frequencia, nunca como tamanho de cesta, porque o informe da kg por ano e nao publica frequencia de compra domestica.

| armazem | pedidos | clientes distintos | pedidos por cliente |
|---|---:|---:|---:|
| bcn1 | 76446 | 54323 | 1,41 |
| mad1 | 83997 | 63209 | 1,33 |
| svq1 | 19854 | 14583 | 1,36 |
| vlc1 | 26226 | 18856 | 1,39 |

O ANTES congelado nao registra contagem por armazem: ele e anterior a esta medicao existir. A comparacao util e entre os quatro armazens de HOJE, que antes eram iguais por construcao.

## Fechamento do canal

A base de clientes foi dimensionada como **2.2% dos adultos** das quatro AUFs, porque 2.2% do volume de alimentacao passa pelo e-commerce. A pergunta que esta secao responde e se o modelo, depois disso, produz 2.2% do consumo daquelas mesmas AUFs — ou se a taxa de penetracao e a cadencia de pedido, declaradas em fases diferentes e sem relacao uma com a outra, se contradizem.

Janela medida: **9 dia(s)**, de 2026-08-24 a 2026-09-01. O consumo do informe e anual e e dividido por 365.

| armazem | populacao servida | comunidade | kg-L/hab/ano | EUR/hab/ano |
|---|---:|---|---:|---:|
| bcn1 | 5,277,804 | Cataluna | 620.82 | 2130.97 |
| mad1 | 7,104,034 | Comunidad de Madrid | 505.86 | 1754.95 |
| svq1 | 1,585,157 | Andalucia | 544.70 | 1692.44 |
| vlc1 | 1,885,230 | Comunitat Valenciana | 593.07 | 1846.63 |
| **total** | **15,852,225** | | | |

| dimensao | canal esperado | modelo | razao |
|---|---:|---:|---:|
| kg ou litro | 4 801 756 | 4 337 726 | **0,90x** |
| receita (EUR) | 16 207 885 | 15 273 746 | **0,94x** |

O escopo dos dois lados e ALIMENTACAO. `NO_FOOD` fica de fora do numerador — 308 352 kg-L e 3 502 050,10 EUR na janela — porque o per capita do informe e de alimentacao e bebidas e nao cobre drogaria. Soma-lo compararia dois universos e inflaria a razao sem que nada estivesse errado. `SIN_BENCHMARK` FICA: sao grupos alimentares que o informe nao detalha, mas que pertencem ao mesmo universo que o per capita mede.

**Nenhum destes numeros foi ajustado para se aproximar do outro, e e isso que os torna interessantes.** A taxa de penetracao entrou na Fase 6, ancorada no informe. `daily_order_rate`, `basket_lines_*` e `quantity_max` entraram na Fase 3, escolhidos sem nenhuma relacao com ela e sem nenhuma fonte que os medisse. As duas metades so se encontram nesta tabela, e o resto entre elas e o que se ve acima.

A distancia que sobra NAO deve ser fechada mexendo em `daily_order_rate` ate a razao virar 1,00: isso faria uma premissa caber num resultado sem que nada tivesse sido medido, e nenhuma fonte deste repositorio mede cadencia de compra nem cesta online — nao existe criterio para decidir qual dos lados esta errado. Enquanto for assim, esta razao e uma OBSERVACAO, e nao um alvo. GATILHO: uma fonte que meca frequencia de compra domestica ou ticket medio por canal transforma esta linha num teste.

Uma ressalva sobre o denominador, pela mesma razao que ela ja existe para a coorte: o consumo per capita do informe e da populacao INTEIRA da comunidade, criancas incluidas, enquanto os clientes sao adultos. Isso e correto aqui — o consumo de um lar aparece no per capita de todos os seus membros — mas significa que a razao acima nao pode ser lida como 'cada cliente compra X% do que deveria'.

## Fronteira que a calibracao nao atravessa

O MAPA mede consumo domestico do residente. NAO mede pedido de loja online, nem cesta, nem cadencia de compra, nem ticket por canal. Por isso `daily_order_rate`, `basket_lines_min/mode/max` e `quantity_max` continuam premissas `synthetic` em `order_premises_seed.csv` e NAO receberam calibracao nenhuma nesta fase. Chamar o benchmark de fonte para esses numeros seria transforma-lo numa falsa representacao da realidade.

