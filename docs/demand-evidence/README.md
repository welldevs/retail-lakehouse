# Reality check da demanda sintetica

GERADO por `make demand-reality-check`. Nenhum numero desta pagina foi escrito a mao; refaca-a em vez de edita-la.

- modelo de demanda: **mapa_2025_v2**
- benchmark: MAPA, Informe del Consumo Alimentario en Espana 2025
- medido em: 2026-09-01T11:58:45Z
- ANTES congelado em: 2026-09-01T11:32:54Z (`before_mapa_2025_v2`) — o estado IMEDIATAMENTE anterior a esta versao do modelo, e nao o mais antigo que existe. `before_mapa_2025_v1` guarda o mix uniforme de antes da calibracao agregada e continua no disco; misturar os dois numa coluna so faria os efeitos de duas fases serem lidos como um.

## Como ler as colunas

**MAPA %** e o consumo domestico bruto. **ALVO %** e o mesmo numero inclinado pela participacao do e-commerce e renormalizado — e contra ele que o modelo deve ser julgado. Os dois diferem de proposito: o informe mede o que o residente consome, e a cesta online nao tem a mesma composicao (1,1% do volume fresco chega por e-commerce contra 2,8% do resto). Fruta fresca DEVE aparecer abaixo dos 14,13% domesticos numa loja online; se aparecesse em 14,13% o modelo estaria confundindo consumo total com canal.

Receita sozinha nao serve como indicador de realismo: ela mistura QUANTO se compra com QUANTO CUSTA. Foi assim que 10 linhas de catalogo com preco de teto de API responderam por 16,6% de toda a receita simulada sem que nenhum total fechasse errado.

## Totais

| dimensao | ANTES | DEPOIS | variacao |
|---|---:|---:|---:|
| unidades | 204 824 | 169 445 | -17,3 % |
| kg ou litro | 144 425,1 | 118 524,7 | -17,9 % |
| receita (EUR) | 583 154,43 | 481 201,94 | -17,5 % |
| EUR por kg | 4,04 | 4,06 | +0,5 % |

O EUR/kg do MAPA para o total da alimentacao domestica em 2025 e **3,25**. O nosso cobre tambem o terco nao alimentar do catalogo, que o informe nao mede, entao os dois nao sao diretamente comparaveis — a comparacao util e grupo a grupo, mais abaixo.

## Os tres blocos

O share alimentar e premissa declarada; a divisao entre calibrado e nao calibrado sai da cobertura do proprio benchmark, e nao de um numero novo.

| bloco | peso declarado | % do volume observado | origem do peso |
|---|---:|---:|---|
| calibrado pelo MAPA | 73,20 % (linhas) | 81,98 % (kg) | food_line_share x cobertura do benchmark (86.12%) |
| alimentar sem benchmark | 11,80 % (linhas) | 11,44 % (kg) | complemento, dividido por tamanho de sortimento |
| nao alimentar | 15,00 % (linhas) | 6,58 % (kg) | 1 - food_line_share; fora do universo do MAPA |

Peso declarado e share de LINHAS; a coluna observada e share de VOLUME. Nao devem coincidir: um pacote de agua pesa 3,5 kg e um sache de tempero pesa 30 g.

## Volume (kg ou litro) — dentro do bloco calibrado

Todas as colunas somam 100 sobre os mesmos grupos. E a dimensao que o MAPA publica e onde a calibracao age.

| grupo | ANTES % | MAPA % | ALVO % | DEPOIS % | erro | canal |
|---|---:|---:|---:|---:|---:|---|
| AGUA | 18,26 | 10,90 | 18,01 | 17,89 | -0,12 | coarse |
| FRUTAS_FRESCAS | 9,51 | 14,13 | 9,17 | 9,25 | +0,08 | coarse |
| LECHE_SEMIDESNATADA | 8,82 | 4,94 | 8,16 | 8,68 | +0,52 | coarse |
| HORTALIZAS_FRESCAS | 5,62 | 8,50 | 6,02 | 5,72 | -0,30 | fine |
| LECHE_ENTERA | 5,30 | 3,24 | 5,35 | 5,60 | +0,25 | coarse |
| CERVEZA | 5,38 | 3,10 | 5,12 | 5,24 | +0,12 | coarse |
| PATATAS | 4,67 | 4,52 | 4,53 | 4,98 | +0,45 | fine |
| LECHE_DESNATADA | 3,57 | 2,36 | 3,90 | 3,89 | -0,01 | coarse |
| LECHES_FERMENTADAS | 3,88 | 2,30 | 3,80 | 3,94 | +0,14 | coarse |
| PLATOS_PREPARADOS | 3,23 | 3,14 | 3,52 | 3,30 | -0,22 | fine |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 3,39 | 2,03 | 3,35 | 3,48 | +0,13 | coarse |
| QUESOS | 2,47 | 1,42 | 2,35 | 2,39 | +0,04 | coarse |
| PAN | 2,24 | 4,76 | 2,25 | 2,28 | +0,04 | fine |
| ACEITES_OLIVA | 1,78 | 1,18 | 1,95 | 1,90 | -0,05 | coarse |
| VINO | 2,11 | 1,18 | 1,95 | 1,97 | +0,02 | coarse |
| CARNE_POLLO | 1,95 | 2,29 | 1,89 | 1,94 | +0,04 | group |
| BOLLERIA_PASTELERIA | 1,80 | 1,01 | 1,73 | 1,86 | +0,14 | fine |
| ZUMOS | 1,59 | 0,94 | 1,55 | 1,55 | +0,00 | coarse |
| CARNE_TRANSFORMADA | 1,58 | 1,84 | 1,52 | 1,59 | +0,07 | group |
| CARNE_CERDO | 1,56 | 1,73 | 1,43 | 1,45 | +0,02 | group |
| HUEVOS | 0,19 | 1,60 | 1,42 | 0,15 | -1,26 | fine |
| PASTAS | 1,25 | 0,74 | 1,22 | 1,16 | -0,06 | coarse |
| LEGUMBRES | 1,06 | 0,60 | 1,06 | 1,12 | +0,05 | fine |
| FRUTOS_SECOS | 1,06 | 0,62 | 1,02 | 1,05 | +0,03 | coarse |
| ARROZ | 1,04 | 0,68 | 1,00 | 0,96 | -0,04 | fine |
| CONSERVAS_PESCADO_MARISCO | 0,95 | 0,71 | 0,92 | 0,93 | +0,00 | fine |
| ACEITE_GIRASOL | 0,86 | 0,55 | 0,91 | 0,86 | -0,05 | coarse |
| CHOCOLATES_CACAOS | 0,90 | 0,49 | 0,81 | 0,81 | +0,00 | coarse |
| PESCADOS_CONGELADOS | 0,62 | 0,35 | 0,58 | 0,61 | +0,03 | coarse |
| PESCADO_FRESCO | 0,56 | 1,21 | 0,57 | 0,56 | -0,01 | fine |
| CARNE_VACUNO | 0,55 | 0,65 | 0,54 | 0,54 | +0,00 | group |
| HARINAS_SEMOLAS | 0,54 | 0,35 | 0,54 | 0,55 | +0,01 | fine |
| CAFES_INFUSIONES | 0,37 | 0,31 | 0,51 | 0,38 | -0,14 | coarse |
| CARNE_FRESCA_OTRAS | 0,45 | 0,55 | 0,45 | 0,49 | +0,04 | group |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0,43 | 0,81 | 0,43 | 0,43 | +0,00 | fine |
| BEBIDAS_ESPIRITUOSAS | 0,19 | 0,12 | 0,20 | 0,21 | +0,01 | coarse |
| MIEL | 0,08 | 0,06 | 0,10 | 0,10 | -0,00 | coarse |
| CARNE_OVINO_CAPRINO | 0,08 | 0,12 | 0,10 | 0,11 | +0,01 | group |
| CARNE_CONEJO | 0,08 | 0,09 | 0,07 | 0,06 | -0,01 | group |

Erro absoluto medio contra o ALVO: **0,116** pontos; maior desvio: **1,263** pontos.

O objetivo NAO e minimizar este numero. Ele esta aqui para que um desvio grande seja explicavel, e nao para ser perseguido — um erro de zero significaria que o sortimento do catalogo casa perfeitamente com a cesta espanhola, o que seria suspeito, nao bom.

Grupos com linhas sem conversao para kg (o desvio deles contra o ALVO nao mede a calibracao, mede a cobertura da conversao): `HORTALIZAS_FRESCAS` (886 linhas), `PLATOS_PREPARADOS` (446 linhas), `CARNE_TRANSFORMADA` (45 linhas), `HUEVOS` (654 linhas), `CAFES_INFUSIONES` (428 linhas)

## Unidades — sobre o total, inclusive nao alimentar

Nao e comparavel ao MAPA: o informe mede peso e volume, nunca peca. Esta tabela existe para mostrar quantas LINHAS cada grupo ocupa na cesta.

| grupo | ANTES % | DEPOIS % |
|---|---:|---:|
| AGUA | 2,97 | 2,94 |
| SIN_BENCHMARK | 11,83 | 11,54 |
| FRUTAS_FRESCAS | 5,86 | 5,81 |
| LECHE_SEMIDESNATADA | 1,33 | 1,38 |
| NO_FOOD | 15,00 | 15,01 |
| HORTALIZAS_FRESCAS | 9,09 | 9,25 |
| LECHE_ENTERA | 0,86 | 0,86 |
| CERVEZA | 1,86 | 1,89 |
| PATATAS | 1,94 | 1,98 |
| LECHES_FERMENTADAS | 3,33 | 3,40 |
| LECHE_DESNATADA | 0,62 | 0,63 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 5,03 | 5,11 |
| PLATOS_PREPARADOS | 4,61 | 4,68 |
| QUESOS | 4,03 | 4,11 |
| PAN | 3,43 | 3,50 |
| VINO | 1,33 | 1,31 |
| CARNE_POLLO | 1,79 | 1,77 |
| ACEITES_OLIVA | 0,49 | 0,49 |
| BOLLERIA_PASTELERIA | 2,69 | 2,80 |
| CARNE_TRANSFORMADA | 1,96 | 1,91 |
| ZUMOS | 0,95 | 0,91 |
| CARNE_CERDO | 1,61 | 1,51 |
| PASTAS | 1,68 | 1,54 |
| LEGUMBRES | 1,06 | 1,06 |
| FRUTOS_SECOS | 2,78 | 2,75 |
| ARROZ | 0,81 | 0,78 |
| CONSERVAS_PESCADO_MARISCO | 3,02 | 2,93 |
| ACEITE_GIRASOL | 0,23 | 0,21 |
| CHOCOLATES_CACAOS | 1,48 | 1,47 |
| PESCADOS_CONGELADOS | 0,77 | 0,72 |
| PESCADO_FRESCO | 0,53 | 0,59 |
| HARINAS_SEMOLAS | 0,25 | 0,29 |
| CARNE_VACUNO | 0,84 | 0,85 |
| CARNE_FRESCA_OTRAS | 0,56 | 0,61 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0,50 | 0,50 |
| CAFES_INFUSIONES | 1,61 | 1,61 |
| BEBIDAS_ESPIRITUOSAS | 0,16 | 0,16 |
| HUEVOS | 0,85 | 0,80 |
| CARNE_OVINO_CAPRINO | 0,17 | 0,21 |
| MIEL | 0,08 | 0,09 |
| CARNE_CONEJO | 0,06 | 0,05 |

## Receita — sobre o total

A coluna do MAPA e o share de VALOR domestico, sem inclinacao de canal. Ela NAO e um alvo: a receita e consequencia do volume e do preco observado da Mercadona, e converge com o MAPA so na medida em que os precos espanhois e os da Mercadona se parecem. Volume e valor devem DIVERGIR entre si — mariscos sao 0,81% do volume e 2,88% do valor domestico, e um simulador que os igualasse estaria errado.

| grupo | ANTES % | MAPA valor % | DEPOIS % |
|---|---:|---:|---:|
| AGUA | 1,84 | 0,80 | 1,78 |
| SIN_BENCHMARK | 8,75 | — | 8,56 |
| FRUTAS_FRESCAS | 4,22 | 9,94 | 4,16 |
| LECHE_SEMIDESNATADA | 1,95 | 1,46 | 1,89 |
| NO_FOOD | 18,80 | — | 18,95 |
| HORTALIZAS_FRESCAS | 5,62 | 6,60 | 5,75 |
| LECHE_ENTERA | 1,20 | 1,02 | 1,22 |
| CERVEZA | 1,98 | 1,57 | 1,89 |
| PATATAS | 1,71 | 1,85 | 1,81 |
| LECHES_FERMENTADAS | 1,87 | 1,85 | 1,89 |
| LECHE_DESNATADA | 0,85 | 0,68 | 0,93 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 3,11 | 1,83 | 3,17 |
| PLATOS_PREPARADOS | 4,72 | 5,51 | 4,79 |
| QUESOS | 5,09 | 4,34 | 5,07 |
| PAN | 1,61 | 3,96 | 1,61 |
| VINO | 1,52 | 1,29 | 1,48 |
| CARNE_POLLO | 2,46 | 3,90 | 2,45 |
| ACEITES_OLIVA | 1,48 | 1,74 | 1,53 |
| BOLLERIA_PASTELERIA | 2,95 | 2,18 | 3,06 |
| CARNE_TRANSFORMADA | 6,06 | 6,79 | 6,04 |
| ZUMOS | 0,51 | 0,44 | 0,49 |
| CARNE_CERDO | 2,28 | 4,05 | 2,21 |
| PASTAS | 0,88 | 0,60 | 0,81 |
| LEGUMBRES | 0,58 | 0,40 | 0,59 |
| FRUTOS_SECOS | 1,95 | 1,72 | 1,94 |
| ARROZ | 0,52 | 0,51 | 0,48 |
| CONSERVAS_PESCADO_MARISCO | 3,14 | 2,87 | 3,03 |
| ACEITE_GIRASOL | 0,39 | 0,30 | 0,38 |
| CHOCOLATES_CACAOS | 1,63 | 1,81 | 1,49 |
| PESCADOS_CONGELADOS | 1,41 | 1,17 | 1,33 |
| PESCADO_FRESCO | 1,41 | 4,10 | 1,45 |
| HARINAS_SEMOLAS | 0,13 | 0,16 | 0,14 |
| CARNE_VACUNO | 2,20 | 2,87 | 2,21 |
| CARNE_FRESCA_OTRAS | 0,70 | 1,30 | 0,80 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0,99 | 2,88 | 1,04 |
| CAFES_INFUSIONES | 1,69 | 2,00 | 1,69 |
| BEBIDAS_ESPIRITUOSAS | 0,44 | 0,41 | 0,45 |
| HUEVOS | 0,79 | 1,90 | 0,76 |
| CARNE_OVINO_CAPRINO | 0,32 | 0,61 | 0,46 |
| MIEL | 0,11 | 0,13 | 0,12 |
| CARNE_CONEJO | 0,15 | 0,25 | 0,12 |

## Preco medio por kg — a coluna comparavel sem conversao nenhuma

Share exige converter volume em linhas e inclinar por canal; EUR/kg nao exige nada. Um grupo cujo EUR/kg bate com o informe tem preco observado saudavel, independentemente de quantas linhas dele saem — e foi esta coluna que denunciou o defeito de preco de granel antes da calibracao existir.

| grupo | ANTES EUR/kg | MAPA EUR/kg | DEPOIS EUR/kg | linhas sem kg |
|---|---:|---:|---:|---:|
| AGUA | 0,50 | 0,24 | 0,49 | 0 |
| SIN_BENCHMARK | 2,93 | — | 3,04 | 13 |
| FRUTAS_FRESCAS | 2,20 | 2,28 | 2,23 | 0 |
| LECHE_SEMIDESNATADA | 1,10 | 0,96 | 1,08 | 0 |
| NO_FOOD | 11,59 | — | 11,68 | 7346 |
| HORTALIZAS_FRESCAS | 4,96 | 2,52 | 4,98 | 886 |
| LECHE_ENTERA | 1,12 | 1,02 | 1,08 | 0 |
| CERVEZA | 1,83 | 1,64 | 1,78 | 0 |
| PATATAS | 1,82 | 1,33 | 1,80 | 0 |
| LECHES_FERMENTADAS | 2,39 | 2,61 | 2,37 | 0 |
| LECHE_DESNATADA | 1,18 | 0,94 | 1,18 | 0 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 4,54 | 2,92 | 4,52 | 0 |
| PLATOS_PREPARADOS | 7,25 | 5,69 | 7,18 | 446 |
| QUESOS | 10,19 | 9,96 | 10,52 | 0 |
| PAN | 3,56 | 2,70 | 3,49 | 0 |
| VINO | 3,57 | 3,55 | 3,71 | 0 |
| CARNE_POLLO | 6,28 | 5,53 | 6,27 | 0 |
| ACEITES_OLIVA | 4,13 | 4,79 | 3,98 | 0 |
| BOLLERIA_PASTELERIA | 8,15 | 6,99 | 8,14 | 0 |
| CARNE_TRANSFORMADA | 19,03 | 11,99 | 18,78 | 45 |
| ZUMOS | 1,60 | 1,51 | 1,56 | 0 |
| CARNE_CERDO | 7,23 | 7,59 | 7,53 | 0 |
| PASTAS | 3,49 | 2,65 | 3,46 | 0 |
| LEGUMBRES | 2,71 | 2,18 | 2,60 | 0 |
| FRUTOS_SECOS | 9,10 | 8,98 | 9,09 | 0 |
| ARROZ | 2,47 | 2,41 | 2,48 | 0 |
| CONSERVAS_PESCADO_MARISCO | 16,37 | 13,16 | 16,23 | 0 |
| ACEITE_GIRASOL | 2,27 | 1,77 | 2,17 | 0 |
| CHOCOLATES_CACAOS | 8,99 | 11,93 | 9,07 | 0 |
| PESCADOS_CONGELADOS | 11,19 | — | 10,77 | 0 |
| PESCADO_FRESCO | 12,53 | 10,98 | 12,76 | 0 |
| HARINAS_SEMOLAS | 1,16 | 1,47 | 1,29 | 0 |
| CARNE_VACUNO | 19,76 | 14,27 | 20,25 | 0 |
| CARNE_FRESCA_OTRAS | 7,80 | — | 8,02 | 0 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 11,39 | 11,50 | 11,92 | 0 |
| CAFES_INFUSIONES | 22,66 | 21,19 | 22,14 | 428 |
| BEBIDAS_ESPIRITUOSAS | 11,25 | 11,58 | 10,60 | 0 |
| HUEVOS | 20,81 | 3,85 | 24,57 | 654 |
| CARNE_OVINO_CAPRINO | 18,99 | 17,04 | 20,25 | 0 |
| MIEL | 6,34 | 7,55 | 6,06 | 0 |
| CARNE_CONEJO | 9,87 | 9,00 | 9,46 | 0 |

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
| CARNE_CONEJO | 0,01 | 0,03 | 0,08 | 0,08 | 6,08 |
| VINO | 0,50 | 0,81 | 1,57 | 2,44 | 4,89 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0,25 | 0,41 | 0,57 | 0,80 | 3,16 |
| BEBIDAS_ESPIRITUOSAS | 0,09 | 0,11 | 0,18 | 0,26 | 3,03 |
| CARNE_OVINO_CAPRINO | 0,13 | 0,13 | 0,24 | 0,33 | 2,57 |
| PESCADO_FRESCO | 0,41 | 0,47 | 0,72 | 0,78 | 1,90 |
| ACEITES_OLIVA | 0,36 | 0,36 | 0,61 | 0,68 | 1,86 |
| FRUTAS_FRESCAS | 4,30 | 5,26 | 5,81 | 7,77 | 1,80 |
| PESCADOS_CONGELADOS | 0,54 | 0,60 | 0,78 | 0,91 | 1,70 |
| MIEL | 0,08 | 0,04 | 0,14 | 0,14 | 1,65 |
| CERVEZA | 1,31 | 1,85 | 2,33 | 2,02 | 1,54 |
| LECHE_DESNATADA | 0,52 | 0,63 | 0,75 | 0,72 | 1,38 |
| FRUTOS_SECOS | 2,19 | 2,70 | 3,07 | 2,97 | 1,35 |
| HORTALIZAS_FRESCAS | 8,46 | 8,10 | 9,28 | 11,13 | 1,31 |
| PAN | 2,80 | 3,50 | 4,03 | 3,62 | 1,29 |
| PATATAS | 1,71 | 1,85 | 2,19 | 1,99 | 1,16 |
| CONSERVAS_PESCADO_MARISCO | 2,67 | 2,84 | 3,37 | 2,96 | 1,11 |
| CARNE_CERDO | 1,33 | 1,64 | 1,61 | 1,42 | 1,07 |
| LECHE_SEMIDESNATADA | 1,31 | 1,47 | 1,35 | 1,35 | 1,03 |
| CARNE_VACUNO | 0,90 | 0,78 | 0,86 | 0,93 | 1,03 |
| SIN_BENCHMARK | 11,67 | 11,70 | 11,77 | 11,62 | 1,00 |
| CAFES_INFUSIONES | 1,62 | 1,51 | 1,51 | 1,59 | 0,99 |
| NO_FOOD | 15,35 | 14,65 | 15,01 | 15,09 | 0,98 |
| ACEITE_GIRASOL | 0,23 | 0,20 | 0,22 | 0,22 | 0,98 |
| CARNE_FRESCA_OTRAS | 0,62 | 0,62 | 0,59 | 0,54 | 0,87 |
| LEGUMBRES | 1,22 | 0,96 | 1,11 | 1,06 | 0,87 |
| LECHES_FERMENTADAS | 3,46 | 3,78 | 3,34 | 2,96 | 0,86 |
| HUEVOS | 1,03 | 0,70 | 0,69 | 0,84 | 0,81 |
| AGUA | 3,07 | 3,33 | 2,71 | 2,48 | 0,81 |
| BOLLERIA_PASTELERIA | 3,01 | 3,08 | 2,58 | 2,39 | 0,79 |
| CARNE_TRANSFORMADA | 1,97 | 2,19 | 2,03 | 1,53 | 0,78 |
| CARNE_POLLO | 1,86 | 2,05 | 1,71 | 1,38 | 0,74 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 6,30 | 5,14 | 4,58 | 4,66 | 0,74 |
| CHOCOLATES_CACAOS | 1,66 | 1,67 | 1,30 | 1,14 | 0,68 |
| QUESOS | 4,77 | 4,57 | 3,67 | 3,17 | 0,66 |
| HARINAS_SEMOLAS | 0,39 | 0,26 | 0,24 | 0,26 | 0,66 |
| LECHE_ENTERA | 1,13 | 1,01 | 0,67 | 0,65 | 0,58 |
| PLATOS_PREPARADOS | 5,66 | 5,51 | 4,07 | 3,12 | 0,55 |
| ZUMOS | 1,34 | 0,97 | 0,72 | 0,64 | 0,48 |
| PASTAS | 2,34 | 1,77 | 1,31 | 0,94 | 0,40 |
| ARROZ | 1,40 | 0,74 | 0,64 | 0,44 | 0,31 |

Uma razao de 1,00 significa que a faixa etaria nao move aquele grupo. `NO_FOOD` e `SIN_BENCHMARK` ficam perto de 1 por construcao: o informe nao os mede, o indice deles e neutro, e a fatia de cada BLOCO e mantida constante entre coortes de proposito. Sem essa fronteira eles absorviam o residuo da normalizacao e o modelo passava a afirmar que idoso compra 40% menos drogaria — numero que nenhuma fonte deste repo mede, e maior que a maioria dos efeitos que sao medidos.

### Frequencia por comunidade autonoma

Antes desta fase os quatro armazens tinham a MESMA contagem de pedidos por construcao. O informe mede consumo per capita por comunidade, e essa diferenca passa a valer — como frequencia, nunca como tamanho de cesta, porque o informe da kg por ano e nao publica frequencia de compra domestica.

| armazem | pedidos | clientes distintos | pedidos por cliente |
|---|---:|---:|---:|
| bcn1 | 1436 | 1251 | 1,15 |
| mad1 | 1176 | 1049 | 1,12 |
| svq1 | 1252 | 1110 | 1,13 |
| vlc1 | 1384 | 1204 | 1,15 |

O ANTES congelado nao registra contagem por armazem: ele e anterior a esta medicao existir. A comparacao util e entre os quatro armazens de HOJE, que antes eram iguais por construcao.

## Fronteira que a calibracao nao atravessa

O MAPA mede consumo domestico do residente. NAO mede pedido de loja online, nem cesta, nem cadencia de compra, nem ticket por canal. Por isso `daily_order_rate`, `basket_lines_min/mode/max` e `quantity_max` continuam premissas `synthetic` em `order_premises_seed.csv` e NAO receberam calibracao nenhuma nesta fase. Chamar o benchmark de fonte para esses numeros seria transforma-lo numa falsa representacao da realidade.

