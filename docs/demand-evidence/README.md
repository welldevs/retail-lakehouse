# Reality check da demanda sintetica

GERADO por `make demand-reality-check`. Nenhum numero desta pagina foi escrito a mao; refaca-a em vez de edita-la.

- modelo de demanda: **mapa_2025_v1**
- benchmark: MAPA, Informe del Consumo Alimentario en Espana 2025
- medido em: 2026-08-31T18:28:09Z
- ANTES congelado em: 2026-08-31T18:03:36Z

## Como ler as colunas

**MAPA %** e o consumo domestico bruto. **ALVO %** e o mesmo numero inclinado pela participacao do e-commerce e renormalizado — e contra ele que o modelo deve ser julgado. Os dois diferem de proposito: o informe mede o que o residente consome, e a cesta online nao tem a mesma composicao (1,1% do volume fresco chega por e-commerce contra 2,8% do resto). Fruta fresca DEVE aparecer abaixo dos 14,13% domesticos numa loja online; se aparecesse em 14,13% o modelo estaria confundindo consumo total com canal.

Receita sozinha nao serve como indicador de realismo: ela mistura QUANTO se compra com QUANTO CUSTA. Foi assim que 10 linhas de catalogo com preco de teto de API responderam por 16,6% de toda a receita simulada sem que nenhum total fechasse errado.

## Totais

| dimensao | ANTES | DEPOIS | variacao |
|---|---:|---:|---:|
| unidades | 204 871 | 204 824 | -0,0 % |
| kg ou litro | 126 550,2 | 144 425,1 | +14,1 % |
| receita (EUR) | 821 121,93 | 583 154,43 | -29,0 % |
| EUR por kg | 6,49 | 4,04 | -37,8 % |

O EUR/kg do MAPA para o total da alimentacao domestica em 2025 e **3,25**. O nosso cobre tambem o terco nao alimentar do catalogo, que o informe nao mede, entao os dois nao sao diretamente comparaveis — a comparacao util e grupo a grupo, mais abaixo.

## Os tres blocos

O share alimentar e premissa declarada; a divisao entre calibrado e nao calibrado sai da cobertura do proprio benchmark, e nao de um numero novo.

| bloco | peso declarado | % do volume observado | origem do peso |
|---|---:|---:|---|
| calibrado pelo MAPA | 73,20 % (linhas) | 81,39 % (kg) | food_line_share x cobertura do benchmark (86.12%) |
| alimentar sem benchmark | 11,80 % (linhas) | 12,06 % (kg) | complemento, dividido por tamanho de sortimento |
| nao alimentar | 15,00 % (linhas) | 6,55 % (kg) | 1 - food_line_share; fora do universo do MAPA |

Peso declarado e share de LINHAS; a coluna observada e share de VOLUME. Nao devem coincidir: um pacote de agua pesa 3,5 kg e um sache de tempero pesa 30 g.

## Volume (kg ou litro) — dentro do bloco calibrado

Todas as colunas somam 100 sobre os mesmos grupos. E a dimensao que o MAPA publica e onde a calibracao age.

| grupo | ANTES % | MAPA % | ALVO % | DEPOIS % | erro | canal |
|---|---:|---:|---:|---:|---:|---|
| AGUA | 8,91 | 10,90 | 18,01 | 18,26 | +0,26 | coarse |
| FRUTAS_FRESCAS | 3,11 | 14,13 | 9,17 | 9,51 | +0,34 | coarse |
| LECHE_SEMIDESNATADA | 4,79 | 4,94 | 8,16 | 8,82 | +0,66 | coarse |
| HORTALIZAS_FRESCAS | 2,16 | 8,50 | 6,02 | 5,62 | -0,40 | fine |
| LECHE_ENTERA | 2,62 | 3,24 | 5,35 | 5,30 | -0,05 | coarse |
| CERVEZA | 7,70 | 3,10 | 5,12 | 5,38 | +0,26 | coarse |
| PATATAS | 1,22 | 4,52 | 4,53 | 4,67 | +0,13 | fine |
| LECHE_DESNATADA | 4,49 | 2,36 | 3,90 | 3,57 | -0,33 | coarse |
| LECHES_FERMENTADAS | 4,08 | 2,30 | 3,80 | 3,88 | +0,08 | coarse |
| PLATOS_PREPARADOS | 3,70 | 3,14 | 3,52 | 3,23 | -0,29 | fine |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 2,77 | 2,03 | 3,35 | 3,39 | +0,04 | coarse |
| QUESOS | 2,32 | 1,42 | 2,35 | 2,47 | +0,13 | coarse |
| PAN | 2,50 | 4,76 | 2,25 | 2,24 | -0,00 | fine |
| ACEITES_OLIVA | 2,04 | 1,18 | 1,95 | 1,78 | -0,17 | coarse |
| VINO | 3,75 | 1,18 | 1,95 | 2,11 | +0,16 | coarse |
| CARNE_POLLO | 2,08 | 2,29 | 1,89 | 1,95 | +0,05 | group |
| BOLLERIA_PASTELERIA | 2,19 | 1,01 | 1,73 | 1,80 | +0,07 | fine |
| ZUMOS | 3,26 | 0,94 | 1,55 | 1,59 | +0,04 | coarse |
| CARNE_TRANSFORMADA | 5,47 | 1,84 | 1,52 | 1,58 | +0,06 | group |
| CARNE_CERDO | 1,43 | 1,73 | 1,43 | 1,56 | +0,14 | group |
| HUEVOS | 0,07 | 1,60 | 1,42 | 0,19 | -1,23 | fine |
| PASTAS | 1,64 | 0,74 | 1,22 | 1,25 | +0,03 | coarse |
| LEGUMBRES | 0,65 | 0,60 | 1,06 | 1,06 | +0,00 | fine |
| FRUTOS_SECOS | 0,85 | 0,62 | 1,02 | 1,06 | +0,04 | coarse |
| ARROZ | 0,90 | 0,68 | 1,00 | 1,04 | +0,04 | fine |
| CONSERVAS_PESCADO_MARISCO | 0,46 | 0,71 | 0,92 | 0,95 | +0,03 | fine |
| ACEITE_GIRASOL | 0,32 | 0,55 | 0,91 | 0,86 | -0,05 | coarse |
| CHOCOLATES_CACAOS | 1,26 | 0,49 | 0,81 | 0,90 | +0,09 | coarse |
| PESCADOS_CONGELADOS | 1,39 | 0,35 | 0,58 | 0,62 | +0,05 | coarse |
| PESCADO_FRESCO | 5,03 | 1,21 | 0,57 | 0,56 | -0,01 | fine |
| CARNE_VACUNO | 0,28 | 0,65 | 0,54 | 0,55 | +0,02 | group |
| HARINAS_SEMOLAS | 0,94 | 0,35 | 0,54 | 0,54 | +0,01 | fine |
| CAFES_INFUSIONES | 0,74 | 0,31 | 0,51 | 0,37 | -0,14 | coarse |
| CARNE_FRESCA_OTRAS | 1,07 | 0,55 | 0,45 | 0,45 | -0,01 | group |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 11,44 | 0,81 | 0,43 | 0,43 | -0,00 | fine |
| BEBIDAS_ESPIRITUOSAS | 2,07 | 0,12 | 0,20 | 0,19 | -0,00 | coarse |
| MIEL | 0,13 | 0,06 | 0,10 | 0,08 | -0,02 | coarse |
| CARNE_OVINO_CAPRINO | 0,06 | 0,12 | 0,10 | 0,08 | -0,02 | group |
| CARNE_CONEJO | 0,11 | 0,09 | 0,07 | 0,08 | +0,00 | group |

Erro absoluto medio contra o ALVO: **0,139** pontos; maior desvio: **1,227** pontos.

O objetivo NAO e minimizar este numero. Ele esta aqui para que um desvio grande seja explicavel, e nao para ser perseguido — um erro de zero significaria que o sortimento do catalogo casa perfeitamente com a cesta espanhola, o que seria suspeito, nao bom.

Grupos com linhas sem conversao para kg (o desvio deles contra o ALVO nao mede a calibracao, mede a cobertura da conversao): `HORTALIZAS_FRESCAS` (1080 linhas), `PLATOS_PREPARADOS` (534 linhas), `CARNE_TRANSFORMADA` (55 linhas), `HUEVOS` (809 linhas), `CAFES_INFUSIONES` (529 linhas)

## Unidades — sobre o total, inclusive nao alimentar

Nao e comparavel ao MAPA: o informe mede peso e volume, nunca peca. Esta tabela existe para mostrar quantas LINHAS cada grupo ocupa na cesta.

| grupo | ANTES % | DEPOIS % |
|---|---:|---:|
| AGUA | 1,00 | 2,97 |
| SIN_BENCHMARK | 20,14 | 11,83 |
| FRUTAS_FRESCAS | 1,24 | 5,86 |
| LECHE_SEMIDESNATADA | 0,51 | 1,33 |
| NO_FOOD | 31,36 | 15,00 |
| HORTALIZAS_FRESCAS | 2,34 | 9,09 |
| CERVEZA | 1,84 | 1,86 |
| LECHE_ENTERA | 0,26 | 0,86 |
| PATATAS | 0,36 | 1,94 |
| LECHES_FERMENTADAS | 2,31 | 3,33 |
| LECHE_DESNATADA | 0,51 | 0,62 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 2,73 | 5,03 |
| PLATOS_PREPARADOS | 3,44 | 4,61 |
| QUESOS | 2,66 | 4,03 |
| PAN | 2,59 | 3,43 |
| VINO | 1,59 | 1,33 |
| CARNE_POLLO | 1,26 | 1,79 |
| BOLLERIA_PASTELERIA | 2,19 | 2,69 |
| ACEITES_OLIVA | 0,32 | 0,49 |
| ZUMOS | 1,29 | 0,95 |
| CARNE_TRANSFORMADA | 4,53 | 1,96 |
| CARNE_CERDO | 0,99 | 1,61 |
| PASTAS | 1,45 | 1,68 |
| LEGUMBRES | 0,41 | 1,06 |
| FRUTOS_SECOS | 1,47 | 2,78 |
| ARROZ | 0,47 | 0,81 |
| CONSERVAS_PESCADO_MARISCO | 0,98 | 3,02 |
| CHOCOLATES_CACAOS | 1,54 | 1,48 |
| ACEITE_GIRASOL | 0,07 | 0,23 |
| PESCADOS_CONGELADOS | 1,14 | 0,77 |
| PESCADO_FRESCO | 1,10 | 0,53 |
| CARNE_VACUNO | 0,29 | 0,84 |
| HARINAS_SEMOLAS | 0,32 | 0,25 |
| CARNE_FRESCA_OTRAS | 0,84 | 0,56 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 0,79 | 0,50 |
| CAFES_INFUSIONES | 2,13 | 1,61 |
| BEBIDAS_ESPIRITUOSAS | 1,07 | 0,16 |
| HUEVOS | 0,24 | 0,85 |
| CARNE_OVINO_CAPRINO | 0,08 | 0,17 |
| MIEL | 0,09 | 0,08 |
| CARNE_CONEJO | 0,06 | 0,06 |

## Receita — sobre o total

A coluna do MAPA e o share de VALOR domestico, sem inclinacao de canal. Ela NAO e um alvo: a receita e consequencia do volume e do preco observado da Mercadona, e converge com o MAPA so na medida em que os precos espanhois e os da Mercadona se parecem. Volume e valor devem DIVERGIR entre si — mariscos sao 0,81% do volume e 2,88% do valor domestico, e um simulador que os igualasse estaria errado.

| grupo | ANTES % | MAPA valor % | DEPOIS % |
|---|---:|---:|---:|
| AGUA | 0,43 | 0,80 | 1,84 |
| SIN_BENCHMARK | 10,67 | — | 8,75 |
| FRUTAS_FRESCAS | 0,65 | 9,94 | 4,22 |
| LECHE_SEMIDESNATADA | 0,51 | 1,46 | 1,95 |
| NO_FOOD | 27,73 | — | 18,80 |
| HORTALIZAS_FRESCAS | 1,00 | 6,60 | 5,62 |
| CERVEZA | 1,35 | 1,57 | 1,98 |
| LECHE_ENTERA | 0,26 | 1,02 | 1,20 |
| PATATAS | 0,21 | 1,85 | 1,71 |
| LECHES_FERMENTADAS | 0,94 | 1,85 | 1,87 |
| LECHE_DESNATADA | 0,52 | 0,68 | 0,85 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 1,21 | 1,83 | 3,11 |
| PLATOS_PREPARADOS | 2,49 | 5,51 | 4,72 |
| QUESOS | 2,30 | 4,34 | 5,09 |
| PAN | 0,83 | 3,96 | 1,61 |
| VINO | 1,26 | 1,29 | 1,52 |
| CARNE_POLLO | 1,28 | 3,90 | 2,46 |
| BOLLERIA_PASTELERIA | 1,71 | 2,18 | 2,95 |
| ACEITES_OLIVA | 0,76 | 1,74 | 1,48 |
| ZUMOS | 0,50 | 0,44 | 0,51 |
| CARNE_TRANSFORMADA | 10,58 | 6,79 | 6,06 |
| CARNE_CERDO | 1,04 | 4,05 | 2,28 |
| PASTAS | 0,54 | 0,60 | 0,88 |
| LEGUMBRES | 0,16 | 0,40 | 0,58 |
| FRUTOS_SECOS | 0,71 | 1,72 | 1,95 |
| ARROZ | 0,21 | 0,51 | 0,52 |
| CONSERVAS_PESCADO_MARISCO | 0,72 | 2,87 | 3,14 |
| CHOCOLATES_CACAOS | 1,09 | 1,81 | 1,63 |
| ACEITE_GIRASOL | 0,07 | 0,30 | 0,39 |
| PESCADOS_CONGELADOS | 1,45 | 1,17 | 1,41 |
| PESCADO_FRESCO | 4,64 | 4,10 | 1,41 |
| CARNE_VACUNO | 0,53 | 2,87 | 2,20 |
| HARINAS_SEMOLAS | 0,11 | 0,16 | 0,13 |
| CARNE_FRESCA_OTRAS | 0,83 | 1,30 | 0,70 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 16,45 | 2,88 | 0,99 |
| CAFES_INFUSIONES | 1,64 | 2,00 | 1,69 |
| BEBIDAS_ESPIRITUOSAS | 2,18 | 0,41 | 0,44 |
| HUEVOS | 0,16 | 1,90 | 0,79 |
| CARNE_OVINO_CAPRINO | 0,10 | 0,61 | 0,32 |
| MIEL | 0,08 | 0,13 | 0,11 |
| CARNE_CONEJO | 0,10 | 0,25 | 0,15 |

## Preco medio por kg — a coluna comparavel sem conversao nenhuma

Share exige converter volume em linhas e inclinar por canal; EUR/kg nao exige nada. Um grupo cujo EUR/kg bate com o informe tem preco observado saudavel, independentemente de quantas linhas dele saem — e foi esta coluna que denunciou o defeito de preco de granel antes da calibracao existir.

| grupo | ANTES EUR/kg | MAPA EUR/kg | DEPOIS EUR/kg | linhas sem kg |
|---|---:|---:|---:|---:|
| AGUA | 0,50 | 0,24 | 0,50 | 0 |
| SIN_BENCHMARK | 3,02 | — | 2,93 | 19 |
| FRUTAS_FRESCAS | 2,19 | 2,28 | 2,20 | 0 |
| LECHE_SEMIDESNATADA | 1,13 | 0,96 | 1,10 | 0 |
| NO_FOOD | 11,69 | — | 11,59 | 8850 |
| HORTALIZAS_FRESCAS | 4,87 | 2,52 | 4,96 | 1080 |
| CERVEZA | 1,85 | 1,64 | 1,83 | 0 |
| LECHE_ENTERA | 1,06 | 1,02 | 1,12 | 0 |
| PATATAS | 1,84 | 1,33 | 1,82 | 0 |
| LECHES_FERMENTADAS | 2,42 | 2,61 | 2,39 | 0 |
| LECHE_DESNATADA | 1,21 | 0,94 | 1,18 | 0 |
| FRUTAS_HORTALIZAS_TRANSFORMADAS | 4,60 | 2,92 | 4,54 | 0 |
| PLATOS_PREPARADOS | 7,09 | 5,69 | 7,25 | 534 |
| QUESOS | 10,44 | 9,96 | 10,19 | 0 |
| PAN | 3,51 | 2,70 | 3,56 | 0 |
| VINO | 3,53 | 3,55 | 3,57 | 0 |
| CARNE_POLLO | 6,47 | 5,53 | 6,28 | 0 |
| BOLLERIA_PASTELERIA | 8,20 | 6,99 | 8,15 | 0 |
| ACEITES_OLIVA | 3,93 | 4,79 | 4,13 | 0 |
| ZUMOS | 1,60 | 1,51 | 1,60 | 0 |
| CARNE_TRANSFORMADA | 20,36 | 11,99 | 19,03 | 55 |
| CARNE_CERDO | 7,67 | 7,59 | 7,23 | 0 |
| PASTAS | 3,47 | 2,65 | 3,49 | 0 |
| LEGUMBRES | 2,54 | 2,18 | 2,71 | 0 |
| FRUTOS_SECOS | 8,76 | 8,98 | 9,10 | 0 |
| ARROZ | 2,52 | 2,41 | 2,47 | 0 |
| CONSERVAS_PESCADO_MARISCO | 16,40 | 13,16 | 16,37 | 0 |
| CHOCOLATES_CACAOS | 9,14 | 11,93 | 8,99 | 0 |
| ACEITE_GIRASOL | 2,44 | 1,77 | 2,27 | 0 |
| PESCADOS_CONGELADOS | 11,00 | — | 11,19 | 0 |
| PESCADO_FRESCO | 9,71 | 10,98 | 12,53 | 0 |
| CARNE_VACUNO | 19,70 | 14,27 | 19,76 | 0 |
| HARINAS_SEMOLAS | 1,28 | 1,47 | 1,16 | 0 |
| CARNE_FRESCA_OTRAS | 8,11 | — | 7,80 | 0 |
| MARISCOS_MOLUSCOS_CRUSTACEOS | 15,13 | 11,50 | 11,39 | 0 |
| CAFES_INFUSIONES | 23,19 | 21,19 | 22,66 | 529 |
| BEBIDAS_ESPIRITUOSAS | 11,10 | 11,58 | 11,25 | 0 |
| HUEVOS | 22,86 | 3,85 | 20,81 | 809 |
| CARNE_OVINO_CAPRINO | 17,23 | 17,04 | 18,99 | 0 |
| MIEL | 6,38 | 7,55 | 6,34 | 0 |
| CARNE_CONEJO | 9,21 | 9,00 | 9,87 | 0 |

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

## Fronteira que a calibracao nao atravessa

O MAPA mede consumo domestico do residente. NAO mede pedido de loja online, nem cesta, nem cadencia de compra, nem ticket por canal. Por isso `daily_order_rate`, `basket_lines_min/mode/max` e `quantity_max` continuam premissas `synthetic` em `order_premises_seed.csv` e NAO receberam calibracao nenhuma nesta fase. Chamar o benchmark de fonte para esses numeros seria transforma-lo numa falsa representacao da realidade.

