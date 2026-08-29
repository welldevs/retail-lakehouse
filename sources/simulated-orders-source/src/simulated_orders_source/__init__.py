"""Simulated Orders Source — pedidos sinteticos como LOG DE EVENTOS.

Quinta source do repo, e a segunda DERIVADA: consome o Silver que as outras produziram e
devolve uma RAW nova. A regra de ouro da Fase 1 vale igual, deslocada um nivel:

    o PEDIDO e inventado; quem compra, o que se compra, quanto custa e onde mora nao.

Cliente vem de `silver_customer`, produto e preco vem de `silver_product_price` do MESMO
armazem na MESMA data. Nenhum produto, preco ou cliente e fabricado aqui.

O QUE ESTA SOURCE ENTREGA E UM LOG, NAO UMA FOTOGRAFIA
------------------------------------------------------
A particao contem `order_events.jsonl` e mais nada. NAO existe `orders.json` com o estado
dobrado ao lado, de proposito: duas representacoes da mesma verdade divergem. O estado do
pedido e o FOLD dos seus eventos, e o fold mora no Silver.

O fold e nao-trivial por construcao: substituicao e remocao de linha alteram a cesta DEPOIS
da colocacao, entao o valor final do pedido nao e derivavel do evento `order_placed`. Se ele
fosse, o log seria um carimbo de data e o modelo de eventos seria enfeite.

Somente biblioteca padrao. Ver CONTRACT.md.
"""

from __future__ import annotations

__version__ = "1.0.0"

SOURCE_NAME = "simulated_orders"
MANIFEST_VERSION = 1

__all__ = ["SOURCE_NAME", "MANIFEST_VERSION", "__version__"]
