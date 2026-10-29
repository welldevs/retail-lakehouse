"""Simulated Orders Source — synthetic orders as an EVENT LOG.

Fifth source in the repo, and the second DERIVED one: it consumes the Silver the others
produced and returns a new RAW. Phase 1's golden rule still holds, shifted one level:

    the ORDER is invented; who buys, what is bought, what it costs and where they live are not.

The customer comes from `silver_customer`; product and price come from `silver_product_price`
of the SAME warehouse on the SAME date. No product, price or customer is fabricated here.

WHAT THIS SOURCE DELIVERS IS A LOG, NOT A SNAPSHOT
--------------------------------------------------
The partition contains `order_events.jsonl` and nothing else. There is deliberately NO
`orders.json` with the folded state next to it: two representations of the same truth drift
apart. An order's state is the FOLD of its events, and the fold lives in Silver.

The fold is non-trivial by construction: line substitution and removal change the basket AFTER
placement, so the order's final value cannot be derived from the `order_placed` event. If it
could, the log would be a date stamp and the event model would be decoration.

Standard library only. See CONTRACT.md.
"""

from __future__ import annotations

__version__ = "1.0.0"

SOURCE_NAME = "simulated_orders"
MANIFEST_VERSION = 1

__all__ = ["SOURCE_NAME", "MANIFEST_VERSION", "__version__"]
