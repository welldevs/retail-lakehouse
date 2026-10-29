"""The shape of an indicator, and the bound filters every query shares.

WHAT EACH FIELD CARRIES, and why none of them is optional:

  `question` — what the indicator answers. If it doesn't fit in one sentence, the indicator
               is doing two things.
  `grain`    — the source's grain. It's what says whether a sum is legitimate: summing
               `orders_touching_category` across categories counts the same order twice.
  `kind`     — `observed` (came from a real source), `synthetic` (was generated), `mixed` or
               `derived`. A panel — or a language model — that doesn't distinguish the two
               invites reading simulation density as market penetration.
  `caveats`  — what goes wrong when rebuilding this elsewhere. Not a footnote: these are the
               cases where the obvious measure produces a PLAUSIBLE and wrong number, which
               is the only class of error no test catches.

PARAMETERS. Every query with a date axis accepts `%(start)s`, `%(end)s` and
`%(warehouses)s` (comma-separated list), always bound, never concatenated — so there is no
way to inject anything through a selector or a prompt.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DB = "retail"
# Quoted, uppercase: dbt_project.yml declares `+schema: MART` literally, and Postgres
# case-folds an UNQUOTED identifier to lowercase — `from MART.mart_order_funnel` would
# look for a schema named `mart`, which does not exist (measured: "schema does not
# exist"). No database prefix either: Postgres has no cross-database query.
MART = '"MART"'
DATE_FILTER = "order_date between %(start)s and %(end)s"
SNAPSHOT_DATE_FILTER = "snapshot_date between %(start)s and %(end)s"
STOCK_DATE_FILTER = "stock_date between %(start)s and %(end)s"
WAREHOUSE_FILTER = "wh = any(string_to_array(%(warehouses)s, ','))"


@dataclass(frozen=True)
class Indicator:
    key: str
    title: str
    group: str
    question: str
    grain: str
    kind: str
    marts: tuple[str, ...]
    sql: str
    caveats: tuple[str, ...] = field(default_factory=tuple)
    dated: bool = True
