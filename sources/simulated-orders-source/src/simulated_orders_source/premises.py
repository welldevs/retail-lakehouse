"""Acesso tipado a tabela de premissas declaradas.

Toda premissa desta Source e SINTETICA e esta declarada: nenhuma fonte ingerida por esta
plataforma mede venda, cesta, cadencia de compra ou disponibilidade de produto. Este modulo
existe para que essa afirmacao seja imposta, e nao apenas escrita.

DUAS REGRAS, IMPOSTAS AQUI
---------------------------
1. NAO EXISTE DEFAULT. Ler uma chave ausente levanta erro em vez de devolver um numero
   escolhido pelo codigo. Um default escondido no gerador seria uma premissa nao declarada —
   exatamente o que a tabela existe para impedir.
2. FAIXAS SAO CONFERIDAS. `*_min` maior que `*_max` produziria uma amostragem invertida que
   nenhum teste a jusante pegaria, porque o resultado continuaria plausivel.
"""

from __future__ import annotations

# Pares (min, max) que precisam estar ordenados. Uma faixa invertida nao gera erro na
# amostragem, gera numero plausivel — que e a classe de defeito mais cara deste repo.
RANGE_PAIRS = (
    ("basket_lines_min", "basket_lines_max"),
    ("order_hour_min", "order_hour_max"),
    ("slot_lead_hours_min", "slot_lead_hours_max"),
    ("minutes_to_payment_min", "minutes_to_payment_max"),
    ("minutes_to_cancel_min", "minutes_to_cancel_max"),
    ("minutes_to_picking_min", "minutes_to_picking_max"),
    ("minutes_to_dispatch_min", "minutes_to_dispatch_max"),
    ("minutes_to_delivered_min", "minutes_to_delivered_max"),
    ("minutes_to_return_min", "minutes_to_return_max"),
)

# Toda chave cujo valor e uma fracao de 0 a 1. Uma taxa acima de 1 seria aceita pela
# amostragem e produziria, por exemplo, 100% de pedidos cancelados sem erro nenhum.
PROPORTIONS = (
    "daily_order_rate",
    "substitution_rate",
    "removal_rate",
    "payment_failure_rate",
    "cancellation_rate",
    "delivery_failure_rate",
    "return_rate",
)


class PremiseError(Exception):
    """Premissa ausente, nao numerica ou incoerente."""


class Premises:
    """Leitura tipada e conferida da tabela de premissas."""

    def __init__(self, values: dict):
        self._values = dict(values)
        self._check()

    def _check(self) -> None:
        for key in PROPORTIONS:
            value = self.number(key)
            if not 0.0 <= value <= 1.0:
                raise PremiseError(
                    f"premissa {key!r} = {value}: e uma proporcao e tem de ficar entre 0 e 1"
                )
        for low, high in RANGE_PAIRS:
            if self.number(low) > self.number(high):
                raise PremiseError(
                    f"faixa invertida: {low}={self.number(low)} > {high}={self.number(high)}. "
                    f"A amostragem nao falharia; ela produziria numero plausivel e errado."
                )
        mode = self.integer("basket_lines_mode")
        if not self.integer("basket_lines_min") <= mode <= self.integer("basket_lines_max"):
            raise PremiseError(
                f"basket_lines_mode={mode} fora de "
                f"[{self.integer('basket_lines_min')}, {self.integer('basket_lines_max')}]"
            )
        if self.integer("basket_lines_min") < 1:
            raise PremiseError("basket_lines_min tem de ser ao menos 1")
        if self.integer("quantity_max") < 1:
            raise PremiseError("quantity_max tem de ser ao menos 1")

    def number(self, key: str) -> float:
        if key not in self._values:
            raise PremiseError(
                f"premissa ausente: {key!r}. Nao ha default para nenhuma premissa, de "
                f"proposito: um default escondido no gerador seria uma premissa nao declarada."
            )
        try:
            return float(self._values[key])
        except (TypeError, ValueError) as exc:
            raise PremiseError(f"premissa {key!r} nao e numerica: {self._values[key]!r}") from exc

    def integer(self, key: str) -> int:
        value = self.number(key)
        if value != int(value):
            raise PremiseError(f"premissa {key!r} tem de ser inteira: {value}")
        return int(value)

    def as_dict(self) -> dict:
        return dict(self._values)
