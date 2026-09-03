from dataclasses import dataclass

from .domain import ExchangeOrder
from .exchange import ExchangeGateway


@dataclass(frozen=True)
class ReconciliationResult:
    found: bool
    order: ExchangeOrder | None
    note: str


class ReconciliationEngine:
    def __init__(self, exchange: ExchangeGateway):
        self.exchange = exchange

    def reconcile_order(self, symbol: str, client_order_id: str) -> ReconciliationResult:
        order = self.exchange.query_order_by_client_id(symbol, client_order_id)
        if order is None:
            return ReconciliationResult(False, None, "Order not found on exchange")
        return ReconciliationResult(True, order, f"Exchange says {order.status}")
