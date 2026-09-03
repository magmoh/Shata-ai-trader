from __future__ import annotations

import threading

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
import uuid

from .domain import ExchangeOrder, ExchangeProtection


class UnknownSubmissionState(RuntimeError):
    """Ambiguous submission outcome: never blindly retry."""


class ExchangeRejected(RuntimeError):
    pass


class RateLimited(RuntimeError):
    pass


class Maintenance(RuntimeError):
    pass


class ExchangeGateway(Protocol):
    def get_market_price(self, symbol: str) -> Decimal: ...
    def submit_market_buy(self, symbol: str, quote_amount: Decimal, client_order_id: str) -> ExchangeOrder: ...
    def query_order_by_client_id(self, symbol: str, client_order_id: str) -> ExchangeOrder | None: ...
    def cancel_remainder(self, symbol: str, client_order_id: str) -> None: ...
    def get_free_base_balance(self, symbol: str) -> Decimal: ...
    def place_protection(self, symbol: str, base_qty: Decimal, stop_price: Decimal, take_profit_price: Decimal, client_order_id: str) -> str: ...
    def protection_exists(self, symbol: str, protection_id: str) -> bool: ...
    def protection_by_client_id(self, symbol: str, client_order_id: str) -> str | None: ...
    def protection_details_by_client_id(self, symbol: str, client_order_id: str) -> ExchangeProtection | None: ...
    def cancel_protection_by_client_id(self, symbol: str, client_order_id: str) -> None: ...
    def emergency_market_sell(self, symbol: str, base_qty: Decimal, client_order_id: str) -> ExchangeOrder: ...


@dataclass
class SimulatedExchange:
    price: Decimal
    partial_fill_ratio: Decimal = Decimal("1")
    fail_protection: bool = False
    ambiguous_submit: bool = False
    commission_rate: Decimal = Decimal("0.001")
    commission_asset_mode: str = "BASE"  # BASE or QUOTE
    symbol_status: str = "TRADING"
    reject_entry: bool = False
    maintenance: bool = False
    rate_limited: bool = False

    def __post_init__(self):
        self.orders: dict[str, ExchangeOrder] = {}
        self.protections: set[str] = set()
        self.protection_clients: dict[str,str] = {}
        self.protection_qty: dict[str,Decimal] = {}
        self.base_balance = Decimal("0")
        self.call_count = 0
        # v0.8.1: submitters and the ProtectionSupervisor reach this object from
        # different threads. Read-modify-write on balances/dicts must be atomic.
        self._lock = threading.RLock()

    def _guard(self):
        self.call_count += 1
        if self.maintenance:
            raise Maintenance("Simulated exchange maintenance")
        if self.rate_limited:
            raise RateLimited("Simulated rate limit")
        if self.symbol_status != "TRADING":
            raise ExchangeRejected(f"Symbol status is {self.symbol_status}")

    def get_market_price(self, symbol: str) -> Decimal:
        with self._lock:
            self._guard()
            return self.price

    def submit_market_buy(self, symbol: str, quote_amount: Decimal, client_order_id: str) -> ExchangeOrder:
        with self._lock:
            self._guard()
            if self.reject_entry:
                raise ExchangeRejected("Simulated entry rejection")
            if client_order_id in self.orders:
                return self.orders[client_order_id]

            filled_quote = quote_amount * self.partial_fill_ratio
            gross_qty = filled_quote / self.price
            commission = Decimal("0")
            commission_asset = None
            net_qty = gross_qty
            if self.commission_asset_mode == "BASE":
                commission = gross_qty * self.commission_rate
                commission_asset = symbol.replace("USDT", "")
                net_qty = gross_qty - commission
            elif self.commission_asset_mode == "QUOTE":
                commission = filled_quote * self.commission_rate
                commission_asset = "USDT"

            self.base_balance += net_qty
            status = "FILLED" if self.partial_fill_ratio == Decimal("1") else "PARTIALLY_FILLED"
            order = ExchangeOrder(
                client_order_id=client_order_id,
                symbol=symbol,
                status=status,
                requested_quote_amount=quote_amount,
                filled_base_qty=gross_qty,
                avg_fill_price=self.price,
                commission_amount=commission,
                commission_asset=commission_asset,
            )
            self.orders[client_order_id] = order

            if self.ambiguous_submit:
                raise UnknownSubmissionState("Simulated timeout after exchange accepted order")
            return order

    def query_order_by_client_id(self, symbol: str, client_order_id: str) -> ExchangeOrder | None:
        with self._lock:
            self._guard()
            order = self.orders.get(client_order_id)
            if order and order.symbol == symbol:
                return order
            return None

    def cancel_remainder(self, symbol: str, client_order_id: str) -> None:
        with self._lock:
            self._guard()
            # Demo: remaining unfilled quantity is considered canceled.

    def get_free_base_balance(self, symbol: str) -> Decimal:
        with self._lock:
            self._guard()
            return self.base_balance

    def place_protection(self, symbol: str, base_qty: Decimal, stop_price: Decimal, take_profit_price: Decimal, client_order_id: str) -> str:
        with self._lock:
            self._guard()
            if self.fail_protection:
                raise ExchangeRejected("Simulated protection failure")
            if base_qty <= 0 or base_qty > self.base_balance:
                raise ExchangeRejected("Insufficient free base balance")
            if client_order_id in self.protection_clients:
                return self.protection_clients[client_order_id]
            protection_id = f"prot-{client_order_id}"
            self.protections.add(protection_id)
            self.protection_clients[client_order_id] = protection_id
            self.protection_qty[client_order_id] = Decimal(base_qty)
            return protection_id

    def protection_exists(self, symbol: str, protection_id: str) -> bool:
        with self._lock:
            self._guard()
            return protection_id in self.protections

    def protection_by_client_id(self, symbol: str, client_order_id: str) -> str | None:
        with self._lock:
            self._guard()
            return self.protection_clients.get(client_order_id)

    def protection_details_by_client_id(self, symbol: str, client_order_id: str) -> ExchangeProtection | None:
        with self._lock:
            self._guard()
            pid = self.protection_clients.get(client_order_id)
            if not pid or pid not in self.protections:
                return None
            return ExchangeProtection(pid, client_order_id, symbol, self.protection_qty[client_order_id], True)

    def cancel_protection_by_client_id(self, symbol: str, client_order_id: str) -> None:
        with self._lock:
            self._guard()
            pid = self.protection_clients.get(client_order_id)
            if pid:
                self.protections.discard(pid)

    def emergency_market_sell(self, symbol: str, base_qty: Decimal, client_order_id: str) -> ExchangeOrder:
        with self._lock:
            self._guard()
            if client_order_id in self.orders:
                return self.orders[client_order_id]
            sell_qty = min(base_qty, self.base_balance)
            self.base_balance -= sell_qty
            order = ExchangeOrder(
                client_order_id=client_order_id,
                symbol=symbol,
                status="FILLED",
                requested_quote_amount=sell_qty * self.price,
                filled_base_qty=sell_qty,
                avg_fill_price=self.price,
            )
            self.orders[client_order_id] = order
            return order
