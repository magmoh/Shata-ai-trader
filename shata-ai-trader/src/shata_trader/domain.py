from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TradeState(str, Enum):
    CREATED = "CREATED"
    RISK_APPROVED = "RISK_APPROVED"
    SUBMITTED = "SUBMITTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    PARTIAL_PROTECTION_PENDING = "PARTIAL_PROTECTION_PENDING"
    PARTIALLY_PROTECTED = "PARTIALLY_PROTECTED"
    PROTECTION_PENDING = "PROTECTION_PENDING"
    PROTECTED = "PROTECTED"
    UNDER_PROTECTED = "UNDER_PROTECTED"
    EXIT_PENDING = "EXIT_PENDING"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"
    RECONCILING = "RECONCILING"
    PROTECTION_FAILED = "PROTECTION_FAILED"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"
    HALTED = "HALTED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class TradeIntent:
    trade_intent_id: str
    strategy_id: str
    strategy_version: str
    risk_policy_version: int
    symbol: str
    side: Side
    quote_amount: Decimal
    reference_entry_price: Decimal
    stop_price: Decimal
    take_profit_price: Decimal
    max_entry_deviation_pct: Decimal
    created_at: datetime
    expires_at: datetime

    def is_expired(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(timezone.utc)
        return now >= self.expires_at


@dataclass(frozen=True)
class PortfolioSnapshot:
    quote_balance: Decimal
    portfolio_value: Decimal
    current_exposure: Decimal
    reconciled_at: datetime | None = None


@dataclass(frozen=True)
class RiskPolicy:
    version: int
    max_risk_per_trade_pct: Decimal
    max_position_allocation_pct: Decimal
    max_portfolio_exposure_pct: Decimal
    min_risk_reward: Decimal
    max_entry_deviation_pct: Decimal
    max_intent_age_seconds: int
    max_reconciliation_age_seconds: int = 5
    max_orders_per_hour: int = 20
    max_notional_per_day_pct: Decimal = Decimal("0.50")
    max_consecutive_execution_errors: int = 3
    emergency_exit_on_unprotected_new_entry: bool = True


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reason: str
    max_quote_amount: Decimal


@dataclass(frozen=True)
class ExchangeOrder:
    client_order_id: str
    symbol: str
    status: str
    requested_quote_amount: Decimal
    filled_base_qty: Decimal
    avg_fill_price: Decimal
    commission_amount: Decimal = Decimal("0")
    commission_asset: str | None = None


@dataclass(frozen=True)
class ExchangeProtection:
    protection_id: str
    client_order_id: str
    symbol: str
    base_qty: Decimal
    active: bool = True
