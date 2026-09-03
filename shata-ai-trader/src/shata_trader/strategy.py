from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid

from .domain import Side, TradeIntent


class DeterministicDemoStrategy:
    """
    Not an alpha strategy.
    Generates a deterministic, structurally valid intent for Phase 0 plumbing tests.
    """

    strategy_id = "phase0-demo-plumbing"
    strategy_version = "0.1.0"

    def create_intent(
        self,
        symbol: str,
        reference_price: Decimal,
        quote_amount: Decimal,
        risk_policy_version: int,
    ) -> TradeIntent:
        now = datetime.now(timezone.utc)
        return TradeIntent(
            trade_intent_id=f"demo-{uuid.uuid4().hex}",
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            risk_policy_version=risk_policy_version,
            symbol=symbol,
            side=Side.BUY,
            quote_amount=quote_amount,
            reference_entry_price=reference_price,
            stop_price=reference_price * Decimal("0.98"),
            take_profit_price=reference_price * Decimal("1.05"),
            max_entry_deviation_pct=Decimal("0.005"),
            created_at=now,
            expires_at=now + timedelta(seconds=30),
        )
