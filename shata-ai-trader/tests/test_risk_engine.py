import sys
from pathlib import Path
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.domain import PortfolioSnapshot, RiskPolicy, Side, TradeIntent
from shata_trader.risk_engine import DeterministicRiskEngine


def make_policy():
    return RiskPolicy(
        version=1,
        max_risk_per_trade_pct=Decimal("0.0075"),
        max_position_allocation_pct=Decimal("0.10"),
        max_portfolio_exposure_pct=Decimal("0.50"),
        min_risk_reward=Decimal("2.0"),
        max_entry_deviation_pct=Decimal("0.005"),
        max_intent_age_seconds=30,
    )


def make_intent(amount="500"):
    now = datetime.now(timezone.utc)
    return TradeIntent(
        trade_intent_id="risk-test-1",
        strategy_id="test",
        strategy_version="1",
        risk_policy_version=1,
        symbol="TESTUSDT",
        side=Side.BUY,
        quote_amount=Decimal(amount),
        reference_entry_price=Decimal("100"),
        stop_price=Decimal("98"),
        take_profit_price=Decimal("105"),
        max_entry_deviation_pct=Decimal("0.005"),
        created_at=now,
        expires_at=now + timedelta(seconds=30),
    )


class TestRiskEngine(unittest.TestCase):
    def setUp(self):
        self.engine = DeterministicRiskEngine(make_policy())
        self.portfolio = PortfolioSnapshot(
            quote_balance=Decimal("10000"),
            portfolio_value=Decimal("10000"),
            current_exposure=Decimal("0"),
        )

    def test_approves_valid_intent(self):
        d = self.engine.evaluate(make_intent("500"), self.portfolio, Decimal("100.1"))
        self.assertTrue(d.approved)

    def test_rejects_price_deviation(self):
        d = self.engine.evaluate(make_intent("500"), self.portfolio, Decimal("102"))
        self.assertFalse(d.approved)

    def test_rejects_oversized_position(self):
        d = self.engine.evaluate(make_intent("5000"), self.portfolio, Decimal("100"))
        self.assertFalse(d.approved)


if __name__ == "__main__":
    unittest.main()
