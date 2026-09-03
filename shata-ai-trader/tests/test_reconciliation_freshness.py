import sys
from pathlib import Path
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.domain import PortfolioSnapshot, RiskPolicy
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.strategy import DeterministicDemoStrategy


class TestReconciliationFreshness(unittest.TestCase):
    def test_stale_portfolio_state_rejected(self):
        policy = RiskPolicy(
            version=1,
            max_risk_per_trade_pct=Decimal("0.0075"),
            max_position_allocation_pct=Decimal("0.10"),
            max_portfolio_exposure_pct=Decimal("0.50"),
            min_risk_reward=Decimal("2.0"),
            max_entry_deviation_pct=Decimal("0.005"),
            max_intent_age_seconds=30,
            max_reconciliation_age_seconds=5,
        )
        intent = DeterministicDemoStrategy().create_intent(
            "TESTUSDT", Decimal("100"), Decimal("500"), 1
        )
        p = PortfolioSnapshot(
            quote_balance=Decimal("10000"),
            portfolio_value=Decimal("10000"),
            current_exposure=Decimal("0"),
            reconciled_at=datetime.now(timezone.utc) - timedelta(seconds=30),
        )
        d = DeterministicRiskEngine(policy).evaluate(intent, p, Decimal("100"))
        self.assertFalse(d.approved)
        self.assertIn("stale", d.reason.lower())


if __name__ == "__main__":
    unittest.main()
