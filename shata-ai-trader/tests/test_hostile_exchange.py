import sys
from pathlib import Path
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.audit import HashChainedAuditLog
from shata_trader.domain import PortfolioSnapshot, RiskPolicy, TradeState
from shata_trader.exchange import SimulatedExchange
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.strategy import DeterministicDemoStrategy
from shata_trader.testing import boot_submit


def policy():
    return RiskPolicy(
        version=1,
        max_risk_per_trade_pct=Decimal("0.0075"),
        max_position_allocation_pct=Decimal("0.10"),
        max_portfolio_exposure_pct=Decimal("0.50"),
        min_risk_reward=Decimal("2.0"),
        max_entry_deviation_pct=Decimal("0.005"),
        max_intent_age_seconds=30,
    )

def portfolio():
    return PortfolioSnapshot(
        quote_balance=Decimal("10000"),
        portfolio_value=Decimal("10000"),
        current_exposure=Decimal("0"),
        reconciled_at=datetime.now(timezone.utc),
    )

def intent():
    return DeterministicDemoStrategy().create_intent(
        "TESTUSDT", Decimal("100"), Decimal("500"), 1
    )

class TestHostileExchange(unittest.TestCase):
    def engine(self, ex, td):
        return DemoExecutionEngine(
            ex,
            DeterministicRiskEngine(policy()),
            IdempotencyStore(Path(td) / "idem.sqlite"),
            HashChainedAuditLog(Path(td) / "audit.jsonl"),
        )

    def test_timeout_after_acceptance_reconciles_same_order(self):
        with tempfile.TemporaryDirectory() as td:
            ex = SimulatedExchange(Decimal("100"), ambiguous_submit=True)
            sm = boot_submit(self.engine(ex,td),intent(),portfolio())
            self.assertEqual(sm.state, TradeState.PROTECTED)
            # entry + no duplicate entry; protection is separate exchange artifact
            entry_orders = [o for k, o in ex.orders.items() if "emergency" not in k]
            self.assertEqual(len(entry_orders), 1)

    def test_symbol_halt_rejects_safely(self):
        with tempfile.TemporaryDirectory() as td:
            ex = SimulatedExchange(Decimal("100"), symbol_status="HALT")
            sm = boot_submit(self.engine(ex,td),intent(),portfolio())
            self.assertEqual(sm.state, TradeState.HALTED)

    def test_maintenance_rejects_safely(self):
        with tempfile.TemporaryDirectory() as td:
            ex = SimulatedExchange(Decimal("100"), maintenance=True)
            sm = boot_submit(self.engine(ex,td),intent(),portfolio())
            self.assertEqual(sm.state, TradeState.HALTED)


if __name__ == "__main__":
    unittest.main()
