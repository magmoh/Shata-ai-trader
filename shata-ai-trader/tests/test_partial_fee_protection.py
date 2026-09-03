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


def make_policy(emergency=True):
    return RiskPolicy(
        version=1,
        max_risk_per_trade_pct=Decimal("0.0075"),
        max_position_allocation_pct=Decimal("0.10"),
        max_portfolio_exposure_pct=Decimal("0.50"),
        min_risk_reward=Decimal("2.0"),
        max_entry_deviation_pct=Decimal("0.005"),
        max_intent_age_seconds=30,
        emergency_exit_on_unprotected_new_entry=emergency,
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

class TestPartialFeeProtection(unittest.TestCase):
    def engine(self, exchange, td, emergency=True):
        return DemoExecutionEngine(
            exchange,
            DeterministicRiskEngine(make_policy(emergency)),
            IdempotencyStore(Path(td) / "idem.sqlite"),
            HashChainedAuditLog(Path(td) / "audit.jsonl"),
        )

    def test_partial_fill_is_protected_before_halt(self):
        with tempfile.TemporaryDirectory() as td:
            ex = SimulatedExchange(
                Decimal("100"),
                partial_fill_ratio=Decimal("0.37"),
                commission_rate=Decimal("0.001"),
                commission_asset_mode="BASE",
            )
            e = self.engine(ex, td)
            sm = boot_submit(e,intent(),portfolio())
            self.assertEqual(sm.state, TradeState.HALTED)
            self.assertIn(TradeState.PARTIALLY_PROTECTED, sm.history)
            self.assertGreater(len(ex.protections), 0)

    def test_fee_deduction_does_not_break_protection(self):
        with tempfile.TemporaryDirectory() as td:
            ex = SimulatedExchange(
                Decimal("100"),
                commission_rate=Decimal("0.001"),
                commission_asset_mode="BASE",
            )
            e = self.engine(ex, td)
            sm = boot_submit(e,intent(),portfolio())
            self.assertEqual(sm.state, TradeState.PROTECTED)

    def test_protection_failure_can_emergency_exit(self):
        with tempfile.TemporaryDirectory() as td:
            ex = SimulatedExchange(Decimal("100"), fail_protection=True)
            e = self.engine(ex, td, emergency=True)
            sm = boot_submit(e,intent(),portfolio())
            self.assertEqual(sm.state, TradeState.CLOSED)
            self.assertIn(TradeState.EMERGENCY_EXIT, sm.history)
            self.assertEqual(ex.base_balance, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
