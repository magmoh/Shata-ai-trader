import sys
from pathlib import Path
import tempfile
import unittest
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
    )


def intent():
    return DeterministicDemoStrategy().create_intent(
        "TESTUSDT", Decimal("100"), Decimal("500"), 1
    )


class TestExecution(unittest.TestCase):
    def engine(self, exchange, audit_path):
        return DemoExecutionEngine(
            exchange,
            DeterministicRiskEngine(policy()),
            IdempotencyStore(":memory:"),
            HashChainedAuditLog(audit_path),
        )

    def test_happy_path_protected(self):
        with tempfile.TemporaryDirectory() as td:
            audit = Path(td) / "audit.jsonl"
            e = self.engine(SimulatedExchange(Decimal("100")), audit)
            sm = boot_submit(e,intent(),portfolio())
            self.assertEqual(sm.state, TradeState.PROTECTED)
            self.assertTrue(e.audit.verify())

    def test_ambiguous_submit_reconciles_without_duplicate(self):
        with tempfile.TemporaryDirectory() as td:
            audit = Path(td) / "audit.jsonl"
            exchange = SimulatedExchange(Decimal("100"), ambiguous_submit=True)
            e = self.engine(exchange, audit)
            sm = boot_submit(e,intent(),portfolio())
            self.assertEqual(sm.state, TradeState.PROTECTED)
            self.assertEqual(len(exchange.orders), 1)

    def test_protection_failure_is_explicit(self):
        with tempfile.TemporaryDirectory() as td:
            audit = Path(td) / "audit.jsonl"
            e = self.engine(
                SimulatedExchange(Decimal("100"), fail_protection=True), audit
            )
            sm = boot_submit(e,intent(),portfolio())
            self.assertEqual(sm.state, TradeState.CLOSED)
            self.assertIn(TradeState.EMERGENCY_EXIT, sm.history)

    def test_partial_fill_halts(self):
        with tempfile.TemporaryDirectory() as td:
            audit = Path(td) / "audit.jsonl"
            e = self.engine(
                SimulatedExchange(
                    Decimal("100"), partial_fill_ratio=Decimal("0.60")
                ),
                audit,
            )
            sm = boot_submit(e,intent(),portfolio())
            self.assertEqual(sm.state, TradeState.HALTED)
            self.assertIn(TradeState.PARTIALLY_PROTECTED, sm.history)


if __name__ == "__main__":
    unittest.main()
