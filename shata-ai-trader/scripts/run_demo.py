import sys
from pathlib import Path
from decimal import Decimal

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from shata_trader.audit import HashChainedAuditLog
from shata_trader.domain import PortfolioSnapshot, RiskPolicy
from shata_trader.exchange import SimulatedExchange
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.strategy import DeterministicDemoStrategy
from shata_trader.runtime import TradingCoreRuntime


policy = RiskPolicy(
    version=1,
    max_risk_per_trade_pct=Decimal("0.0075"),
    max_position_allocation_pct=Decimal("0.10"),
    max_portfolio_exposure_pct=Decimal("0.50"),
    min_risk_reward=Decimal("2.0"),
    max_entry_deviation_pct=Decimal("0.005"),
    max_intent_age_seconds=30,
)

portfolio = PortfolioSnapshot(
    quote_balance=Decimal("10000"),
    portfolio_value=Decimal("10000"),
    current_exposure=Decimal("0"),
)

exchange = SimulatedExchange(price=Decimal("100"))
audit = HashChainedAuditLog(ROOT / "demo_audit.jsonl")
engine = DemoExecutionEngine(
    exchange=exchange,
    risk_engine=DeterministicRiskEngine(policy),
    idempotency=IdempotencyStore(ROOT / "demo_idempotency.sqlite"),
    audit=audit,
)

intent = DeterministicDemoStrategy().create_intent(
    symbol="TESTUSDT",
    reference_price=Decimal("100"),
    quote_amount=Decimal("500"),
    risk_policy_version=1,
)

runtime = TradingCoreRuntime(engine)
runtime.start()
sm = runtime.submit(intent, portfolio)

print("Final state:", sm.state.value)
print("History:", " -> ".join(s.value for s in sm.history))
print("Audit chain valid:", audit.verify())
