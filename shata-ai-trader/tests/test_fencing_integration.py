import sys, tempfile, time, unittest
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from shata_trader.audit import HashChainedAuditLog
from shata_trader.domain import PortfolioSnapshot, RiskPolicy, TradeState
from shata_trader.exchange import SimulatedExchange
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease, StaleEpoch
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.strategy import DeterministicDemoStrategy
from shata_trader.runtime import TradingCoreRuntime

P=RiskPolicy(version=1,max_risk_per_trade_pct=Decimal('0.0075'),max_position_allocation_pct=Decimal('0.10'),max_portfolio_exposure_pct=Decimal('0.50'),min_risk_reward=Decimal('2'),max_entry_deviation_pct=Decimal('0.005'),max_intent_age_seconds=30)
PF=lambda: PortfolioSnapshot(Decimal('10000'),Decimal('10000'),Decimal('0'),datetime.now(timezone.utc))
IN=lambda: DeterministicDemoStrategy().create_intent('TESTUSDT',Decimal('100'),Decimal('500'),1)

class TestFencingIntegration(unittest.TestCase):
    def test_zombie_engine_cannot_reach_exchange_after_takeover(self):
        with tempfile.TemporaryDirectory() as td:
            lease=SingleWriterLease(Path(td)/'lease.db')
            raw=SimulatedExchange(Decimal('100'))
            a=DemoExecutionEngine(raw,DeterministicRiskEngine(P),IdempotencyStore(Path(td)/'ia.db'),HashChainedAuditLog(Path(td)/'aa.jsonl'),lease=lease,holder_id='A')
            rt_a=TradingCoreRuntime(a); rt_a.start()
            # Expire A without letting it renew; B takes the authoritative epoch.
            lease.conn.execute("UPDATE writer_lease SET expires_at=? WHERE lease_name='execution-core'",('2000-01-01T00:00:00+00:00',))
            b=DemoExecutionEngine(raw,DeterministicRiskEngine(P),IdempotencyStore(Path(td)/'ib.db'),HashChainedAuditLog(Path(td)/'ab.jsonl'),lease=lease,holder_id='B')
            calls_before=raw.call_count
            with self.assertRaises(Exception):
                rt_a.submit(IN(),PF())
            self.assertEqual(raw.call_count,calls_before, 'stale leader touched raw exchange')
            self.assertGreater(b.epoch,a.epoch)

if __name__=='__main__': unittest.main()
