import sys, tempfile, threading, json, unittest
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from shata_trader.audit import HashChainedAuditLog
from shata_trader.activity import TradingActivityStore
from shata_trader.domain import PortfolioSnapshot, RiskPolicy, TradeState
from shata_trader.exchange import SimulatedExchange, RateLimited
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.strategy import DeterministicDemoStrategy
from shata_trader.testing import boot_submit

P=lambda **kw: RiskPolicy(version=1,max_risk_per_trade_pct=Decimal('0.0075'),max_position_allocation_pct=Decimal('0.10'),max_portfolio_exposure_pct=Decimal('0.50'),min_risk_reward=Decimal('2'),max_entry_deviation_pct=Decimal('0.005'),max_intent_age_seconds=30,**kw)
PF=lambda: PortfolioSnapshot(Decimal('10000'),Decimal('10000'),Decimal('0'),datetime.now(timezone.utc))
IN=lambda: DeterministicDemoStrategy().create_intent('TESTUSDT',Decimal('100'),Decimal('500'),1)

class FlakyExchange(SimulatedExchange):
    fail_on_call=999
    def _guard(self):
        self.call_count+=1
        if self.call_count>=self.fail_on_call: raise RateLimited('burst limit mid-flow')
        if self.maintenance: raise Exception('maint')

class TestSecurityRegressionsLegacy(unittest.TestCase):
    def engine(self,ex,td,policy=None,activity=None):
        return DemoExecutionEngine(ex,DeterministicRiskEngine(policy or P()),IdempotencyStore(Path(td)/'i.db'),HashChainedAuditLog(Path(td)/'a.jsonl'),activity=activity)

    def test_midflow_rate_limit_is_tracked_not_uncaught(self):
        with tempfile.TemporaryDirectory() as td:
            ex=FlakyExchange(Decimal('100')); ex.fail_on_call=3
            eng=self.engine(ex,td); it=IN(); sm=boot_submit(eng,it,PF())
            self.assertEqual(sm.state,TradeState.UNKNOWN)
            rec=eng.ledger.get(it.trade_intent_id)
            self.assertEqual(rec['state'],'UNKNOWN')
            self.assertGreater(ex.base_balance,0)

    def test_preexisting_balance_not_overprotected(self):
        with tempfile.TemporaryDirectory() as td:
            ex=SimulatedExchange(Decimal('100')); ex.base_balance=Decimal('50')
            eng=self.engine(ex,td); sm=boot_submit(eng,IN(),PF())
            self.assertEqual(sm.state,TradeState.PROTECTED)
            rows=[json.loads(x) for x in (Path(td)/'a.jsonl').read_text().splitlines()]
            q=Decimal([r['payload']['base_qty'] for r in rows if r['event_type']=='POSITION_PROTECTED'][0])
            self.assertEqual(q,Decimal('4.995'))

    def test_duplicate_intent_is_not_redriven_after_predispatch_failure(self):
        with tempfile.TemporaryDirectory() as td:
            ex=SimulatedExchange(Decimal('100'),maintenance=True); eng=self.engine(ex,td); it=IN()
            self.assertEqual(boot_submit(eng,it,PF()).state,TradeState.HALTED)
            ex.maintenance=False
            self.assertEqual(boot_submit(eng,it,PF()).state,TradeState.REJECTED)
            self.assertEqual(boot_submit(eng,IN(),PF()).state,TradeState.PROTECTED)

    def test_concurrent_audit_chain_stays_valid(self):
        with tempfile.TemporaryDirectory() as td:
            log=HashChainedAuditLog(Path(td)/'a.jsonl'); b=threading.Barrier(8)
            def w(i): b.wait(); log.append('EVT',{'i':i})
            ts=[threading.Thread(target=w,args=(i,)) for i in range(8)]
            [t.start() for t in ts]; [t.join() for t in ts]
            self.assertTrue(log.verify())
            self.assertEqual(len((Path(td)/'a.jsonl').read_text().splitlines()),8)

    def test_torn_tail_is_detected_not_crash(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'a.jsonl'; log=HashChainedAuditLog(p); log.append('A',{}); log.append('B',{})
            p.write_text(p.read_text()[:-15])
            self.assertFalse(log.verify())

    def test_activity_limits_are_live(self):
        with tempfile.TemporaryDirectory() as td:
            activity=TradingActivityStore(Path(td)/'act.db')
            policy=P(max_orders_per_hour=1)
            ex=SimulatedExchange(Decimal('100')); eng=self.engine(ex,td,policy,activity)
            self.assertEqual(boot_submit(eng,IN(),PF()).state,TradeState.PROTECTED)
            second=IN(); self.assertEqual(boot_submit(eng,second,PF()).state,TradeState.REJECTED)

if __name__=='__main__': unittest.main()
