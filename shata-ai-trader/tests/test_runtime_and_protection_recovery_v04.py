import os,sys,time,tempfile,subprocess,unittest
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from shata_trader.audit import HashChainedAuditLog
from shata_trader.activity import TradingActivityStore
from shata_trader.domain import PortfolioSnapshot,RiskPolicy
from shata_trader.execution import DemoExecutionEngine, deterministic_client_order_id
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime,RuntimeNotReady
from shata_trader.strategy import DeterministicDemoStrategy
from shata_trader.events import ExchangeEvent

P=RiskPolicy(version=1,max_risk_per_trade_pct=Decimal('0.0075'),max_position_allocation_pct=Decimal('0.10'),max_portfolio_exposure_pct=Decimal('0.50'),min_risk_reward=Decimal('2'),max_entry_deviation_pct=Decimal('0.005'),max_intent_age_seconds=30)
PF=lambda:PortfolioSnapshot(Decimal('10000'),Decimal('10000'),Decimal('0'),datetime.now(timezone.utc))
ROOT=Path(__file__).resolve().parents[1]

def engine(td,holder='R'):
    b=Path(td);ex=PersistentSimulatedExchange(b/'exchange.db');ledger=TradeLedger(b/'ledger.db')
    e=DemoExecutionEngine(ex,DeterministicRiskEngine(P),IdempotencyStore(b/'idem.db'),HashChainedAuditLog(b/'audit.jsonl'),ledger=ledger,lease=SingleWriterLease(b/'lease.db'),holder_id=holder,activity=TradingActivityStore(b/'activity.db'),lease_ttl_seconds=1)
    return e,ex

class TestRuntimeAndProtectionRecovery(unittest.TestCase):
    def test_runtime_blocks_submit_before_start(self):
        with tempfile.TemporaryDirectory() as td:
            e,ex=engine(td);rt=TradingCoreRuntime(e);it=DeterministicDemoStrategy().create_intent('TESTUSDT',Decimal('100'),Decimal('500'),1)
            with self.assertRaises(RuntimeNotReady):rt.submit(it,PF())
            rep=rt.start();self.assertEqual(rep.unresolved,0);self.assertTrue(rt.ready);self.assertEqual(rt.submit(it,PF()).state.value,'PROTECTED')
    def test_restart_during_protection_pending_confirms_existing_protection(self):
        with tempfile.TemporaryDirectory() as td:
            env=os.environ.copy();env['CASE_DIR']=td;env['CRASH_POINT']='AFTER_PROTECTION_SUBMIT_BEFORE_VERIFY'
            p=subprocess.run([sys.executable,str(ROOT/'scripts/crash_worker.py')],env=env,cwd=ROOT);self.assertEqual(p.returncode,73);time.sleep(.25)
            e,ex=engine(td,'REC');rt=TradingCoreRuntime(e);rep=rt.start();self.assertEqual(rep.unresolved,0);self.assertIn('PROTECTED',rep.states.values());self.assertEqual(len(ex.active_protections()),1)
    def test_fill_event_wakes_reconciliation_but_does_not_override_exchange_truth(self):
        with tempfile.TemporaryDirectory() as td:
            # Crash after exchange accepted -> durable state SUBMITTED and exchange has FILLED order.
            env=os.environ.copy();env['CASE_DIR']=td;env['CRASH_POINT']='AFTER_SUBMIT_BEFORE_RECONCILE';p=subprocess.run([sys.executable,str(ROOT/'scripts/crash_worker.py')],env=env,cwd=ROOT);self.assertEqual(p.returncode,73);time.sleep(.25)
            e,ex=engine(td,'REC2');rt=TradingCoreRuntime(e)
            rec=e.ledger.nonterminal_records()[0]
            before=rec['state']
            # Event is persisted, but the cold-boot gate prevents it from mutating
            # durable financial state before reconciliation authority is granted.
            sm=rt.ingest_exchange_event(ExchangeEvent('fill-1',rec['entry_client_order_id'],'FILLED',1000))
            self.assertIsNone(sm)
            self.assertEqual(e.ledger.get(rec['intent_id'])['state'],before)
            rep=rt.start();self.assertEqual(rep.unresolved,0);self.assertTrue(rt.ready)
            self.assertEqual(e.ledger.get(rec['intent_id'])['state'],'PROTECTED')
            # Late ACK cannot regress projection or durable state.
            rt.ingest_exchange_event(ExchangeEvent('ack-late',rec['entry_client_order_id'],'ACKNOWLEDGED',900))
            self.assertEqual(rt.events.status(rec['entry_client_order_id']),'FILLED')
            self.assertEqual(e.ledger.get(rec['intent_id'])['state'],'PROTECTED')

if __name__=='__main__':unittest.main()
