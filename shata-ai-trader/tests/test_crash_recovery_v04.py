import os,sys,time,tempfile,subprocess,unittest
from pathlib import Path
from decimal import Decimal
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from shata_trader.audit import HashChainedAuditLog
from shata_trader.activity import TradingActivityStore
from shata_trader.domain import RiskPolicy
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.cold_boot import ColdBootCoordinator

P=RiskPolicy(version=1,max_risk_per_trade_pct=Decimal('0.0075'),max_position_allocation_pct=Decimal('0.10'),max_portfolio_exposure_pct=Decimal('0.50'),min_risk_reward=Decimal('2'),max_entry_deviation_pct=Decimal('0.005'),max_intent_age_seconds=30)
ROOT=Path(__file__).resolve().parents[1]

def crash(td,point):
    env=os.environ.copy();env['CASE_DIR']=td;env['CRASH_POINT']=point
    p=subprocess.run([sys.executable,str(ROOT/'scripts/crash_worker.py')],env=env,cwd=ROOT)
    assert p.returncode==73,p.returncode
    time.sleep(0.25)

def restart(td):
    b=Path(td);ex=PersistentSimulatedExchange(b/'exchange.db');ledger=TradeLedger(b/'ledger.db')
    eng=DemoExecutionEngine(ex,DeterministicRiskEngine(P),IdempotencyStore(b/'idem.db'),HashChainedAuditLog(b/'audit.jsonl'),ledger=ledger,lease=SingleWriterLease(b/'lease.db'),holder_id='recovery',activity=TradingActivityStore(b/'activity.db'),lease_ttl_seconds=1)
    rep=ColdBootCoordinator(eng).reconcile_all();return ex,ledger,rep

class TestCrashRecoveryV04(unittest.TestCase):
    def test_crash_after_wal_before_submit_resolves_no_ghost(self):
        with tempfile.TemporaryDirectory() as td:
            crash(td,'AFTER_WAL_BEFORE_SUBMIT');ex,ledger,rep=restart(td)
            self.assertEqual(ex.all_orders(),[]);self.assertEqual(rep.unresolved,1);self.assertIn('UNKNOWN',rep.states.values())
    def test_crash_after_exchange_accept_before_local_reconcile_recovers_and_protects(self):
        with tempfile.TemporaryDirectory() as td:
            crash(td,'AFTER_SUBMIT_BEFORE_RECONCILE');ex,ledger,rep=restart(td)
            self.assertEqual(len(ex.all_orders()),1);self.assertEqual(rep.unresolved,0);self.assertIn('PROTECTED',rep.states.values());self.assertEqual(len(ex.active_protections()),1)

if __name__=='__main__':unittest.main()
