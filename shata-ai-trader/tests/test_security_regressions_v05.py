import os,sys,tempfile,threading,time,subprocess,unittest
from pathlib import Path
from decimal import Decimal
from datetime import datetime,timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from shata_trader.audit import HashChainedAuditLog
from shata_trader.activity import TradingActivityStore
from shata_trader.domain import PortfolioSnapshot,RiskPolicy,TradeState
from shata_trader.exchange import SimulatedExchange
from shata_trader.execution import DemoExecutionEngine,BootGateClosed
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease,LeaseUnavailable
from shata_trader.ledger import TradeLedger,LedgerAuthorityRequired
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime
from shata_trader.strategy import DeterministicDemoStrategy

P=RiskPolicy(version=1,max_risk_per_trade_pct=Decimal('0.0075'),max_position_allocation_pct=Decimal('0.10'),max_portfolio_exposure_pct=Decimal('0.50'),min_risk_reward=Decimal('2'),max_entry_deviation_pct=Decimal('0.005'),max_intent_age_seconds=30)
PF=lambda:PortfolioSnapshot(Decimal('10000'),Decimal('10000'),Decimal('0'),datetime.now(timezone.utc))
IN=lambda:DeterministicDemoStrategy().create_intent('TESTUSDT',Decimal('100'),Decimal('500'),1)
ROOT=Path(__file__).resolve().parents[1]

def make(b,ex=None,ttl=2,label='core',audit=None):
    b=Path(b);ex=ex or PersistentSimulatedExchange(b/'exchange.db')
    e=DemoExecutionEngine(ex,DeterministicRiskEngine(P),IdempotencyStore(b/'idem.db'),audit or HashChainedAuditLog(b/'audit.jsonl'),ledger=TradeLedger(b/'ledger.db'),lease=SingleWriterLease(b/'lease.db'),holder_id=label,activity=TradingActivityStore(b/'activity.db'),lease_ttl_seconds=ttl)
    return e,ex

class TestSecurityRegressionsV05(unittest.TestCase):
    def test_second_cold_boot_with_protected_position(self):
        with tempfile.TemporaryDirectory() as td:
            e1,ex=make(td);r1=TradingCoreRuntime(e1);self.assertEqual(r1.start().unresolved,0);self.assertEqual(r1.submit(IN(),PF()).state,TradeState.PROTECTED);r1.stop(release_lease=True)
            e2,_=make(td,ex=ex,label='restart');r2=TradingCoreRuntime(e2);rep=r2.start();self.assertEqual(rep.unresolved,0);self.assertTrue(r2.ready);self.assertIn('PROTECTED',rep.states.values());r2.stop()

    def test_real_but_temporarily_unqueryable_order_stays_unknown(self):
        with tempfile.TemporaryDirectory() as td:
            env=os.environ.copy();env['CASE_DIR']=td;env['CRASH_POINT']='AFTER_SUBMIT_BEFORE_RECONCILE'
            self.assertEqual(subprocess.run([sys.executable,str(ROOT/'scripts/crash_worker.py')],env=env,cwd=ROOT).returncode,73);time.sleep(.25)
            ex=PersistentSimulatedExchange(Path(td)/'exchange.db');ex.query_visibility_lag_calls=5
            e,_=make(td,ex=ex,label='recovery');rt=TradingCoreRuntime(e);rep=rt.start();self.assertEqual(rep.unresolved,1);self.assertIn('UNKNOWN',rep.states.values());self.assertFalse(rt.ready);self.assertGreater(ex._balance(),0);self.assertEqual(ex.active_protections(),[]);rt.stop()

    def test_direct_process_is_structurally_blocked(self):
        with tempfile.TemporaryDirectory() as td:
            e,_=make(td)
            with self.assertRaises(BootGateClosed):e.process(IN(),PF())

    def test_zombie_ledger_write_is_fenced(self):
        with tempfile.TemporaryDirectory() as td:
            e1,_=make(td,ttl=.2,label='old');it=IN();e1.ledger.ensure(it,'cid',e1.epoch);time.sleep(.25)
            e2,_=make(td,label='new')
            with self.assertRaises(Exception):e1.ledger.transition(it.trade_intent_id,'CREATED','RISK_APPROVED')
            self.assertGreater(e2.epoch,e1.epoch)

    def test_eight_workers_via_runtime_no_raw_db_errors(self):
        with tempfile.TemporaryDirectory() as td:
            e,_=make(td,ex=SimulatedExchange(Decimal('100')),ttl=5);rt=TradingCoreRuntime(e);self.assertEqual(rt.start().unresolved,0)
            out=[];bar=threading.Barrier(8);lock=threading.Lock()
            def w():
                it=IN();bar.wait()
                try:v=rt.submit(it,PF()).state.value
                except Exception as x:v='EXC:'+type(x).__name__
                with lock:out.append(v)
            ts=[threading.Thread(target=w) for _ in range(8)];[t.start() for t in ts];[t.join() for t in ts]
            self.assertFalse([x for x in out if x.startswith('EXC')],out);rt.stop()

    def test_lease_loss_after_wal_is_in_band_unknown(self):
        with tempfile.TemporaryDirectory() as td:
            holder={}
            def hook(name):
                if name=='AFTER_WAL_BEFORE_SUBMIT':
                    # simulate authoritative takeover between WAL and socket write
                    e.lease.conn.execute("UPDATE writer_lease SET holder_id='attacker',epoch=epoch+1,expires_at='2999-01-01T00:00:00+00:00' WHERE lease_name='execution-core'")
            e,_=make(td,ex=SimulatedExchange(Decimal('100')),ttl=5);e.fault_hook=hook;rt=TradingCoreRuntime(e);rt.start();sm=rt.submit(IN(),PF());self.assertEqual(sm.state,TradeState.UNKNOWN);self.assertFalse(e._boot_verified);rt.stop(release_lease=False)

    def test_anchor_outage_does_not_abort_trade(self):
        class Dead:
            def publish(self,h):raise OSError('anchor unreachable')
            def read(self):raise OSError('anchor unreachable')
        with tempfile.TemporaryDirectory() as td:
            b=Path(td);audit=HashChainedAuditLog(b/'audit.jsonl',anchor=Dead());e,_=make(b,ex=SimulatedExchange(Decimal('100')),audit=audit)
            # Runtime correctly blocks NEW work when anchor cannot be verified.
            rt=TradingCoreRuntime(e);rep=rt.start();self.assertFalse(rt.ready);self.assertTrue(audit.verify());self.assertTrue(audit.anchor_degraded);rt.stop()

    def test_corrupt_row_quarantines_without_hiding_healthy_row(self):
        with tempfile.TemporaryDirectory() as td:
            b=Path(td);e,ex=make(b);good=IN();e.ledger.ensure(good,'cid-good',e.epoch)
            e.ledger.conn.execute("INSERT INTO trades(intent_id,payload,state,entry_client_order_id,epoch,side_effect_prepared,updated_at) VALUES('bad','{not json','SUBMITTED','cid-bad',?,1,'2026-01-01T00:00:00+00:00')",(e.epoch,))
            rt=TradingCoreRuntime(e);rep=rt.start();self.assertEqual(rep.quarantined,1);self.assertIn(good.trade_intent_id,rep.states);self.assertIn('bad',rep.states);self.assertFalse(rt.ready);rt.stop()

    def test_recover_every_nonterminal_state_does_not_throw(self):
        nonterm=[s for s in TradeState if s not in {TradeState.CLOSED,TradeState.REJECTED,TradeState.CANCELED,TradeState.EXPIRED}]
        broken=[]
        for st in nonterm:
            with tempfile.TemporaryDirectory() as td:
                b=Path(td);e,ex=make(b);it=IN();cid='shata-test-entry';e.ledger.ensure(it,cid,e.epoch)
                ex.submit_market_buy('TESTUSDT',Decimal('500'),cid)
                e.ledger.conn.execute('UPDATE trades SET state=?,side_effect_prepared=1 WHERE intent_id=?',(st.value,it.trade_intent_id))
                try:e.recover_intent(it)
                except Exception as x:broken.append((st.value,type(x).__name__))
        self.assertEqual(broken,[])

    def test_persistent_simulator_rejects_zero_protection(self):
        with tempfile.TemporaryDirectory() as td:
            ex=PersistentSimulatedExchange(Path(td)/'e.db');ex.submit_market_buy('TESTUSDT',Decimal('500'),'c1')
            with self.assertRaises(Exception):ex.place_protection('TESTUSDT',Decimal('0'),Decimal('98'),Decimal('105'),'p-zero')

    def test_lease_supervisor_renews_long_running_runtime(self):
        with tempfile.TemporaryDirectory() as td:
            e,_=make(td,ex=SimulatedExchange(Decimal('100')),ttl=.3);rt=TradingCoreRuntime(e);rt.start();time.sleep(.8);self.assertTrue(rt.ready);self.assertFalse(rt.supervisor.lost);self.assertEqual(rt.submit(IN(),PF()).state,TradeState.PROTECTED);rt.stop()

if __name__=='__main__':unittest.main()
