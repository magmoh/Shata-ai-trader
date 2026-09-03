import sys,time,tempfile,unittest,json
from pathlib import Path
from decimal import Decimal
from datetime import datetime,timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))

from shata_trader.audit import HashChainedAuditLog
from shata_trader.audit_anchor import FileAuditAnchor
from shata_trader.activity import TradingActivityStore
from shata_trader.domain import PortfolioSnapshot,RiskPolicy,TradeState
from shata_trader.execution import DemoExecutionEngine,deterministic_client_order_id
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime
from shata_trader.strategy import DeterministicDemoStrategy

P=RiskPolicy(version=1,max_risk_per_trade_pct=Decimal('0.0075'),max_position_allocation_pct=Decimal('0.10'),max_portfolio_exposure_pct=Decimal('0.50'),min_risk_reward=Decimal('2'),max_entry_deviation_pct=Decimal('0.005'),max_intent_age_seconds=30)
PF=lambda:PortfolioSnapshot(Decimal('10000'),Decimal('10000'),Decimal('0'),datetime.now(timezone.utc))
IN=lambda amt='500':DeterministicDemoStrategy().create_intent('TESTUSDT',Decimal('100'),Decimal(amt),1)

def make(base,ex=None,ledger=None,lease=None,label='v06',ttl=1.0,audit=None):
    b=Path(base)
    ex=ex or PersistentSimulatedExchange(b/'exchange.db')
    ledger=ledger or TradeLedger(b/'ledger.db')
    lease=lease or SingleWriterLease(b/'lease.db')
    e=DemoExecutionEngine(ex,DeterministicRiskEngine(P),IdempotencyStore(b/f'idem-{label}.db'),audit or HashChainedAuditLog(b/f'audit-{label}.jsonl'),ledger=ledger,lease=lease,holder_id=label,activity=TradingActivityStore(b/f'activity-{label}.db'),lease_ttl_seconds=ttl)
    return e,ex

class DrainedExchange(PersistentSimulatedExchange):
    drain=Decimal('0')
    def get_free_base_balance(self,symbol):
        return max(Decimal('0'),super().get_free_base_balance(symbol)-self.drain)

class TestPhase0V06ProtectionInvariants(unittest.TestCase):
    def test_shortfall_never_claims_protected(self):
        with tempfile.TemporaryDirectory() as td:
            b=Path(td);ex=DrainedExchange(b/'exchange.db');ex.drain=Decimal('3')
            e,_=make(b,ex=ex);rt=TradingCoreRuntime(e,protection_check_interval_seconds=.05,max_protection_age_seconds=.15)
            self.assertEqual(rt.start().unresolved,0)
            it=IN();sm=rt.submit(it,PF())
            self.assertEqual(sm.state,TradeState.UNDER_PROTECTED)
            self.assertEqual(e.ledger.get(it.trade_intent_id)['state'],'UNDER_PROTECTED')
            self.assertFalse(rt.ready)
            protected=sum((Decimal(q) for _,q in ex.active_protections()),Decimal('0'))
            self.assertEqual(protected,Decimal('1.995'))
            rt.stop()

    def test_out_of_band_cancel_is_detected_inside_live_session(self):
        with tempfile.TemporaryDirectory() as td:
            b=Path(td);e,ex=make(b);rt=TradingCoreRuntime(e,protection_check_interval_seconds=.04,max_protection_age_seconds=.15)
            self.assertEqual(rt.start().unresolved,0)
            it=IN();self.assertEqual(rt.submit(it,PF()).state,TradeState.PROTECTED)
            pid=deterministic_client_order_id(it,'protection')
            ex.cancel_protection_by_client_id(it.symbol,pid)  # external/manual cancellation
            deadline=time.time()+1.0
            while rt.ready and time.time()<deadline:time.sleep(.02)
            self.assertFalse(rt.ready)
            self.assertEqual(e.ledger.get(it.trade_intent_id)['state'],'UNKNOWN')
            rt.stop()

    def test_query_uncertainty_expires_protection_freshness(self):
        with tempfile.TemporaryDirectory() as td:
            b=Path(td);e,ex=make(b);rt=TradingCoreRuntime(e,protection_check_interval_seconds=.04,max_protection_age_seconds=.12)
            self.assertEqual(rt.start().unresolved,0)
            it=IN();self.assertEqual(rt.submit(it,PF()).state,TradeState.PROTECTED)
            original=ex.protection_details_by_client_id
            ex.protection_details_by_client_id=lambda *a,**k: (_ for _ in ()).throw(TimeoutError('visibility uncertain'))
            deadline=time.time()+1.0
            while rt.ready and time.time()<deadline:time.sleep(.02)
            ex.protection_details_by_client_id=original
            self.assertFalse(rt.ready)
            self.assertEqual(e.ledger.get(it.trade_intent_id)['state'],'UNKNOWN')
            rt.stop()

    def test_second_position_emergency_exit_does_not_orphan_first(self):
        with tempfile.TemporaryDirectory() as td:
            b=Path(td);e,ex=make(b);rt=TradingCoreRuntime(e,protection_check_interval_seconds=.05,max_protection_age_seconds=.2)
            self.assertEqual(rt.start().unresolved,0)
            first=IN();self.assertEqual(rt.submit(first,PF()).state,TradeState.PROTECTED)
            first_pid=deterministic_client_order_id(first,'protection')
            self.assertIsNotNone(ex.protection_details_by_client_id(first.symbol,first_pid))
            ex.fail_protection=True
            second=IN();self.assertEqual(rt.submit(second,PF()).state,TradeState.CLOSED)
            self.assertIsNotNone(ex.protection_details_by_client_id(first.symbol,first_pid))
            self.assertEqual(e.ledger.get(first.trade_intent_id)['state'],'PROTECTED')
            self.assertTrue(rt.ready)
            rt.stop()

    def test_same_raw_ledger_object_does_not_share_new_leader_authority(self):
        with tempfile.TemporaryDirectory() as td:
            b=Path(td);raw=TradeLedger(b/'ledger.db');lease=SingleWriterLease(b/'lease.db');ex=PersistentSimulatedExchange(b/'exchange.db')
            e1,_=make(b,ex=ex,ledger=raw,lease=lease,label='old',ttl=.15)
            rt1=TradingCoreRuntime(e1);self.assertEqual(rt1.start().unresolved,0)
            it=IN();self.assertEqual(rt1.submit(it,PF()).state,TradeState.PROTECTED)
            old_epoch=e1.epoch
            rt1.supervisor.stop(release=False);time.sleep(.18)
            e2,_=make(b,ex=ex,ledger=raw,lease=lease,label='new',ttl=1.0)
            rt2=TradingCoreRuntime(e2);self.assertEqual(rt2.start().unresolved,0);self.assertGreater(e2.epoch,old_epoch)
            with self.assertRaises(Exception):
                e1.ledger.recovery_set_state(it.trade_intent_id,'UNKNOWN','zombie')
            self.assertEqual(e2.ledger.get(it.trade_intent_id)['state'],'PROTECTED')
            rt1.stop(release_lease=False);rt2.stop()

    def test_runtime_stop_start_restarts_lease_supervisor(self):
        with tempfile.TemporaryDirectory() as td:
            e,_=make(td,ttl=.25);rt=TradingCoreRuntime(e)
            self.assertEqual(rt.start().unresolved,0);self.assertTrue(rt.supervisor.alive)
            rt.stop(release_lease=False)
            self.assertEqual(rt.start().unresolved,0);self.assertTrue(rt.supervisor.alive);self.assertTrue(rt.ready)
            time.sleep(.45);self.assertTrue(e.has_authority());self.assertTrue(rt.ready)
            rt.stop()

    def test_boot_anchor_mismatch_rejects_without_overwrite(self):
        with tempfile.TemporaryDirectory() as td:
            b=Path(td);anchor=FileAuditAnchor(b/'external-anchor.json');audit=HashChainedAuditLog(b/'audit.jsonl',anchor=anchor)
            audit.append('ORIGINAL',{'x':1});honest=anchor.read()['head_hash']
            # Rebuild a valid local chain with different content while leaving witness untouched.
            log=HashChainedAuditLog(b/'replacement.jsonl');log.append('TAMPERED',{'x':999})
            (b/'audit.jsonl').write_bytes((b/'replacement.jsonl').read_bytes())
            e,_=make(b,audit=audit);rt=TradingCoreRuntime(e);rt.start()
            self.assertFalse(rt.ready)
            self.assertEqual(anchor.read()['head_hash'],honest)
            rt.stop()

if __name__=='__main__':unittest.main()
