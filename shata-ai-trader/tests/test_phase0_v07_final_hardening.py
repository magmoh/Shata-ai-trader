import hashlib
import json
import tempfile
import threading
import time
import unittest
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from shata_trader.activity import TradingActivityStore
from shata_trader.audit import HashChainedAuditLog
from shata_trader.audit_anchor import FileAuditAnchor
from shata_trader.domain import PortfolioSnapshot, RiskPolicy, TradeState
from shata_trader.events import ExchangeEvent
from shata_trader.execution import BootGateClosed, DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.rate_governor import PriorityRateGovernor
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime
from shata_trader.strategy import DeterministicDemoStrategy

P = RiskPolicy(
    version=1,
    max_risk_per_trade_pct=Decimal('0.0075'),
    max_position_allocation_pct=Decimal('0.10'),
    max_portfolio_exposure_pct=Decimal('0.50'),
    min_risk_reward=Decimal('2'),
    max_entry_deviation_pct=Decimal('0.005'),
    max_intent_age_seconds=30,
    max_orders_per_hour=100,
    max_notional_per_day_pct=Decimal('1.0'),
)
PF = lambda: PortfolioSnapshot(Decimal('10000'), Decimal('10000'), Decimal('0'), datetime.now(timezone.utc))

def IN(symbol='TESTUSDT', amount='300'):
    return DeterministicDemoStrategy().create_intent(symbol, Decimal('100'), Decimal(amount), 1)

def build(base, ex=None, anchor=None, label='v07', ttl=2.0, interval=.03, maxage=.15):
    b = Path(base)
    ex = ex or PersistentSimulatedExchange(b/'exchange.db')
    audit = HashChainedAuditLog(b/'audit.jsonl', anchor=anchor)
    e = DemoExecutionEngine(
        ex,
        DeterministicRiskEngine(P),
        IdempotencyStore(b/f'idem-{label}.db'),
        audit,
        ledger=TradeLedger(b/'ledger.db'),
        lease=SingleWriterLease(b/'lease.db'),
        holder_id=label,
        activity=TradingActivityStore(b/f'activity-{label}.db'),
        lease_ttl_seconds=ttl,
    )
    return e, ex, TradingCoreRuntime(e, protection_check_interval_seconds=interval, max_protection_age_seconds=maxage)


def rebuild_valid_chain(path: Path, mutate):
    rows = [json.loads(x) for x in path.read_text().splitlines()]
    mutate(rows)
    prev='GENESIS'; out=[]
    for row in rows:
        row.pop('hash', None)
        row['prev_hash']=prev
        digest=hashlib.sha256(json.dumps(row,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        prev=digest
        out.append(json.dumps({**row,'hash':digest},sort_keys=True))
    path.write_text('\n'.join(out)+'\n')
    return prev


class TestPhase0V07FinalHardening(unittest.TestCase):
    def test_append_does_not_overwrite_divergent_audit_witness(self):
        with tempfile.TemporaryDirectory() as td:
            b=Path(td); anchor=FileAuditAnchor(b/'external'/'anchor.json')
            e,ex,rt=build(b,anchor=anchor)
            self.assertEqual(rt.start().unresolved,0)
            self.assertEqual(rt.submit(IN(),PF()).state,TradeState.PROTECTED)
            honest=anchor.read()['head_hash']
            rebuild_valid_chain(b/'audit.jsonl', lambda rows: [r['payload'].__setitem__('base_qty','0.00000001') for r in rows if r['event_type']=='POSITION_PROTECTED'])
            self.assertTrue(e.audit.verify())
            e.audit.append('HEARTBEAT',{'x':1})
            self.assertEqual(anchor.read()['head_hash'],honest)
            self.assertTrue(e.audit.anchor_degraded)
            deadline=time.time()+.6
            while rt.ready and time.time()<deadline: time.sleep(.01)
            self.assertFalse(rt.ready)
            rt.stop()

    def test_stalled_protection_supervisor_trips_progress_watchdog(self):
        with tempfile.TemporaryDirectory() as td:
            b=Path(td); e,ex,rt=build(b,interval=.02,maxage=.12)
            self.assertEqual(rt.start().unresolved,0)
            it=IN(); self.assertEqual(rt.submit(it,PF()).state,TradeState.PROTECTED)
            original=ex.protection_details_by_client_id
            def hanging(*args,**kwargs):
                if threading.current_thread().name=='shata-protection-supervisor':
                    time.sleep(.8)
                return original(*args,**kwargs)
            ex.protection_details_by_client_id=hanging
            # Wait until the supervisor enters the hanging call, then remove protection externally.
            time.sleep(.04)
            pid=e._protection_client_id(it,False)
            ex.cancel_protection_by_client_id(it.symbol,pid)
            t0=time.monotonic(); deadline=time.time()+.6
            while rt.ready and time.time()<deadline: time.sleep(.01)
            elapsed=time.monotonic()-t0
            self.assertFalse(rt.ready)
            self.assertLess(elapsed,.45, f'watchdog detection too slow: {elapsed}')
            rt.stop(release_lease=False)

    def test_interrupted_rate_governor_caller_cleans_ticket(self):
        g=PriorityRateGovernor(.03)
        g.acquire(priority=1)
        original_wait=threading.Condition.wait
        armed={'v':True}
        def evil_wait(self,timeout=None):
            if armed['v']:
                armed['v']=False
                raise KeyboardInterrupt('test interrupt')
            return original_wait(self,timeout)
        try:
            threading.Condition.wait=evil_wait
            try:g.acquire(priority=0)
            except KeyboardInterrupt:pass
        finally:
            threading.Condition.wait=original_wait
        self.assertEqual(g._queue,[])
        done=threading.Event()
        t=threading.Thread(target=lambda:(g.acquire(priority=0),done.set()),daemon=True)
        t.start();t.join(timeout=1)
        self.assertTrue(done.is_set())

    def test_malformed_exchange_events_are_quarantined_not_raised(self):
        with tempfile.TemporaryDirectory() as td:
            e,ex,rt=build(td);self.assertEqual(rt.start().unresolved,0)
            self.assertIsNone(rt.ingest_exchange_event(ExchangeEvent('bad1','cid','ALIEN',1)))
            self.assertIsNone(rt.ingest_exchange_event(ExchangeEvent('bad2','cid','FILLED',None)))
            rows=[json.loads(x) for x in (Path(td)/'audit.jsonl').read_text().splitlines()]
            self.assertGreaterEqual(sum(r['event_type']=='MALFORMED_EXCHANGE_EVENT' for r in rows),2)
            self.assertTrue(rt.ready)
            rt.stop()

    def test_concurrent_submitters_are_serialized_to_safe_outcomes(self):
        with tempfile.TemporaryDirectory() as td:
            e,ex,rt=build(td,interval=.02,maxage=.5);self.assertEqual(rt.start().unresolved,0)
            out=[];lock=threading.Lock();bar=threading.Barrier(8);syms=['TESTUSDT','ALTUSDT','COINUSDT']
            def worker(i):
                it=IN(syms[i%3],'150');bar.wait()
                try:v=rt.submit(it,PF()).state.value
                except Exception as exc:v=type(exc).__name__
                with lock:out.append(v)
            threads=[threading.Thread(target=worker,args=(i,)) for i in range(8)]
            [t.start() for t in threads];[t.join(timeout=5) for t in threads]
            self.assertEqual(Counter(out),Counter({'PROTECTED':8}),out)
            self.assertTrue(e.audit.verify())
            self.assertTrue(rt.ready)
            rt.stop()

    def test_boot_authority_requires_runtime_capability(self):
        with tempfile.TemporaryDirectory() as td:
            e,ex,rt=build(td)
            with self.assertRaises(BootGateClosed):e.grant_boot_authority()
            self.assertEqual(rt.start().unresolved,0)
            with self.assertRaises(BootGateClosed):e.grant_boot_authority()
            self.assertTrue(rt.ready)
            rt.stop()


if __name__=='__main__':unittest.main()
