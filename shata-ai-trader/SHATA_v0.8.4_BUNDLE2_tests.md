# SHATA v0.8.4 — SOURCE BUNDLE 2/3 (tests)

**Run this first. If it does not print `Ran 95 tests`, you are on an older tree and any
verdict is void.**

```bash
python3 -m unittest discover -s tests -v     # MUST print: Ran 95 tests ... OK
```

`Ran 93` means v0.8.3, which contains CG-4 — a confirmed defect ChatGPT raised and this
release closes.

## What v0.8.4 changes

**CG-4 — foreground traffic masked a frozen background supervisor.** Introduced by my own
D-2 patch in v0.8.3: `verify_one()` kept advancing `_last_progress_monotonic`, the counter
the watchdog reads as the supervisor liveness signal. A steady stream of `submit()` calls
kept that signal fresh while the background thread was frozen inside one call.

```
before:  supervisor frozen >1.2s inside protected_records()
         ready=True  gate_open=True  watchdog=None  progress_age=0.050s (bound 0.3s)
after:   ready=False gate_open=False
         watchdog=PROTECTION_SUPERVISOR_STALLED:0.356357s   progress_age=1.219s
```

Fix: `_background_progress_monotonic` is advanced **only** by `verify_once()` — at cycle
start and after each record. `verify_one()` no longer touches the liveness signal.
Liveness of a supervisor can only be evidenced by that supervisor doing work.

Plus 64 striped per-record `RLock`s: removing `_cycle_lock` in D-2 had allowed the
background cycle and the foreground path to verify the **same** record simultaneously,
where one side may write `UNKNOWN`. Striping serialises per-record verification without
reintroducing the portfolio-size coupling D-2 removed.

## Reconstruct

```python
import re, pathlib
for b in ['bundle1.md','bundle2.md','bundle3.md']:
    text = pathlib.Path(b).read_text(encoding='utf-8')
    for m in re.finditer(r'^=== FILE: tests/runtime_helpers.py ===
from shata_trader.runtime import TradingCoreRuntime

def boot_submit(engine,intent,portfolio,keep_running=False):
    rt=TradingCoreRuntime(engine);rep=rt.start()
    if rep.unresolved:
        if not keep_running: rt.stop(release_lease=True)
        raise RuntimeError(f'boot unresolved: {rep.states}')
    sm=rt.submit(intent,portfolio)
    if keep_running:return sm,rt
    rt.stop(release_lease=True);return sm

=== FILE: tests/test_audit_anchor_v04.py ===
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
import tempfile,unittest,json
from pathlib import Path
from shata_trader.audit import HashChainedAuditLog
from shata_trader.audit_anchor import FileAuditAnchor

class TestAuditAnchorV04(unittest.TestCase):
    def test_external_anchor_matches_head_and_detects_rewritten_log(self):
        with tempfile.TemporaryDirectory() as td:
            logp=Path(td)/'core'/'audit.jsonl';anch=FileAuditAnchor(Path(td)/'external'/'anchor.json');log=HashChainedAuditLog(logp,anch)
            log.append('A',{'x':1});log.append('B',{'x':2});self.assertTrue(log.verify(verify_anchor=True))
            lines=logp.read_text().splitlines();rec=json.loads(lines[-1]);rec['payload']['x']=999;lines[-1]=json.dumps(rec,sort_keys=True);logp.write_text('\n'.join(lines)+'\n');self.assertFalse(log.verify(verify_anchor=True))
if __name__=='__main__':unittest.main()

=== FILE: tests/test_concurrency_and_lease.py ===
import sys
from pathlib import Path
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.idempotency import DuplicateIntent, IdempotencyStore
from shata_trader.lease import LeaseUnavailable, SingleWriterLease


class TestConcurrencyAndLease(unittest.TestCase):
    def test_atomic_idempotency_eight_workers(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "idem.sqlite"

            # Initialize schema/WAL before racing workers.
            init = IdempotencyStore(db)
            init.close()

            wins = []
            errors = []
            lock = threading.Lock()
            barrier = threading.Barrier(8)

            def worker(worker_id):
                store = None
                try:
                    store = IdempotencyStore(db)
                    barrier.wait(timeout=5)
                    store.claim("same-intent")
                    with lock:
                        wins.append(worker_id)
                except DuplicateIntent:
                    pass
                except Exception as exc:
                    with lock:
                        errors.append((worker_id, type(exc).__name__, str(exc)))
                finally:
                    if store is not None:
                        store.close()

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)

            # Thread exceptions must fail the test; no false-pass.
            self.assertEqual(errors, [], f"Worker errors: {errors}")
            self.assertEqual(len(wins), 1, f"Expected exactly one winner, got {wins}")

    def test_single_writer_lease(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "lease.sqlite"
            a = SingleWriterLease(db)
            b = SingleWriterLease(db)
            a.acquire("core", "instance-a", ttl_seconds=30)
            with self.assertRaises(LeaseUnavailable):
                b.acquire("core", "instance-b", ttl_seconds=30)


if __name__ == "__main__":
    unittest.main()

=== FILE: tests/test_crash_recovery_v04.py ===
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

=== FILE: tests/test_decimal_roundtrip.py ===
import sys
from pathlib import Path
import json
import unittest
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


class TestDecimalRoundTrip(unittest.TestCase):
    def test_decimal_string_roundtrip_exact(self):
        original = Decimal("0.1234567890123456789012345678")
        wire = json.dumps({"value": str(original)})
        recovered = Decimal(json.loads(wire)["value"])
        self.assertEqual(original, recovered)


if __name__ == "__main__":
    unittest.main()

=== FILE: tests/test_execution.py ===
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

=== FILE: tests/test_fencing_integration.py ===
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

=== FILE: tests/test_hostile_exchange.py ===
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
        reconciled_at=datetime.now(timezone.utc),
    )

def intent():
    return DeterministicDemoStrategy().create_intent(
        "TESTUSDT", Decimal("100"), Decimal("500"), 1
    )

class TestHostileExchange(unittest.TestCase):
    def engine(self, ex, td):
        return DemoExecutionEngine(
            ex,
            DeterministicRiskEngine(policy()),
            IdempotencyStore(Path(td) / "idem.sqlite"),
            HashChainedAuditLog(Path(td) / "audit.jsonl"),
        )

    def test_timeout_after_acceptance_reconciles_same_order(self):
        with tempfile.TemporaryDirectory() as td:
            ex = SimulatedExchange(Decimal("100"), ambiguous_submit=True)
            sm = boot_submit(self.engine(ex,td),intent(),portfolio())
            self.assertEqual(sm.state, TradeState.PROTECTED)
            # entry + no duplicate entry; protection is separate exchange artifact
            entry_orders = [o for k, o in ex.orders.items() if "emergency" not in k]
            self.assertEqual(len(entry_orders), 1)

    def test_symbol_halt_rejects_safely(self):
        with tempfile.TemporaryDirectory() as td:
            ex = SimulatedExchange(Decimal("100"), symbol_status="HALT")
            sm = boot_submit(self.engine(ex,td),intent(),portfolio())
            self.assertEqual(sm.state, TradeState.HALTED)

    def test_maintenance_rejects_safely(self):
        with tempfile.TemporaryDirectory() as td:
            ex = SimulatedExchange(Decimal("100"), maintenance=True)
            sm = boot_submit(self.engine(ex,td),intent(),portfolio())
            self.assertEqual(sm.state, TradeState.HALTED)


if __name__ == "__main__":
    unittest.main()

=== FILE: tests/test_idempotency.py ===
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.idempotency import DuplicateIntent, IdempotencyStore


class TestIdempotency(unittest.TestCase):
    def test_duplicate_rejected(self):
        store = IdempotencyStore(":memory:")
        store.claim("abc")
        with self.assertRaises(DuplicateIntent):
            store.claim("abc")


if __name__ == "__main__":
    unittest.main()

=== FILE: tests/test_out_of_order_events_v04.py ===
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
import tempfile,unittest
from pathlib import Path
from shata_trader.events import OrderEventStore,ExchangeEvent

class TestOutOfOrderEventsV04(unittest.TestCase):
    def test_fill_before_ack_does_not_regress(self):
        with tempfile.TemporaryDirectory() as td:
            s=OrderEventStore(Path(td)/'e.db');self.assertTrue(s.ingest(ExchangeEvent('e-fill','cid','FILLED',200)));self.assertTrue(s.ingest(ExchangeEvent('e-ack','cid','ACKNOWLEDGED',100)));self.assertEqual(s.status('cid'),'FILLED')
    def test_duplicate_event_is_idempotent(self):
        s=OrderEventStore(':memory:');e=ExchangeEvent('same','cid','PARTIALLY_FILLED',100);self.assertTrue(s.ingest(e));self.assertFalse(s.ingest(e));self.assertEqual(s.status('cid'),'PARTIALLY_FILLED')
    def test_late_partial_cannot_regress_filled(self):
        s=OrderEventStore(':memory:');s.ingest(ExchangeEvent('1','cid','FILLED',100));s.ingest(ExchangeEvent('2','cid','PARTIALLY_FILLED',200));self.assertEqual(s.status('cid'),'FILLED')
if __name__=='__main__':unittest.main()

=== FILE: tests/test_partial_fee_protection.py ===
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

=== FILE: tests/test_phase0_v06_protection_invariants.py ===
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

=== FILE: tests/test_phase0_v07_final_hardening.py ===
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

=== FILE: tests/test_rate_governor_priority_v06.py ===
import sys,time,threading,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from shata_trader.rate_governor import PriorityRateGovernor

class TestRateGovernorPriorityV06(unittest.TestCase):
    def test_safety_priority_jumps_ahead_of_waiting_low_priority(self):
        g=PriorityRateGovernor(.04)
        g.acquire(priority=0)  # establish a pacing window
        order=[];lock=threading.Lock()
        def worker(name,priority):
            g.acquire(priority=priority)
            with lock:order.append(name)
        low=threading.Thread(target=worker,args=('market-data',5));low.start()
        time.sleep(.005)
        high=threading.Thread(target=worker,args=('protection',0));high.start()
        low.join();high.join()
        self.assertEqual(order[0],'protection',order)

if __name__=='__main__':unittest.main()

=== FILE: tests/test_reconciliation_freshness.py ===
import sys
from pathlib import Path
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.domain import PortfolioSnapshot, RiskPolicy
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.strategy import DeterministicDemoStrategy


class TestReconciliationFreshness(unittest.TestCase):
    def test_stale_portfolio_state_rejected(self):
        policy = RiskPolicy(
            version=1,
            max_risk_per_trade_pct=Decimal("0.0075"),
            max_position_allocation_pct=Decimal("0.10"),
            max_portfolio_exposure_pct=Decimal("0.50"),
            min_risk_reward=Decimal("2.0"),
            max_entry_deviation_pct=Decimal("0.005"),
            max_intent_age_seconds=30,
            max_reconciliation_age_seconds=5,
        )
        intent = DeterministicDemoStrategy().create_intent(
            "TESTUSDT", Decimal("100"), Decimal("500"), 1
        )
        p = PortfolioSnapshot(
            quote_balance=Decimal("10000"),
            portfolio_value=Decimal("10000"),
            current_exposure=Decimal("0"),
            reconciled_at=datetime.now(timezone.utc) - timedelta(seconds=30),
        )
        d = DeterministicRiskEngine(policy).evaluate(intent, p, Decimal("100"))
        self.assertFalse(d.approved)
        self.assertIn("stale", d.reason.lower())


if __name__ == "__main__":
    unittest.main()

=== FILE: tests/test_risk_engine.py ===
import sys
from pathlib import Path
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.domain import PortfolioSnapshot, RiskPolicy, Side, TradeIntent
from shata_trader.risk_engine import DeterministicRiskEngine


def make_policy():
    return RiskPolicy(
        version=1,
        max_risk_per_trade_pct=Decimal("0.0075"),
        max_position_allocation_pct=Decimal("0.10"),
        max_portfolio_exposure_pct=Decimal("0.50"),
        min_risk_reward=Decimal("2.0"),
        max_entry_deviation_pct=Decimal("0.005"),
        max_intent_age_seconds=30,
    )


def make_intent(amount="500"):
    now = datetime.now(timezone.utc)
    return TradeIntent(
        trade_intent_id="risk-test-1",
        strategy_id="test",
        strategy_version="1",
        risk_policy_version=1,
        symbol="TESTUSDT",
        side=Side.BUY,
        quote_amount=Decimal(amount),
        reference_entry_price=Decimal("100"),
        stop_price=Decimal("98"),
        take_profit_price=Decimal("105"),
        max_entry_deviation_pct=Decimal("0.005"),
        created_at=now,
        expires_at=now + timedelta(seconds=30),
    )


class TestRiskEngine(unittest.TestCase):
    def setUp(self):
        self.engine = DeterministicRiskEngine(make_policy())
        self.portfolio = PortfolioSnapshot(
            quote_balance=Decimal("10000"),
            portfolio_value=Decimal("10000"),
            current_exposure=Decimal("0"),
        )

    def test_approves_valid_intent(self):
        d = self.engine.evaluate(make_intent("500"), self.portfolio, Decimal("100.1"))
        self.assertTrue(d.approved)

    def test_rejects_price_deviation(self):
        d = self.engine.evaluate(make_intent("500"), self.portfolio, Decimal("102"))
        self.assertFalse(d.approved)

    def test_rejects_oversized_position(self):
        d = self.engine.evaluate(make_intent("5000"), self.portfolio, Decimal("100"))
        self.assertFalse(d.approved)


if __name__ == "__main__":
    unittest.main()

=== FILE: tests/test_runtime_and_protection_recovery_v04.py ===
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

=== FILE: tests/test_security_regressions_legacy.py ===
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

=== FILE: tests/test_security_regressions_v05.py ===
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

=== FILE: tests/test_state_machine.py ===
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.domain import TradeState
from shata_trader.state_machine import InvalidTransition, TradeStateMachine


class TestStateMachine(unittest.TestCase):
    def test_normal_prefix(self):
        sm = TradeStateMachine()
        sm.transition(TradeState.RISK_APPROVED)
        sm.transition(TradeState.SUBMITTED)
        sm.transition(TradeState.ACKNOWLEDGED)
        sm.transition(TradeState.FILLED)
        sm.transition(TradeState.PROTECTION_PENDING)
        sm.transition(TradeState.PROTECTED)
        self.assertEqual(sm.state, TradeState.PROTECTED)

    def test_invalid_transition_rejected(self):
        sm = TradeStateMachine()
        with self.assertRaises(InvalidTransition):
            sm.transition(TradeState.PROTECTED)


if __name__ == "__main__":
    unittest.main()

=== FILE: tests/test_v06_protection_supervisor.py ===
import sys, tempfile, time, unittest, json, hashlib
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from shata_trader.activity import TradingActivityStore
from shata_trader.audit import HashChainedAuditLog
from shata_trader.audit_anchor import FileAuditAnchor
from shata_trader.domain import PortfolioSnapshot, RiskPolicy, TradeState
from shata_trader.execution import DemoExecutionEngine, deterministic_client_order_id
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
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
)
PF = lambda: PortfolioSnapshot(Decimal('10000'), Decimal('10000'), Decimal('0'), datetime.now(timezone.utc))


def make(b, ex=None, audit=None, ttl=1.0, **runtime_kwargs):
    b = Path(b)
    ex = ex or PersistentSimulatedExchange(b / 'exchange.db')
    e = DemoExecutionEngine(
        ex,
        DeterministicRiskEngine(P),
        IdempotencyStore(b / 'idem.db'),
        audit or HashChainedAuditLog(b / 'audit.jsonl'),
        ledger=TradeLedger(b / 'ledger.db'),
        lease=SingleWriterLease(b / 'lease.db'),
        holder_id='v06',
        activity=TradingActivityStore(b / 'activity.db'),
        lease_ttl_seconds=ttl,
    )
    rt = TradingCoreRuntime(e, **runtime_kwargs)
    return e, ex, rt


def intent(symbol='TESTUSDT', amt='500'):
    return DeterministicDemoStrategy().create_intent(symbol, Decimal('100'), Decimal(amt), 1)


class Drained(PersistentSimulatedExchange):
    drain = Decimal('0')
    def get_free_base_balance(self, symbol):
        return max(Decimal('0'), super().get_free_base_balance(symbol) - self.drain)


class TestV06ProtectionSupervisor(unittest.TestCase):
    def test_short_protection_is_never_labeled_protected(self):
        with tempfile.TemporaryDirectory() as td:
            ex = Drained(Path(td) / 'exchange.db')
            ex.drain = Decimal('3')
            e, ex, rt = make(td, ex=ex)
            self.assertEqual(rt.start().unresolved, 0)
            it = intent()
            sm = rt.submit(it, PF())
            self.assertEqual(sm.state, TradeState.UNDER_PROTECTED)
            self.assertFalse(rt.ready)
            rec = e.ledger.get(it.trade_intent_id)
            self.assertEqual(rec['state'], 'UNDER_PROTECTED')
            self.assertNotEqual(rec['protection_expected_qty'], rec['protection_actual_qty'])
            rt.stop()

    def test_out_of_band_cancel_is_detected_inside_window(self):
        with tempfile.TemporaryDirectory() as td:
            e, ex, rt = make(
                td,
                protection_check_interval_seconds=0.03,
                max_protection_age_seconds=0.15,
            )
            self.assertEqual(rt.start().unresolved, 0)
            it = intent()
            self.assertEqual(rt.submit(it, PF()).state, TradeState.PROTECTED)
            pid = deterministic_client_order_id(it, 'protection')
            ex.cancel_protection_by_client_id(it.symbol, pid)
            deadline = time.monotonic() + 1.0
            while rt.ready and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertFalse(rt.ready)
            self.assertEqual(e.ledger.get(it.trade_intent_id)['state'], 'UNKNOWN')
            rt.stop(release_lease=False)

    def test_transient_protection_query_failure_does_not_immediately_regress(self):
        with tempfile.TemporaryDirectory() as td:
            e, ex, rt = make(
                td,
                protection_check_interval_seconds=0.03,
                max_protection_age_seconds=0.35,
            )
            rt.start(); it = intent(); rt.submit(it, PF())
            original = ex.protection_details_by_client_id
            ex.protection_details_by_client_id = lambda *a, **k: (_ for _ in ()).throw(TimeoutError('transient'))
            time.sleep(0.12)
            self.assertTrue(rt.ready)
            self.assertEqual(e.ledger.get(it.trade_intent_id)['state'], 'PROTECTED')
            ex.protection_details_by_client_id = original
            time.sleep(0.08)
            self.assertTrue(rt.ready)
            self.assertEqual(e.ledger.get(it.trade_intent_id)['state'], 'PROTECTED')
            rt.stop()

    def test_persistent_query_uncertainty_expires_protected_claim(self):
        with tempfile.TemporaryDirectory() as td:
            e, ex, rt = make(
                td,
                protection_check_interval_seconds=0.03,
                max_protection_age_seconds=0.12,
            )
            rt.start(); it = intent(); rt.submit(it, PF())
            ex.protection_details_by_client_id = lambda *a, **k: (_ for _ in ()).throw(TimeoutError('persistent'))
            deadline = time.monotonic() + 1.0
            while rt.ready and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertFalse(rt.ready)
            self.assertEqual(e.ledger.get(it.trade_intent_id)['state'], 'UNKNOWN')
            rt.stop(release_lease=False)

    def test_second_position_emergency_exit_does_not_orphan_first(self):
        with tempfile.TemporaryDirectory() as td:
            e, ex, rt = make(td)
            rt.start()
            first = intent('TESTUSDT', '500')
            self.assertEqual(rt.submit(first, PF()).state, TradeState.PROTECTED)
            ex.fail_protection = True
            second = intent('TESTUSDT', '500')
            self.assertEqual(rt.submit(second, PF()).state, TradeState.CLOSED)
            self.assertTrue(rt.ready)
            self.assertEqual(e.ledger.get(first.trade_intent_id)['state'], 'PROTECTED')
            self.assertEqual(len(ex.active_protections('TESTUSDT')), 1)
            self.assertEqual(ex._balance('TESTUSDT'), Decimal('4.995'))
            rt.stop()

    def test_balances_are_isolated_by_symbol(self):
        with tempfile.TemporaryDirectory() as td:
            e, ex, rt = make(td)
            rt.start()
            a = intent('TESTUSDT', '400')
            b = intent('ALTUSDT', '300')
            self.assertEqual(rt.submit(a, PF()).state, TradeState.PROTECTED)
            self.assertEqual(rt.submit(b, PF()).state, TradeState.PROTECTED)
            self.assertEqual(ex._balance('TESTUSDT'), Decimal('3.996'))
            self.assertEqual(ex._balance('ALTUSDT'), Decimal('2.997'))
            self.assertEqual(len(ex.active_protections('TESTUSDT')), 1)
            self.assertEqual(len(ex.active_protections('ALTUSDT')), 1)
            rt.stop()

    def test_boot_rejects_rebuilt_log_and_does_not_overwrite_anchor(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td)
            anchor = FileAuditAnchor(b / 'external' / 'anchor.json')
            audit = HashChainedAuditLog(b / 'audit.jsonl', anchor=anchor)
            audit.append('A', {'x': 1})
            audit.append('B', {'x': 2})
            honest = anchor.read()['head_hash']

            # Attacker with local-file access rewrites payload AND rebuilds the local chain.
            records = [json.loads(x) for x in audit.path.read_text().splitlines()]
            records[0]['payload']['x'] = 999
            prev = 'GENESIS'
            rebuilt = []
            for rec in records:
                rec.pop('hash', None)
                rec['prev_hash'] = prev
                canonical = json.dumps(rec, sort_keys=True, separators=(',', ':'))
                digest = hashlib.sha256(canonical.encode()).hexdigest()
                rebuilt.append(json.dumps({**rec, 'hash': digest}, sort_keys=True))
                prev = digest
            audit.path.write_text('\n'.join(rebuilt) + '\n')
            self.assertNotEqual(prev, honest)

            e, ex, rt = make(b, audit=audit)
            rep = rt.start()
            self.assertFalse(rt.ready)
            self.assertEqual(anchor.read()['head_hash'], honest)
            rt.stop(release_lease=False)

    def test_runtime_stop_start_reacquires_and_renews_authority(self):
        with tempfile.TemporaryDirectory() as td:
            e, ex, rt = make(td, ttl=0.18)
            self.assertEqual(rt.start().unresolved, 0)
            old_epoch = e.epoch
            rt.stop(release_lease=True)
            self.assertIsNone(e.epoch)
            self.assertEqual(rt.start().unresolved, 0)
            self.assertTrue(rt.ready)
            self.assertGreater(e.epoch, old_epoch)
            time.sleep(0.45)
            self.assertTrue(rt.ready)
            self.assertTrue(e.has_authority())
            self.assertTrue(rt.supervisor.alive)
            rt.stop()

    def test_event_is_recorded_but_cannot_mutate_when_gate_closed(self):
        from shata_trader.events import ExchangeEvent
        with tempfile.TemporaryDirectory() as td:
            e, ex, rt = make(td)
            rt.start(); it = intent(); rt.submit(it, PF())
            rec = e.ledger.get(it.trade_intent_id)
            rt.ready = False; e.revoke_boot_authority('TEST_GATE_CLOSED')
            result = rt.ingest_exchange_event(ExchangeEvent('stale', rec['entry_client_order_id'], 'ACKNOWLEDGED', 1))
            self.assertIsNone(result)
            self.assertEqual(e.ledger.get(it.trade_intent_id)['state'], 'PROTECTED')
            self.assertEqual(rt.events.status(rec['entry_client_order_id']), 'ACKNOWLEDGED')
            rt.stop(release_lease=False)


    def test_dead_protection_supervisor_blocks_submit(self):
        with tempfile.TemporaryDirectory() as td:
            e, ex, rt = make(td, protection_check_interval_seconds=0.03)
            rt.start()
            rt.protection_supervisor.stop()
            self.assertFalse(rt.protection_supervisor.alive)
            with self.assertRaises(Exception):
                rt.submit(intent(), PF())
            self.assertFalse(rt.ready)



if __name__ == '__main__':
    unittest.main()

=== FILE: tests/test_v081_concurrency_regression.py ===
"""Concurrent submitters + live ProtectionSupervisor stress regression.

Added in v0.8.1 for ChatGPT Fast Gate finding CG-1: `PersistentSimulatedExchange`
shared one `sqlite3.Connection` across threads, so a supervisor query racing a
submitter produced `InterfaceError: bad parameter or other API misuse`, which surfaced
as `PROTECTION_REVERIFY_QUERY_FAILED`, a false `UNKNOWN`, readiness loss, and then
`RuntimeNotReady` on later submissions.

`TradingCoreRuntime._submit_lock` never protected this: the supervisor is not a
submitter, and it reaches the exchange on its own thread.

The supervisor interval is deliberately set to 1ms so that revalidation runs
continuously *during* the concurrent submissions rather than between them.

BUILDER-WRITTEN. Lower evidentiary weight per REVIEW_PROTOCOL §5. The independent
regression for CG-1 is owed by ChatGPT and/or Gemini.
"""
import io
import sys
import threading
import tempfile
import unittest
import warnings
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from shata_trader.activity import TradingActivityStore
from shata_trader.audit import HashChainedAuditLog
from shata_trader.domain import PortfolioSnapshot, RiskPolicy
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime
from shata_trader.strategy import DeterministicDemoStrategy

ITERATIONS = 100
WORKERS = 8
SYMBOLS = ['TESTUSDT', 'TESTUSDT', 'ALTUSDT', 'COINUSDT']

P = RiskPolicy(
    version=1, max_risk_per_trade_pct=Decimal('0.0075'),
    max_position_allocation_pct=Decimal('0.10'), max_portfolio_exposure_pct=Decimal('0.50'),
    min_risk_reward=Decimal('2'), max_entry_deviation_pct=Decimal('0.005'),
    max_intent_age_seconds=30, max_orders_per_hour=200, max_notional_per_day_pct=Decimal('1.0'),
)
PF = lambda: PortfolioSnapshot(Decimal('10000'), Decimal('10000'), Decimal('0'), datetime.now(timezone.utc))


class TestConcurrentSubmitAndSupervision(unittest.TestCase):

    def _one_iteration(self, base: Path, holder: str):
        ex = PersistentSimulatedExchange(base / 'exchange.db')
        eng = DemoExecutionEngine(
            ex, DeterministicRiskEngine(P), IdempotencyStore(base / 'idem.db'),
            HashChainedAuditLog(base / 'audit.jsonl'), ledger=TradeLedger(base / 'ledger.db'),
            lease=SingleWriterLease(base / 'lease.db'), holder_id=holder,
            activity=TradingActivityStore(base / 'activity.db'), lease_ttl_seconds=5.0,
        )
        # 1ms interval: revalidation overlaps the submissions instead of following them.
        rt = TradingCoreRuntime(
            eng, protection_check_interval_seconds=0.001, max_protection_age_seconds=2.0
        )
        rt.start()
        self.assertTrue(rt.ready)

        results = []
        bar = threading.Barrier(WORKERS)

        def worker(i):
            intent = DeterministicDemoStrategy().create_intent(
                SYMBOLS[i % len(SYMBOLS)], Decimal('100'), Decimal('150'), 1
            )
            bar.wait()
            try:
                results.append(rt.submit(intent, PF()).state.value)
            except Exception as exc:
                results.append(f'EXC:{type(exc).__name__}')

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(WORKERS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        errors = [
            r['last_error'] for r in eng.ledger.nonterminal_records() if r['last_error']
        ]
        false_protected = []
        for r in eng.ledger.protected_records():
            it = eng.ledger.intent_from_payload(r['payload'])
            pid = eng._protection_client_id(it, r['state'] == 'PARTIALLY_PROTECTED')
            d = ex.protection_details_by_client_id(it.symbol, pid)
            if not d or Decimal(d.base_qty) != Decimal(r['protection_expected_qty']):
                false_protected.append(r['intent_id'])

        state = dict(
            results=Counter(results),
            errors=errors,
            false_protected=false_protected,
            ready=rt.ready,
            sup_error=rt.protection_supervisor.last_error if rt.protection_supervisor else None,
        )
        rt.stop(release_lease=False)
        return state

    def test_concurrent_submitters_with_live_supervisor_are_stable(self):
        bad = []
        for i in range(ITERATIONS):
            with tempfile.TemporaryDirectory() as td:
                s = self._one_iteration(Path(td), f'stress-{i}')
                if s['results'].get('PROTECTED', 0) != WORKERS:
                    bad.append((i, dict(s['results']), s['errors'][:3], s['sup_error']))
                    continue
                if s['false_protected']:
                    bad.append((i, 'FALSE_PROTECTED', s['false_protected']))
                    continue
                if not s['ready']:
                    bad.append((i, 'READINESS_LOST_ON_HEALTHY_RUN'))
                    continue
                for e in s['errors']:
                    if 'REVERIFY_QUERY_FAILED' in e or 'InterfaceError' in e or 'OperationalError' in e:
                        bad.append((i, 'LOCAL_DB_CONCURRENCY_ERROR', e))
                        break
        self.assertEqual(bad, [], f'{len(bad)}/{ITERATIONS} iterations degraded: {bad[:5]}')

    def test_raw_exchange_is_safe_under_cross_thread_use(self):
        """Direct attack on the exchange persistence layer, no runtime involved."""
        errors = Counter()
        with tempfile.TemporaryDirectory() as td:
            ex = PersistentSimulatedExchange(Path(td) / 'e.db')
            ex.submit_market_buy('TESTUSDT', Decimal('500'), 'seed')
            ex.place_protection('TESTUSDT', Decimal('1'), Decimal('98'), Decimal('105'), 'p-seed')
            stop = threading.Event()

            def writer(i):
                for n in range(300):
                    if stop.is_set():
                        return
                    try:
                        ex.submit_market_buy('TESTUSDT', Decimal('10'), f'w{i}-{n}')
                        ex.place_protection(
                            'TESTUSDT', Decimal('0.05'), Decimal('98'), Decimal('105'), f'p{i}-{n}'
                        )
                    except Exception as exc:
                        errors[f'{type(exc).__name__}'] += 1

            def reader():
                while not stop.is_set():
                    try:
                        ex.protection_details_by_client_id('TESTUSDT', 'P-p-seed')
                        ex.get_free_base_balance('TESTUSDT')
                    except Exception as exc:
                        errors[f'READER:{type(exc).__name__}'] += 1

            ws = [threading.Thread(target=writer, args=(i,)) for i in range(6)]
            rs = [threading.Thread(target=reader) for _ in range(2)]
            for t in ws + rs:
                t.start()
            for t in ws:
                t.join()
            stop.set()
            for t in rs:
                t.join()
        self.assertEqual(dict(errors), {}, f'cross-thread exchange errors: {dict(errors)}')

    def test_no_resource_warnings_on_store_lifecycle(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            with tempfile.TemporaryDirectory() as td:
                s = self._one_iteration(Path(td), 'warn-check')
                self.assertEqual(s['results'].get('PROTECTED', 0), WORKERS)
        leaks = [w for w in caught if issubclass(w.category, ResourceWarning)]
        self.assertEqual(leaks, [], f'ResourceWarnings: {[str(w.message) for w in leaks]}')


if __name__ == '__main__':
    unittest.main()

=== FILE: tests/test_v082_cg2_and_thread_lifetime.py ===
"""v0.8.2 builder tests — ChatGPT CG-2 minimum list + B-6 thread-lifetime.

BUILDER-WRITTEN. Lower evidentiary weight per REVIEW_PROTOCOL §5. Independent
regressions for CG-2 and B-6 are owed by ChatGPT and/or Gemini.

Covers ChatGPT's required list 1-9 plus the thread-lifetime defect the full matrix
surfaced while fixing CG-2.
"""
import gc
import sys
import threading
import tempfile
import time
import unittest
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from shata_trader.activity import TradingActivityStore
from shata_trader.audit import HashChainedAuditLog
from shata_trader.domain import PortfolioSnapshot, RiskPolicy
from shata_trader.execution import BootGateClosed, DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime
from shata_trader.strategy import DeterministicDemoStrategy

P = RiskPolicy(
    version=1, max_risk_per_trade_pct=Decimal('0.0075'),
    max_position_allocation_pct=Decimal('0.10'), max_portfolio_exposure_pct=Decimal('0.50'),
    min_risk_reward=Decimal('2'), max_entry_deviation_pct=Decimal('0.005'),
    max_intent_age_seconds=30, max_orders_per_hour=200, max_notional_per_day_pct=Decimal('1.0'),
)
PF = lambda: PortfolioSnapshot(Decimal('10000'), Decimal('10000'), Decimal('0'), datetime.now(timezone.utc))
IN = lambda s='TESTUSDT', q='100': DeterministicDemoStrategy().create_intent(s, Decimal('100'), Decimal(q), 1)
SYMS = ['TESTUSDT', 'ALTUSDT', 'COINUSDT']


def build(b, ex=None, interval=0.02, maxage=0.5, ttl=5.0, ceiling=None):
    ex = ex if ex is not None else PersistentSimulatedExchange(b / 'exchange.db')
    e = DemoExecutionEngine(
        ex, DeterministicRiskEngine(P), IdempotencyStore(b / 'idem.db'),
        HashChainedAuditLog(b / 'audit.jsonl'), ledger=TradeLedger(b / 'ledger.db'),
        lease=SingleWriterLease(b / 'lease.db'), holder_id='core',
        activity=TradingActivityStore(b / 'activity.db'), lease_ttl_seconds=ttl,
    )
    rt = TradingCoreRuntime(
        e, protection_check_interval_seconds=interval, max_protection_age_seconds=maxage,
        protection_freshness_ceiling_seconds=ceiling,
    )
    return e, ex, rt


def _wait_not_ready(rt, budget):
    t0 = time.monotonic()
    while rt.ready and time.monotonic() - t0 < budget:
        time.sleep(0.005)
    return time.monotonic() - t0


class TestV082(unittest.TestCase):

    # ---- 1. CG-2: healthy slow multi-record cycle must not be called a stall ----
    def test_healthy_slow_full_cycle_does_not_trip_stalled(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b, interval=0.02, maxage=0.5)
            rt.start()
            for i in range(8):
                self.assertEqual(rt.submit(IN(SYMS[i % 3]), PF()).state.value, 'PROTECTED')
            ex.latency_seconds = 0.08          # 8 x 0.08 = 0.64s cycle > 0.5s target
            time.sleep(1.2)
            wd = rt.safety_watchdog.last_error or ''
            self.assertNotIn('STALLED', wd, f'healthy slow cycle mislabelled: {wd}')
            self.assertTrue(rt.ready, f'readiness lost on a healthy portfolio: {wd}')
            self.assertTrue(e.gate_open)
            self.assertEqual(len(ex.active_protections('TESTUSDT'))
                             + len(ex.active_protections('ALTUSDT'))
                             + len(ex.active_protections('COINUSDT')), 8)
            # Degradation must still be REPORTED, not silently tolerated.
            self.assertTrue(rt.protection_supervisor.freshness_degraded)
            rt.stop(release_lease=False)

    # ---- 2. a genuinely frozen query still closes ready/gate (N3 must survive) ----
    def test_frozen_query_still_closes_gate(self):
        class Frozen(PersistentSimulatedExchange):
            armed = False
            def protection_details_by_client_id(self, s, c):
                if self.armed and threading.current_thread().name == 'shata-protection-supervisor':
                    time.sleep(30)
                return super().protection_details_by_client_id(s, c)
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); ex = Frozen(b / 'exchange.db')
            e, _, rt = build(b, ex=ex, interval=0.02, maxage=0.3)
            rt.start(); rt.submit(IN(), PF())
            ex.armed = True
            _wait_not_ready(rt, 2.0)
            self.assertFalse(rt.ready)
            self.assertFalse(e.gate_open)     # closed synchronously by the health probe
            time.sleep(0.5)                   # give the watchdog a tick to record its reason
            self.assertIn('STALLED', rt.safety_watchdog.last_error or '')
            self.assertFalse(rt._ready)       # and to revoke the latch, not just the computed view
            rt.stop(release_lease=False)

    # ---- 3/4/5. dead supervisors still close ready/gate ----
    def test_dead_supervisors_close_gate(self):
        for which in ('protection_supervisor', 'safety_watchdog', 'supervisor'):
            with self.subTest(which=which), tempfile.TemporaryDirectory() as td:
                b = Path(td); e, ex, rt = build(b, interval=0.02, maxage=0.3, ttl=0.4)
                rt.start(); rt.submit(IN(), PF())
                sup = getattr(rt, which)
                sup._stop.set()
                if sup._thread:
                    sup._thread.join(timeout=1.0)
                _wait_not_ready(rt, 2.0)
                self.assertFalse(rt.ready, which)
                self.assertFalse(e.gate_open, which)
                rt.stop(release_lease=False)

    # ---- 6. post-trade validation is O(1) on the affected trade only ----
    def test_post_trade_validation_is_scoped_to_the_new_trade(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b, interval=5.0, maxage=5.0)
            rt.start()
            for i in range(6):
                rt.submit(IN(SYMS[i % 3]), PF())
            seen = []
            original = e.verify_protected_record
            e.verify_protected_record = lambda rec: (seen.append(rec['intent_id']), original(rec))[1]
            last = IN(SYMS[0])
            rt.submit(last, PF())
            e.verify_protected_record = original
            self.assertEqual(set(seen), {last.trade_intent_id},
                             f'post-trade scan touched {len(set(seen))} positions, expected 1')
            rt.stop(release_lease=False)

    # ---- 7. no false PROTECTED rows under the slow-cycle condition ----
    def test_no_false_protected_under_slow_cycle(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b, interval=0.02, maxage=0.5)
            rt.start()
            for i in range(8):
                rt.submit(IN(SYMS[i % 3]), PF())
            ex.latency_seconds = 0.08
            time.sleep(1.0)
            bad = []
            for r in e.ledger.protected_records():
                it = e.ledger.intent_from_payload(r['payload'])
                pid = e._protection_client_id(it, r['state'] == 'PARTIALLY_PROTECTED')
                d = ex.protection_details_by_client_id(it.symbol, pid)
                if not d or Decimal(d.base_qty) != Decimal(r['protection_expected_qty']):
                    bad.append(r['intent_id'])
            self.assertEqual(bad, [])
            rt.stop(release_lease=False)

    # ---- 8. B-4 retained: a completed side effect is never hidden ----
    def test_completed_side_effect_is_returned_even_if_health_drops(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b, interval=0.02, maxage=0.3)
            rt.start()
            it = IN()
            original = e.process
            def process_then_kill(intent, portfolio):
                sm = original(intent, portfolio)
                rt.safety_watchdog._stop.set()
                if rt.safety_watchdog._thread:
                    rt.safety_watchdog._thread.join(timeout=1.0)
                return sm
            e.process = process_then_kill
            sm = rt.submit(it, PF())          # must NOT raise
            self.assertEqual(sm.state.value, 'PROTECTED')
            self.assertFalse(rt.ready)
            rt.stop(release_lease=False)

    # ---- 9. B-5 retained: boot authority loss fails closed, never raises ----
    def test_boot_authority_loss_returns_fail_closed_report(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b, ttl=5.0)
            # Force the exact B-5 condition at the point of the contract. Note that
            # start() constructs a fresh ColdBootCoordinator, so patching rt.boot here
            # would be silently discarded - inject at issue_boot_proof instead.
            e.issue_boot_proof = lambda *a, **k: (_ for _ in ()).throw(
                BootGateClosed('Cannot issue a boot proof without a valid lease'))
            report = rt.start()               # the B-5 contract: must NOT raise
            # Fail closed, by whichever branch: authority missing at proof time is
            # reported as an unclean boot, never as an exception at the caller.
            self.assertIsNotNone(report)
            self.assertFalse(e.gate_open)
            self.assertFalse(rt._ready)
            rt.stop(release_lease=False)

    # ---- B-6: supervisory threads must not outlive their runtime ----
    def test_dropped_runtime_does_not_leak_supervisory_threads(self):
        base = threading.active_count()
        for _ in range(30):
            with tempfile.TemporaryDirectory() as td:
                b = Path(td); e, ex, rt = build(b, interval=0.02, maxage=0.5)
                rt.start()
                rt.submit(IN(), PF())
                del rt, e, ex           # caller forgets stop() on purpose
        gc.collect()
        time.sleep(0.5)
        leaked = threading.active_count() - base
        self.assertLess(leaked, 12, f'{leaked} supervisory threads leaked over 30 runtimes')


    # ---- CG-2/D-2: post-trade check must not wait behind a background cycle ----
    def test_post_trade_check_is_not_serialised_behind_the_background_cycle(self):
        """ChatGPT CG-2/D follow-up: verify_one() previously took _cycle_lock, so its
        wall-clock cost stayed tied to portfolio size even though its work was O(1)."""
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b, interval=0.01, maxage=5.0, ceiling=50.0)
            rt.start()
            for i in range(12):
                rt.submit(IN(SYMS[i % 3]), PF())
            target = e.ledger.protected_records()[0]['intent_id']
            ex.latency_seconds = 0.05          # background cycle ~= 12 x 0.05 = 0.6s
            time.sleep(0.15)                   # ensure a cycle is genuinely in flight
            worst = 0.0
            for _ in range(6):
                t0 = time.monotonic()
                rt.protection_supervisor.verify_one(target)
                worst = max(worst, time.monotonic() - t0)
                time.sleep(0.02)
            self.assertLess(worst, 0.20,
                            f'verify_one took {worst:.3f}s; one query is 0.05s, a full '
                            f'cycle is ~0.60s — it is still serialised behind the cycle')
            rt.stop(release_lease=False)

    # ---- CG-2/D-3: a stall inside the ledger read must still close the gate ----
    def test_stall_inside_ledger_read_still_closes_gate(self):
        """`healthy` no longer scans the ledger on the hot path, so prove that a freeze
        inside protected_records() is still caught by progress liveness."""
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b, interval=0.02, maxage=0.3)
            rt.start(); rt.submit(IN(), PF())
            original = e.ledger.protected_records
            def frozen():
                if threading.current_thread().name == 'shata-protection-supervisor':
                    time.sleep(30)
                return original()
            e.ledger.protected_records = frozen
            _wait_not_ready(rt, 2.0)
            self.assertFalse(rt.ready)
            self.assertFalse(e.gate_open)
            rt.stop(release_lease=False)

    # ---- readiness must stay O(1) on the gated hot path ----
    def test_readiness_check_does_not_scan_the_ledger(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b, interval=5.0, maxage=5.0, ceiling=50.0)
            rt.start()
            for i in range(12):
                rt.submit(IN(SYMS[i % 3]), PF())
            reads = []
            original = e.ledger.protected_records
            e.ledger.protected_records = lambda: (reads.append(1), original())[1]
            for _ in range(50):
                _ = rt.ready
            e.ledger.protected_records = original
            self.assertEqual(reads, [], f'{len(reads)} ledger scans for 50 readiness checks')
            rt.stop(release_lease=False)


    # ---- CG-4: foreground traffic must not mask a frozen background supervisor ----
    def test_foreground_traffic_cannot_mask_a_frozen_supervisor(self):
        """ChatGPT CG-4, run verbatim. D-2 made verify_one() advance the same liveness
        counter the watchdog reads, so a steady stream of submits kept the supervisor
        looking alive while its thread was frozen inside one call."""
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b, interval=0.02, maxage=0.3)
            rt.start()
            intent = IN()
            rt.submit(intent, PF())

            original = e.ledger.protected_records
            def freeze_protected_records_for_supervisor_thread():
                if threading.current_thread().name == 'shata-protection-supervisor':
                    time.sleep(30)
                return original()
            e.ledger.protected_records = freeze_protected_records_for_supervisor_thread

            deadline = time.monotonic() + 1.2
            while time.monotonic() < deadline:
                rt.protection_supervisor.verify_one(intent.trade_intent_id)
                time.sleep(0.05)

            self.assertFalse(rt.ready)
            self.assertFalse(e.gate_open)
            self.assertIn('STALLED', rt.safety_watchdog.last_error or '')
            rt.stop(release_lease=False)

    # ---- CG-4/5: concurrent verification of one record must not race ----
    def test_same_record_is_never_verified_concurrently(self):
        """Removing _cycle_lock in D-2 allowed the background cycle and the foreground
        path to verify the SAME record at once, where one side may write UNKNOWN.
        Striped per-record locks keep single-record verification serialised while
        leaving foreground cost bounded by one query, not by portfolio size."""
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b, interval=0.01, maxage=5.0, ceiling=50.0)
            rt.start()
            intent = IN()
            rt.submit(intent, PF())
            target = intent.trade_intent_id

            overlap = []
            inflight = {}
            guard = threading.Lock()
            original = e.verify_protected_record
            def instrumented(rec):
                rid = rec['intent_id']
                with guard:
                    if inflight.get(rid):
                        overlap.append(rid)
                    inflight[rid] = True
                try:
                    return original(rec)
                finally:
                    with guard:
                        inflight[rid] = False
            e.verify_protected_record = instrumented
            ex.latency_seconds = 0.01

            stop = threading.Event()
            def hammer():
                while not stop.is_set():
                    rt.protection_supervisor.verify_one(target)
            threads = [threading.Thread(target=hammer) for _ in range(4)]
            for t in threads:
                t.start()
            time.sleep(1.0)
            stop.set()
            for t in threads:
                t.join()
            e.verify_protected_record = original

            self.assertEqual(overlap, [], f'{len(overlap)} concurrent verifications of the same record')
            self.assertEqual(e.ledger.get(target)['state'], 'PROTECTED')
            rt.stop(release_lease=False)


if __name__ == '__main__':
    unittest.main()

=== FILE: tests/test_v08_self_attack.py ===
"""v0.8 SELF-ATTACK TESTS — LOWER EVIDENTIARY WEIGHT.

These were written by the same party that wrote the v0.8 patches (Claude, Lead
Builder). Per REVIEW_PROTOCOL.md section 5, the builder does not write the
regression proof for his own patch. This file exists so v0.8 does not ship
untested, NOT as the acceptance evidence.

Independent regression tests for N1 / N2 / N3 must be written by Gemini and/or
ChatGPT. Do not count these toward "independent attack suites passed".
"""
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from shata_trader.activity import TradingActivityStore
from shata_trader.audit import HashChainedAuditLog
from shata_trader.audit_anchor import FileAuditAnchor
from shata_trader.domain import PortfolioSnapshot, RiskPolicy
from shata_trader.execution import BootGateClosed, DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime
from shata_trader.strategy import DeterministicDemoStrategy

P = RiskPolicy(
    version=1, max_risk_per_trade_pct=Decimal('0.0075'),
    max_position_allocation_pct=Decimal('0.10'), max_portfolio_exposure_pct=Decimal('0.50'),
    min_risk_reward=Decimal('2'), max_entry_deviation_pct=Decimal('0.005'),
    max_intent_age_seconds=30, max_orders_per_hour=100, max_notional_per_day_pct=Decimal('1.0'),
)
PF = lambda: PortfolioSnapshot(Decimal('10000'), Decimal('10000'), Decimal('0'), datetime.now(timezone.utc))
IN = lambda s='TESTUSDT', q='300': DeterministicDemoStrategy().create_intent(s, Decimal('100'), Decimal(q), 1)


def build(b, ex=None, anchor=None, label='core', interval=0.02, maxage=0.10, ttl=3.0):
    ex = ex if ex is not None else PersistentSimulatedExchange(b / 'exchange.db')
    e = DemoExecutionEngine(
        ex, DeterministicRiskEngine(P), IdempotencyStore(b / 'idem.db'),
        HashChainedAuditLog(b / 'audit.jsonl', anchor=anchor),
        ledger=TradeLedger(b / 'ledger.db'), lease=SingleWriterLease(b / 'lease.db'),
        holder_id=label, activity=TradingActivityStore(b / 'activity.db'), lease_ttl_seconds=ttl,
    )
    rt = TradingCoreRuntime(e, protection_check_interval_seconds=interval, max_protection_age_seconds=maxage)
    return e, ex, rt


class TestV08SelfAttack(unittest.TestCase):

    # ---- N1 -------------------------------------------------------------
    def test_capability_cannot_be_rebound_after_a_safety_fault(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b)
            rt.start(); rt.submit(IN(), PF())
            rt._on_protection_violation('SIMULATED_SAFETY_FAULT', None)
            with self.assertRaises(BootGateClosed):
                e.bind_runtime_capability(object())
            with self.assertRaises(BootGateClosed):
                e.grant_boot_authority(object(), object())
            with self.assertRaises(BootGateClosed):
                e.process(IN('ALTUSDT', '200'), PF())
            rt.stop(release_lease=False)

    def test_boot_authority_requires_a_fresh_single_use_proof(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b)
            rt.start()
            tok = rt._boot_capability
            proof = e.issue_boot_proof(tok, 0, 0)
            e.grant_boot_authority(tok, proof)
            e.revoke_boot_authority('SAFETY')
            with self.assertRaises(BootGateClosed):
                e.grant_boot_authority(tok, proof)   # single use, and cleared on revoke
            rt.stop(release_lease=False)

    def test_boot_proof_is_refused_for_an_unclean_boot(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b)
            rt.start()
            with self.assertRaises(BootGateClosed):
                e.issue_boot_proof(rt._boot_capability, 1, 0)
            with self.assertRaises(BootGateClosed):
                e.issue_boot_proof(rt._boot_capability, 0, 2)
            rt.stop(release_lease=False)

    # ---- N2 -------------------------------------------------------------
    def test_truncated_history_is_rejected_by_witness_height(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); anch = FileAuditAnchor(b / 'ext' / 'anchor.json')
            e, ex, rt = build(b, anchor=anch, maxage=0.5)
            rt.start(); rt.submit(IN(), PF())
            lines = (b / 'audit.jsonl').read_text().splitlines()
            self.assertGreater(anch.read().get('height', 0), 1)
            rt.stop(release_lease=True)
            (b / 'audit.jsonl').write_text(lines[0] + '\n')     # history deleted
            e2, _, rt2 = build(b, ex=ex, anchor=anch, label='reboot', maxage=0.5)
            rt2.start()
            self.assertFalse(rt2.ready)
            rt2.stop(release_lease=True)

    def test_height_less_witness_is_treated_as_a_downgrade(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); anch = FileAuditAnchor(b / 'ext' / 'anchor.json')
            e, ex, rt = build(b, anchor=anch, maxage=0.5)
            rt.start(); rt.submit(IN(), PF())
            head = anch.read()['head_hash']
            rt.stop(release_lease=True)
            anch.publish(head)                                  # height stripped
            e2, _, rt2 = build(b, ex=ex, anchor=anch, label='reboot', maxage=0.5)
            rt2.start()
            self.assertFalse(rt2.ready)
            rt2.stop(release_lease=True)

    # ---- N3 -------------------------------------------------------------
    def test_watchdog_death_is_itself_detected_without_any_submit(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b)
            rt.start(); rt.submit(IN(), PF())
            wd = rt.safety_watchdog
            wd._stop.set(); wd._thread.join(timeout=1.0)
            t0 = time.monotonic()
            while rt.ready and time.monotonic() - t0 < 1.5:
                time.sleep(0.005)
            self.assertFalse(rt.ready)
            self.assertFalse(e.gate_open)
            rt.stop(release_lease=False)

    def test_all_supervisors_dead_closes_the_execution_gate(self):
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); e, ex, rt = build(b)
            rt.start(); rt.submit(IN(), PF())
            for sup in (rt.supervisor, rt.protection_supervisor, rt.safety_watchdog):
                sup._stop.set()
                if sup._thread:
                    sup._thread.join(timeout=1.0)
            self.assertFalse(rt.ready)
            self.assertFalse(e.gate_open)
            with self.assertRaises(BootGateClosed):
                e.process(IN('ALTUSDT', '200'), PF())
            rt.stop(release_lease=False)

    def test_stalled_supervisor_degrades_readiness_without_submit(self):
        class Stall(PersistentSimulatedExchange):
            armed = False
            def protection_details_by_client_id(self, s, c):
                if self.armed and threading.current_thread().name == 'shata-protection-supervisor':
                    time.sleep(30)
                return super().protection_details_by_client_id(s, c)
        with tempfile.TemporaryDirectory() as td:
            b = Path(td); ex = Stall(b / 'exchange.db')
            e, _, rt = build(b, ex=ex)
            rt.start(); rt.submit(IN(), PF())
            ex.armed = True
            t0 = time.monotonic()
            while rt.ready and time.monotonic() - t0 < 1.5:
                time.sleep(0.005)
            self.assertFalse(rt.ready)
            self.assertFalse(e.gate_open)
            rt.stop(release_lease=False)


if __name__ == '__main__':
    unittest.main()

