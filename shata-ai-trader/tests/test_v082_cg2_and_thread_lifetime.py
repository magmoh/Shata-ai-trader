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
