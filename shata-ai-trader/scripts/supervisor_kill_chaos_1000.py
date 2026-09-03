"""Supervisory thread-failure chaos.

v0.8 item 4. This is the harness that turns N3 from "a bug we patched" into a
permanent invariant.

Invariant under test:

    If ANY critical supervisory loop dies or stops making progress, runtime
    readiness must become False within a bounded window, WITHOUT any submit()
    and WITHOUT any manual verify_once() call.

The manual-call exclusion is the whole point: calling verify_once() by hand is
exactly what hid N3 in the v0.7 chaos suites. Here the only thing the harness does
after injecting the fault is poll `rt.ready` and wait.

Faults injected (one per run, chosen at random):
  kill_lease        - lease supervisor thread gone
  kill_protection   - protection supervisor thread gone
  kill_watchdog     - runtime safety watchdog thread gone
  stall_protection  - protection loop alive but blocked in exchange I/O
  stall_lease       - renewal loop alive but blocked in lease I/O
  kill_all_but_one  - only one supervisory loop survives
"""
from __future__ import annotations

import random
import sys
import tempfile
import threading
import time
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

RUNS = 1000
SYMBOLS = ['TESTUSDT', 'ALTUSDT', 'COINUSDT']

INTERVAL = 0.02          # supervisory loop period
MAX_AGE = 0.10           # freshness deadline the system promises
LEASE_TTL = 0.30
DETECT_BUDGET = 1.50     # generous multiple of the promised deadline

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
PF = lambda: PortfolioSnapshot(
    Decimal('10000'), Decimal('10000'), Decimal('0'), datetime.now(timezone.utc)
)

FAULTS = [
    'kill_lease',
    'kill_protection',
    'kill_watchdog',
    'stall_protection',
    'stall_lease',
    'kill_all_but_one',
]


class StallableExchange(PersistentSimulatedExchange):
    """Blocks a named thread inside exchange I/O, leaving it alive but frozen."""

    stall_thread_name = None
    stall_seconds = 30.0

    def _maybe_stall(self):
        name = self.stall_thread_name
        if name and threading.current_thread().name == name:
            time.sleep(self.stall_seconds)

    def protection_details_by_client_id(self, symbol, client_order_id):
        self._maybe_stall()
        return super().protection_details_by_client_id(symbol, client_order_id)


class StallableLease(SingleWriterLease):
    """Blocks the renewal thread inside lease I/O."""

    stall_renew = False

    def renew(self, *a, **k):
        if self.stall_renew and threading.current_thread().name == 'shata-lease-supervisor':
            time.sleep(30.0)
        return super().renew(*a, **k)


def make(base: Path, holder: str):
    ex = StallableExchange(base / 'exchange.db')
    lease = StallableLease(base / 'lease.db')
    eng = DemoExecutionEngine(
        ex,
        DeterministicRiskEngine(P),
        IdempotencyStore(base / 'idem.db'),
        HashChainedAuditLog(base / 'audit.jsonl'),
        ledger=TradeLedger(base / 'ledger.db'),
        lease=lease,
        holder_id=holder,
        activity=TradingActivityStore(base / 'activity.db'),
        lease_ttl_seconds=LEASE_TTL,
    )
    rt = TradingCoreRuntime(
        eng,
        protection_check_interval_seconds=INTERVAL,
        max_protection_age_seconds=MAX_AGE,
    )
    return eng, ex, lease, rt


def kill_thread(sup):
    """Simulate a supervisory thread dying: stop the loop, leave the object in place."""
    sup._stop.set()
    t = getattr(sup, '_thread', None)
    if t:
        t.join(timeout=1.0)


failures = []
faults_seen = Counter()
latencies = []
rng = random.Random(80820260)

for run in range(RUNS):
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        eng, ex, lease, rt = make(base, f'kill-run-{run}')
        try:
            rep = rt.start()
            if rep.unresolved != 0 or not rt.ready:
                failures.append((run, 'BOOT_NOT_READY'))
                continue

            # Open one position so there is a durable PROTECTED claim to police.
            sym = rng.choice(SYMBOLS)
            it = DeterministicDemoStrategy().create_intent(
                sym, Decimal('100'), Decimal(str(rng.choice([150, 200, 300]))), 1
            )
            rt.submit(it, PF())
            if not rt.ready:
                failures.append((run, 'NOT_READY_AFTER_CLEAN_SUBMIT'))
                continue

            fault = rng.choice(FAULTS)
            faults_seen[fault] += 1

            # ---- inject; from here on: no submit(), no verify_once() ----------
            if fault == 'kill_lease':
                kill_thread(rt.supervisor)
            elif fault == 'kill_protection':
                kill_thread(rt.protection_supervisor)
            elif fault == 'kill_watchdog':
                kill_thread(rt.safety_watchdog)
            elif fault == 'stall_protection':
                ex.stall_thread_name = 'shata-protection-supervisor'
            elif fault == 'stall_lease':
                lease.stall_renew = True
            elif fault == 'kill_all_but_one':
                survivors = [rt.supervisor, rt.protection_supervisor, rt.safety_watchdog]
                keep = rng.randrange(3)
                for i, s in enumerate(survivors):
                    if i != keep:
                        kill_thread(s)

            t0 = time.monotonic()
            deadline = t0 + DETECT_BUDGET
            while rt.ready and time.monotonic() < deadline:
                time.sleep(0.005)
            latency = time.monotonic() - t0

            if rt.ready:
                failures.append((run, f'READY_AFTER_{fault.upper()}:{latency:.3f}s'))
                continue
            latencies.append(latency)

            # A degraded runtime must also refuse new work.
            try:
                rt.submit(
                    DeterministicDemoStrategy().create_intent(sym, Decimal('100'), Decimal('150'), 1),
                    PF(),
                )
                failures.append((run, f'SUBMIT_ACCEPTED_AFTER_{fault.upper()}'))
                continue
            except Exception:
                pass

            # The execution gate itself must be shut, not just the readiness flag.
            # gate_open is what engine.process() actually consults.
            if eng.gate_open:
                failures.append((run, f'GATE_STILL_OPEN_AFTER_{fault.upper()}'))
                continue
        except Exception as exc:
            failures.append((run, f'UNCAUGHT:{type(exc).__name__}:{exc}'))
        finally:
            try:
                ex.stall_thread_name = None
                lease.stall_renew = False
                rt.stop(release_lease=False)
            except Exception:
                pass

print(f'SUPERVISOR KILL/STALL CHAOS RUNS: {RUNS}')
print(f'FAILURES: {len(failures)}')
print('FAULTS:', dict(faults_seen))
if latencies:
    print(
        f'DETECTION LATENCY: max={max(latencies):.3f}s '
        f'mean={sum(latencies)/len(latencies):.3f}s budget={DETECT_BUDGET}s'
    )
if failures:
    print('FIRST FAILURES:', failures[:20])
    raise SystemExit(1)
print(
    'RESULT: PASS - every supervisory death/stall degraded readiness within budget '
    'with no submit() and no manual verify_once()'
)
