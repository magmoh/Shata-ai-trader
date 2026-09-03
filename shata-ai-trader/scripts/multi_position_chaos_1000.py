from __future__ import annotations

import random
import tempfile
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys

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
from shata_trader.runtime import TradingCoreRuntime, RuntimeNotReady
from shata_trader.strategy import DeterministicDemoStrategy

RUNS = 1000
SYMBOLS = ['TESTUSDT', 'ALTUSDT', 'COINUSDT']
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


def make(base: Path, holder: str):
    ex = PersistentSimulatedExchange(base / 'exchange.db')
    eng = DemoExecutionEngine(
        ex,
        DeterministicRiskEngine(P),
        IdempotencyStore(base / 'idem.db'),
        HashChainedAuditLog(base / 'audit.jsonl'),
        ledger=TradeLedger(base / 'ledger.db'),
        lease=SingleWriterLease(base / 'lease.db'),
        holder_id=holder,
        activity=TradingActivityStore(base / 'activity.db'),
        lease_ttl_seconds=0.5,
    )
    rt = TradingCoreRuntime(
        eng,
        protection_check_interval_seconds=0.02,
        max_protection_age_seconds=0.10,
    )
    return eng, ex, rt


def expected_reservation_invariant(eng, ex, baseline):
    # Every row that claims PROTECTED must have a matching active protection with
    # exact expected quantity. When runtime is ready, unexplained free balance may
    # only be the explicitly injected pre-existing baseline.
    for rec in eng.ledger.protected_records():
        intent = eng.ledger.intent_from_payload(rec['payload'])
        partial = rec['state'] == 'PARTIALLY_PROTECTED'
        pid = eng._protection_client_id(intent, partial)
        d = ex.protection_details_by_client_id(intent.symbol, pid)
        if not d:
            return False, f"FALSE_PROTECTED_MISSING:{rec['intent_id']}"
        exp = Decimal(rec['protection_expected_qty'])
        if Decimal(d.base_qty) != exp:
            return False, f"FALSE_PROTECTED_QTY:{rec['intent_id']}:{d.base_qty}!={exp}"

    if rt_ready := getattr(eng, '_boot_verified', False):
        for sym in SYMBOLS:
            total = ex._balance(sym)
            reserved = sum((Decimal(q) for _, q in ex.active_protections(sym)), Decimal('0'))
            unexplained = total - reserved
            if unexplained != baseline[sym]:
                return False, f"READY_EXPOSURE_DRIFT:{sym}:{unexplained}!={baseline[sym]}"
    return True, ''


failures = []
states = Counter()
rng = random.Random(6062026)

for run in range(RUNS):
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        eng, ex, rt = make(base, f'run-{run}')
        baseline = {s: Decimal('0') for s in SYMBOLS}
        # Pre-existing holdings test old-balance isolation without treating them as
        # system-managed exposure.
        for s in SYMBOLS:
            if rng.random() < 0.25:
                baseline[s] = Decimal(str(rng.choice([1, 2, 5])))
                ex.external_adjust_balance(s, baseline[s])
        try:
            rep = rt.start()
            if rep.unresolved != 0 or not rt.ready:
                failures.append((run, 'BOOT_NOT_READY'))
                continue

            opened = []
            count = rng.choice([2, 3])
            for idx in range(count):
                if not rt.ready:
                    break
                sym = rng.choice(SYMBOLS)
                amt = Decimal(str(rng.choice([100, 150, 200, 250, 300])))
                it = DeterministicDemoStrategy().create_intent(sym, Decimal('100'), amt, 1)
                # Occasionally fail protection on a later trade; emergency exit must
                # not disturb already-protected positions on the same symbol.
                ex.fail_protection = idx > 0 and rng.random() < 0.08
                try:
                    sm = rt.submit(it, PF())
                    states[sm.state.value] += 1
                except RuntimeNotReady:
                    states['RUNTIME_NOT_READY'] += 1
                    break
                finally:
                    ex.fail_protection = False
                opened.append((it, sm.state.value))

            anomaly = 'none'
            active = ex.conn.execute(
                "SELECT client_id,symbol,base_qty FROM protections WHERE active=1 ORDER BY rowid"
            ).fetchall()
            if active and rt.ready:
                roll = rng.random()
                cid, sym, qty = rng.choice(active)
                if roll < 0.12:
                    ex.cancel_protection_by_client_id(sym, cid)
                    anomaly = 'cancel'
                elif roll < 0.22:
                    ex.conn.execute(
                        "UPDATE protections SET base_qty=? WHERE client_id=?",
                        (str(Decimal(qty) / Decimal('2')), cid),
                    )
                    anomaly = 'qty_mismatch'

            # Deterministic immediate check, in addition to the background loop.
            if rt.protection_supervisor:
                rt.protection_supervisor.verify_once()

            if anomaly != 'none' and rt.ready:
                failures.append((run, f'ANOMALY_NOT_HALTED:{anomaly}'))
                continue

            ok, reason = expected_reservation_invariant(eng, ex, baseline)
            if not ok:
                failures.append((run, reason))
                continue

            # Half the runs cold-boot again with all prior positions still present.
            if rng.random() < 0.5:
                was_ready = rt.ready
                rt.stop(release_lease=True)
                eng2, ex2, rt2 = make(base, f'restart-{run}')
                rep2 = rt2.start()
                if anomaly == 'none' and was_ready:
                    if rep2.unresolved != 0 or not rt2.ready:
                        failures.append((run, 'CLEAN_RESTART_NOT_READY'))
                    else:
                        ok2, reason2 = expected_reservation_invariant(eng2, ex2, baseline)
                        if not ok2:
                            failures.append((run, 'RESTART_' + reason2))
                else:
                    if rt2.ready:
                        failures.append((run, 'UNSAFE_RESTART_READY'))
                rt2.stop(release_lease=True)
            else:
                rt.stop(release_lease=True)
        except Exception as exc:
            failures.append((run, f'UNCAUGHT:{type(exc).__name__}:{exc}'))
            try:
                rt.stop(release_lease=False)
            except Exception:
                pass

print(f'MULTI-POSITION CHAOS RUNS: {RUNS}')
print(f'FAILURES: {len(failures)}')
print('STATES:', dict(states))
if failures:
    print('FIRST FAILURES:', failures[:20])
    raise SystemExit(1)
print('RESULT: PASS - multi-position, multi-symbol, out-of-band protection mutation, and restart invariants held')
