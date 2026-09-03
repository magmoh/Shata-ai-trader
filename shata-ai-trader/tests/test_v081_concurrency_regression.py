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
