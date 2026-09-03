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
