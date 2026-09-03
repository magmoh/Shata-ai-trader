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
