from __future__ import annotations

import hashlib
import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from .activity import TradingActivityStore
from .audit import HashChainedAuditLog
from .domain import ExchangeOrder, PortfolioSnapshot, TradeIntent, TradeState
from .exchange import ExchangeGateway, ExchangeRejected, Maintenance, RateLimited, UnknownSubmissionState
from .fenced_gateway import FencedExchangeFacade
from .idempotency import DuplicateIntent, IdempotencyStore
from .lease import LeaseUnavailable, SingleWriterLease, StaleEpoch
from .ledger import TradeLedger
from .rate_governor import PriorityRateGovernor
from .reconciliation import ReconciliationEngine
from .risk_engine import DeterministicRiskEngine
from .state_machine import TradeStateMachine


class BootGateClosed(RuntimeError):
    pass


class _BootProof:
    """Unforgeable evidence that a clean cold boot completed for a specific epoch.

    Identity-checked, not value-checked: holding a look-alike object is useless.
    """
    __slots__ = ('epoch', 'issued_monotonic')

    def __init__(self, epoch: int, issued_monotonic: float):
        self.epoch = int(epoch)
        self.issued_monotonic = float(issued_monotonic)


def deterministic_client_order_id(intent: TradeIntent, suffix: str) -> str:
    raw = f"{intent.trade_intent_id}|{intent.strategy_id}|{intent.strategy_version}|{suffix}"
    return "shata-" + hashlib.sha256(raw.encode()).hexdigest()[:24]


class DemoExecutionEngine:
    """Deterministic demo execution core.

    v0.6: authority is a per-engine capability over the raw ledger/exchange.  A
    stale engine cannot borrow authority from another engine that shares the
    same underlying TradeLedger object.
    """

    def __init__(
        self,
        exchange: ExchangeGateway,
        risk_engine: DeterministicRiskEngine,
        idempotency: IdempotencyStore,
        audit: HashChainedAuditLog,
        ledger: TradeLedger | None = None,
        lease: SingleWriterLease | None = None,
        holder_id: str | None = None,
        activity: TradingActivityStore | None = None,
        lease_ttl_seconds: float = 3.0,
        fault_hook=None,
        rate_governor: PriorityRateGovernor | None = None,
    ):
        self.risk_engine = risk_engine
        self.idempotency = idempotency
        self.audit = audit
        self._raw_exchange = exchange
        self._raw_ledger = ledger or TradeLedger(":memory:")
        self.lease = lease or SingleWriterLease(":memory:")
        label = holder_id or "core"
        self.holder_id = f"{label}:{os.getpid()}:{uuid.uuid4().hex}"
        self.lease_ttl_seconds = float(lease_ttl_seconds)
        self.activity = activity or TradingActivityStore(":memory:")
        self.fault_hook = fault_hook
        self.rate_governor = rate_governor or PriorityRateGovernor(0.0005)
        self._boot_verified = False
        self._boot_reason = "NOT_STARTED"
        self._runtime_capability = None
        self._issued_boot_proof = None
        self._health_probe = None
        self.epoch: int | None = None
        self.exchange = None
        self.reconciliation = None
        self.ledger = self._raw_ledger
        # Best effort only.  If a dead leader still owns an unexpired lease,
        # construction remains alive and Runtime.start() manages WAITING_FOR_LEASE.
        self.acquire_authority(wait_timeout_seconds=0.0)

    def _activate_authority(self, epoch: int) -> None:
        epoch = int(epoch)
        self.epoch = epoch
        validator = lambda lease=self.lease, holder=self.holder_id, ep=epoch: lease.assert_epoch(
            "execution-core", holder, ep
        )
        self.ledger = self._raw_ledger.scoped(validator, epoch)
        self.exchange = FencedExchangeFacade(
            self._raw_exchange,
            self.lease,
            self.holder_id,
            epoch,
            rate_governor=self.rate_governor,
        )
        self.reconciliation = ReconciliationEngine(self.exchange)

    def has_authority(self) -> bool:
        if self.epoch is None:
            return False
        try:
            self.lease.assert_epoch("execution-core", self.holder_id, self.epoch)
            return True
        except Exception:
            return False

    def acquire_authority(self, wait_timeout_seconds: float = 0.0, poll_seconds: float = 0.05) -> bool:
        if self.has_authority():
            return True
        self.epoch = None
        self.exchange = None
        self.reconciliation = None
        self.ledger = self._raw_ledger
        deadline = time.monotonic() + max(0.0, float(wait_timeout_seconds))
        while True:
            try:
                epoch = self.lease.acquire(
                    "execution-core", self.holder_id, ttl_seconds=self.lease_ttl_seconds
                )
                self._activate_authority(epoch)
                return True
            except LeaseUnavailable:
                if time.monotonic() >= deadline:
                    return False
                time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))

    def release_authority(self) -> None:
        if self.epoch is not None:
            try:
                self.lease.release("execution-core", self.holder_id, self.epoch)
            except Exception:
                pass
        self.epoch = None
        self.exchange = None
        self.reconciliation = None
        self.ledger = self._raw_ledger
        self.revoke_boot_authority("AUTHORITY_RELEASED")

    def bind_runtime_capability(self, token):
        """Bind the single runtime capability. One-shot for the life of the engine.

        v0.8/N1: rebinding is refused unconditionally, including while the boot gate
        is closed.  A closed gate is exactly when a hostile or buggy component would
        try to substitute its own token, so "closed" is not a rebinding window.
        """
        if token is None:
            raise BootGateClosed("Runtime capability token required")
        if self._runtime_capability is None:
            self._runtime_capability = token
            return
        if self._runtime_capability is not token:
            raise BootGateClosed(
                "Execution engine is already bound to a runtime capability; rebinding is not permitted"
            )

    def bind_health_probe(self, token, probe):
        """Bind a live health probe consulted on every gated call.

        v0.8/N3: the gate must not be a latch that only a *running* supervisor can
        clear. If every supervisory loop dies, nobody is left to call
        revoke_boot_authority — so the gate itself asks, synchronously, whether the
        monitoring chain is still healthy. Structural, not another watcher thread.
        """
        if self._runtime_capability is None or token is not self._runtime_capability:
            raise BootGateClosed("Only the bound runtime may bind a health probe")
        self._health_probe = probe

    @property
    def gate_open(self) -> bool:
        if not self._boot_verified:
            return False
        probe = self._health_probe
        if probe is None:
            return True
        try:
            return bool(probe())
        except Exception:
            return False

    def release_runtime_capability(self, token):
        """Hand the engine back. Only the current holder may do this.

        This is what keeps one-shot binding compatible with sequential ownership:
        a runtime that has fully stopped releases the engine, and a later runtime may
        bind.  A component without the token can neither rebind nor release, so N1
        stays closed.
        """
        if self._runtime_capability is None:
            return
        if token is not self._runtime_capability:
            raise BootGateClosed("Only the bound runtime may release the runtime capability")
        self._runtime_capability = None
        self._issued_boot_proof = None
        self._health_probe = None
        self.revoke_boot_authority("RUNTIME_CAPABILITY_RELEASED")

    def issue_boot_proof(self, token, unresolved: int, quarantined: int = 0):
        """Mint a boot proof for the epoch that a clean cold boot just reconciled.

        Only the bound runtime may mint one, and only for a boot that resolved
        everything.  The proof is unforgeable by construction: it is a private object
        whose identity is checked, not a value that can be reconstructed.
        """
        if self._runtime_capability is None or token is not self._runtime_capability:
            raise BootGateClosed("Only the bound runtime may issue a boot proof")
        if not self.has_authority():
            raise BootGateClosed("Cannot issue a boot proof without a valid lease")
        if int(unresolved) != 0 or int(quarantined) != 0:
            raise BootGateClosed(
                f"Boot proof requires a clean reconciliation (unresolved={unresolved}, quarantined={quarantined})"
            )
        proof = _BootProof(epoch=int(self.epoch), issued_monotonic=time.monotonic())
        self._issued_boot_proof = proof
        return proof

    def grant_boot_authority(self, token=None, boot_proof=None):
        """Open the execution gate. Requires the bound capability AND a fresh boot proof."""
        if self._runtime_capability is None or token is not self._runtime_capability:
            raise BootGateClosed("Only the bound runtime may grant boot authority")
        if not self.has_authority():
            raise BootGateClosed("Cannot grant boot authority without a valid lease")
        if boot_proof is None or boot_proof is not self._issued_boot_proof:
            raise BootGateClosed("Boot authority requires the boot proof issued by this engine")
        if boot_proof.epoch != int(self.epoch):
            raise BootGateClosed(
                f"Boot proof epoch {boot_proof.epoch} does not match current epoch {self.epoch}"
            )
        # A proof is single-use: it cannot reopen the gate after a later safety fault.
        self._issued_boot_proof = None
        self._boot_verified = True
        self._boot_reason = "READY"

    def revoke_boot_authority(self, reason="REVOKED"):
        self._boot_verified = False
        self._boot_reason = reason
        # N1: a revoked gate must not be reopenable with a previously minted proof.
        self._issued_boot_proof = None

    def _hook(self, name):
        if self.fault_hook:
            self.fault_hook(name)

    def _sm(self, intent, initial=TradeState.CREATED):
        def persist(old, new):
            self.ledger.transition(intent.trade_intent_id, old.value, new.value)

        return TradeStateMachine(initial, on_transition=persist)

    def _recovery_result(self, intent, state: TradeState, error=None):
        self.ledger.recovery_set_state(intent.trade_intent_id, state.value, error)
        return TradeStateMachine(state)

    @staticmethod
    def _expected_net_trade_qty(order: ExchangeOrder, symbol: str) -> Decimal:
        qty = Decimal(order.filled_base_qty)
        base = symbol[:-4] if symbol.endswith("USDT") else symbol.split("/")[0]
        if order.commission_asset == base:
            qty -= Decimal(order.commission_amount)
        return max(Decimal("0"), qty)

    def _available_trade_qty(self, order: ExchangeOrder, symbol: str) -> tuple[Decimal, Decimal]:
        expected = self._expected_net_trade_qty(order, symbol)
        free = Decimal(self.exchange.get_free_base_balance(symbol))
        return expected, max(Decimal("0"), min(expected, free))

    # Compatibility name retained; callers that need safety must also inspect expected.
    def _net_trade_qty(self, order: ExchangeOrder, symbol: str) -> Decimal:
        return self._available_trade_qty(order, symbol)[1]

    def _protection_client_id(self, intent: TradeIntent, partial: bool) -> str:
        return deterministic_client_order_id(
            intent, "partial-protection" if partial else "protection"
        )

    def _protect_order_qty(self, intent, order, sm, partial: bool):
        pending = (
            TradeState.PARTIAL_PROTECTION_PENDING if partial else TradeState.PROTECTION_PENDING
        )
        protected = TradeState.PARTIALLY_PROTECTED if partial else TradeState.PROTECTED
        sm.transition(pending)
        try:
            expected, qty = self._available_trade_qty(order, intent.symbol)
        except Exception as exc:
            sm.transition(TradeState.PROTECTION_FAILED)
            self.activity.record_error()
            self.audit.append("PROTECTION_QTY_UNKNOWN", {"error": type(exc).__name__})
            return "failed"
        if expected <= 0 or qty <= 0:
            sm.transition(TradeState.PROTECTION_FAILED)
            self.activity.record_error()
            self.audit.append(
                "PROTECTION_ZERO_QTY", {"expected_qty": str(expected), "available_qty": str(qty)}
            )
            return "failed"

        pid = self._protection_client_id(intent, partial)
        self.audit.append(
            "PROTECTION_INTENT_PREPARED",
            {
                "trade_intent_id": intent.trade_intent_id,
                "client_order_id": pid,
                "expected_qty": str(expected),
                "requested_protection_qty": str(qty),
                "epoch": self.epoch,
            },
        )
        try:
            self.exchange.place_protection(
                intent.symbol, qty, intent.stop_price, intent.take_profit_price, pid
            )
            self._hook("AFTER_PROTECTION_SUBMIT_BEFORE_VERIFY")
        except Exception as exc:
            sm.transition(TradeState.PROTECTION_FAILED)
            self.activity.record_error()
            self.audit.append("PROTECTION_FAILED", {"error": type(exc).__name__})
            return "failed"

        try:
            details = self.exchange.protection_details_by_client_id(intent.symbol, pid)
        except Exception as exc:
            sm.transition(TradeState.UNKNOWN)
            self.activity.record_error()
            self.audit.append("PROTECTION_VERIFY_UNKNOWN", {"error": type(exc).__name__})
            return "unknown"
        if not details:
            sm.transition(TradeState.PROTECTION_FAILED)
            return "failed"

        actual = Decimal(details.base_qty)
        self.ledger.mark_protection_verified(intent.trade_intent_id, expected, actual)
        if actual != expected:
            sm.transition(TradeState.UNDER_PROTECTED)
            self.activity.record_error()
            self.audit.append(
                "PROTECTION_QUANTITY_MISMATCH",
                {
                    "trade_intent_id": intent.trade_intent_id,
                    "expected_qty": str(expected),
                    "actual_qty": str(actual),
                    "shortfall": str(max(Decimal("0"), expected - actual)),
                },
            )
            return "under"

        sm.transition(protected)
        self.audit.append(
            "POSITION_PROTECTED",
            {
                "trade_intent_id": intent.trade_intent_id,
                "base_qty": str(actual),
                "verified_at": datetime.now(timezone.utc).isoformat(),
                "epoch": self.epoch,
            },
        )
        return "protected"

    def _emergency_exit(self, intent, order, sm):
        if not self.risk_engine.policy.emergency_exit_on_unprotected_new_entry:
            if sm.state != TradeState.HALTED:
                sm.transition(TradeState.HALTED)
            return
        try:
            expected, qty = self._available_trade_qty(order, intent.symbol)
        except Exception:
            if sm.state != TradeState.UNKNOWN:
                sm.transition(TradeState.UNKNOWN)
            return
        if qty <= 0:
            if sm.state != TradeState.UNKNOWN:
                sm.transition(TradeState.UNKNOWN)
            return
        if sm.state != TradeState.EMERGENCY_EXIT:
            sm.transition(TradeState.EMERGENCY_EXIT)
        cid = deterministic_client_order_id(intent, "emergency-exit")
        self.audit.append(
            "EMERGENCY_EXIT_PREPARED",
            {
                "client_order_id": cid,
                "expected_qty": str(expected),
                "sellable_qty": str(qty),
                "epoch": self.epoch,
            },
        )
        try:
            self.exchange.emergency_market_sell(intent.symbol, qty, cid)
            if qty == expected:
                sm.transition(TradeState.CLOSED)
            else:
                sm.transition(TradeState.UNKNOWN)
                self.audit.append(
                    "EMERGENCY_EXIT_SHORTFALL",
                    {"expected_qty": str(expected), "sold_qty": str(qty)},
                )
        except Exception as exc:
            sm.transition(TradeState.UNKNOWN)
            self.activity.record_error()
            self.audit.append("EMERGENCY_EXIT_UNKNOWN", {"error": type(exc).__name__})

    def _activity_gate(self, intent, portfolio):
        p = self.risk_engine.policy
        if self.activity.orders_last_hour() >= p.max_orders_per_hour:
            return False, "Hourly order cap reached"
        if (
            self.activity.notional_today() + intent.quote_amount
            > portfolio.portfolio_value * p.max_notional_per_day_pct
        ):
            return False, "Daily notional cap reached"
        if self.activity.consecutive_errors() >= p.max_consecutive_execution_errors:
            return False, "Consecutive execution error cap reached"
        return True, "PASS"

    def process(self, intent: TradeIntent, portfolio: PortfolioSnapshot) -> TradeStateMachine:
        # v0.8.2/B-6: authority can lapse at ANY point of this method, not only around the
        # dispatch window that already handles StaleEpoch. An escaping StaleEpoch is an
        # uncaught exception out of the public entry point, so contain it here and fail
        # closed: the gate is shut and the caller gets a definite error, not a traceback.
        try:
            return self._process(intent, portfolio)
        except StaleEpoch as exc:
            self.revoke_boot_authority(f'AUTHORITY_LOST_DURING_PROCESS:{exc}')
            try:
                self.audit.append('EXECUTION_AUTHORITY_LOST',
                                  {'trade_intent_id': intent.trade_intent_id, 'reason': str(exc)})
            except Exception:
                pass
            raise BootGateClosed(f'Execution authority lost during processing: {exc}') from exc

    def _process(self, intent: TradeIntent, portfolio: PortfolioSnapshot) -> TradeStateMachine:
        if not self.gate_open:
            raise BootGateClosed(f"Runtime cold-boot gate closed: {self._boot_reason}")
        if not self.has_authority():
            self.revoke_boot_authority("EXECUTION_AUTHORITY_LOST")
            raise BootGateClosed("Execution authority lost")

        entry_id = deterministic_client_order_id(intent, "entry")
        self.ledger.ensure(intent, entry_id, self.epoch)
        try:
            self.idempotency.claim(intent.trade_intent_id)
        except DuplicateIntent:
            return self.recover_intent(intent)
        sm = self._sm(intent, TradeState.CREATED)
        self.audit.append(
            "INTENT_CLAIMED",
            {
                "trade_intent_id": intent.trade_intent_id,
                "entry_client_order_id": entry_id,
                "epoch": self.epoch,
            },
        )
        try:
            price = self.exchange.get_market_price(intent.symbol)
        except Exception as exc:
            sm.transition(TradeState.HALTED)
            self.audit.append("MARKET_READ_FAILED", {"error": type(exc).__name__})
            return sm
        ok, reason = self._activity_gate(intent, portfolio)
        if not ok:
            sm.transition(TradeState.REJECTED)
            self.audit.append("ACTIVITY_GATE_REJECT", {"reason": reason})
            return sm
        dec = self.risk_engine.evaluate(intent, portfolio, price)
        if not dec.approved:
            sm.transition(TradeState.REJECTED)
            return sm
        sm.transition(TradeState.RISK_APPROVED)
        self.ledger.mark_dispatch_prepared(intent.trade_intent_id, self.epoch)
        sm.state = TradeState.SUBMITTED
        sm.history.append(TradeState.SUBMITTED)
        self._hook("AFTER_WAL_BEFORE_SUBMIT")
        self.audit.append(
            "ENTRY_SUBMITTING",
            {
                "trade_intent_id": intent.trade_intent_id,
                "client_order_id": entry_id,
                "epoch": self.epoch,
            },
        )
        self.activity.record_submission(intent.quote_amount)
        order = None
        try:
            order = self.exchange.submit_market_buy(intent.symbol, intent.quote_amount, entry_id)
            self._hook("AFTER_SUBMIT_BEFORE_RECONCILE")
        except UnknownSubmissionState:
            sm.transition(TradeState.UNKNOWN)
            return self._reconcile_after_submit(intent, sm, entry_id, None)
        except StaleEpoch as exc:
            self.revoke_boot_authority("STALE_EPOCH_DURING_DISPATCH")
            self.activity.record_error()
            self.audit.append("LEASE_LOST_DURING_DISPATCH", {"error": type(exc).__name__})
            return TradeStateMachine(TradeState.UNKNOWN)
        except (ExchangeRejected, Maintenance, RateLimited) as exc:
            self.activity.record_error()
            sm.transition(TradeState.UNKNOWN)
            self.audit.append("SUBMIT_UNKNOWN_OR_REJECTED", {"error": type(exc).__name__})
            return sm
        except Exception as exc:
            self.activity.record_error()
            sm.transition(TradeState.UNKNOWN)
            self.audit.append("SUBMIT_UNEXPECTED_UNKNOWN", {"error": type(exc).__name__})
            return sm
        return self._reconcile_after_submit(intent, sm, entry_id, order)

    def _reconcile_after_submit(self, intent, sm, entry_id, fallback_order):
        try:
            rec = self.reconciliation.reconcile_order(intent.symbol, entry_id)
            order = rec.order if rec.found else None
        except Exception as exc:
            if sm.state != TradeState.UNKNOWN:
                sm.transition(TradeState.UNKNOWN)
            self.activity.record_error()
            self.audit.append(
                "POST_SUBMIT_RECONCILE_FAILED",
                {
                    "error": type(exc).__name__,
                    "exposure_estimate": str(fallback_order.filled_base_qty)
                    if fallback_order
                    else None,
                },
            )
            return sm
        if order is None:
            if sm.state != TradeState.UNKNOWN:
                sm.transition(TradeState.UNKNOWN)
            return sm
        if sm.state == TradeState.UNKNOWN:
            sm.transition(TradeState.RECONCILING)
        if sm.state in {TradeState.SUBMITTED, TradeState.RECONCILING}:
            sm.transition(TradeState.ACKNOWLEDGED)
        if order.status == "PARTIALLY_FILLED":
            if sm.state == TradeState.ACKNOWLEDGED:
                sm.transition(TradeState.PARTIALLY_FILLED)
            try:
                self.exchange.cancel_remainder(intent.symbol, entry_id)
            except Exception as exc:
                if sm.state != TradeState.UNKNOWN:
                    sm.transition(TradeState.UNKNOWN)
                self.audit.append("CANCEL_REMAINDER_UNKNOWN", {"error": type(exc).__name__})
                return sm
            result = self._protect_order_qty(intent, order, sm, True)
            if result == "protected":
                sm.transition(TradeState.HALTED)
            elif result == "failed":
                self._emergency_exit(intent, order, sm)
            return sm
        if order.status == "FILLED":
            if sm.state == TradeState.ACKNOWLEDGED:
                sm.transition(TradeState.FILLED)
            result = self._protect_order_qty(intent, order, sm, False)
            if result == "failed":
                self._emergency_exit(intent, order, sm)
            return sm
        if order.status == "CANCELED":
            if sm.state == TradeState.ACKNOWLEDGED:
                sm.transition(TradeState.CANCELED)
            return sm
        if order.status == "EXPIRED":
            if sm.state == TradeState.ACKNOWLEDGED:
                sm.transition(TradeState.EXPIRED)
            return sm
        if sm.state != TradeState.UNKNOWN:
            sm.transition(TradeState.UNKNOWN)
        return sm

    def _recovery_protect(self, intent, order, partial: bool):
        pid = self._protection_client_id(intent, partial)
        expected = self._expected_net_trade_qty(order, intent.symbol)
        try:
            details = self.exchange.protection_details_by_client_id(intent.symbol, pid)
        except Exception:
            return None
        if details:
            actual = Decimal(details.base_qty)
            self.ledger.mark_protection_verified(intent.trade_intent_id, expected, actual)
            if actual != expected:
                return TradeState.UNDER_PROTECTED
            return TradeState.PARTIALLY_PROTECTED if partial else TradeState.PROTECTED
        try:
            _, qty = self._available_trade_qty(order, intent.symbol)
            if qty <= 0:
                return None
            self.exchange.place_protection(
                intent.symbol, qty, intent.stop_price, intent.take_profit_price, pid
            )
            details = self.exchange.protection_details_by_client_id(intent.symbol, pid)
            if details:
                actual = Decimal(details.base_qty)
                self.ledger.mark_protection_verified(intent.trade_intent_id, expected, actual)
                if actual != expected:
                    return TradeState.UNDER_PROTECTED
                return TradeState.PARTIALLY_PROTECTED if partial else TradeState.PROTECTED
        except Exception:
            return TradeState.PROTECTION_FAILED
        return None

    def _expected_qty_for_record(self, intent, rec) -> Decimal | None:
        if rec.get("protection_expected_qty") is not None:
            return Decimal(rec["protection_expected_qty"])
        try:
            x = self.reconciliation.reconcile_order(intent.symbol, rec["entry_client_order_id"])
        except Exception:
            return None
        if not x.found or x.order is None:
            return None
        return self._expected_net_trade_qty(x.order, intent.symbol)

    def verify_protected_record(self, rec) -> bool | None:
        """Revalidate an already-protected durable row against exchange truth.

        True = verified, False = definite missing/mismatch and durable state was
        downgraded, None = query uncertainty (caller applies verification-age policy).
        """
        try:
            intent = self.ledger.intent_from_payload(rec["payload"])
            state = TradeState(rec["state"])
        except Exception:
            return False
        if state not in {TradeState.PROTECTED, TradeState.PARTIALLY_PROTECTED}:
            return True
        partial = state == TradeState.PARTIALLY_PROTECTED
        pid = self._protection_client_id(intent, partial)
        expected = self._expected_qty_for_record(intent, rec)
        if expected is None:
            return None
        try:
            details = self.exchange.protection_details_by_client_id(intent.symbol, pid)
        except Exception as exc:
            try:
                self.ledger.mark_error(
                    intent.trade_intent_id, f"PROTECTION_REVERIFY_QUERY_FAILED:{type(exc).__name__}"
                )
            except Exception:
                pass
            return None
        if not details:
            self.ledger.clear_protection_verification(
                intent.trade_intent_id, "EXPECTED_PROTECTION_MISSING"
            )
            self.ledger.recovery_set_state(
                intent.trade_intent_id, TradeState.UNKNOWN.value, "EXPECTED_PROTECTION_MISSING"
            )
            self.audit.append(
                "PROTECTION_LOST_IN_SESSION", {"trade_intent_id": intent.trade_intent_id}
            )
            return False
        actual = Decimal(details.base_qty)
        self.ledger.mark_protection_verified(intent.trade_intent_id, expected, actual)
        if actual != expected:
            self.ledger.recovery_set_state(
                intent.trade_intent_id,
                TradeState.UNDER_PROTECTED.value,
                "PROTECTION_QUANTITY_MISMATCH",
            )
            self.audit.append(
                "PROTECTION_MISMATCH_IN_SESSION",
                {
                    "trade_intent_id": intent.trade_intent_id,
                    "expected_qty": str(expected),
                    "actual_qty": str(actual),
                },
            )
            return False
        return True

    def recover_intent(self, intent, strict: bool = True):
        rec = self.ledger.get(intent.trade_intent_id)
        if not rec:
            return TradeStateMachine(TradeState.UNKNOWN)
        try:
            state = TradeState(rec["state"])
        except Exception:
            return self._recovery_result(intent, TradeState.UNKNOWN, "UNKNOWN_DURABLE_STATE")
        if state in {
            TradeState.CLOSED,
            TradeState.REJECTED,
            TradeState.CANCELED,
            TradeState.EXPIRED,
        }:
            return TradeStateMachine(state)
        if not rec["side_effect_prepared"]:
            if state in {
                TradeState.CREATED,
                TradeState.RISK_APPROVED,
                TradeState.HALTED,
                TradeState.UNKNOWN,
            }:
                return self._recovery_result(
                    intent, TradeState.REJECTED, "RECOVERY_NO_SIDE_EFFECT_WAL"
                )
            return self._recovery_result(
                intent, TradeState.UNKNOWN, "INVARIANT_NO_SIDE_EFFECT_FLAG"
            )

        if state in {TradeState.PROTECTED, TradeState.PARTIALLY_PROTECTED}:
            partial = state == TradeState.PARTIALLY_PROTECTED
            pid = self._protection_client_id(intent, partial)
            expected = self._expected_qty_for_record(intent, rec)
            if expected is None:
                return (
                    self._recovery_result(intent, TradeState.UNKNOWN, "PROTECTION_EXPECTED_QTY_UNKNOWN")
                    if strict
                    else TradeStateMachine(state)
                )
            try:
                details = self.exchange.protection_details_by_client_id(intent.symbol, pid)
            except Exception as exc:
                try:
                    self.ledger.mark_error(
                        intent.trade_intent_id, f"PROTECTION_QUERY_FAILED:{type(exc).__name__}"
                    )
                except Exception:
                    pass
                return (
                    self._recovery_result(intent, TradeState.UNKNOWN, "PROTECTION_QUERY_FAILED")
                    if strict
                    else TradeStateMachine(state)
                )
            if not details:
                return self._recovery_result(
                    intent, TradeState.UNKNOWN, "EXPECTED_PROTECTION_MISSING"
                )
            actual = Decimal(details.base_qty)
            self.ledger.mark_protection_verified(intent.trade_intent_id, expected, actual)
            if actual != expected:
                return self._recovery_result(
                    intent, TradeState.UNDER_PROTECTED, "PROTECTION_QUANTITY_MISMATCH"
                )
            return TradeStateMachine(state)

        if state == TradeState.UNDER_PROTECTED:
            return self._recovery_result(
                intent, TradeState.UNDER_PROTECTED, "UNDER_PROTECTED_REQUIRES_INTERVENTION"
            )

        if state in {TradeState.PROTECTION_PENDING, TradeState.PARTIAL_PROTECTION_PENDING}:
            partial = state == TradeState.PARTIAL_PROTECTION_PENDING
            pid = self._protection_client_id(intent, partial)
            try:
                details = self.exchange.protection_details_by_client_id(intent.symbol, pid)
            except Exception:
                return self._recovery_result(intent, TradeState.UNKNOWN, "PROTECTION_QUERY_FAILED")
            if details:
                try:
                    x = self.reconciliation.reconcile_order(
                        intent.symbol, rec["entry_client_order_id"]
                    )
                except Exception:
                    return self._recovery_result(intent, TradeState.UNKNOWN, "ENTRY_QUERY_FAILED")
                if not x.found or x.order is None:
                    return self._recovery_result(intent, TradeState.UNKNOWN, "ENTRY_NOT_VISIBLE")
                expected = self._expected_net_trade_qty(x.order, intent.symbol)
                actual = Decimal(details.base_qty)
                self.ledger.mark_protection_verified(intent.trade_intent_id, expected, actual)
                target = (
                    TradeState.UNDER_PROTECTED
                    if actual != expected
                    else (TradeState.PARTIALLY_PROTECTED if partial else TradeState.PROTECTED)
                )
                return self._recovery_result(intent, target)
            try:
                x = self.reconciliation.reconcile_order(
                    intent.symbol, rec["entry_client_order_id"]
                )
            except Exception:
                return self._recovery_result(intent, TradeState.UNKNOWN, "ENTRY_QUERY_FAILED")
            if not x.found or x.order is None:
                return self._recovery_result(intent, TradeState.UNKNOWN, "ENTRY_NOT_VISIBLE")
            target = self._recovery_protect(intent, x.order, partial)
            return self._recovery_result(
                intent, target or TradeState.UNKNOWN, "PROTECTION_RECOVERY_UNRESOLVED"
            )

        if state == TradeState.EMERGENCY_EXIT:
            cid = deterministic_client_order_id(intent, "emergency-exit")
            try:
                x = self.reconciliation.reconcile_order(intent.symbol, cid)
            except Exception:
                return self._recovery_result(intent, TradeState.UNKNOWN, "EXIT_QUERY_FAILED")
            if x.found and x.order and x.order.status == "FILLED":
                return self._recovery_result(intent, TradeState.CLOSED)
            return self._recovery_result(
                intent, TradeState.UNKNOWN, "EMERGENCY_EXIT_UNRESOLVED"
            )

        if state == TradeState.EXIT_PENDING:
            return self._recovery_result(
                intent, TradeState.UNKNOWN, "EXIT_PENDING_REQUIRES_EXIT_LEDGER"
            )

        try:
            x = self.reconciliation.reconcile_order(intent.symbol, rec["entry_client_order_id"])
        except Exception:
            return self._recovery_result(intent, TradeState.UNKNOWN, "ENTRY_QUERY_FAILED")
        if not x.found or x.order is None:
            return self._recovery_result(intent, TradeState.UNKNOWN, "ENTRY_NOT_VISIBLE")
        order = x.order
        if order.status == "CANCELED":
            return self._recovery_result(
                intent, TradeState.CANCELED, "EXCHANGE_CONFIRMED_CANCELED"
            )
        if order.status == "EXPIRED":
            return self._recovery_result(
                intent, TradeState.EXPIRED, "EXCHANGE_CONFIRMED_EXPIRED"
            )
        if order.status == "PARTIALLY_FILLED":
            try:
                self.exchange.cancel_remainder(intent.symbol, rec["entry_client_order_id"])
            except Exception:
                return self._recovery_result(
                    intent, TradeState.UNKNOWN, "CANCEL_REMAINDER_UNKNOWN"
                )
            target = self._recovery_protect(intent, order, True)
            return self._recovery_result(
                intent, target or TradeState.UNKNOWN, "PARTIAL_RECOVERY_UNRESOLVED"
            )
        if order.status == "FILLED":
            target = self._recovery_protect(intent, order, False)
            return self._recovery_result(
                intent, target or TradeState.UNKNOWN, "FULL_RECOVERY_UNRESOLVED"
            )
        return self._recovery_result(
            intent, TradeState.UNKNOWN, "UNRECOGNIZED_EXCHANGE_STATUS"
        )
