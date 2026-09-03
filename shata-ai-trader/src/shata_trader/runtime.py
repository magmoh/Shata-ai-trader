from __future__ import annotations

import threading
import weakref

from .cold_boot import ColdBootCoordinator
from .execution import BootGateClosed
from .events import OrderEventStore, ExchangeEvent
from .lease_supervisor import LeaseSupervisor
from .protection_supervisor import ProtectionSupervisor
from .runtime_watchdog import RuntimeSafetyWatchdog


def _runtime_health_probe(runtime):
    """Weak health probe.

    v0.8.2/B-6: a strong `lambda: self.ready` made the engine hold the runtime, the
    supervisors hold the engine, and the live threads hold the supervisors — an
    unbreakable cycle that kept every dropped runtime (and its three threads) alive.
    A dead runtime must read as unhealthy, not keep itself running.
    """
    ref=weakref.ref(runtime)
    def probe():
        rt=ref()
        return False if rt is None else bool(rt.ready)
    return probe


class RuntimeNotReady(RuntimeError):
    pass


class TradingCoreRuntime:
    """Supported single-writer entry point for deterministic trading-core work."""

    def __init__(self,engine,event_store=None,protection_check_interval_seconds:float=0.25,max_protection_age_seconds:float=1.0,authority_wait_timeout_seconds:float|None=None,protection_freshness_ceiling_seconds:float|None=None):
        self.engine=engine
        self.events=event_store or OrderEventStore(':memory:')
        self._ready=False
        self.protection_check_interval_seconds=float(protection_check_interval_seconds)
        self.max_protection_age_seconds=float(max_protection_age_seconds)
        # CG-2: hard bound beyond which slow-but-healthy becomes genuinely unsafe.
        self.protection_freshness_ceiling_seconds=(float(protection_freshness_ceiling_seconds)
            if protection_freshness_ceiling_seconds is not None
            else self.max_protection_age_seconds*10.0)
        self.authority_wait_timeout_seconds=(min(max(engine.lease_ttl_seconds+0.25,0.5),5.0) if authority_wait_timeout_seconds is None else max(0.0,float(authority_wait_timeout_seconds)))
        self.boot=ColdBootCoordinator(engine)
        self.supervisor=None;self.protection_supervisor=None;self.safety_watchdog=None
        self._finalizer=None
        self._submit_lock=threading.RLock()
        self._boot_capability=object()
        self.engine.bind_runtime_capability(self._boot_capability)

    @staticmethod
    def _shutdown_supervisors(lease_sup,protection_sup,watchdog):
        for sup in (watchdog,protection_sup,lease_sup):
            if sup is None:continue
            try:
                sup.stop(release=False) if hasattr(sup,'lost') else sup.stop()
            except Exception:
                try:sup._stop.set()
                except Exception:pass

    @property
    def ready(self):
        """Computed liveness: latched boot result AND every supervisory loop healthy.

        v0.8/N3: a passive reader (status endpoint, future mobile client, chaos
        harness) sees the truth without needing a submit() to force a health check.
        """
        if not self._ready:
            return False
        ps=self.protection_supervisor
        if ps is None or not ps.healthy:
            return False
        wd=self.safety_watchdog
        if wd is None or not wd.alive:
            return False
        sup=self.supervisor
        if sup is None or not sup.healthy:
            return False
        try:
            if self.engine.audit.anchor_degraded:
                return False
        except Exception:
            return False
        return True

    @ready.setter
    def ready(self,value):
        self._ready=bool(value)

    def _new_supervisors(self):
        if self.engine.epoch is None:
            self.supervisor=None;self.protection_supervisor=None;self.safety_watchdog=None;return
        self.supervisor=LeaseSupervisor(self.engine.lease,self.engine.holder_id,self.engine.epoch,self.engine.lease_ttl_seconds,on_loss=self._on_lease_loss)
        self.protection_supervisor=ProtectionSupervisor(self.engine,interval_seconds=self.protection_check_interval_seconds,max_verification_age_seconds=self.max_protection_age_seconds,on_violation=self._on_protection_violation,freshness_ceiling_seconds=self.protection_freshness_ceiling_seconds)
        self.safety_watchdog=RuntimeSafetyWatchdog(self)
        _wd=self.safety_watchdog
        self.protection_supervisor.peer_health=lambda w=_wd: w.alive
        # v0.8.2/B-6: supervisory threads keep their supervisor object alive through the
        # thread target, so a caller who drops the runtime without stop() leaks three
        # threads per runtime. Enough of them starve the scheduler and a short lease
        # lapses mid-trade. Tie their lifetime to the runtime object instead of trusting
        # every caller to be disciplined.
        if self._finalizer is not None:
            try:self._finalizer.detach()
            except Exception:pass
        self._finalizer=weakref.finalize(self,TradingCoreRuntime._shutdown_supervisors,
                                         self.supervisor,self.protection_supervisor,self.safety_watchdog)

    def _on_lease_loss(self,exc):
        self._ready=False;self.engine.revoke_boot_authority(f'LEASE_LOST:{type(exc).__name__}')

    def _on_protection_violation(self,reason,intent_id=None):
        self._ready=False;self.engine.revoke_boot_authority(f'PROTECTION_SAFETY_FAULT:{reason}')
        try:self.engine.audit.append('RUNTIME_PROTECTION_SAFETY_FAULT',{'reason':reason,'trade_intent_id':intent_id})
        except Exception:pass

    def _on_audit_fault(self,reason):
        self._ready=False;self.engine.revoke_boot_authority(f'AUDIT_SAFETY_FAULT:{reason}')

    def _verify_anchor_before_boot(self)->bool:
        audit=self.engine.audit
        if not audit.anchor:return True
        try:external=audit.anchor.read()
        except Exception as exc:
            audit.anchor_degraded=True;audit.last_anchor_error=f'{type(exc).__name__}: {exc}';return False
        if external is None:
            if audit.head()!='GENESIS':return False
            return audit.sync_anchor() and audit.verify(verify_anchor=True)
        return audit.verify(verify_anchor=True)

    def start(self):
        self._ready=False;self.engine.revoke_boot_authority('RUNTIME_STARTING')
        self.engine.bind_runtime_capability(self._boot_capability)
        self.engine.bind_health_probe(self._boot_capability,_runtime_health_probe(self))
        if self.safety_watchdog:self.safety_watchdog.stop()
        if self.protection_supervisor:self.protection_supervisor.stop()
        if self.supervisor:self.supervisor.stop(release=False)
        if not self.engine.has_authority():
            if not self.engine.acquire_authority(wait_timeout_seconds=self.authority_wait_timeout_seconds):
                self.boot=ColdBootCoordinator(self.engine);report=self.boot.reconcile_all() if self.engine.epoch is not None else None
                if report is None:
                    from .cold_boot import BootReport
                    return BootReport(0,0,1,{'__runtime__':'WAITING_FOR_LEASE'},0)
                return report
        self.boot=ColdBootCoordinator(self.engine);self._new_supervisors();self.supervisor.start()
        local_ok=self.engine.audit.verify();anchor_ok=self._verify_anchor_before_boot();report=self.boot.reconcile_all()
        authority_ok=self.engine.has_authority() and not self.supervisor.lost
        protection_ok=True
        if authority_ok:
            violations=self.protection_supervisor.verify_once();protection_ok=not bool(violations)
        self._ready=bool(local_ok and anchor_ok and report.unresolved==0 and authority_ok and protection_ok and not self.engine.audit.anchor_degraded)
        if self._ready:
            self.protection_supervisor.start();self.safety_watchdog.start()
            try:
                proof=self.engine.issue_boot_proof(self._boot_capability,report.unresolved,report.quarantined)
                self.engine.grant_boot_authority(self._boot_capability,proof)
            except BootGateClosed as exc:
                # Authority lapsed between acquire and the end of reconciliation. Fail
                # closed with an unresolved report; never let boot raise at the caller.
                self._ready=False
                self.engine.revoke_boot_authority(f'BOOT_PROOF_REFUSED:{exc}')
                from .cold_boot import BootReport
                return BootReport(report.inspected,report.resolved,max(1,report.unresolved),
                                  {**report.states,'__runtime__':'AUTHORITY_LOST_DURING_BOOT'},
                                  report.quarantined)
        else:self.engine.revoke_boot_authority('COLD_BOOT_NOT_CLEAN')
        return report

    def stop(self,release_lease=True):
        self._ready=False;self.engine.revoke_boot_authority('RUNTIME_STOPPED')
        if self.safety_watchdog:self.safety_watchdog.stop()
        if self.protection_supervisor:self.protection_supervisor.stop()
        if self.supervisor:self.supervisor.stop(release=False)
        if release_lease:self.engine.release_authority()
        try:self.engine.release_runtime_capability(self._boot_capability)
        except Exception:pass

    def close(self,release_lease=True):
        try:self.stop(release_lease=release_lease)
        except Exception:pass
        try:self.events.close()
        except Exception:pass

    def __del__(self):
        try:self.close(release_lease=False)
        except Exception:pass

    @staticmethod
    def _safe_terminal_or_protected(state_value:str)->bool:
        return state_value in {'PROTECTED','CLOSED','REJECTED','CANCELED','EXPIRED'}

    def _assert_runtime_health(self):
        if not self.ready:raise RuntimeNotReady('Cold boot reconciliation not complete')
        if not self.supervisor or not self.supervisor.alive or self.supervisor.lost:
            self._on_lease_loss(RuntimeError('LEASE_SUPERVISOR_NOT_HEALTHY'));raise RuntimeNotReady('Lease supervisor is not healthy')
        ps=self.protection_supervisor
        if not ps or not ps.alive:
            self._on_protection_violation('PROTECTION_SUPERVISOR_NOT_HEALTHY',None);raise RuntimeNotReady('Protection supervisor is not healthy')
        age=ps.cycle_age_seconds()
        if age>self.max_protection_age_seconds:
            self._on_protection_violation(f'PROTECTION_SUPERVISOR_STALLED:{age:.6f}s',None);raise RuntimeNotReady('Protection verification progress is stale')
        if self.engine.audit.anchor_degraded:
            self._on_audit_fault(self.engine.audit.last_anchor_error or 'AUDIT_WITNESS_DEGRADED');raise RuntimeNotReady('Audit witness is degraded')
        if not self.safety_watchdog or not self.safety_watchdog.alive:
            self._on_protection_violation('RUNTIME_SAFETY_WATCHDOG_NOT_HEALTHY',None);raise RuntimeNotReady('Runtime safety watchdog is not healthy')

    def submit(self,intent,portfolio):
        with self._submit_lock:
            self._assert_runtime_health()
            sm=self.engine.process(intent,portfolio)
            # A side effect has now happened. Whatever we discover next, the caller must
            # still learn the outcome: raising here would hide a real position from the
            # only party that can act on it. Degrade readiness, but always return `sm`.
            safe=self._safe_terminal_or_protected(sm.state.value)
            violations=self.protection_supervisor.verify_one(intent.trade_intent_id)
            if violations:
                self._on_protection_violation('POST_TRADE_PROTECTION_VIOLATION',intent.trade_intent_id)
            elif self.engine.audit.anchor_degraded:
                self._on_audit_fault(self.engine.audit.last_anchor_error or 'AUDIT_WITNESS_DEGRADED')
            elif not safe:
                self._ready=False;self.engine.revoke_boot_authority(f'UNRESOLVED_TRADE_STATE:{sm.state.value}')
            elif not self.ready:
                # Health was lost mid-trade (e.g. a supervisory stall under load). The
                # trade itself is safe; the runtime is not. Close the gate, keep the result.
                self.engine.revoke_boot_authority('SAFETY_HEALTH_LOST_DURING_TRADE')
            return sm

    def ingest_exchange_event(self,event:ExchangeEvent):
        try:inserted=self.events.ingest(event)
        except Exception as exc:
            try:self.engine.audit.append('MALFORMED_EXCHANGE_EVENT',{'error':type(exc).__name__})
            except Exception:pass
            if self.engine.audit.anchor_degraded:self._on_audit_fault(self.engine.audit.last_anchor_error or 'AUDIT_WITNESS_DEGRADED')
            return None
        if not inserted:return None
        if not self.ready or not self.engine.has_authority():return None
        with self._submit_lock:
            if not self.ready or not self.engine.has_authority():return None
            rec=self.engine.ledger.get_by_entry_client_id(event.client_order_id)
            if rec and rec['state'] not in {'CLOSED','REJECTED','CANCELED','EXPIRED'}:
                try:intent=self.engine.ledger.intent_from_payload(rec['payload'])
                except Exception:return None
                sm=self.engine.recover_intent(intent,strict=False)
                if not self._safe_terminal_or_protected(sm.state.value):
                    self._ready=False;self.engine.revoke_boot_authority(f'EVENT_RECONCILIATION_UNSAFE:{sm.state.value}')
                return sm
        return None
