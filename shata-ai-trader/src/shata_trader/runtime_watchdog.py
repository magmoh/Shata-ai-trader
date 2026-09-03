from __future__ import annotations
import threading,time,weakref


class RuntimeSafetyWatchdog:
    """Independent in-process progress watchdog for Phase-0 simulation.

    Production still requires an out-of-process/host watchdog.  This thread detects a
    live-but-stalled protection supervisor and audit-witness degradation even when the
    worker thread itself cannot make progress.
    """
    def __init__(self,runtime,interval_seconds:float|None=None):
        self._runtime_ref=weakref.ref(runtime)
        maxage=max(0.02,float(runtime.max_protection_age_seconds))
        self.interval_seconds=max(0.01,min(maxage/4.0,0.25)) if interval_seconds is None else max(0.01,float(interval_seconds))
        self._stop=threading.Event();self._thread=None;self._last_error=None
    @property
    def alive(self):return bool(self._thread and self._thread.is_alive())
    @property
    def last_error(self):return self._last_error
    def start(self):
        if self.alive:return
        self._stop=threading.Event();self._thread=threading.Thread(target=self._run,name='shata-runtime-safety-watchdog',daemon=True);self._thread.start()
    def stop(self):
        self._stop.set()
        if self._thread:self._thread.join(timeout=max(0.2,self.interval_seconds*4))
        self._thread=None
    def _run(self):
        # v0.8/N3: the watchdog must never die silently. Any escape from the loop is
        # itself a safety fault, and it is reported before the thread unwinds.
        try:
            while not self._stop.wait(self.interval_seconds):
                rt = self._runtime_ref()
                if rt is None:
                    return
                # Use the latched intent flag, not the computed `ready` property:
                # a stalled supervisor already makes `ready` False, and skipping on
                # that would stop us from ever revoking the gate.
                if not rt._ready:
                    continue
                ps = rt.protection_supervisor
                if ps is None or not ps.alive:
                    self._last_error = 'PROTECTION_SUPERVISOR_NOT_ALIVE'
                    rt._on_protection_violation(self._last_error, None)
                    continue
                # v0.8.2/CG-2: a stall means NO PROGRESS, not "the full portfolio cycle
                # has not finished yet". A healthy O(N) cycle that exceeds the per-position
                # target is a capacity condition, reported separately and never as a stall.
                gap = ps.progress_age_seconds()
                if gap > ps.max_progress_gap_seconds:
                    self._last_error = f'PROTECTION_SUPERVISOR_STALLED:{gap:.6f}s'
                    rt._on_protection_violation(self._last_error, None)
                    continue
                oldest = ps.cached_oldest_verification_age_seconds()
                if oldest > ps.freshness_ceiling_seconds:
                    self._last_error = (
                        f'PROTECTION_FRESHNESS_CEILING_EXCEEDED:{oldest:.6f}s'
                        f'>ceiling={ps.freshness_ceiling_seconds:.3f}s'
                        f' positions={ps.last_record_count}'
                    )
                    rt._on_protection_violation(self._last_error, None)
                    continue
                sup = rt.supervisor
                if sup is None or not sup.healthy:
                    self._last_error = f'LEASE_SUPERVISOR_NOT_HEALTHY:renew_age={0.0 if sup is None else sup.renew_age_seconds():.3f}s'
                    rt._on_lease_loss(RuntimeError(self._last_error))
                    continue
                if rt.engine.audit.anchor_degraded:
                    self._last_error = f'AUDIT_WITNESS_DEGRADED:{rt.engine.audit.last_anchor_error}'
                    rt._on_audit_fault(self._last_error)
        except BaseException as exc:
            self._last_error = f'WATCHDOG_DIED:{type(exc).__name__}'
            if not self._stop.is_set():
                rt = self._runtime_ref()
                if rt is not None:
                    try:
                        rt._on_protection_violation(self._last_error, None)
                    except Exception:
                        pass
            raise
