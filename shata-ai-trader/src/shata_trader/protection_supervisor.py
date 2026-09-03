from __future__ import annotations

import threading
import time
import weakref
from datetime import datetime, timezone
from .domain import TradeState


class ProtectionSupervisor:
    """Continuously revalidate durable protection and expose cycle-progress health."""

    def __init__(self,engine,interval_seconds:float=0.25,max_verification_age_seconds:float=1.0,
                 on_violation=None,freshness_ceiling_seconds:float|None=None):
        self.engine=engine
        self.interval_seconds=max(0.02,float(interval_seconds))
        # v0.8.2/CG-2: three separate bounds instead of one overloaded number.
        #   max_verification_age_seconds -> SOFT per-position freshness target.
        #   max_progress_gap_seconds     -> LIVENESS. No verification progress for this
        #                                   long means the verifier is frozen. Bounded by
        #                                   a SINGLE query, so it is independent of N.
        #   freshness_ceiling_seconds    -> HARD bound. Beyond this the system genuinely
        #                                   cannot police what it holds; gate closes.
        self.max_verification_age_seconds=max(self.interval_seconds,float(max_verification_age_seconds))
        self.max_progress_gap_seconds=self.max_verification_age_seconds
        self.freshness_ceiling_seconds=(float(freshness_ceiling_seconds)
                                        if freshness_ceiling_seconds is not None
                                        else self.max_verification_age_seconds*10.0)
        self.freshness_degraded=False
        self.last_cycle_duration_seconds=None
        self.last_record_count=0
        self._on_violation_ref=weakref.WeakMethod(on_violation) if getattr(on_violation,'__self__',None) is not None else None
        self._on_violation_strong=None if self._on_violation_ref else on_violation
        self._stop=threading.Event();self._thread=None;self._last_error=None
        self._cycle_lock=threading.Lock();self._progress_lock=threading.RLock()
        self.peer_health=None   # v0.8/N3: closes the supervision ring; set by the runtime
        self._last_cycle_started_monotonic=None;self._last_cycle_completed_monotonic=None
        self._last_progress_monotonic=None   # advances after EVERY record, not per cycle
        self._oldest_age_seconds=None;self._oldest_age_measured_monotonic=None
        # v0.8.4/CG-4: BACKGROUND progress only. The foreground post-trade path must
        # never touch this, or continuous submit() traffic masks a frozen supervisor.
        self._background_progress_monotonic=None
        # Striped per-record locks: two concurrent verifications of the SAME record
        # could otherwise race to contradictory conclusions (one writing UNKNOWN).
        # Striping keeps this O(1) in memory and never needs cleanup.
        self._record_locks=tuple(threading.RLock() for _ in range(64))

    @property
    def alive(self)->bool:return bool(self._thread and self._thread.is_alive())
    @property
    def last_error(self):return self._last_error
    @property
    def last_cycle_completed_monotonic(self):
        with self._progress_lock:return self._last_cycle_completed_monotonic
    def cycle_age_seconds(self)->float:
        with self._progress_lock:completed=self._last_cycle_completed_monotonic
        return float('inf') if completed is None else max(0.0,time.monotonic()-completed)

    def progress_age_seconds(self)->float:
        """Seconds since the BACKGROUND verifier last completed a unit of work.

        This is the supervisor liveness signal, and it is deliberately blind to the
        foreground post-trade path.

        v0.8.4/CG-4: verify_one() used to advance the same counter, so a steady stream
        of submits kept the signal fresh while the background thread was frozen inside
        a single call. Liveness of a supervisor can only be evidenced by that
        supervisor doing work.
        """
        with self._progress_lock:
            last=self._background_progress_monotonic
            if last is None:last=self._last_cycle_started_monotonic
        return float('inf') if last is None else max(0.0,time.monotonic()-last)

    def _record_lock(self,intent_id):
        return self._record_locks[hash(str(intent_id))%len(self._record_locks)]

    def _mark_background_progress(self):
        with self._progress_lock:
            now=time.monotonic()
            self._background_progress_monotonic=now;self._last_progress_monotonic=now

    def oldest_verification_age_seconds(self)->float:
        """Age of the least recently verified protected position, from durable state.

        O(N). Used by the background cycle and by explicit diagnostics, never on the
        hot path — see cached_oldest_verification_age_seconds().
        """
        try:records=self.engine.ledger.protected_records()
        except Exception:return float('inf')
        if not records:return 0.0
        return max(self._verification_age_seconds(r) for r in records)

    def cached_oldest_verification_age_seconds(self)->float:
        """O(1) conservative estimate of the oldest verification age.

        v0.8.3/CG-2/D-3: `healthy` is read by the engine health probe on EVERY gated
        call, so it must not scan the ledger. The background cycle records the true
        value; between cycles we extrapolate forward by elapsed time. Extrapolating
        forward can only over-estimate age, so the error is always fail-safe.
        """
        with self._progress_lock:
            measured=self._oldest_age_seconds
            at=self._oldest_age_measured_monotonic
        if measured is None or at is None:
            return 0.0 if self._last_cycle_completed_monotonic is None else float('inf')
        return measured+max(0.0,time.monotonic()-at)

    @property
    def stalled(self)->bool:
        """True only when no verification progress is being made at all."""
        return self.progress_age_seconds() > self.max_progress_gap_seconds

    @property
    def healthy(self)->bool:
        if not self.alive or self.stalled:
            return False
        # Slow-but-progressing is DEGRADED, not unhealthy, until the hard ceiling.
        return self.cached_oldest_verification_age_seconds() <= self.freshness_ceiling_seconds

    def start(self):
        if self.alive:return
        self._stop=threading.Event()
        self._thread=threading.Thread(target=self._run,name='shata-protection-supervisor',daemon=True)
        self._thread.start()
    def stop(self):
        self._stop.set()
        if self._thread:self._thread.join(timeout=max(0.2,self.interval_seconds*4))
        self._thread=None

    def _notify(self,reason:str,intent_id:str|None=None):
        cb=self._on_violation_ref() if self._on_violation_ref else self._on_violation_strong
        if cb:
            try:cb(reason,intent_id)
            except Exception:pass

    @staticmethod
    def _verification_age_seconds(rec)->float:
        raw=rec.get('protection_verified_at')
        if not raw:return float('inf')
        try:
            ts=datetime.fromisoformat(raw)
            if ts.tzinfo is None:ts=ts.replace(tzinfo=timezone.utc)
            return max(0.0,(datetime.now(timezone.utc)-ts).total_seconds())
        except Exception:return float('inf')

    def verify_one(self,intent_id:str):
        """O(1) post-trade validation of a single affected position.

        v0.8.2/CG-2 item D: submit() used to run a full O(N) portfolio scan after every
        trade, so cost grew quadratically with position count. The background supervisor
        keeps portfolio-wide responsibility; submit() only checks what it just changed.

        v0.8.3/CG-2/D-2: this must NOT take _cycle_lock. Holding it made submit() wait
        behind an entire background cycle, so post-trade latency stayed tied to portfolio
        size — O(1) in work but O(N) in wall clock. Verifying one record concurrently
        with the background cycle is safe: the ledger has its own locking, and a
        duplicate verification of the same record is idempotent.
        """
        # No liveness update here: this is the FOREGROUND path (CG-4).
        try:rec=self.engine.ledger.get(intent_id)
        except Exception as exc:
            self._last_error=f'LEDGER_READ:{type(exc).__name__}';self._notify(self._last_error,None)
            return [(intent_id,self._last_error)]
        if not rec or rec.get('state') not in (TradeState.PROTECTED.value,TradeState.PARTIALLY_PROTECTED.value):
            return []
        with self._record_lock(intent_id):
            return self._verify_record(rec,mark_progress=False)

    def _verify_record(self,rec,mark_progress=True):
        intent_id=rec.get('intent_id');violations=[]
        try:result=self.engine.verify_protected_record(rec)
        except Exception as exc:
            result=None;self._last_error=f'VERIFY_EXCEPTION:{type(exc).__name__}'
        finally:
            if mark_progress:self._mark_background_progress()
        if result is False:
            reason='PROTECTION_MISSING_OR_MISMATCH';violations.append((intent_id,reason));self._notify(reason,intent_id)
        elif result is None and self._verification_age_seconds(rec)>self.max_verification_age_seconds:
            reason='PROTECTION_VERIFICATION_STALE'
            try:
                self.engine.ledger.clear_protection_verification(intent_id,reason)
                self.engine.ledger.recovery_set_state(intent_id,TradeState.UNKNOWN.value,reason)
                self.engine.audit.append('PROTECTION_VERIFICATION_EXPIRED',{'trade_intent_id':intent_id,'reason':reason})
            except Exception as exc:reason=f'PROTECTION_STALE_WRITE_FAILED:{type(exc).__name__}'
            violations.append((intent_id,reason));self._notify(reason,intent_id)
        return violations

    def verify_once(self):
        with self._cycle_lock:
            with self._progress_lock:
                self._last_cycle_started_monotonic=time.monotonic()
            self._mark_background_progress()
            violations=[]
            try:
                try:records=self.engine.ledger.protected_records()
                except Exception as exc:
                    self._last_error=f'LEDGER_READ:{type(exc).__name__}';self._notify(self._last_error,None);return [(None,self._last_error)]
                self.last_record_count=len(records)
                # Oldest first: freshness degrades uniformly instead of starving a record.
                records=sorted(records,key=self._verification_age_seconds,reverse=True)
                for rec in records:
                    with self._record_lock(rec.get('intent_id')):
                        violations.extend(self._verify_record(rec))
                return violations
            finally:
                now=time.monotonic()
                with self._progress_lock:
                    started=self._last_cycle_started_monotonic
                    self._last_cycle_completed_monotonic=now
                    self._last_progress_monotonic=now
                    self._background_progress_monotonic=now
                if started is not None:self.last_cycle_duration_seconds=max(0.0,now-started)
                oldest=self.oldest_verification_age_seconds()
                with self._progress_lock:
                    self._oldest_age_seconds=oldest;self._oldest_age_measured_monotonic=now
                was=self.freshness_degraded
                self.freshness_degraded=oldest>self.max_verification_age_seconds
                if self.freshness_degraded and not was:
                    # Report capacity honestly; do NOT call it a stall and do NOT close
                    # the gate. The ceiling handles the genuinely unsafe case.
                    try:
                        self.engine.audit.append('PROTECTION_FRESHNESS_DEGRADED',{
                            'oldest_verification_age_seconds':round(oldest,4),
                            'soft_target_seconds':self.max_verification_age_seconds,
                            'hard_ceiling_seconds':self.freshness_ceiling_seconds,
                            'positions':self.last_record_count,
                            'cycle_seconds':round(self.last_cycle_duration_seconds or 0.0,4)})
                    except Exception:pass

    def _run(self):
        try:
            while not self._stop.wait(self.interval_seconds):
                # v0.8/N3: mutual supervision. If the runtime safety watchdog is gone,
                # this loop reports it, so no single supervisory thread is a single
                # point of failure for the whole monitoring chain.
                peer=self.peer_health
                if peer is not None:
                    try:ok=bool(peer())
                    except Exception:ok=False
                    if not ok:
                        self._last_error='RUNTIME_SAFETY_WATCHDOG_NOT_ALIVE'
                        self._notify(self._last_error,None)
                self.verify_once()
        except BaseException as exc:
            self._last_error=f'SUPERVISOR_DIED:{type(exc).__name__}'
            if not self._stop.is_set():self._notify(self._last_error,None)
