from __future__ import annotations

import threading
import time
import weakref


class LeaseSupervisor:
    """Background lease renewal. On failure, new trading authority is revoked."""

    def __init__(self, lease, holder_id, epoch, ttl_seconds, on_loss=None):
        self.lease = lease
        self.holder_id = holder_id
        self.epoch = int(epoch)
        self.ttl_seconds = float(ttl_seconds)
        self._on_loss_ref = (
            weakref.WeakMethod(on_loss)
            if getattr(on_loss, "__self__", None) is not None
            else None
        )
        self._on_loss_strong = None if self._on_loss_ref else on_loss
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread = None
        # v0.8/N3: renewal progress, not just thread aliveness. A renewer that hangs
        # inside lease I/O is alive and not "lost", yet the lease is silently expiring.
        self._progress_lock = threading.RLock()
        self._last_renew_monotonic = None

    def renew_age_seconds(self) -> float:
        with self._progress_lock:
            last = self._last_renew_monotonic
        return float('inf') if last is None else max(0.0, time.monotonic() - last)

    @property
    def healthy(self) -> bool:
        """Alive, not lost, and authority is actually still valid.

        A late renewal is only a fault if the lease it protects has in fact lapsed.
        Checking the real invariant instead of the progress proxy avoids false trips
        from scheduler jitter under short TTLs, while a genuinely hung renewer still
        fails within one TTL because the lease expires underneath it.
        """
        if not self.alive or self.lost:
            return False
        if self.renew_age_seconds() <= max(self.ttl_seconds, 0.06):
            return True
        try:
            self.lease.assert_epoch('execution-core', self.holder_id, self.epoch)
            return True
        except Exception:
            return False

    @property
    def lost(self):
        return self._lost.is_set()

    @property
    def alive(self):
        return bool(self._thread and self._thread.is_alive())

    def start(self):
        if self.alive:
            return
        # A stopped supervisor is restartable. A newly-acquired epoch gets a new
        # supervisor object from TradingCoreRuntime.
        self._stop = threading.Event()
        self._lost = threading.Event()
        with self._progress_lock:
            self._last_renew_monotonic = time.monotonic()
        self._thread = threading.Thread(
            target=self._run,
            name="shata-lease-supervisor",
            daemon=True,
        )
        self._thread.start()

    def _run(self):
        interval = max(0.02, self.ttl_seconds / 3.0)
        while not self._stop.wait(interval):
            try:
                self.lease.renew(
                    "execution-core",
                    self.holder_id,
                    self.epoch,
                    ttl_seconds=self.ttl_seconds,
                )
                with self._progress_lock:
                    self._last_renew_monotonic = time.monotonic()
            except Exception as exc:
                self._lost.set()
                cb = self._on_loss_ref() if self._on_loss_ref else self._on_loss_strong
                if cb:
                    try:
                        cb(exc)
                    except Exception:
                        pass
                return

    def stop(self, release=False):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=max(0.2, self.ttl_seconds))
        self._thread = None
        if release:
            try:
                self.lease.release("execution-core", self.holder_id, self.epoch)
            except Exception:
                pass
