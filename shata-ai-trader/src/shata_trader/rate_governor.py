from __future__ import annotations

import heapq
import threading
import time


class RateGovernorTimeout(TimeoutError):
    pass


class PriorityRateGovernor:
    """Thread-safe demo pacing with priority ordering and abandoned-ticket cleanup.

    Lower numeric priority wins among queued callers. Production must replace the
    fixed-call interval with Binance request-weight accounting and endpoint costs.
    """

    def __init__(self, min_interval_seconds: float = 0.01, default_timeout_seconds: float | None = 5.0):
        self.min_interval = max(0.0, float(min_interval_seconds))
        self.default_timeout_seconds = None if default_timeout_seconds is None else max(0.0, float(default_timeout_seconds))
        self._cv = threading.Condition()
        self._last = 0.0
        self._seq = 0
        self._queue = []

    def acquire(self, priority: int = 1, timeout: float | None = None):
        if timeout is None:
            timeout = self.default_timeout_seconds
        deadline = None if timeout is None else time.monotonic() + max(0.0, float(timeout))
        with self._cv:
            self._seq += 1
            ticket = (int(priority), self._seq)
            heapq.heappush(self._queue, ticket)
            granted = False
            try:
                while True:
                    head = bool(self._queue) and self._queue[0] == ticket
                    now = time.monotonic()
                    if deadline is not None and now >= deadline:
                        raise RateGovernorTimeout(f'rate governor wait exceeded {timeout}s')
                    wait_for = max(0.0, self.min_interval - (now - self._last))
                    if head and wait_for <= 0:
                        heapq.heappop(self._queue)
                        granted = True
                        self._last = time.monotonic()
                        self._cv.notify_all()
                        return
                    sleep_for = wait_for if head else max(self.min_interval, 0.001)
                    if deadline is not None:
                        sleep_for = min(sleep_for, max(0.0, deadline - now))
                    self._cv.wait(timeout=sleep_for)
            finally:
                if not granted:
                    try:
                        self._queue.remove(ticket)
                        heapq.heapify(self._queue)
                    except ValueError:
                        pass
                    self._cv.notify_all()
