"""Thread-local SQLite connections.

v0.8.1. A single `sqlite3.Connection` shared across threads is not safe even with
``check_same_thread=False``: concurrent ``execute`` calls interleave on one C-level
statement machine, producing ``InterfaceError: bad parameter or other API misuse``,
and two threads issuing ``BEGIN IMMEDIATE`` on the same connection produce
``OperationalError: cannot start a transaction within a transaction``.

The structural fix is to stop sharing the connection at all rather than to wrap a
lock around every caller: one connection per thread, with SQLite's own file-level
locking (plus ``busy_timeout``) providing the cross-thread serialization it was
designed for. This also preserves the project rule that no long-lived transaction
is held across external I/O, because each thread's transaction is its own.
"""
from __future__ import annotations

import sqlite3
import threading


class ThreadLocalSqlite:
    """Owns one SQLite connection per accessing thread, created on first use."""

    def __init__(self, db_path: str, *, wal: bool = True, synchronous: str | None = 'FULL',
                 timeout: float = 10.0, busy_timeout_ms: int = 10000):
        self.db_path = str(db_path)
        self._wal = wal and self.db_path != ':memory:'
        self._synchronous = synchronous
        self._timeout = timeout
        self._busy_timeout_ms = busy_timeout_ms
        self._local = threading.local()
        self._registry_lock = threading.Lock()
        self._all_conns: list[sqlite3.Connection] = []
        self._closed = False

    def _new_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path, timeout=self._timeout, isolation_level=None, check_same_thread=False
        )
        if self._wal:
            conn.execute('PRAGMA journal_mode=WAL')
        if self._synchronous:
            conn.execute(f'PRAGMA synchronous={self._synchronous}')
        conn.execute(f'PRAGMA busy_timeout={self._busy_timeout_ms}')
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        if self._closed:
            raise sqlite3.ProgrammingError('Cannot operate on a closed store')
        c = getattr(self._local, 'conn', None)
        if c is None:
            c = self._new_conn()
            self._local.conn = c
            with self._registry_lock:
                self._all_conns.append(c)
        return c

    def close(self):
        self._closed = True
        with self._registry_lock:
            conns, self._all_conns = self._all_conns, []
        for c in conns:
            try:
                c.close()
            except Exception:
                pass
        try:
            self._local.conn = None
        except Exception:
            pass


class SharedMemorySqlite(ThreadLocalSqlite):
    """``:memory:`` variant: an in-memory DB cannot be reopened per thread, so it
    falls back to one connection guarded by a lock. Callers must hold ``lock``
    around any multi-statement sequence."""

    def __init__(self, **kw):
        super().__init__(':memory:', wal=False, **kw)
        self.lock = threading.RLock()
        self._shared = self._new_conn()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._closed:
            raise sqlite3.ProgrammingError('Cannot operate on a closed store')
        return self._shared

    def close(self):
        self._closed = True
        try:
            self._shared.close()
        except Exception:
            pass
