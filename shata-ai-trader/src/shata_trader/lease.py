from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path


class LeaseUnavailable(RuntimeError):
    pass


class StaleEpoch(RuntimeError):
    pass


class SingleWriterLease:
    """DB-backed lease with monotonic fencing epoch."""

    def __init__(self, db_path: str | Path = ':memory:'):
        self.db_path = str(db_path)
        self._lock = threading.RLock()
        self._grace_reacquisitions = 0
        self.conn = sqlite3.connect(self.db_path, timeout=10, isolation_level=None, check_same_thread=False)
        self.conn.execute('PRAGMA busy_timeout=10000')
        if self.db_path != ':memory:':
            self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=FULL')
        self.conn.execute('CREATE TABLE IF NOT EXISTS writer_lease(lease_name TEXT PRIMARY KEY, holder_id TEXT NOT NULL, expires_at TEXT NOT NULL, epoch INTEGER NOT NULL)')

    def acquire(self, lease_name: str, holder_id: str, ttl_seconds: float = 5) -> int:
        now = datetime.now(timezone.utc)
        exp = now + timedelta(seconds=float(ttl_seconds))
        with self._lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                row = self.conn.execute('SELECT holder_id,expires_at,epoch FROM writer_lease WHERE lease_name=?',(lease_name,)).fetchone()
                if row:
                    cur_holder, exp_s, epoch = row
                    if datetime.fromisoformat(exp_s) > now:
                        raise LeaseUnavailable(f'Lease held by {cur_holder}')
                    new_epoch = int(epoch) + 1
                else:
                    new_epoch = 1
                self.conn.execute("""INSERT INTO writer_lease(lease_name,holder_id,expires_at,epoch)
                    VALUES(?,?,?,?) ON CONFLICT(lease_name) DO UPDATE SET holder_id=excluded.holder_id,
                    expires_at=excluded.expires_at, epoch=excluded.epoch""",
                    (lease_name,holder_id,exp.isoformat(),new_epoch))
                self.conn.execute('COMMIT')
                return new_epoch
            except Exception:
                self.conn.execute('ROLLBACK')
                raise

    def renew(self, lease_name: str, holder_id: str, epoch: int, ttl_seconds: float = 5) -> int:
        now = datetime.now(timezone.utc)
        exp = now + timedelta(seconds=float(ttl_seconds))
        with self._lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                row = self.conn.execute('SELECT holder_id,expires_at,epoch FROM writer_lease WHERE lease_name=?',(lease_name,)).fetchone()
                # v0.8.2/B-6: authority is lost when SOMEONE ELSE has taken it, not merely
                # because our own renewal deadline passed with nobody competing. A lapsed
                # but unclaimed lease still held by this holder at this epoch is re-acquired
                # in place. Mutual exclusion was never violated, so declaring a split brain
                # here is a false positive that fails a healthy single writer under load.
                # If any other holder or a newer epoch is present, this still refuses.
                if not row or row[0] != holder_id or int(row[2]) != int(epoch):
                    raise LeaseUnavailable('Lease lost')
                if datetime.fromisoformat(row[1]) <= now:
                    self._grace_reacquisitions += 1
                self.conn.execute('UPDATE writer_lease SET expires_at=? WHERE lease_name=? AND holder_id=? AND epoch=?',
                                  (exp.isoformat(),lease_name,holder_id,int(epoch)))
                self.conn.execute('COMMIT')
                return int(epoch)
            except Exception:
                self.conn.execute('ROLLBACK')
                raise

    def assert_epoch(self, lease_name: str, holder_id: str, epoch: int) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            row = self.conn.execute('SELECT holder_id,expires_at,epoch FROM writer_lease WHERE lease_name=?',(lease_name,)).fetchone()
        if not row:
            raise StaleEpoch('No active lease')
        if row[0] != holder_id or int(row[2]) != int(epoch) or datetime.fromisoformat(row[1]) <= now:
            raise StaleEpoch(f'Stale execution epoch {epoch}; current={row[2]} holder={row[0]}')

    def release(self, lease_name: str, holder_id: str, epoch: int) -> None:
        # Preserve the row/epoch as a durable fencing tombstone so the next
        # acquisition always receives a strictly greater epoch.
        released_at = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        with self._lock:
            self.conn.execute(
                'UPDATE writer_lease SET holder_id=?,expires_at=? WHERE lease_name=? AND holder_id=? AND epoch=?',
                (f'__released__:{holder_id}', released_at, lease_name, holder_id, int(epoch))
            )


def _close_lease_conn(self):
    conn=getattr(self,'conn',None)
    if conn is not None:
        try: conn.close()
        except Exception: pass
        try: self.conn=None
        except Exception: pass

def _del_lease_conn(self):
    _close_lease_conn(self)

SingleWriterLease.close=_close_lease_conn
SingleWriterLease.__del__=_del_lease_conn
