# SHATA v0.8.4 — SOURCE BUNDLE 1/3 (src)

**Run this first. If it does not print `Ran 95 tests`, you are on an older tree and any
verdict is void.**

```bash
python3 -m unittest discover -s tests -v     # MUST print: Ran 95 tests ... OK
```

`Ran 93` means v0.8.3, which contains CG-4 — a confirmed defect ChatGPT raised and this
release closes.

## What v0.8.4 changes

**CG-4 — foreground traffic masked a frozen background supervisor.** Introduced by my own
D-2 patch in v0.8.3: `verify_one()` kept advancing `_last_progress_monotonic`, the counter
the watchdog reads as the supervisor liveness signal. A steady stream of `submit()` calls
kept that signal fresh while the background thread was frozen inside one call.

```
before:  supervisor frozen >1.2s inside protected_records()
         ready=True  gate_open=True  watchdog=None  progress_age=0.050s (bound 0.3s)
after:   ready=False gate_open=False
         watchdog=PROTECTION_SUPERVISOR_STALLED:0.356357s   progress_age=1.219s
```

Fix: `_background_progress_monotonic` is advanced **only** by `verify_once()` — at cycle
start and after each record. `verify_one()` no longer touches the liveness signal.
Liveness of a supervisor can only be evidenced by that supervisor doing work.

Plus 64 striped per-record `RLock`s: removing `_cycle_lock` in D-2 had allowed the
background cycle and the foreground path to verify the **same** record simultaneously,
where one side may write `UNKNOWN`. Striping serialises per-record verification without
reintroducing the portfolio-size coupling D-2 removed.

## Reconstruct

```python
import re, pathlib
for b in ['bundle1.md','bundle2.md','bundle3.md']:
    text = pathlib.Path(b).read_text(encoding='utf-8')
    for m in re.finditer(r'^=== FILE: src/shata_trader/__init__.py ===
"""SHATA AI TRADER Phase 0 — deterministic demo core."""

=== FILE: src/shata_trader/activity.py ===
from __future__ import annotations
import sqlite3,threading
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from pathlib import Path
class TradingActivityStore:
    def __init__(self,db_path:str|Path=':memory:'):
        self.db_path=str(db_path);self._lock=threading.RLock();self.conn=sqlite3.connect(self.db_path,timeout=10,isolation_level=None,check_same_thread=False)
        self.conn.execute('PRAGMA busy_timeout=10000')
        if self.db_path!=':memory:':self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=FULL');self.conn.execute('CREATE TABLE IF NOT EXISTS activity(ts TEXT NOT NULL,kind TEXT NOT NULL,notional TEXT NOT NULL DEFAULT "0")')
    def record_submission(self,n):
        with self._lock:self.conn.execute('INSERT INTO activity VALUES(?,?,?)',(datetime.now(timezone.utc).isoformat(),'SUBMIT',str(n)))
    def record_error(self):
        with self._lock:self.conn.execute('INSERT INTO activity VALUES(?,?,?)',(datetime.now(timezone.utc).isoformat(),'ERROR','0'))
    def orders_last_hour(self):
        c=(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()
        with self._lock:return self.conn.execute('SELECT COUNT(*) FROM activity WHERE kind="SUBMIT" AND ts>=?',(c,)).fetchone()[0]
    def notional_today(self):
        c=datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0).isoformat()
        with self._lock:rows=self.conn.execute('SELECT notional FROM activity WHERE kind="SUBMIT" AND ts>=?',(c,)).fetchall()
        return sum((Decimal(x[0]) for x in rows),Decimal('0'))
    def consecutive_errors(self):
        with self._lock:rows=self.conn.execute('SELECT kind FROM activity ORDER BY rowid DESC LIMIT 100').fetchall()
        n=0
        for (k,) in rows:
            if k=='ERROR':n+=1
            else:break
        return n


# Explicit lifecycle cleanup for test/runtime resource hygiene.
def _close_conn_activity(self):
    conn=getattr(self,'conn',None)
    if conn is not None:
        try: conn.close()
        except Exception: pass
        try: self.conn=None
        except Exception: pass

def _del_conn_activity(self):
    _close_conn_activity(self)

TradingActivityStore.close=_close_conn_activity
TradingActivityStore.__del__=_del_conn_activity

=== FILE: src/shata_trader/audit.py ===
from __future__ import annotations
import fcntl, hashlib, json, os, threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class HashChainedAuditLog:
    """Local durable audit with guarded external witness publication.

    The local hash chain is an integrity structure, not a cryptographic proof against
    an attacker who can rewrite the entire host.  The external witness is therefore
    never overwritten when it diverges from the lineage of the local chain.
    Production must place the witness in an independent/WORM/signed trust domain.
    """

    def __init__(self, path: str | Path, anchor=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.anchor = anchor
        self.lock_path = self.path.with_suffix(self.path.suffix + '.lock')
        self.pending_anchor_path = self.path.with_suffix(self.path.suffix + '.anchor_pending')
        self._thread_lock = threading.RLock()
        self.anchor_degraded = False
        self.last_anchor_error = None

    def _last_hash_unlocked(self):
        if not self.path.exists() or self.path.stat().st_size == 0:
            return 'GENESIS'
        lines = self.path.read_text(encoding='utf-8', errors='strict').splitlines()
        if not lines:
            return 'GENESIS'
        try:
            return json.loads(lines[-1])['hash']
        except Exception:
            raise RuntimeError('Audit tail is corrupt; recovery required before append')

    def _records_unlocked(self):
        if not self.path.exists() or self.path.stat().st_size == 0:
            return []
        out = []
        for line in self.path.read_text(encoding='utf-8', errors='strict').splitlines():
            if not line:
                continue
            out.append(json.loads(line))
        return out

    def head(self):
        with self._thread_lock, self.lock_path.open('a+') as lf:
            fcntl.flock(lf.fileno(), fcntl.LOCK_SH)
            try:
                return self._last_hash_unlocked()
            finally:
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    def _local_height(self) -> int:
        try:
            return len(self._records_unlocked())
        except Exception:
            return -1

    def _height_relation(self, external, local_height: int) -> str:
        """v0.8/N2: compare the witnessed chain height against the local one.

        A witness recorded at a height greater than what the local chain now holds
        means local history was truncated or replaced. Hash-head comparison alone
        cannot see this, because a truncated prefix is itself a valid chain.
        """
        if not isinstance(external, dict):
            return 'unknown'
        h = external.get('height')
        if local_height < 0:
            return 'unknown'
        if h is None:
            # A witness written by this version always carries a height. A height-less
            # witness against a non-empty local chain is a downgrade attempt, not a
            # legacy record: treat it as shrunk rather than trusting it.
            return 'ok' if local_height == 0 else 'shrunk'
        try:
            h = int(h)
        except Exception:
            return 'unknown'
        if h > local_height:
            return 'shrunk'
        return 'ok'

    def _lineage_relation(self, external_head: str | None, candidate_head: str) -> str:
        """Return ancestor/equal/descendant/divergent relative to candidate_head.

        ancestor: external witness is behind candidate and can safely advance.
        descendant: external witness is already ahead; never roll it back.
        divergent: witness is not on the local lineage; do not overwrite it.
        """
        if external_head is None:
            return 'missing'
        if external_head == candidate_head:
            return 'equal'
        try:
            records = self._records_unlocked()
        except Exception:
            return 'divergent'
        prev_of = {r.get('hash'): r.get('prev_hash') for r in records if r.get('hash')}

        cur = candidate_head
        seen = set()
        while cur and cur not in seen:
            if cur == external_head:
                return 'ancestor'
            seen.add(cur)
            cur = prev_of.get(cur)
            if cur == 'GENESIS':
                if external_head == 'GENESIS':
                    return 'ancestor'
                break

        cur = external_head
        seen.clear()
        while cur and cur not in seen:
            if cur == candidate_head:
                return 'descendant'
            seen.add(cur)
            cur = prev_of.get(cur)
            if cur == 'GENESIS':
                break
        return 'divergent'

    def _mark_anchor_pending(self, digest, exc):
        self.anchor_degraded = True
        self.last_anchor_error = f'{type(exc).__name__}: {exc}'
        tmp = self.pending_anchor_path.with_suffix(self.pending_anchor_path.suffix + '.tmp')
        data = json.dumps(
            {'head_hash': digest, 'error': self.last_anchor_error, 'ts': datetime.now(timezone.utc).isoformat()},
            sort_keys=True,
        ).encode()
        fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self.pending_anchor_path)

    def _mark_lineage_mismatch(self, digest, external_head):
        self.anchor_degraded = True
        self.last_anchor_error = f'ANCHOR_LINEAGE_MISMATCH external={external_head} local={digest}'
        tmp = self.pending_anchor_path.with_suffix(self.pending_anchor_path.suffix + '.tmp')
        data = json.dumps(
            {'head_hash': digest, 'error': self.last_anchor_error, 'ts': datetime.now(timezone.utc).isoformat()},
            sort_keys=True,
        ).encode()
        fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self.pending_anchor_path)

    def _publish_best_effort(self, digest):
        if not self.anchor:
            return
        try:
            cur = self.anchor.read()
            external_head = cur.get('head_hash') if cur else None
            local_height = self._local_height()
            if self._height_relation(cur, local_height) == 'shrunk':
                self._mark_lineage_mismatch(digest, f"{external_head}@height{cur.get('height')}>local{local_height}")
                return
            relation = self._lineage_relation(external_head, digest)
            if relation == 'missing':
                # A witness may only be initialized at GENESIS or the first append.
                records = self._records_unlocked()
                if len(records) > 1:
                    self._mark_lineage_mismatch(digest, None)
                    return
            elif relation == 'divergent':
                self._mark_lineage_mismatch(digest, external_head)
                return
            elif relation in {'equal', 'descendant'}:
                # Already witnessed at this head or at a newer descendant; never roll back.
                self.anchor_degraded = False
                self.last_anchor_error = None
                if self.pending_anchor_path.exists():
                    self.pending_anchor_path.unlink()
                return

            self.anchor.publish(digest, self._local_height())
            self.anchor_degraded = False
            self.last_anchor_error = None
            if self.pending_anchor_path.exists():
                self.pending_anchor_path.unlink()
        except Exception as exc:
            self._mark_anchor_pending(digest, exc)

    def sync_anchor(self):
        if not self.anchor:
            return True
        digest = self.head()
        try:
            cur = self.anchor.read()
            external_head = cur.get('head_hash') if cur else None
            local_height = self._local_height()
            if self._height_relation(cur, local_height) == 'shrunk':
                self._mark_lineage_mismatch(digest, f"{external_head}@height{cur.get('height')}>local{local_height}")
                return False
            if external_head is None:
                if digest != 'GENESIS':
                    self._mark_lineage_mismatch(digest, None)
                    return False
                self.anchor.publish(digest, local_height)
            else:
                relation = self._lineage_relation(external_head, digest)
                if relation == 'divergent':
                    self._mark_lineage_mismatch(digest, external_head)
                    return False
                if relation == 'ancestor':
                    self.anchor.publish(digest, local_height)
                # equal/descendant: do not overwrite/roll back.
            self.anchor_degraded = False
            self.last_anchor_error = None
            if self.pending_anchor_path.exists():
                self.pending_anchor_path.unlink()
            return True
        except Exception as exc:
            self._mark_anchor_pending(digest, exc)
            return False

    def append(self, event_type: str, payload: dict[str, Any]) -> str:
        with self._thread_lock, self.lock_path.open('a+') as lf:
            fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
            try:
                prev = self._last_hash_unlocked()
                body = {
                    'ts': datetime.now(timezone.utc).isoformat(),
                    'event_type': event_type,
                    'payload': payload,
                    'prev_hash': prev,
                }
                canonical = json.dumps(body, sort_keys=True, separators=(',', ':'))
                digest = hashlib.sha256(canonical.encode()).hexdigest()
                data = (json.dumps({**body, 'hash': digest}, sort_keys=True) + '\n').encode()
                fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
                try:
                    off = 0
                    while off < len(data):
                        off += os.write(fd, data[off:])
                    os.fsync(fd)
                finally:
                    os.close(fd)
            finally:
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
        self._publish_best_effort(digest)
        return digest

    def verify(self, verify_anchor=False):
        if not self.path.exists():
            return True
        try:
            lines = self.path.read_text(encoding='utf-8', errors='strict').splitlines()
        except Exception:
            return False
        prev = 'GENESIS'
        for line in lines:
            try:
                rec = json.loads(line)
                claimed = rec.pop('hash')
            except Exception:
                return False
            if rec.get('prev_hash') != prev:
                return False
            if hashlib.sha256(json.dumps(rec, sort_keys=True, separators=(',', ':')).encode()).hexdigest() != claimed:
                return False
            prev = claimed
        if verify_anchor and self.anchor:
            try:
                a = self.anchor.read()
            except Exception:
                return False
            if not a or a.get('head_hash') != prev:
                return False
            # v0.8/N2: the head alone cannot see a truncated prefix — a truncated
            # chain is itself a valid chain. Height closes that.
            if self._height_relation(a, len(lines)) == 'shrunk':
                self.anchor_degraded = True
                self.last_anchor_error = (
                    f"ANCHOR_HEIGHT_MISMATCH external={a.get('height')} local={len(lines)}"
                )
                return False
        return True

=== FILE: src/shata_trader/audit_anchor.py ===
from __future__ import annotations
import json, os, tempfile
from datetime import datetime, timezone
from pathlib import Path

class FileAuditAnchor:
    """Prototype only. Production anchor must live in a separate trust domain/WORM service."""
    def __init__(self,path:str|Path): self.path=Path(path); self.path.parent.mkdir(parents=True,exist_ok=True)
    def publish(self,head_hash:str,height:int|None=None):
        # v0.8/N2: height makes truncation and witness rollback detectable without a
        # secret key. A witness that claims a height above the local chain is divergent.
        rec={'ts':datetime.now(timezone.utc).isoformat(),'head_hash':head_hash}
        if height is not None: rec['height']=int(height)
        fd,tmp=tempfile.mkstemp(prefix=self.path.name+'.',dir=str(self.path.parent))
        try:
            with os.fdopen(fd,'w') as f: json.dump(rec,f,sort_keys=True); f.flush(); os.fsync(f.fileno())
            os.replace(tmp,self.path)
            dfd=os.open(self.path.parent,os.O_DIRECTORY)
            try: os.fsync(dfd)
            finally: os.close(dfd)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
    def read(self): return json.loads(self.path.read_text()) if self.path.exists() else None

=== FILE: src/shata_trader/cold_boot.py ===
from __future__ import annotations
from dataclasses import dataclass
from .domain import TradeState
from .rate_governor import PriorityRateGovernor

@dataclass
class BootReport:
    inspected:int
    resolved:int
    unresolved:int
    states:dict[str,str]
    quarantined:int=0

class ColdBootCoordinator:
    """Reconcile every durable nonterminal row; one corrupt row cannot hide the rest."""
    def __init__(self,engine,rate_governor=None):
        self.engine=engine;self.ready=False;self.rate_governor=rate_governor or engine.rate_governor
    def reconcile_all(self)->BootReport:
        records=self.engine.ledger.nonterminal_records();states={};resolved=0;unresolved=0;quarantined=0
        for r in records:
            try:
                self.rate_governor.acquire(priority=1)
                intent=self.engine.ledger.intent_from_payload(r['payload'])
                if intent.trade_intent_id!=r['intent_id']:
                    raise ValueError('payload intent_id mismatch')
                sm=self.engine.recover_intent(intent)
                states[r['intent_id']]=sm.state.value
                if sm.state in {TradeState.CLOSED,TradeState.CANCELED,TradeState.EXPIRED,TradeState.REJECTED,TradeState.PROTECTED,TradeState.PARTIALLY_PROTECTED}:
                    resolved+=1
                else:
                    unresolved+=1
            except Exception as exc:
                quarantined+=1;unresolved+=1;states[r['intent_id']]=f'QUARANTINED:{type(exc).__name__}'
                try:self.engine.ledger.mark_error(r['intent_id'],f'COLD_BOOT_QUARANTINE:{type(exc).__name__}')
                except Exception:pass
        self.ready=(unresolved==0)
        return BootReport(len(records),resolved,unresolved,states,quarantined)

=== FILE: src/shata_trader/db.py ===
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

=== FILE: src/shata_trader/domain.py ===
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TradeState(str, Enum):
    CREATED = "CREATED"
    RISK_APPROVED = "RISK_APPROVED"
    SUBMITTED = "SUBMITTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    PARTIAL_PROTECTION_PENDING = "PARTIAL_PROTECTION_PENDING"
    PARTIALLY_PROTECTED = "PARTIALLY_PROTECTED"
    PROTECTION_PENDING = "PROTECTION_PENDING"
    PROTECTED = "PROTECTED"
    UNDER_PROTECTED = "UNDER_PROTECTED"
    EXIT_PENDING = "EXIT_PENDING"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"
    RECONCILING = "RECONCILING"
    PROTECTION_FAILED = "PROTECTION_FAILED"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"
    HALTED = "HALTED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class TradeIntent:
    trade_intent_id: str
    strategy_id: str
    strategy_version: str
    risk_policy_version: int
    symbol: str
    side: Side
    quote_amount: Decimal
    reference_entry_price: Decimal
    stop_price: Decimal
    take_profit_price: Decimal
    max_entry_deviation_pct: Decimal
    created_at: datetime
    expires_at: datetime

    def is_expired(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(timezone.utc)
        return now >= self.expires_at


@dataclass(frozen=True)
class PortfolioSnapshot:
    quote_balance: Decimal
    portfolio_value: Decimal
    current_exposure: Decimal
    reconciled_at: datetime | None = None


@dataclass(frozen=True)
class RiskPolicy:
    version: int
    max_risk_per_trade_pct: Decimal
    max_position_allocation_pct: Decimal
    max_portfolio_exposure_pct: Decimal
    min_risk_reward: Decimal
    max_entry_deviation_pct: Decimal
    max_intent_age_seconds: int
    max_reconciliation_age_seconds: int = 5
    max_orders_per_hour: int = 20
    max_notional_per_day_pct: Decimal = Decimal("0.50")
    max_consecutive_execution_errors: int = 3
    emergency_exit_on_unprotected_new_entry: bool = True


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reason: str
    max_quote_amount: Decimal


@dataclass(frozen=True)
class ExchangeOrder:
    client_order_id: str
    symbol: str
    status: str
    requested_quote_amount: Decimal
    filled_base_qty: Decimal
    avg_fill_price: Decimal
    commission_amount: Decimal = Decimal("0")
    commission_asset: str | None = None


@dataclass(frozen=True)
class ExchangeProtection:
    protection_id: str
    client_order_id: str
    symbol: str
    base_qty: Decimal
    active: bool = True

=== FILE: src/shata_trader/events.py ===
from __future__ import annotations
import sqlite3, threading
from dataclasses import dataclass
from pathlib import Path

from .db import SharedMemorySqlite, ThreadLocalSqlite

RANK={'NEW':10,'ACKNOWLEDGED':20,'PARTIALLY_FILLED':30,'FILLED':40,'CANCELED':40,'EXPIRED':40}


class MalformedExchangeEvent(ValueError):
    pass


@dataclass(frozen=True)
class ExchangeEvent:
    event_id:str
    client_order_id:str
    status:str
    event_time_ms:int


class OrderEventStore:
    """Idempotent ingestion; malformed and stale inputs are treated as untrusted data."""
    def __init__(self,db_path:str|Path=':memory:'):
        # v0.8.1: ingest may be called from any thread; never share one connection.
        self._db=SharedMemorySqlite() if str(db_path)==':memory:' else ThreadLocalSqlite(str(db_path))
        self._ingest_lock=threading.RLock()
        self.conn.execute('CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,client_id TEXT NOT NULL,status TEXT NOT NULL,event_time_ms INTEGER NOT NULL)')
        self.conn.execute('CREATE TABLE IF NOT EXISTS projection(client_id TEXT PRIMARY KEY,status TEXT NOT NULL,rank INTEGER NOT NULL,event_time_ms INTEGER NOT NULL)')

    @property
    def conn(self):
        return self._db.conn

    @staticmethod
    def _validate(e: ExchangeEvent) -> int:
        if not isinstance(e.event_id,str) or not e.event_id.strip(): raise MalformedExchangeEvent('invalid event_id')
        if not isinstance(e.client_order_id,str) or not e.client_order_id.strip(): raise MalformedExchangeEvent('invalid client_order_id')
        if e.status not in RANK: raise MalformedExchangeEvent('unknown event status')
        if isinstance(e.event_time_ms,bool): raise MalformedExchangeEvent('invalid event_time_ms')
        try: t=int(e.event_time_ms)
        except Exception as exc: raise MalformedExchangeEvent('invalid event_time_ms') from exc
        if t < 0: raise MalformedExchangeEvent('negative event_time_ms')
        return t

    def ingest(self,e:ExchangeEvent)->bool:
        event_time=self._validate(e)
        with self._ingest_lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                try:self.conn.execute('INSERT INTO events VALUES(?,?,?,?)',(e.event_id,e.client_order_id,e.status,event_time))
                except sqlite3.IntegrityError:
                    self.conn.execute('ROLLBACK');return False
                cur=self.conn.execute('SELECT status,rank,event_time_ms FROM projection WHERE client_id=?',(e.client_order_id,)).fetchone();nr=RANK[e.status]
                if cur is None or nr>cur[1] or (nr==cur[1] and event_time>=cur[2]):
                    self.conn.execute('INSERT INTO projection VALUES(?,?,?,?) ON CONFLICT(client_id) DO UPDATE SET status=excluded.status,rank=excluded.rank,event_time_ms=excluded.event_time_ms',(e.client_order_id,e.status,nr,event_time))
                self.conn.execute('COMMIT');return True
            except Exception:
                try:self.conn.execute('ROLLBACK')
                except Exception:pass
                raise

    def status(self,client_id):
        r=self.conn.execute('SELECT status FROM projection WHERE client_id=?',(client_id,)).fetchone();return r[0] if r else None

    def close(self):
        db=getattr(self,'_db',None)
        if db is not None:db.close()

    def __del__(self):
        try:self.close()
        except Exception:pass

=== FILE: src/shata_trader/exchange.py ===
from __future__ import annotations

import threading

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
import uuid

from .domain import ExchangeOrder, ExchangeProtection


class UnknownSubmissionState(RuntimeError):
    """Ambiguous submission outcome: never blindly retry."""


class ExchangeRejected(RuntimeError):
    pass


class RateLimited(RuntimeError):
    pass


class Maintenance(RuntimeError):
    pass


class ExchangeGateway(Protocol):
    def get_market_price(self, symbol: str) -> Decimal: ...
    def submit_market_buy(self, symbol: str, quote_amount: Decimal, client_order_id: str) -> ExchangeOrder: ...
    def query_order_by_client_id(self, symbol: str, client_order_id: str) -> ExchangeOrder | None: ...
    def cancel_remainder(self, symbol: str, client_order_id: str) -> None: ...
    def get_free_base_balance(self, symbol: str) -> Decimal: ...
    def place_protection(self, symbol: str, base_qty: Decimal, stop_price: Decimal, take_profit_price: Decimal, client_order_id: str) -> str: ...
    def protection_exists(self, symbol: str, protection_id: str) -> bool: ...
    def protection_by_client_id(self, symbol: str, client_order_id: str) -> str | None: ...
    def protection_details_by_client_id(self, symbol: str, client_order_id: str) -> ExchangeProtection | None: ...
    def cancel_protection_by_client_id(self, symbol: str, client_order_id: str) -> None: ...
    def emergency_market_sell(self, symbol: str, base_qty: Decimal, client_order_id: str) -> ExchangeOrder: ...


@dataclass
class SimulatedExchange:
    price: Decimal
    partial_fill_ratio: Decimal = Decimal("1")
    fail_protection: bool = False
    ambiguous_submit: bool = False
    commission_rate: Decimal = Decimal("0.001")
    commission_asset_mode: str = "BASE"  # BASE or QUOTE
    symbol_status: str = "TRADING"
    reject_entry: bool = False
    maintenance: bool = False
    rate_limited: bool = False

    def __post_init__(self):
        self.orders: dict[str, ExchangeOrder] = {}
        self.protections: set[str] = set()
        self.protection_clients: dict[str,str] = {}
        self.protection_qty: dict[str,Decimal] = {}
        self.base_balance = Decimal("0")
        self.call_count = 0
        # v0.8.1: submitters and the ProtectionSupervisor reach this object from
        # different threads. Read-modify-write on balances/dicts must be atomic.
        self._lock = threading.RLock()

    def _guard(self):
        self.call_count += 1
        if self.maintenance:
            raise Maintenance("Simulated exchange maintenance")
        if self.rate_limited:
            raise RateLimited("Simulated rate limit")
        if self.symbol_status != "TRADING":
            raise ExchangeRejected(f"Symbol status is {self.symbol_status}")

    def get_market_price(self, symbol: str) -> Decimal:
        with self._lock:
            self._guard()
            return self.price

    def submit_market_buy(self, symbol: str, quote_amount: Decimal, client_order_id: str) -> ExchangeOrder:
        with self._lock:
            self._guard()
            if self.reject_entry:
                raise ExchangeRejected("Simulated entry rejection")
            if client_order_id in self.orders:
                return self.orders[client_order_id]

            filled_quote = quote_amount * self.partial_fill_ratio
            gross_qty = filled_quote / self.price
            commission = Decimal("0")
            commission_asset = None
            net_qty = gross_qty
            if self.commission_asset_mode == "BASE":
                commission = gross_qty * self.commission_rate
                commission_asset = symbol.replace("USDT", "")
                net_qty = gross_qty - commission
            elif self.commission_asset_mode == "QUOTE":
                commission = filled_quote * self.commission_rate
                commission_asset = "USDT"

            self.base_balance += net_qty
            status = "FILLED" if self.partial_fill_ratio == Decimal("1") else "PARTIALLY_FILLED"
            order = ExchangeOrder(
                client_order_id=client_order_id,
                symbol=symbol,
                status=status,
                requested_quote_amount=quote_amount,
                filled_base_qty=gross_qty,
                avg_fill_price=self.price,
                commission_amount=commission,
                commission_asset=commission_asset,
            )
            self.orders[client_order_id] = order

            if self.ambiguous_submit:
                raise UnknownSubmissionState("Simulated timeout after exchange accepted order")
            return order

    def query_order_by_client_id(self, symbol: str, client_order_id: str) -> ExchangeOrder | None:
        with self._lock:
            self._guard()
            order = self.orders.get(client_order_id)
            if order and order.symbol == symbol:
                return order
            return None

    def cancel_remainder(self, symbol: str, client_order_id: str) -> None:
        with self._lock:
            self._guard()
            # Demo: remaining unfilled quantity is considered canceled.

    def get_free_base_balance(self, symbol: str) -> Decimal:
        with self._lock:
            self._guard()
            return self.base_balance

    def place_protection(self, symbol: str, base_qty: Decimal, stop_price: Decimal, take_profit_price: Decimal, client_order_id: str) -> str:
        with self._lock:
            self._guard()
            if self.fail_protection:
                raise ExchangeRejected("Simulated protection failure")
            if base_qty <= 0 or base_qty > self.base_balance:
                raise ExchangeRejected("Insufficient free base balance")
            if client_order_id in self.protection_clients:
                return self.protection_clients[client_order_id]
            protection_id = f"prot-{client_order_id}"
            self.protections.add(protection_id)
            self.protection_clients[client_order_id] = protection_id
            self.protection_qty[client_order_id] = Decimal(base_qty)
            return protection_id

    def protection_exists(self, symbol: str, protection_id: str) -> bool:
        with self._lock:
            self._guard()
            return protection_id in self.protections

    def protection_by_client_id(self, symbol: str, client_order_id: str) -> str | None:
        with self._lock:
            self._guard()
            return self.protection_clients.get(client_order_id)

    def protection_details_by_client_id(self, symbol: str, client_order_id: str) -> ExchangeProtection | None:
        with self._lock:
            self._guard()
            pid = self.protection_clients.get(client_order_id)
            if not pid or pid not in self.protections:
                return None
            return ExchangeProtection(pid, client_order_id, symbol, self.protection_qty[client_order_id], True)

    def cancel_protection_by_client_id(self, symbol: str, client_order_id: str) -> None:
        with self._lock:
            self._guard()
            pid = self.protection_clients.get(client_order_id)
            if pid:
                self.protections.discard(pid)

    def emergency_market_sell(self, symbol: str, base_qty: Decimal, client_order_id: str) -> ExchangeOrder:
        with self._lock:
            self._guard()
            if client_order_id in self.orders:
                return self.orders[client_order_id]
            sell_qty = min(base_qty, self.base_balance)
            self.base_balance -= sell_qty
            order = ExchangeOrder(
                client_order_id=client_order_id,
                symbol=symbol,
                status="FILLED",
                requested_quote_amount=sell_qty * self.price,
                filled_base_qty=sell_qty,
                avg_fill_price=self.price,
            )
            self.orders[client_order_id] = order
            return order

=== FILE: src/shata_trader/execution.py ===
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

=== FILE: src/shata_trader/fenced_gateway.py ===
from __future__ import annotations


class FencedExchangeFacade:
    """Fence immediately before/after external I/O and prioritize safety requests."""
    _PRIORITY={
        'emergency_market_sell':0,
        'place_protection':0,
        'protection_details_by_client_id':0,
        'query_order_by_client_id':1,
        'cancel_remainder':1,
        'submit_market_buy':2,
        'get_free_base_balance':2,
        'get_market_price':5,
    }
    def __init__(self,raw_exchange,lease,holder_id:str,epoch:int,rate_governor=None):
        self.__raw=raw_exchange;self._lease=lease;self._holder=holder_id;self._epoch=int(epoch);self._rate_governor=rate_governor
    def _call(self,name,*args,**kwargs):
        self._lease.assert_epoch('execution-core',self._holder,self._epoch)
        if self._rate_governor is not None:self._rate_governor.acquire(priority=self._PRIORITY.get(name,3))
        result=getattr(self.__raw,name)(*args,**kwargs)
        self._lease.assert_epoch('execution-core',self._holder,self._epoch)
        return result
    def get_market_price(self,*a,**k):return self._call('get_market_price',*a,**k)
    def submit_market_buy(self,*a,**k):return self._call('submit_market_buy',*a,**k)
    def query_order_by_client_id(self,*a,**k):return self._call('query_order_by_client_id',*a,**k)
    def cancel_remainder(self,*a,**k):return self._call('cancel_remainder',*a,**k)
    def get_free_base_balance(self,*a,**k):return self._call('get_free_base_balance',*a,**k)
    def place_protection(self,*a,**k):return self._call('place_protection',*a,**k)
    def protection_details_by_client_id(self,*a,**k):return self._call('protection_details_by_client_id',*a,**k)
    def emergency_market_sell(self,*a,**k):return self._call('emergency_market_sell',*a,**k)

=== FILE: src/shata_trader/idempotency.py ===
import sqlite3,threading
from pathlib import Path
class DuplicateIntent(RuntimeError):pass
class IdempotencyStore:
    def __init__(self,db_path:str|Path=':memory:'):
        self.db_path=str(db_path);self._lock=threading.RLock();self.conn=sqlite3.connect(self.db_path,timeout=10,isolation_level=None,check_same_thread=False)
        self.conn.execute('PRAGMA busy_timeout=10000')
        if self.db_path!=':memory:':self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=FULL');self.conn.execute('CREATE TABLE IF NOT EXISTS intents(trade_intent_id TEXT PRIMARY KEY,first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)')
    def claim(self,trade_intent_id:str):
        try:
            with self._lock:self.conn.execute('INSERT INTO intents(trade_intent_id) VALUES(?)',(trade_intent_id,))
        except sqlite3.IntegrityError as exc:raise DuplicateIntent(trade_intent_id) from exc
    def close(self):self.conn.close()


def _del_idempotency(self):
    try:
        self.close()
    except Exception:
        pass

IdempotencyStore.__del__ = _del_idempotency

=== FILE: src/shata_trader/lease.py ===
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

=== FILE: src/shata_trader/lease_supervisor.py ===
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

=== FILE: src/shata_trader/ledger.py ===
from __future__ import annotations

import json, sqlite3, threading
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from .domain import TradeIntent, Side

TERMINAL={'CLOSED','REJECTED','CANCELED','EXPIRED'}

class LedgerAuthorityRequired(RuntimeError): pass

class TradeLedger:
    """Durable WAL/state ledger.

    Mutations require a per-caller authority context.  Authority is never stored as
    mutable global state on the ledger object, so two engines sharing one ledger
    cannot borrow each other's fencing epoch.
    """
    def __init__(self, db_path: str | Path=':memory:'):
        self.db_path=str(db_path); self._lock=threading.RLock()
        self.conn=sqlite3.connect(self.db_path, timeout=10, isolation_level=None, check_same_thread=False)
        self.conn.execute('PRAGMA busy_timeout=10000')
        if self.db_path!=':memory:': self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=FULL')
        self.conn.execute("""CREATE TABLE IF NOT EXISTS trades(
          intent_id TEXT PRIMARY KEY, payload TEXT NOT NULL, state TEXT NOT NULL,
          entry_client_order_id TEXT NOT NULL, epoch INTEGER NOT NULL,
          side_effect_prepared INTEGER NOT NULL DEFAULT 0,
          last_error TEXT, updated_at TEXT NOT NULL,
          protection_expected_qty TEXT,
          protection_actual_qty TEXT,
          protection_verified_at TEXT)""")
        cols={r[1] for r in self.conn.execute('PRAGMA table_info(trades)').fetchall()}
        for col,typ in [('protection_expected_qty','TEXT'),('protection_actual_qty','TEXT'),('protection_verified_at','TEXT')]:
            if col not in cols:self.conn.execute(f'ALTER TABLE trades ADD COLUMN {col} {typ}')
        self.conn.execute("""CREATE TABLE IF NOT EXISTS transitions(
          id INTEGER PRIMARY KEY AUTOINCREMENT, intent_id TEXT NOT NULL,
          from_state TEXT, to_state TEXT NOT NULL, epoch INTEGER, ts TEXT NOT NULL)""")
        tcols={r[1] for r in self.conn.execute('PRAGMA table_info(transitions)').fetchall()}
        if 'epoch' not in tcols: self.conn.execute('ALTER TABLE transitions ADD COLUMN epoch INTEGER')

    def scoped(self, validator, epoch:int):
        return BoundTradeLedger(self, validator, int(epoch))

    @staticmethod
    def _payload(i:TradeIntent):
        return json.dumps({'trade_intent_id':i.trade_intent_id,'strategy_id':i.strategy_id,'strategy_version':i.strategy_version,
          'risk_policy_version':i.risk_policy_version,'symbol':i.symbol,'side':i.side.value,'quote_amount':str(i.quote_amount),
          'reference_entry_price':str(i.reference_entry_price),'stop_price':str(i.stop_price),'take_profit_price':str(i.take_profit_price),
          'max_entry_deviation_pct':str(i.max_entry_deviation_pct),'created_at':i.created_at.isoformat(),'expires_at':i.expires_at.isoformat()},sort_keys=True)

    @staticmethod
    def intent_from_payload(payload:str)->TradeIntent:
        d=json.loads(payload)
        return TradeIntent(trade_intent_id=d['trade_intent_id'],strategy_id=d['strategy_id'],strategy_version=d['strategy_version'],
          risk_policy_version=int(d['risk_policy_version']),symbol=d['symbol'],side=Side(d['side']),quote_amount=Decimal(d['quote_amount']),
          reference_entry_price=Decimal(d['reference_entry_price']),stop_price=Decimal(d['stop_price']),take_profit_price=Decimal(d['take_profit_price']),
          max_entry_deviation_pct=Decimal(d['max_entry_deviation_pct']),created_at=datetime.fromisoformat(d['created_at']),expires_at=datetime.fromisoformat(d['expires_at']))

    @staticmethod
    def _auth(authority):
        if authority is None:raise LedgerAuthorityRequired('Ledger mutation requires per-caller execution authority')
        validator,epoch=authority;validator();return int(epoch)

    def ensure(self,i:TradeIntent,entry_id:str,authority):
        epoch=self._auth(authority);now=datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.conn.execute("""INSERT OR IGNORE INTO trades(intent_id,payload,state,entry_client_order_id,epoch,updated_at)
              VALUES(?,?,?,?,?,?)""",(i.trade_intent_id,self._payload(i),'CREATED',entry_id,epoch,now))
        return self.get(i.trade_intent_id)

    def get(self,intent_id:str):
        with self._lock:
            r=self.conn.execute('''SELECT intent_id,payload,state,entry_client_order_id,epoch,side_effect_prepared,last_error,updated_at,
              protection_expected_qty,protection_actual_qty,protection_verified_at FROM trades WHERE intent_id=?''',(intent_id,)).fetchone()
        if not r:return None
        keys=['intent_id','payload','state','entry_client_order_id','epoch','side_effect_prepared','last_error','updated_at','protection_expected_qty','protection_actual_qty','protection_verified_at'];return dict(zip(keys,r))

    def _set_state(self,intent_id:str,new:str,authority,error=None,expected_old=None):
        epoch=self._auth(authority);now=datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                row=self.conn.execute('SELECT state,epoch FROM trades WHERE intent_id=?',(intent_id,)).fetchone()
                if not row: raise RuntimeError(f'Unknown intent {intent_id}')
                old,row_epoch=row
                if int(row_epoch)>epoch: raise LedgerAuthorityRequired(f'Stale ledger epoch {epoch}; row={row_epoch}')
                if expected_old is not None and old!=expected_old: raise RuntimeError(f'Ledger state drift: expected {expected_old}, durable {old}')
                if old!=new:
                    self.conn.execute('UPDATE trades SET state=?,last_error=?,updated_at=?,epoch=? WHERE intent_id=?',(new,error,now,epoch,intent_id))
                    self.conn.execute('INSERT INTO transitions(intent_id,from_state,to_state,epoch,ts) VALUES(?,?,?,?,?)',(intent_id,old,new,epoch,now))
                self.conn.execute('COMMIT')
            except Exception:
                self.conn.execute('ROLLBACK'); raise

    def transition(self,intent_id:str,old:str,new:str,authority,error=None):self._set_state(intent_id,new,authority,error,expected_old=old)
    def recovery_set_state(self,intent_id:str,new:str,authority,error=None):self._set_state(intent_id,new,authority,error,expected_old=None)

    def mark_dispatch_prepared(self,intent_id:str,authority):
        epoch=self._auth(authority);now=datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                row=self.conn.execute('SELECT state,epoch FROM trades WHERE intent_id=?',(intent_id,)).fetchone()
                if not row or row[0]!='RISK_APPROVED': raise RuntimeError('Dispatch WAL requires durable RISK_APPROVED state')
                if int(row[1])>epoch: raise LedgerAuthorityRequired('Stale dispatch epoch')
                self.conn.execute('UPDATE trades SET side_effect_prepared=1,epoch=?,state=?,updated_at=? WHERE intent_id=?',(epoch,'SUBMITTED',now,intent_id))
                self.conn.execute('INSERT INTO transitions(intent_id,from_state,to_state,epoch,ts) VALUES(?,?,?,?,?)',(intent_id,'RISK_APPROVED','SUBMITTED',epoch,now))
                self.conn.execute('COMMIT')
            except Exception:
                self.conn.execute('ROLLBACK'); raise

    def mark_error(self,intent_id:str,error:str,authority):
        epoch=self._auth(authority)
        with self._lock:self.conn.execute('UPDATE trades SET last_error=?,epoch=?,updated_at=? WHERE intent_id=?',(error,epoch,datetime.now(timezone.utc).isoformat(),intent_id))

    def mark_protection_verified(self,intent_id:str,expected_qty:Decimal,actual_qty:Decimal,authority):
        epoch=self._auth(authority);now=datetime.now(timezone.utc).isoformat()
        with self._lock:self.conn.execute('''UPDATE trades SET protection_expected_qty=?,protection_actual_qty=?,protection_verified_at=?,epoch=?,updated_at=? WHERE intent_id=?''',
            (str(expected_qty),str(actual_qty),now,epoch,now,intent_id))

    def clear_protection_verification(self,intent_id:str,authority,error=None):
        epoch=self._auth(authority);now=datetime.now(timezone.utc).isoformat()
        with self._lock:self.conn.execute('''UPDATE trades SET protection_verified_at=NULL,last_error=?,epoch=?,updated_at=? WHERE intent_id=?''',(error,epoch,now,intent_id))

    def get_by_entry_client_id(self,client_id:str):
        with self._lock:r=self.conn.execute('''SELECT intent_id,payload,state,entry_client_order_id,epoch,side_effect_prepared,last_error,updated_at,
              protection_expected_qty,protection_actual_qty,protection_verified_at FROM trades WHERE entry_client_order_id=?''',(client_id,)).fetchone()
        if not r:return None
        keys=['intent_id','payload','state','entry_client_order_id','epoch','side_effect_prepared','last_error','updated_at','protection_expected_qty','protection_actual_qty','protection_verified_at'];return dict(zip(keys,r))

    def nonterminal_records(self):
        with self._lock:rows=self.conn.execute('''SELECT intent_id,payload,state,entry_client_order_id,epoch,side_effect_prepared,last_error,updated_at,
              protection_expected_qty,protection_actual_qty,protection_verified_at FROM trades WHERE state NOT IN (?,?,?,?) ORDER BY updated_at''',tuple(TERMINAL)).fetchall()
        keys=['intent_id','payload','state','entry_client_order_id','epoch','side_effect_prepared','last_error','updated_at','protection_expected_qty','protection_actual_qty','protection_verified_at'];return [dict(zip(keys,r)) for r in rows]
    def protected_records(self):
        with self._lock:rows=self.conn.execute('''SELECT intent_id,payload,state,entry_client_order_id,epoch,side_effect_prepared,last_error,updated_at,
              protection_expected_qty,protection_actual_qty,protection_verified_at FROM trades WHERE state IN ('PROTECTED','PARTIALLY_PROTECTED') ORDER BY updated_at''').fetchall()
        keys=['intent_id','payload','state','entry_client_order_id','epoch','side_effect_prepared','last_error','updated_at','protection_expected_qty','protection_actual_qty','protection_verified_at'];return [dict(zip(keys,r)) for r in rows]
    def nonterminal(self):return [(r['intent_id'],r['state'],r['entry_client_order_id'],r['epoch'],r['side_effect_prepared']) for r in self.nonterminal_records()]

    def close(self):
        conn=getattr(self,'conn',None)
        if conn is not None:
            try:conn.close()
            except Exception:pass
            self.conn=None
    def __del__(self):
        try:self.close()
        except Exception:pass


class BoundTradeLedger:
    """Per-engine capability view over a shared TradeLedger."""
    def __init__(self,raw:TradeLedger,validator,epoch:int):self.raw=raw;self.validator=validator;self.epoch=int(epoch)
    @property
    def conn(self):return self.raw.conn
    def _a(self):return (self.validator,self.epoch)
    def ensure(self,i,entry_id,epoch=None):
        if epoch is not None and int(epoch)!=self.epoch:raise LedgerAuthorityRequired('Epoch mismatch')
        return self.raw.ensure(i,entry_id,self._a())
    def get(self,*a,**k):return self.raw.get(*a,**k)
    def get_by_entry_client_id(self,*a,**k):return self.raw.get_by_entry_client_id(*a,**k)
    def nonterminal_records(self):return self.raw.nonterminal_records()
    def protected_records(self):return self.raw.protected_records()
    def nonterminal(self):return self.raw.nonterminal()
    def intent_from_payload(self,p):return self.raw.intent_from_payload(p)
    def transition(self,intent_id,old,new,error=None):return self.raw.transition(intent_id,old,new,self._a(),error)
    def recovery_set_state(self,intent_id,new,error=None):return self.raw.recovery_set_state(intent_id,new,self._a(),error)
    def mark_dispatch_prepared(self,intent_id,epoch=None):
        if epoch is not None and int(epoch)!=self.epoch:raise LedgerAuthorityRequired('Dispatch epoch mismatch')
        return self.raw.mark_dispatch_prepared(intent_id,self._a())
    def mark_error(self,intent_id,error):return self.raw.mark_error(intent_id,error,self._a())
    def mark_protection_verified(self,intent_id,expected_qty,actual_qty):return self.raw.mark_protection_verified(intent_id,expected_qty,actual_qty,self._a())
    def clear_protection_verification(self,intent_id,error=None):return self.raw.clear_protection_verification(intent_id,self._a(),error)
    def close(self):return self.raw.close()

=== FILE: src/shata_trader/persistent_exchange.py ===
from __future__ import annotations

import sqlite3
import threading
import time
from decimal import Decimal
from pathlib import Path

from .db import ThreadLocalSqlite
from .domain import ExchangeOrder, ExchangeProtection
from .exchange import UnknownSubmissionState, ExchangeRejected


class PersistentSimulatedExchange:
    """Persistent adversarial exchange simulator.

    v0.6 models independent balances per symbol, partial fills, base-asset fees,
    protection-reserved balance, query-visibility lag, zero-quantity rejection,
    multiple simultaneous protected positions, and crash-surviving state.
    """

    def __init__(
        self,
        db_path: str | Path,
        price: Decimal = Decimal("100"),
        commission_rate: Decimal = Decimal("0.001"),
        partial_fill_ratio: Decimal = Decimal("1"),
    ):
        self.db_path = str(db_path)
        self.price = Decimal(price)
        self.commission_rate = Decimal(commission_rate)
        self.partial_fill_ratio = Decimal(partial_fill_ratio)
        # v0.8.1: one connection per thread. Submitters and the ProtectionSupervisor
        # reach this object from different threads simultaneously.
        self._db = ThreadLocalSqlite(self.db_path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS balances(symbol TEXT PRIMARY KEY,qty TEXT NOT NULL)"
        )
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS orders(
            client_id TEXT PRIMARY KEY,symbol TEXT NOT NULL,status TEXT NOT NULL,
            requested_quote TEXT NOT NULL,filled_qty TEXT NOT NULL,avg_price TEXT NOT NULL,
            commission TEXT NOT NULL,commission_asset TEXT)"""
        )
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS protections(
            protection_id TEXT PRIMARY KEY,client_id TEXT UNIQUE NOT NULL,
            symbol TEXT NOT NULL,base_qty TEXT NOT NULL,active INTEGER NOT NULL)"""
        )
        self.fail_protection = False
        self.fail_emergency_exit = False
        self.ambiguous_after_accept = False
        self.query_visibility_lag_calls = 0
        self.latency_seconds = 0.0
        self._query_counts = {}
        self._counts_lock = threading.Lock()

    @property
    def conn(self):
        return self._db.conn

    def _sleep(self):
        if self.latency_seconds > 0:
            time.sleep(self.latency_seconds)

    def _ensure_balance(self, symbol):
        self.conn.execute(
            "INSERT OR IGNORE INTO balances(symbol,qty) VALUES(?,?)", (symbol, "0")
        )

    def _balance(self, symbol="TESTUSDT"):
        self._ensure_balance(symbol)
        return Decimal(
            self.conn.execute(
                "SELECT qty FROM balances WHERE symbol=?", (symbol,)
            ).fetchone()[0]
        )

    def _set_balance(self, x, symbol="TESTUSDT"):
        self._ensure_balance(symbol)
        self.conn.execute(
            "UPDATE balances SET qty=? WHERE symbol=?", (str(Decimal(x)), symbol)
        )

    def external_adjust_balance(self, symbol, delta):
        """Test-only out-of-band wallet change, e.g. manual withdrawal/deposit."""
        self._set_balance(max(Decimal("0"), self._balance(symbol) + Decimal(delta)), symbol)

    def _reserved(self, symbol):
        rows = self.conn.execute(
            "SELECT base_qty FROM protections WHERE symbol=? AND active=1", (symbol,)
        ).fetchall()
        return sum((Decimal(r[0]) for r in rows), Decimal("0"))

    def get_market_price(self, symbol):
        self._sleep()
        return self.price

    def submit_market_buy(self, symbol, quote_amount, client_order_id):
        self._sleep()
        existing = self.query_order_by_client_id(
            symbol, client_order_id, ignore_visibility=True
        )
        if existing:
            return existing
        filled_quote = Decimal(quote_amount) * self.partial_fill_ratio
        gross = filled_quote / self.price
        commission = gross * self.commission_rate
        net = gross - commission
        status = "FILLED" if self.partial_fill_ratio == Decimal("1") else "PARTIALLY_FILLED"
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?)",
                (
                    client_order_id,
                    symbol,
                    status,
                    str(quote_amount),
                    str(gross),
                    str(self.price),
                    str(commission),
                    symbol.replace("USDT", ""),
                ),
            )
            self._set_balance(self._balance(symbol) + net, symbol)
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        order = self.query_order_by_client_id(
            symbol, client_order_id, ignore_visibility=True
        )
        if self.ambiguous_after_accept:
            raise UnknownSubmissionState("accepted then response lost")
        return order

    def query_order_by_client_id(self, symbol, client_order_id, ignore_visibility=False):
        self._sleep()
        if not ignore_visibility and self.query_visibility_lag_calls > 0:
            with self._counts_lock:
                n = self._query_counts.get(client_order_id, 0)
                self._query_counts[client_order_id] = n + 1
            if n < self.query_visibility_lag_calls:
                return None
        r = self.conn.execute(
            """SELECT client_id,symbol,status,requested_quote,filled_qty,avg_price,
               commission,commission_asset FROM orders WHERE client_id=? AND symbol=?""",
            (client_order_id, symbol),
        ).fetchone()
        if not r:
            return None
        return ExchangeOrder(
            r[0], r[1], r[2], Decimal(r[3]), Decimal(r[4]), Decimal(r[5]), Decimal(r[6]), r[7]
        )

    def cancel_remainder(self, symbol, client_order_id):
        self._sleep()
        return None

    def get_free_base_balance(self, symbol):
        self._sleep()
        return max(Decimal("0"), self._balance(symbol) - self._reserved(symbol))

    def place_protection(self, symbol, base_qty, stop_price, take_profit_price, client_order_id):
        self._sleep()
        q = Decimal(base_qty)
        row = self.conn.execute(
            "SELECT protection_id FROM protections WHERE client_id=?", (client_order_id,)
        ).fetchone()
        if row:
            return row[0]
        if self.fail_protection:
            raise ExchangeRejected("protection rejected")
        if q <= 0:
            raise ExchangeRejected("zero protection quantity")
        if q > self.get_free_base_balance(symbol):
            raise ExchangeRejected("insufficient free balance")
        pid = "P-" + client_order_id
        self.conn.execute(
            "INSERT INTO protections VALUES(?,?,?,?,1)",
            (pid, client_order_id, symbol, str(q)),
        )
        return pid

    def protection_exists(self, symbol, protection_id):
        self._sleep()
        return (
            self.conn.execute(
                "SELECT 1 FROM protections WHERE protection_id=? AND symbol=? AND active=1",
                (protection_id, symbol),
            ).fetchone()
            is not None
        )

    def protection_by_client_id(self, symbol, client_order_id):
        self._sleep()
        r = self.conn.execute(
            "SELECT protection_id FROM protections WHERE client_id=? AND symbol=? AND active=1",
            (client_order_id, symbol),
        ).fetchone()
        return r[0] if r else None

    def protection_details_by_client_id(self, symbol, client_order_id):
        self._sleep()
        r = self.conn.execute(
            "SELECT protection_id,base_qty,active FROM protections WHERE client_id=? AND symbol=?",
            (client_order_id, symbol),
        ).fetchone()
        if not r or not int(r[2]):
            return None
        return ExchangeProtection(r[0], client_order_id, symbol, Decimal(r[1]), True)

    def cancel_protection_by_client_id(self, symbol, client_order_id):
        self._sleep()
        self.conn.execute(
            "UPDATE protections SET active=0 WHERE client_id=? AND symbol=?",
            (client_order_id, symbol),
        )

    def emergency_market_sell(self, symbol, base_qty, client_order_id):
        self._sleep()
        existing = self.query_order_by_client_id(
            symbol, client_order_id, ignore_visibility=True
        )
        if existing:
            return existing
        if self.fail_emergency_exit:
            raise ExchangeRejected("emergency exit rejected")
        q = min(Decimal(base_qty), self.get_free_base_balance(symbol))
        if q <= 0:
            raise ExchangeRejected("zero emergency exit quantity")
        self._set_balance(self._balance(symbol) - q, symbol)
        self.conn.execute(
            "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?)",
            (
                client_order_id,
                symbol,
                "FILLED",
                str(q * self.price),
                str(q),
                str(self.price),
                "0",
                None,
            ),
        )
        return self.query_order_by_client_id(
            symbol, client_order_id, ignore_visibility=True
        )

    def all_orders(self):
        return self.conn.execute(
            "SELECT client_id,status FROM orders ORDER BY rowid"
        ).fetchall()

    def active_protections(self, symbol=None):
        if symbol is None:
            return self.conn.execute(
                "SELECT protection_id,base_qty FROM protections WHERE active=1"
            ).fetchall()
        return self.conn.execute(
            "SELECT protection_id,base_qty FROM protections WHERE active=1 AND symbol=?",
            (symbol,),
        ).fetchall()

    def close(self):
        db = getattr(self, "_db", None)
        if db is not None:
            db.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

=== FILE: src/shata_trader/protection_supervisor.py ===
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

=== FILE: src/shata_trader/rate_governor.py ===
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

=== FILE: src/shata_trader/reconciliation.py ===
from dataclasses import dataclass

from .domain import ExchangeOrder
from .exchange import ExchangeGateway


@dataclass(frozen=True)
class ReconciliationResult:
    found: bool
    order: ExchangeOrder | None
    note: str


class ReconciliationEngine:
    def __init__(self, exchange: ExchangeGateway):
        self.exchange = exchange

    def reconcile_order(self, symbol: str, client_order_id: str) -> ReconciliationResult:
        order = self.exchange.query_order_by_client_id(symbol, client_order_id)
        if order is None:
            return ReconciliationResult(False, None, "Order not found on exchange")
        return ReconciliationResult(True, order, f"Exchange says {order.status}")

=== FILE: src/shata_trader/risk_engine.py ===
from datetime import datetime, timezone
from decimal import Decimal

from .domain import PortfolioSnapshot, RiskDecision, RiskPolicy, Side, TradeIntent

ZERO = Decimal("0")


class DeterministicRiskEngine:
    def __init__(self, policy: RiskPolicy):
        self.policy = policy

    def evaluate(
        self,
        intent: TradeIntent,
        portfolio: PortfolioSnapshot,
        fresh_market_price: Decimal,
        now: datetime | None = None,
    ) -> RiskDecision:
        now = now or datetime.now(timezone.utc)

        if intent.risk_policy_version != self.policy.version:
            return RiskDecision(False, "Risk policy version mismatch", ZERO)

        if intent.side != Side.BUY:
            return RiskDecision(False, "Phase 0 supports BUY intents only", ZERO)

        if intent.is_expired(now):
            return RiskDecision(False, "Trade intent expired", ZERO)

        intent_age = (now - intent.created_at).total_seconds()
        if intent_age > self.policy.max_intent_age_seconds:
            return RiskDecision(False, "Trade intent exceeded maximum age", ZERO)

        if portfolio.reconciled_at is not None:
            age = (now - portfolio.reconciled_at).total_seconds()
            if age > self.policy.max_reconciliation_age_seconds:
                return RiskDecision(False, "Portfolio reconciliation state is stale", ZERO)

        if intent.quote_amount <= ZERO or portfolio.portfolio_value <= ZERO:
            return RiskDecision(False, "Invalid amount or portfolio value", ZERO)

        if not (intent.stop_price < intent.reference_entry_price < intent.take_profit_price):
            return RiskDecision(False, "Invalid stop/entry/target relationship", ZERO)

        deviation = abs(fresh_market_price - intent.reference_entry_price) / intent.reference_entry_price
        max_dev = min(intent.max_entry_deviation_pct, self.policy.max_entry_deviation_pct)
        if deviation > max_dev:
            return RiskDecision(False, "Fresh market price exceeded allowed deviation", ZERO)

        risk_per_unit = intent.reference_entry_price - intent.stop_price
        reward_per_unit = intent.take_profit_price - intent.reference_entry_price
        rr = reward_per_unit / risk_per_unit
        if rr < self.policy.min_risk_reward:
            return RiskDecision(False, "Risk/reward below policy minimum", ZERO)

        capital_cap = portfolio.portfolio_value * self.policy.max_position_allocation_pct
        exposure_room = (
            portfolio.portfolio_value * self.policy.max_portfolio_exposure_pct
            - portfolio.current_exposure
        )
        exposure_room = max(exposure_room, ZERO)

        risk_fraction_of_position = risk_per_unit / intent.reference_entry_price
        if risk_fraction_of_position <= ZERO:
            return RiskDecision(False, "Invalid risk fraction", ZERO)

        max_loss = portfolio.portfolio_value * self.policy.max_risk_per_trade_pct
        risk_based_cap = max_loss / risk_fraction_of_position

        max_quote = min(capital_cap, exposure_room, portfolio.quote_balance, risk_based_cap)
        if max_quote <= ZERO:
            return RiskDecision(False, "No available risk/exposure capacity", ZERO)

        if intent.quote_amount > max_quote:
            return RiskDecision(False, "Requested quote amount exceeds risk limits", max_quote)

        return RiskDecision(True, "PASS", max_quote)

=== FILE: src/shata_trader/runtime.py ===
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

=== FILE: src/shata_trader/runtime_watchdog.py ===
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

=== FILE: src/shata_trader/state_machine.py ===
from .domain import TradeState


_ALLOWED = {
    TradeState.CREATED: {TradeState.RISK_APPROVED, TradeState.REJECTED, TradeState.HALTED},
    TradeState.RISK_APPROVED: {TradeState.SUBMITTED, TradeState.REJECTED, TradeState.HALTED},
    TradeState.SUBMITTED: {TradeState.ACKNOWLEDGED, TradeState.UNKNOWN, TradeState.REJECTED, TradeState.HALTED},
    TradeState.ACKNOWLEDGED: {
        TradeState.PARTIALLY_FILLED, TradeState.FILLED, TradeState.CANCELED,
        TradeState.EXPIRED, TradeState.UNKNOWN, TradeState.HALTED
    },
    TradeState.PARTIALLY_FILLED: {
        TradeState.PARTIAL_PROTECTION_PENDING, TradeState.UNKNOWN,
        TradeState.EMERGENCY_EXIT, TradeState.HALTED
    },
    TradeState.PARTIAL_PROTECTION_PENDING: {
        TradeState.PARTIALLY_PROTECTED, TradeState.UNDER_PROTECTED, TradeState.PROTECTION_FAILED,
        TradeState.UNKNOWN, TradeState.EMERGENCY_EXIT, TradeState.HALTED
    },
    TradeState.PARTIALLY_PROTECTED: {TradeState.HALTED, TradeState.EXIT_PENDING, TradeState.UNKNOWN, TradeState.UNDER_PROTECTED},
    TradeState.FILLED: {
        TradeState.PROTECTION_PENDING, TradeState.PROTECTION_FAILED,
        TradeState.EMERGENCY_EXIT, TradeState.HALTED
    },
    TradeState.PROTECTION_PENDING: {
        TradeState.PROTECTED, TradeState.UNDER_PROTECTED, TradeState.PROTECTION_FAILED,
        TradeState.UNKNOWN, TradeState.EMERGENCY_EXIT, TradeState.HALTED
    },
    TradeState.PROTECTED: {TradeState.EXIT_PENDING, TradeState.UNKNOWN, TradeState.UNDER_PROTECTED, TradeState.HALTED},
    TradeState.EXIT_PENDING: {TradeState.CLOSED, TradeState.UNKNOWN, TradeState.HALTED},
    TradeState.UNKNOWN: {TradeState.RECONCILING, TradeState.HALTED},
    TradeState.RECONCILING: {
        TradeState.ACKNOWLEDGED, TradeState.PARTIALLY_FILLED, TradeState.FILLED,
        TradeState.CANCELED, TradeState.EXPIRED,
        TradeState.PARTIAL_PROTECTION_PENDING, TradeState.PARTIALLY_PROTECTED,
        TradeState.PROTECTION_PENDING, TradeState.PROTECTED, TradeState.CLOSED,
        TradeState.PROTECTION_FAILED, TradeState.EMERGENCY_EXIT, TradeState.HALTED,
    },
    TradeState.PROTECTION_FAILED: {TradeState.EMERGENCY_EXIT, TradeState.UNKNOWN, TradeState.HALTED},
    TradeState.UNDER_PROTECTED: {TradeState.EMERGENCY_EXIT, TradeState.UNKNOWN, TradeState.HALTED},
    TradeState.EMERGENCY_EXIT: {TradeState.CLOSED, TradeState.UNKNOWN, TradeState.HALTED},
    TradeState.HALTED: {TradeState.RECONCILING},
    TradeState.REJECTED: set(),
    TradeState.EXPIRED: set(),
    TradeState.CANCELED: set(),
    TradeState.CLOSED: set(),
}


class InvalidTransition(RuntimeError):
    pass


class TradeStateMachine:
    def __init__(self, initial: TradeState = TradeState.CREATED, on_transition=None):
        self.state = initial
        self.history = [initial]
        self.on_transition = on_transition

    def transition(self, new_state: TradeState) -> None:
        if new_state not in _ALLOWED[self.state]:
            raise InvalidTransition(f"{self.state.value} -> {new_state.value} is not allowed")
        old = self.state
        self.state = new_state
        self.history.append(new_state)
        if self.on_transition:
            self.on_transition(old, new_state)

=== FILE: src/shata_trader/strategy.py ===
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid

from .domain import Side, TradeIntent


class DeterministicDemoStrategy:
    """
    Not an alpha strategy.
    Generates a deterministic, structurally valid intent for Phase 0 plumbing tests.
    """

    strategy_id = "phase0-demo-plumbing"
    strategy_version = "0.1.0"

    def create_intent(
        self,
        symbol: str,
        reference_price: Decimal,
        quote_amount: Decimal,
        risk_policy_version: int,
    ) -> TradeIntent:
        now = datetime.now(timezone.utc)
        return TradeIntent(
            trade_intent_id=f"demo-{uuid.uuid4().hex}",
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            risk_policy_version=risk_policy_version,
            symbol=symbol,
            side=Side.BUY,
            quote_amount=quote_amount,
            reference_entry_price=reference_price,
            stop_price=reference_price * Decimal("0.98"),
            take_profit_price=reference_price * Decimal("1.05"),
            max_entry_deviation_pct=Decimal("0.005"),
            created_at=now,
            expires_at=now + timedelta(seconds=30),
        )

=== FILE: src/shata_trader/testing.py ===
"""TEST SUPPORT ONLY. Not imported by runtime/production code."""
from .runtime import TradingCoreRuntime

def boot_submit(engine,intent,portfolio,keep_running=False):
    rt=TradingCoreRuntime(engine);rep=rt.start()
    if rep.unresolved:
        if not keep_running:rt.stop(release_lease=False)
        raise RuntimeError(f'boot unresolved: {rep.states}')
    sm=rt.submit(intent,portfolio)
    if keep_running:return sm,rt
    rt.stop(release_lease=False);return sm

=== FILE: CHANGES_v0.8.4.md ===
# Phase 0 — v0.7 → v0.8

Scope agreed by all three reviewers: N1 · N3 · N2 · supervisory thread-failure chaos.

## N1 — one-shot runtime capability + single-use boot proof
`execution.py`
- `bind_runtime_capability` refuses rebinding unconditionally. A closed gate is no
  longer a rebinding window; it is exactly when a hostile component would try.
- `release_runtime_capability(token)` lets only the current holder hand the engine
  back, so sequential runtime ownership still works.
- `issue_boot_proof(token, unresolved, quarantined)` mints a private `_BootProof`
  object, only for a clean reconciliation, only for the current epoch.
- `grant_boot_authority(token, boot_proof)` now needs both. The proof is
  identity-checked (unforgeable by construction), single-use, and cleared on revoke.

## N3 — closed supervision ring, structural gate
`runtime.py` · `runtime_watchdog.py` · `protection_supervisor.py` · `lease_supervisor.py`
- `TradingCoreRuntime.ready` is a computed property: latched boot result AND every
  supervisory loop healthy AND witness not degraded. A passive reader sees the truth
  with no submit() needed.
- `RuntimeSafetyWatchdog._run` wrapped: the watchdog can no longer die silently, and
  it now also polices the lease supervisor.
- `ProtectionSupervisor` watches the watchdog (`peer_health`). Mutual supervision, so
  no single supervisory thread is a single point of failure.
- `LeaseSupervisor` exposes renewal progress and a `healthy` property that checks the
  real invariant (is authority still valid?) rather than the progress proxy alone.
- **`engine.gate_open`**: the gate consults a live health probe synchronously.
  Found by the new chaos harness against an earlier v0.8 patch: when every supervisory
  thread is dead, nobody is left to call `revoke_boot_authority`, so a latch-only gate
  stayed open while `ready` correctly read False. Fixed structurally, not with a
  fourth watcher — per the hardening-treadmill rule.

## N2 — witness height
`audit.py` · `audit_anchor.py`
- The published witness carries `height`. A witness recorded above the current local
  height means history was truncated: a truncated prefix is itself a valid chain, so
  head-hash comparison alone cannot see it.
- Enforced in `_publish_best_effort`, `sync_anchor`, and `verify(verify_anchor=True)`.
- A height-less witness against a non-empty local chain is treated as a downgrade
  attempt, not a legacy record.
- Limit unchanged and still documented: an attacker who owns both the log and the
  witness is outside what an unkeyed chain can detect. Production needs a signed or
  WORM witness in an independent trust domain.

## Item 4 — `scripts/supervisor_kill_chaos_1000.py`
Randomly kills or stalls one of the three supervisory loops (lease / protection /
watchdog), including a "kill all but one" mode. Asserts readiness degrades within a
bounded window **with no submit() and no manual verify_once()** — that manual call is
what hid N3 in the v0.7 suites. Also asserts the execution gate itself shuts.

## Results

| suite | result |
|---|---|
| `pytest tests/` | 79 passed (71 pre-existing + 8 self-attack) |
| `chaos_1000` | 1000 runs / 0 failures |
| `restart_chaos_1000` | 1000 / 0 |
| `multi_position_chaos_1000` | 1000 / 0 |
| `protection_chaos_1000_fast` | 1000 / 0 |
| `supervisor_kill_chaos_1000` | 1000 / 0 · detection max 0.299s, mean 0.073s, budget 1.5s |
| v0.7 attack replay (12 attacks) | 0/12 succeed (was 3/12) |

## Evidence status

`tests/test_v08_self_attack.py` is written by the builder and carries **lower
evidentiary weight** per REVIEW_PROTOCOL.md §5. Independent regression tests for
N1/N2/N3 are owed by Gemini and ChatGPT and are not present in this package.

## Known open items carried into review

- `exchange.cancel_protection_by_client_id` still has no caller in the engine.
- `RateGovernorTimeout` is raised but never caught by name; a timed-out safety call
  is indistinguishable from an ordinary protection failure in the audit trail.
- `fail_emergency_exit` remains an unused fault injector: emergency-exit failure is
  still unexercised under chaos.
- `_last_cycle_started_monotonic` is written and never read.

## No-Secrets / No-Live-Authority audit (v0.8)

Rule adopted from v0.8 on: every review package must be fully runnable and completely
incapable of reaching real money.

Automated pre-delivery scan, run on this package:

```
grep -rniE "api[_-]?key|api[_-]?secret|secret|password|credential|binance\.com|wss://|https://" src/ scripts/ tests/ config/
  -> 0 matches
grep -rnE "^\s*(import|from)\s+(requests|urllib|http|socket|websocket|aiohttp|ccxt|binance)" src/ scripts/ tests/
  -> 0 matches
```

- Exchange implementations present: `SimulatedExchange`, `PersistentSimulatedExchange`.
- No live adapter exists in the tree at all. Nothing to disable — there is no path.
- `config/risk-policy.example.json` carries `"environment": "DEMO_ONLY"`.
- No network library is imported anywhere in src/, scripts/ or tests/.

Any future patch that adds a network call, an endpoint, a credential read, or a new
dependency must be flagged here explicitly and is a blocking review item.

---

# v0.8.1 — response to ChatGPT Fast Gate NO-GO

## CG-1 — shared SQLite connection across threads — **CONFIRMED, FIXED**

ChatGPT's root-cause hypothesis was correct. Reproduced independently at the
persistence layer, with no runtime involved (6 writer threads + 2 reader threads on
one `PersistentSimulatedExchange`):

```
781 x OperationalError: cannot start a transaction within a transaction
348 x READER InterfaceError: bad parameter or other API misuse
  3 x InterfaceError: bad parameter or other API misuse
```

`sqlite3.InterfaceError: bad parameter or other API misuse` raised from
`persistent_exchange.py:201 protection_details_by_client_id` — exactly the error
ChatGPT observed, surfacing as `PROTECTION_REVERIFY_QUERY_FAILED:InterfaceError`.

**Honest scope note:** the *runtime-level* symptom is genuinely flaky. 100 iterations
of 8 concurrent submitters with a 1ms supervisor interval did not reproduce it here.
The persistence-layer test reproduces it deterministically and is the reliable
detector. ChatGPT saw the runtime-level manifestation; I saw the cause.

### Fix — structural, per REVIEW_PROTOCOL §11

Option 1 of ChatGPT's list: one connection per thread. Chosen over a global exchange
lock because it removes the shared mutable resource rather than guarding it, and it
preserves the rule that no long-lived transaction is held across external I/O — each
thread's transaction is its own.

- **new** `src/shata_trader/db.py` — `ThreadLocalSqlite` / `SharedMemorySqlite`
- `persistent_exchange.py` — `conn` is now a per-thread property; visibility-lag
  counter guarded
- `events.py` — `OrderEventStore` had the same unguarded shared connection and is
  reachable from any `ingest_exchange_event` caller; now per-thread, with the
  multi-statement ingest transaction under a store lock
- `exchange.py` — in-memory `SimulatedExchange` had no lock at all; read-modify-write
  on balances and dicts is now atomic

### Regression

`tests/test_v081_concurrency_regression.py` — 3 tests:
100 iterations of 8 concurrent submitters with a live 1ms-interval supervisor;
a direct cross-thread attack on the exchange persistence layer; a ResourceWarning check.

Verified to **fail on pre-patch code** (`InterfaceError: 3, OperationalError: 909`)
and pass after. BUILDER-WRITTEN — lower evidentiary weight; the independent
regression for CG-1 is still owed.

## B-4 — losing readiness mid-trade discarded the trade result — **HIGH, FIXED**

Found by the full matrix after the CG-1 fix (`chaos_1000` run 965).

`submit()` raised `RuntimeNotReady('Runtime safety authority was lost during trade')`
*after* `engine.process` had already completed. The side effect had happened, but the
exception discarded `sm` — hiding a real, protected position from the only caller able
to act on it. Now `submit()` always returns `sm` once a side effect has occurred, and
degrades readiness / revokes the gate separately.

## B-5 — cold boot could raise instead of failing closed — **MEDIUM, FIXED**

`chaos_1000` run 770: `BootGateClosed: Cannot issue a boot proof without a valid lease`
escaped from `start()` when the lease lapsed between acquire and the end of
reconciliation. `start()` now returns an unresolved `BootReport` tagged
`AUTHORITY_LOST_DURING_BOOT`, matching the existing `WAITING_FOR_LEASE` behaviour.
Boot must never raise at the caller.

## Re-acceptance evidence

| gate item | result |
|---|---|
| 1. full suite | **82 passed** (79 + 3 new concurrency regressions) |
| 2. `chaos_1000` | 1000 / 0 |
| 3. `restart_chaos_1000` | 1000 / 0 |
| 4. `multi_position_chaos_1000` | 1000 / 0 |
| 5. `protection_chaos_1000_fast` | 1000 / 0 |
| 6. `supervisor_kill_chaos_1000` | 1000 / 0 · detection max 0.306s, budget 1.5s |
| 7. new concurrency regression | 3/3, repeated clean passes |
| 8. ResourceWarning check | `-W error::ResourceWarning` → 0 warnings |
| v0.7 attack replay | 0/12 succeed |
| no-secrets scan | 1 match, a code comment in `audit_anchor.py` ("secret key"); 0 network imports |

---

# v0.8.2 — response to ChatGPT CG-2

## CG-2 — supervisor completion-age conflated with stall — **CONFIRMED, FIXED**

ChatGPT's analysis was exactly right. Reproduced 3/3 on this tree with the packaged
reproducer, before any change:

```
FIRST_NOT_READY_SECONDS: 0.502     RUNTIME_READY: False   ENGINE_GATE_OPEN: False
PROTECTION_SUPERVISOR_ALIVE: True  PROTECTION_SUPERVISOR_LAST_ERROR: None
WATCHDOG_LAST_ERROR: PROTECTION_SUPERVISOR_STALLED:0.555772s
DURABLE_STATES: PROTECTED x 8      ACTIVE_PROTECTIONS: 8   LEDGER_ERRORS: []
```

Eight physically protected positions, a live supervisor making progress, declared stalled.

### Fix — items A–D as specified

**A/B. Progress liveness separated from cycle completion.**
`ProtectionSupervisor._last_progress_monotonic` now advances after **every record**, not
per cycle. `progress_age_seconds()` is the liveness signal, and the watchdog uses it.
Its bound is one query, so it is independent of N.

**C. Freshness enforced independently, with two bounds instead of one.**
- `max_verification_age_seconds` — SOFT per-position target. Exceeding it raises
  `PROTECTION_FRESHNESS_DEGRADED` in the audit trail, carrying position count, cycle
  duration and both bounds. It does **not** close the gate and does **not** claim a stall.
- `freshness_ceiling_seconds` (default 10x soft) — HARD bound. Beyond it the system
  genuinely cannot police what it holds, and the gate closes with
  `PROTECTION_FRESHNESS_CEILING_EXCEEDED`.

The deadline was **not** loosened to hide the failure. One number carrying two different
meanings was split into two numbers with one meaning each. Query *uncertainty*
(`result is None`) still expires at the soft target — that is the v0.6 H-2 contract and
it is unchanged.

Cycles now run **oldest-verified first**, so freshness degrades uniformly instead of
starving one record.

**D. Post-trade validation is O(1).**
`submit()` calls the new `verify_one(intent_id)` instead of a full portfolio
`verify_once()`. Background stays O(N); per-submit cost no longer grows with position
count.

### Verification

```
CG-2 reproducer, post-fix:   NOT REPRODUCED, 3/3
                             RUNTIME_READY True, WATCHDOG_LAST_ERROR None,
                             ACTIVE_PROTECTIONS 8, freshness_degraded reported
N3 frozen-query attack:      still closes ready + gate, watchdog still reports STALLED
supervisor_kill_chaos_1000:  1000 / 0, detection max 0.298s vs 1.5s budget
```

## B-6 — supervisory threads outlived their runtime — **HIGH, FIXED**

Surfaced by the full matrix while fixing CG-2: `chaos_1000` failed 1–2 runs in 1000,
but only when the machine was already loaded. Root cause measured, not guessed:

```
threads at start: 1
after 25 runtimes (no stop()):  76      -> 3 threads leaked per runtime
after 50:                       151
after 100:                      226
```

`chaos_1000` never calls `rt.stop()`. By run ~900 roughly 2,700 supervisory threads are
live, the scheduler starves, and a 3-second lease lapses mid-trade — surfacing as
`UNCAUGHT:StaleEpoch`. v0.8 made this 50% worse by adding a third supervisory thread.

Two defects, both fixed in the product rather than in the harness:

1. **Reference cycle.** `bind_health_probe(lambda: self.ready)` captured the runtime
   strongly; the engine held the probe, the supervisors held the engine, and the live
   threads held the supervisors. A dropped runtime could never be collected. The probe
   now holds a weak reference and reads unhealthy once the runtime is gone.
2. **No lifetime tie.** `weakref.finalize` now stops all three supervisors when the
   runtime is collected, so a caller who forgets `stop()` does not leak threads.

```
after 25 runtimes (still no stop()):  4
after 50:                             7
after 100:                            10
```

`chaos_1000` now passes 1000/0 twice consecutively **without any change to the harness**.

## B-7 — StaleEpoch could escape `process()` — **MEDIUM, FIXED**

Authority can lapse anywhere in `process()`, not only around the dispatch window that
already handled it. `process()` now wraps `_process()`, revokes the gate, audits
`EXECUTION_AUTHORITY_LOST`, and raises a typed `BootGateClosed` instead of letting a raw
`StaleEpoch` traceback escape the public entry point.

## Re-acceptance evidence

| gate item | result |
|---|---|
| full suite | **90 passed** (71 pre-existing + 8 self-attack + 3 concurrency + 8 CG-2/B-6) |
| `chaos_1000` | 1000 / 0 (twice consecutively) |
| `restart_chaos_1000` | 1000 / 0 |
| `multi_position_chaos_1000` | 1000 / 0 |
| `protection_chaos_1000_fast` | 1000 / 0 |
| `supervisor_kill_chaos_1000` | 1000 / 0 · detection max 0.298s, budget 1.5s |
| CG-2 reproducer | NOT REPRODUCED 3/3 |
| v0.7 attack replay | 0/12 succeed |
| raw SQLite cross-thread attack | clean |
| ResourceWarning (`-W error`) | 0 |
| no-secrets / no-live-authority | 0 matches, 0 network imports |

## Open question handed to the reviewers

While shaping the B-5 regression I hit something I did **not** resolve and am not
claiming is safe:

- `start()` constructs a **fresh** `ColdBootCoordinator` (`runtime.py:143`), so a
  reference to `rt.boot` taken before `start()` is silently discarded. Harmless for
  correctness here, but it makes `rt.boot` misleading to any caller or test that holds it.
- More important: **is there any path where a running engine loses and silently
  re-acquires authority without a cold boot?** `acquire_authority()` resets state and
  re-acquires; if it can run mid-life while the gate is open, a leader could reassert
  authority without reconciling. I could not construct that path, but I could not rule
  it out either. Worth an attack.

All builder tests in `tests/test_v082_cg2_and_thread_lifetime.py` are BUILDER-WRITTEN and
carry lower evidentiary weight. Independent regressions for CG-2, B-6 and B-7 are owed.

---

# v0.8.3 — response to ChatGPT CG-2/D follow-up

ChatGPT read the v0.8.2 patch and found item D incomplete. Both claims verified before
changing anything.

## D-2 — `verify_one()` was serialised behind the full background cycle — **CONFIRMED, FIXED**

`verify_one()` took `_cycle_lock`, the same lock `verify_once()` holds for an entire
O(N) portfolio cycle. So post-trade work was O(1) but post-trade **wall clock** stayed
tied to portfolio size — exactly the coupling item D was meant to remove.

Measured, 10–15 positions at 0.05s/query:

```
before:  submit  max=1.008s  mean=0.757s     (high variance = waiting on the lock)
after:   submit  max=0.515s  mean=0.513s     (variance collapsed; this is the trade itself)

verify_one() with a 0.60s background cycle in flight:
before:  blocked for the remainder of the cycle
after:   max=0.051s  mean=0.051s   == exactly one query
```

`verify_one()` no longer takes `_cycle_lock`. Verifying one record concurrently with the
background cycle is safe: the ledger has its own locking and a duplicate verification of
the same record is idempotent.

## D-3 — `healthy` scanned the ledger on every gated call — **CONFIRMED, FIXED**

`healthy` called `oldest_verification_age_seconds()` → `ledger.protected_records()`, an
O(N) read. `healthy` is reached from the engine health probe on **every** gated call, so
every `process()` paid an O(N) ledger scan and contended with the background cycle.

The background cycle now records the true oldest age; the hot path uses
`cached_oldest_verification_age_seconds()`, which extrapolates that measurement forward
by elapsed time. Extrapolating forward can only **over**-estimate age, so the error is
always fail-safe. The watchdog ceiling check uses the cached value too.

```
before:  50 readiness checks -> 50 ledger scans
after:   50 readiness checks ->  0 ledger scans
         200 x rt.ready: 0.015s -> 0.001s
```

## Regressions added

Three, in `tests/test_v082_cg2_and_thread_lifetime.py`, all verified to **fail on
pre-patch v0.8.2**:

- `test_post_trade_check_is_not_serialised_behind_the_background_cycle`
- `test_readiness_check_does_not_scan_the_ledger`
- `test_stall_inside_ledger_read_still_closes_gate` — ChatGPT's own follow-up point:
  since `healthy` no longer reads the ledger, prove a freeze **inside**
  `protected_records()` is still caught. It is, by progress liveness.

## Re-acceptance evidence

| gate item | result |
|---|---|
| full suite | **93 passed** |
| `chaos_1000` | 1000 / 0 |
| `restart_chaos_1000` | 1000 / 0 |
| `multi_position_chaos_1000` | 1000 / 0 |
| `protection_chaos_1000_fast` | 1000 / 0 |
| `supervisor_kill_chaos_1000` | 1000 / 0 · detection max 0.299s, budget 1.5s |
| CG-2 reproducer | NOT REPRODUCED 3/3 |
| v0.7 attack replay | 0/12 succeed |
| no-secrets / no-live-authority | clean |

BUILDER-WRITTEN tests, lower evidentiary weight. Independent regressions still owed.

---

# v0.8.4 — response to ChatGPT CG-4

## CG-4 — foreground traffic masked a frozen background supervisor — **CONFIRMED, FIXED**

**This defect was introduced by my own D-2 patch.** ChatGPT caught it on review of
v0.8.3 before it ever reached a chaos run.

When `verify_one()` stopped taking `_cycle_lock`, it kept updating
`_last_progress_monotonic` — the very counter the watchdog reads as the supervisor
liveness signal. So a steady stream of `submit()` calls kept the signal fresh while the
background thread was frozen inside a single call. Reproduced with ChatGPT's script,
run verbatim:

```
supervisor thread frozen for >1.2s inside protected_records()
  rt.ready            = True
  engine.gate_open    = True
  watchdog last_error = None
  progress_age        = 0.050s   (bound 0.3s)   <- refreshed by the FOREGROUND path
```

The v0.8.3 test `test_stall_inside_ledger_read_still_closes_gate` passed only because it
did no foreground work during the freeze. ChatGPT's exact words: *"the current test does
not detect this because it waits without running foreground checks."*

### Fix — items 1–5 as specified

1–3. **`_background_progress_monotonic`, advanced only by `verify_once()`** — at cycle
start and after each record. `progress_age_seconds()` reads it alone and is deliberately
blind to the foreground path. **Liveness of a supervisor can only be evidenced by that
supervisor doing work.**

5. **Striped per-record locks.** Removing `_cycle_lock` in D-2 also allowed the
background cycle and the foreground path to verify the *same* record simultaneously,
where one side may write `UNKNOWN`. 64 striped `RLock`s serialise per-record
verification while keeping foreground cost bounded by one query, not by portfolio size
— O(1) in memory, no cleanup, no reintroduction of the D-2 coupling.

### Verification

```
CG-4 reproducer, post-fix:
  rt.ready            = False
  engine.gate_open    = False
  watchdog last_error = PROTECTION_SUPERVISOR_STALLED:0.356357s
  progress_age        = 1.219s   (bound 0.3s)

D-2 still holds:  verify_one() with a 0.60s cycle in flight = 0.052s == one query
CG-2 still holds: NOT REPRODUCED
```

### Regressions added (4, 6)

Both verified to **fail on v0.8.3**:

- `test_foreground_traffic_cannot_mask_a_frozen_supervisor` — ChatGPT's script verbatim
- `test_same_record_is_never_verified_concurrently` — 4 hammering threads against the
  background cycle, asserting zero overlapping verifications of one record and that the
  durable state is not knocked to `UNKNOWN`

## Re-acceptance evidence

| gate item | result |
|---|---|
| full suite | **95 passed** |
| `chaos_1000` | 1000 / 0 |
| `restart_chaos_1000` | 1000 / 0 |
| `multi_position_chaos_1000` | 1000 / 0 |
| `protection_chaos_1000_fast` | 1000 / 0 |
| `supervisor_kill_chaos_1000` | 1000 / 0 · detection max 0.298s, budget 1.5s |
| CG-2 reproducer | NOT REPRODUCED |
| CG-4 reproducer | NOT REPRODUCED |
| D-2 latency | 0.052s == one query |
| v0.7 attack replay | 0/12 succeed |
| no-secrets / no-live-authority | clean |

## Note on the pattern

Three of the last four findings (B-6, CG-4, and the earlier gate-open latch) were
**introduced by the fix for the previous finding**. This is the hardening treadmill named
in REVIEW_PROTOCOL §11, and it is the strongest argument for the two rules that caught
them: the builder does not write the proof for his own patch, and every patch that adds
machinery must answer "what watches this?"

CG-4 in particular was caught by review, not by 5,000 chaos runs — because the harnesses
did no foreground work during the injected freeze. Chaos coverage is not review.

