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
