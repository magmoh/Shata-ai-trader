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
