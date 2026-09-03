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
