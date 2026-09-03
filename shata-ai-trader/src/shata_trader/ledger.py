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
