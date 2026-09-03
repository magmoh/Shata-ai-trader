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
