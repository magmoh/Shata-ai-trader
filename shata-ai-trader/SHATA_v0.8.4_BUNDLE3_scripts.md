# SHATA v0.8.4 — SOURCE BUNDLE 3/3 (scripts)

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
    for m in re.finditer(r'^=== FILE: scripts/chaos_1000.py ===
import sys, tempfile, random
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from collections import Counter
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from shata_trader.audit import HashChainedAuditLog
from shata_trader.domain import PortfolioSnapshot, RiskPolicy
from shata_trader.exchange import SimulatedExchange, RateLimited
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.strategy import DeterministicDemoStrategy
from shata_trader.runtime import TradingCoreRuntime

P=RiskPolicy(version=1,max_risk_per_trade_pct=Decimal('0.0075'),max_position_allocation_pct=Decimal('0.10'),max_portfolio_exposure_pct=Decimal('0.50'),min_risk_reward=Decimal('2'),max_entry_deviation_pct=Decimal('0.005'),max_intent_age_seconds=30)
PF=lambda: PortfolioSnapshot(Decimal('10000'),Decimal('10000'),Decimal('0'),datetime.now(timezone.utc))

class ChaosExchange(SimulatedExchange):
    def __init__(self,*a,fail_on_call=None,**k):
        super().__init__(*a,**k); self.fail_on_call=fail_on_call
    def _guard(self):
        self.call_count += 1
        if self.fail_on_call is not None and self.call_count >= self.fail_on_call:
            raise RateLimited('chaos mid-flow rate limit')
        if self.maintenance: raise Exception('maintenance')
        if self.rate_limited: raise RateLimited('rate limited')
        if self.symbol_status!='TRADING': raise Exception('symbol not trading')

def run(seed):
    r=random.Random(seed)
    ratio=Decimal(str(r.choice([1,1,1,0.2,0.37,0.7])))
    ex=ChaosExchange(
        Decimal('100'),
        partial_fill_ratio=ratio,
        fail_protection=(r.random()<0.05),
        ambiguous_submit=(r.random()<0.08),
        commission_rate=Decimal(str(r.choice(['0','0.001','0.00075']))),
        commission_asset_mode=r.choice(['BASE','QUOTE']),
        maintenance=(r.random()<0.02),
        rate_limited=False,
        symbol_status='HALT' if r.random()<0.01 else 'TRADING',
        fail_on_call=r.choice([None,None,None,None,3,4,5,6]) if r.random()<0.12 else None,
    )
    ex.base_balance=Decimal(str(r.choice([0,0,0,10,50])))
    with tempfile.TemporaryDirectory() as td:
        eng=DemoExecutionEngine(ex,DeterministicRiskEngine(P),IdempotencyStore(Path(td)/'i.db'),HashChainedAuditLog(Path(td)/'a.jsonl'))
        it=DeterministicDemoStrategy().create_intent('TESTUSDT',Decimal('100'),Decimal('500'),1)
        try:
            rt=TradingCoreRuntime(eng); rep=rt.start()
            if rep.unresolved: return 'BOOT_UNRESOLVED',False,str(rep.states)
            sm=rt.submit(it,PF())
        except Exception as e:
            return 'UNCAUGHT:'+type(e).__name__, False, str(e)
        ledger=eng.ledger.get(it.trade_intent_id)
        if ledger is None or ledger['state']!=sm.state.value:
            return 'STATE_DRIFT',False,f"sm={sm.state.value} ledger={ledger and ledger['state']}"
        if not eng.audit.verify():
            return 'AUDIT_INVALID',False,''
        return sm.state.value,True,''

counts=Counter(); failures=[]
for seed in range(1000):
    state,ok,msg=run(seed); counts[state]+=1
    if not ok: failures.append((seed,state,msg))
print('CHAOS RUNS: 1000')
print('FAILURES:',len(failures))
print('STATES:',dict(sorted(counts.items())))
if failures:
    print('FIRST FAILURES:',failures[:20])
    raise SystemExit(1)
print('RESULT: PASS - no uncaught exception, ledger/state drift, or audit-chain failure')

=== FILE: scripts/crash_worker.py ===
import os,sys
from pathlib import Path
from decimal import Decimal
from datetime import datetime,timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from shata_trader.audit import HashChainedAuditLog
from shata_trader.activity import TradingActivityStore
from shata_trader.domain import PortfolioSnapshot,RiskPolicy
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.strategy import DeterministicDemoStrategy
from shata_trader.runtime import TradingCoreRuntime

base=Path(os.environ['CASE_DIR']); point=os.environ['CRASH_POINT']
P=RiskPolicy(version=1,max_risk_per_trade_pct=Decimal('0.0075'),max_position_allocation_pct=Decimal('0.10'),max_portfolio_exposure_pct=Decimal('0.50'),min_risk_reward=Decimal('2'),max_entry_deviation_pct=Decimal('0.005'),max_intent_age_seconds=30)
PF=PortfolioSnapshot(Decimal('10000'),Decimal('10000'),Decimal('0'),datetime.now(timezone.utc))

def hook(name):
    if name==point: os._exit(73)

ex=PersistentSimulatedExchange(base/'exchange.db')
eng=DemoExecutionEngine(ex,DeterministicRiskEngine(P),IdempotencyStore(base/'idem.db'),HashChainedAuditLog(base/'audit.jsonl'),ledger=TradeLedger(base/'ledger.db'),lease=SingleWriterLease(base/'lease.db'),holder_id='crash-worker',activity=TradingActivityStore(base/'activity.db'),lease_ttl_seconds=0.2,fault_hook=hook)
it=DeterministicDemoStrategy().create_intent('TESTUSDT',Decimal('100'),Decimal('500'),1)
(base/'intent_id.txt').write_text(it.trade_intent_id)
rt=TradingCoreRuntime(eng); rt.start(); rt.submit(it,PF)

=== FILE: scripts/multi_position_chaos_1000.py ===
from __future__ import annotations

import random
import tempfile
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from shata_trader.activity import TradingActivityStore
from shata_trader.audit import HashChainedAuditLog
from shata_trader.domain import PortfolioSnapshot, RiskPolicy
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime, RuntimeNotReady
from shata_trader.strategy import DeterministicDemoStrategy

RUNS = 1000
SYMBOLS = ['TESTUSDT', 'ALTUSDT', 'COINUSDT']
P = RiskPolicy(
    version=1,
    max_risk_per_trade_pct=Decimal('0.0075'),
    max_position_allocation_pct=Decimal('0.10'),
    max_portfolio_exposure_pct=Decimal('0.50'),
    min_risk_reward=Decimal('2'),
    max_entry_deviation_pct=Decimal('0.005'),
    max_intent_age_seconds=30,
    max_orders_per_hour=100,
    max_notional_per_day_pct=Decimal('1.0'),
)
PF = lambda: PortfolioSnapshot(
    Decimal('10000'), Decimal('10000'), Decimal('0'), datetime.now(timezone.utc)
)


def make(base: Path, holder: str):
    ex = PersistentSimulatedExchange(base / 'exchange.db')
    eng = DemoExecutionEngine(
        ex,
        DeterministicRiskEngine(P),
        IdempotencyStore(base / 'idem.db'),
        HashChainedAuditLog(base / 'audit.jsonl'),
        ledger=TradeLedger(base / 'ledger.db'),
        lease=SingleWriterLease(base / 'lease.db'),
        holder_id=holder,
        activity=TradingActivityStore(base / 'activity.db'),
        lease_ttl_seconds=0.5,
    )
    rt = TradingCoreRuntime(
        eng,
        protection_check_interval_seconds=0.02,
        max_protection_age_seconds=0.10,
    )
    return eng, ex, rt


def expected_reservation_invariant(eng, ex, baseline):
    # Every row that claims PROTECTED must have a matching active protection with
    # exact expected quantity. When runtime is ready, unexplained free balance may
    # only be the explicitly injected pre-existing baseline.
    for rec in eng.ledger.protected_records():
        intent = eng.ledger.intent_from_payload(rec['payload'])
        partial = rec['state'] == 'PARTIALLY_PROTECTED'
        pid = eng._protection_client_id(intent, partial)
        d = ex.protection_details_by_client_id(intent.symbol, pid)
        if not d:
            return False, f"FALSE_PROTECTED_MISSING:{rec['intent_id']}"
        exp = Decimal(rec['protection_expected_qty'])
        if Decimal(d.base_qty) != exp:
            return False, f"FALSE_PROTECTED_QTY:{rec['intent_id']}:{d.base_qty}!={exp}"

    if rt_ready := getattr(eng, '_boot_verified', False):
        for sym in SYMBOLS:
            total = ex._balance(sym)
            reserved = sum((Decimal(q) for _, q in ex.active_protections(sym)), Decimal('0'))
            unexplained = total - reserved
            if unexplained != baseline[sym]:
                return False, f"READY_EXPOSURE_DRIFT:{sym}:{unexplained}!={baseline[sym]}"
    return True, ''


failures = []
states = Counter()
rng = random.Random(6062026)

for run in range(RUNS):
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        eng, ex, rt = make(base, f'run-{run}')
        baseline = {s: Decimal('0') for s in SYMBOLS}
        # Pre-existing holdings test old-balance isolation without treating them as
        # system-managed exposure.
        for s in SYMBOLS:
            if rng.random() < 0.25:
                baseline[s] = Decimal(str(rng.choice([1, 2, 5])))
                ex.external_adjust_balance(s, baseline[s])
        try:
            rep = rt.start()
            if rep.unresolved != 0 or not rt.ready:
                failures.append((run, 'BOOT_NOT_READY'))
                continue

            opened = []
            count = rng.choice([2, 3])
            for idx in range(count):
                if not rt.ready:
                    break
                sym = rng.choice(SYMBOLS)
                amt = Decimal(str(rng.choice([100, 150, 200, 250, 300])))
                it = DeterministicDemoStrategy().create_intent(sym, Decimal('100'), amt, 1)
                # Occasionally fail protection on a later trade; emergency exit must
                # not disturb already-protected positions on the same symbol.
                ex.fail_protection = idx > 0 and rng.random() < 0.08
                try:
                    sm = rt.submit(it, PF())
                    states[sm.state.value] += 1
                except RuntimeNotReady:
                    states['RUNTIME_NOT_READY'] += 1
                    break
                finally:
                    ex.fail_protection = False
                opened.append((it, sm.state.value))

            anomaly = 'none'
            active = ex.conn.execute(
                "SELECT client_id,symbol,base_qty FROM protections WHERE active=1 ORDER BY rowid"
            ).fetchall()
            if active and rt.ready:
                roll = rng.random()
                cid, sym, qty = rng.choice(active)
                if roll < 0.12:
                    ex.cancel_protection_by_client_id(sym, cid)
                    anomaly = 'cancel'
                elif roll < 0.22:
                    ex.conn.execute(
                        "UPDATE protections SET base_qty=? WHERE client_id=?",
                        (str(Decimal(qty) / Decimal('2')), cid),
                    )
                    anomaly = 'qty_mismatch'

            # Deterministic immediate check, in addition to the background loop.
            if rt.protection_supervisor:
                rt.protection_supervisor.verify_once()

            if anomaly != 'none' and rt.ready:
                failures.append((run, f'ANOMALY_NOT_HALTED:{anomaly}'))
                continue

            ok, reason = expected_reservation_invariant(eng, ex, baseline)
            if not ok:
                failures.append((run, reason))
                continue

            # Half the runs cold-boot again with all prior positions still present.
            if rng.random() < 0.5:
                was_ready = rt.ready
                rt.stop(release_lease=True)
                eng2, ex2, rt2 = make(base, f'restart-{run}')
                rep2 = rt2.start()
                if anomaly == 'none' and was_ready:
                    if rep2.unresolved != 0 or not rt2.ready:
                        failures.append((run, 'CLEAN_RESTART_NOT_READY'))
                    else:
                        ok2, reason2 = expected_reservation_invariant(eng2, ex2, baseline)
                        if not ok2:
                            failures.append((run, 'RESTART_' + reason2))
                else:
                    if rt2.ready:
                        failures.append((run, 'UNSAFE_RESTART_READY'))
                rt2.stop(release_lease=True)
            else:
                rt.stop(release_lease=True)
        except Exception as exc:
            failures.append((run, f'UNCAUGHT:{type(exc).__name__}:{exc}'))
            try:
                rt.stop(release_lease=False)
            except Exception:
                pass

print(f'MULTI-POSITION CHAOS RUNS: {RUNS}')
print(f'FAILURES: {len(failures)}')
print('STATES:', dict(states))
if failures:
    print('FIRST FAILURES:', failures[:20])
    raise SystemExit(1)
print('RESULT: PASS - multi-position, multi-symbol, out-of-band protection mutation, and restart invariants held')

=== FILE: scripts/protection_chaos_1000_fast.py ===
from __future__ import annotations
import random,sys
from pathlib import Path
from decimal import Decimal
from datetime import datetime,timezone,timedelta
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from shata_trader.activity import TradingActivityStore
from shata_trader.audit import HashChainedAuditLog
from shata_trader.domain import PortfolioSnapshot,RiskPolicy,TradeState
from shata_trader.execution import DemoExecutionEngine,deterministic_client_order_id
from shata_trader.exchange import SimulatedExchange
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.rate_governor import PriorityRateGovernor
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime
from shata_trader.strategy import DeterministicDemoStrategy

P=RiskPolicy(version=1,max_risk_per_trade_pct=Decimal('0.0075'),max_position_allocation_pct=Decimal('0.10'),max_portfolio_exposure_pct=Decimal('0.50'),min_risk_reward=Decimal('2'),max_entry_deviation_pct=Decimal('0.005'),max_intent_age_seconds=30,max_orders_per_hour=50)
PF=lambda:PortfolioSnapshot(Decimal('10000'),Decimal('10000'),Decimal('0'),datetime.now(timezone.utc))
IN=lambda:DeterministicDemoStrategy().create_intent('TESTUSDT',Decimal('100'),Decimal('300'),1)

class Drained(SimulatedExchange):
    drain=Decimal('0')
    def get_free_base_balance(self,symbol):
        return max(Decimal('0'),super().get_free_base_balance(symbol)-self.drain)

def build(ex,i):
    e=DemoExecutionEngine(ex,DeterministicRiskEngine(P),IdempotencyStore(':memory:'),HashChainedAuditLog(Path('/tmp')/f'shata-pc-{i}.jsonl'),ledger=TradeLedger(':memory:'),lease=SingleWriterLease(':memory:'),holder_id=f'pc{i}',activity=TradingActivityStore(':memory:'),lease_ttl_seconds=5,rate_governor=PriorityRateGovernor(0))
    rt=TradingCoreRuntime(e,protection_check_interval_seconds=999,max_protection_age_seconds=.05)
    rt.start();return e,rt

def verify_invariant(e,ex,rt):
    for rec in e.ledger.nonterminal_records():
        if rec['state'] in ('PROTECTED','PARTIALLY_PROTECTED'):
            it=e.ledger.intent_from_payload(rec['payload']);suffix='partial-protection' if rec['state']=='PARTIALLY_PROTECTED' else 'protection';cid=deterministic_client_order_id(it,suffix)
            d=ex.protection_details_by_client_id(it.symbol,cid)
            if d is None:return False,'MISSING'
            if rec['protection_expected_qty'] is None:return False,'NO_EXPECTED'
            if Decimal(d.base_qty)!=Decimal(rec['protection_expected_qty']):return False,'QTY_MISMATCH'
    if rt.ready:
        unsafe=[r['state'] for r in e.ledger.nonterminal_records() if r['state'] not in ('PROTECTED','PARTIALLY_PROTECTED')]
        if unsafe:return False,'READY_UNSAFE:'+','.join(unsafe)
    return True,'OK'

rng=random.Random(6062026);fails=[];states={}
for i in range(1000):
    path=Path('/tmp')/f'shata-pc-{i}.jsonl'
    try:path.unlink()
    except FileNotFoundError:pass
    action=rng.randrange(5)
    ex=Drained(Decimal('100')) if action==1 else SimulatedExchange(Decimal('100'))
    if action==1:ex.drain=Decimal(str(rng.choice([0.2,0.5,1.0,1.5])))
    e,rt=build(ex,i);it=IN()
    try:
        if action==2:ex.fail_protection=True
        sm=rt.submit(it,PF());ex.fail_protection=False
        if action==3 and sm.state==TradeState.PROTECTED:
            ex.cancel_protection_by_client_id(it.symbol,deterministic_client_order_id(it,'protection'))
            v=rt.protection_supervisor.verify_once()
            if v:rt._on_protection_violation(v[0][1],v[0][0])
        elif action==4 and sm.state==TradeState.PROTECTED:
            raw=e.ledger.raw if hasattr(e.ledger,'raw') else e.ledger
            old=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
            raw.conn.execute('UPDATE trades SET protection_verified_at=? WHERE intent_id=?',(old,it.trade_intent_id))
            orig=ex.protection_details_by_client_id
            ex.protection_details_by_client_id=lambda *a,**k:(_ for _ in ()).throw(TimeoutError('persistent'))
            v=rt.protection_supervisor.verify_once();ex.protection_details_by_client_id=orig
            if v:rt._on_protection_violation(v[0][1],v[0][0])
        ok,why=verify_invariant(e,ex,rt)
        if not ok:fails.append((i,action,sm.state.value,why,e.ledger.get(it.trade_intent_id)['state']))
        state=e.ledger.get(it.trade_intent_id)['state'];states[state]=states.get(state,0)+1
    except Exception as exc:
        fails.append((i,action,'EXC',type(exc).__name__,str(exc)))
    finally:
        rt.stop(release_lease=False)
        try:path.unlink()
        except FileNotFoundError:pass
print('PROTECTION CHAOS RUNS: 1000')
print('FAILURES:',len(fails))
print('STATES:',states)
if fails:print('SAMPLE:',fails[:12])
print('RESULT:', 'PASS - no false PROTECTED durable claims' if not fails else 'FAIL')
raise SystemExit(1 if fails else 0)

=== FILE: scripts/restart_chaos_1000.py ===
import sys,tempfile,random
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal
from collections import Counter
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from shata_trader.audit import HashChainedAuditLog
from shata_trader.activity import TradingActivityStore
from shata_trader.domain import PortfolioSnapshot,RiskPolicy
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime
from shata_trader.strategy import DeterministicDemoStrategy

P=RiskPolicy(version=1,max_risk_per_trade_pct=Decimal('0.0075'),max_position_allocation_pct=Decimal('0.10'),max_portfolio_exposure_pct=Decimal('0.50'),min_risk_reward=Decimal('2'),max_entry_deviation_pct=Decimal('0.005'),max_intent_age_seconds=30)
PF=lambda:PortfolioSnapshot(Decimal('10000'),Decimal('10000'),Decimal('0'),datetime.now(timezone.utc))

def engine(b,ex,label):
    return DemoExecutionEngine(ex,DeterministicRiskEngine(P),IdempotencyStore(b/'idem.db'),HashChainedAuditLog(b/'audit.jsonl'),ledger=TradeLedger(b/'ledger.db'),lease=SingleWriterLease(b/'lease.db'),holder_id=label,activity=TradingActivityStore(b/'activity.db'),lease_ttl_seconds=2)

def run(seed):
    r=random.Random(seed)
    ratio=Decimal(str(r.choice([1,1,1,0.15,0.37,0.70])))
    with tempfile.TemporaryDirectory() as td:
        b=Path(td);ex=PersistentSimulatedExchange(b/'exchange.db',partial_fill_ratio=ratio,commission_rate=Decimal(str(r.choice(['0','0.001','0.00075']))))
        ex.fail_protection=(r.random()<0.04);ex.ambiguous_after_accept=(r.random()<0.08)
        e1=engine(b,ex,'first');rt1=TradingCoreRuntime(e1);rep1=rt1.start()
        if rep1.unresolved:return 'BOOT1_UNRESOLVED',False,str(rep1.states)
        it=DeterministicDemoStrategy().create_intent('TESTUSDT',Decimal('100'),Decimal('500'),1)
        try:sm1=rt1.submit(it,PF())
        except Exception as exc:return 'SUBMIT_EXCEPTION',False,type(exc).__name__
        rt1.stop(release_lease=True)
        # New process-like objects, same durable exchange/ledger. Sometimes hide order on first recovery query.
        ex2=PersistentSimulatedExchange(b/'exchange.db',partial_fill_ratio=ratio)
        ex2.query_visibility_lag_calls=r.choice([0,0,0,0,1])
        e2=engine(b,ex2,'restart');rt2=TradingCoreRuntime(e2)
        try:rep2=rt2.start()
        except Exception as exc:return 'RESTART_EXCEPTION',False,type(exc).__name__
        rec=e2.ledger.get(it.trade_intent_id)
        if rec is None:return 'LOST_LEDGER',False,''
        if rep2.unresolved==0 and not rt2.ready:return 'READY_DRIFT',False,str(rep2.states)
        if rep2.unresolved>0 and rt2.ready:return 'UNSAFE_READY',False,str(rep2.states)
        if rec['state']=='CANCELED' and ex2._balance()>0 and not ex2.active_protections():return 'PHANTOM_CANCEL',False,str(ex2._balance())
        # If runtime says ready and a nonterminal position remains, it must be protected.
        if rt2.ready and rec['state'] in {'PROTECTED','PARTIALLY_PROTECTED'} and not ex2.active_protections():return 'FALSE_PROTECTED',False,rec['state']
        if not e2.audit.verify():return 'AUDIT_INVALID',False,''
        orders=ex2.all_orders();entry_ids=[x[0] for x in orders if 'emergency-exit' not in x[0]]
        if len(entry_ids)!=len(set(entry_ids)):return 'DUPLICATE_ENTRY_ID',False,str(entry_ids)
        state=rec['state'];rt2.stop(release_lease=True);return state,True,''

counts=Counter();fails=[]
for seed in range(1000):
    st,ok,msg=run(seed);counts[st]+=1
    if not ok:fails.append((seed,st,msg))
print('RESTART CHAOS RUNS: 1000');print('FAILURES:',len(fails));print('STATES:',dict(sorted(counts.items())))
if fails:
    print('FIRST FAILURES:',fails[:20]);raise SystemExit(1)
print('RESULT: PASS - restart included in every run')

=== FILE: scripts/run_demo.py ===
import sys
from pathlib import Path
from decimal import Decimal

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from shata_trader.audit import HashChainedAuditLog
from shata_trader.domain import PortfolioSnapshot, RiskPolicy
from shata_trader.exchange import SimulatedExchange
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.strategy import DeterministicDemoStrategy
from shata_trader.runtime import TradingCoreRuntime


policy = RiskPolicy(
    version=1,
    max_risk_per_trade_pct=Decimal("0.0075"),
    max_position_allocation_pct=Decimal("0.10"),
    max_portfolio_exposure_pct=Decimal("0.50"),
    min_risk_reward=Decimal("2.0"),
    max_entry_deviation_pct=Decimal("0.005"),
    max_intent_age_seconds=30,
)

portfolio = PortfolioSnapshot(
    quote_balance=Decimal("10000"),
    portfolio_value=Decimal("10000"),
    current_exposure=Decimal("0"),
)

exchange = SimulatedExchange(price=Decimal("100"))
audit = HashChainedAuditLog(ROOT / "demo_audit.jsonl")
engine = DemoExecutionEngine(
    exchange=exchange,
    risk_engine=DeterministicRiskEngine(policy),
    idempotency=IdempotencyStore(ROOT / "demo_idempotency.sqlite"),
    audit=audit,
)

intent = DeterministicDemoStrategy().create_intent(
    symbol="TESTUSDT",
    reference_price=Decimal("100"),
    quote_amount=Decimal("500"),
    risk_policy_version=1,
)

runtime = TradingCoreRuntime(engine)
runtime.start()
sm = runtime.submit(intent, portfolio)

print("Final state:", sm.state.value)
print("History:", " -> ".join(s.value for s in sm.history))
print("Audit chain valid:", audit.verify())

=== FILE: scripts/supervisor_kill_chaos_1000.py ===
"""Supervisory thread-failure chaos.

v0.8 item 4. This is the harness that turns N3 from "a bug we patched" into a
permanent invariant.

Invariant under test:

    If ANY critical supervisory loop dies or stops making progress, runtime
    readiness must become False within a bounded window, WITHOUT any submit()
    and WITHOUT any manual verify_once() call.

The manual-call exclusion is the whole point: calling verify_once() by hand is
exactly what hid N3 in the v0.7 chaos suites. Here the only thing the harness does
after injecting the fault is poll `rt.ready` and wait.

Faults injected (one per run, chosen at random):
  kill_lease        - lease supervisor thread gone
  kill_protection   - protection supervisor thread gone
  kill_watchdog     - runtime safety watchdog thread gone
  stall_protection  - protection loop alive but blocked in exchange I/O
  stall_lease       - renewal loop alive but blocked in lease I/O
  kill_all_but_one  - only one supervisory loop survives
"""
from __future__ import annotations

import random
import sys
import tempfile
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from shata_trader.activity import TradingActivityStore
from shata_trader.audit import HashChainedAuditLog
from shata_trader.domain import PortfolioSnapshot, RiskPolicy
from shata_trader.execution import DemoExecutionEngine
from shata_trader.idempotency import IdempotencyStore
from shata_trader.lease import SingleWriterLease
from shata_trader.ledger import TradeLedger
from shata_trader.persistent_exchange import PersistentSimulatedExchange
from shata_trader.risk_engine import DeterministicRiskEngine
from shata_trader.runtime import TradingCoreRuntime
from shata_trader.strategy import DeterministicDemoStrategy

RUNS = 1000
SYMBOLS = ['TESTUSDT', 'ALTUSDT', 'COINUSDT']

INTERVAL = 0.02          # supervisory loop period
MAX_AGE = 0.10           # freshness deadline the system promises
LEASE_TTL = 0.30
DETECT_BUDGET = 1.50     # generous multiple of the promised deadline

P = RiskPolicy(
    version=1,
    max_risk_per_trade_pct=Decimal('0.0075'),
    max_position_allocation_pct=Decimal('0.10'),
    max_portfolio_exposure_pct=Decimal('0.50'),
    min_risk_reward=Decimal('2'),
    max_entry_deviation_pct=Decimal('0.005'),
    max_intent_age_seconds=30,
    max_orders_per_hour=100,
    max_notional_per_day_pct=Decimal('1.0'),
)
PF = lambda: PortfolioSnapshot(
    Decimal('10000'), Decimal('10000'), Decimal('0'), datetime.now(timezone.utc)
)

FAULTS = [
    'kill_lease',
    'kill_protection',
    'kill_watchdog',
    'stall_protection',
    'stall_lease',
    'kill_all_but_one',
]


class StallableExchange(PersistentSimulatedExchange):
    """Blocks a named thread inside exchange I/O, leaving it alive but frozen."""

    stall_thread_name = None
    stall_seconds = 30.0

    def _maybe_stall(self):
        name = self.stall_thread_name
        if name and threading.current_thread().name == name:
            time.sleep(self.stall_seconds)

    def protection_details_by_client_id(self, symbol, client_order_id):
        self._maybe_stall()
        return super().protection_details_by_client_id(symbol, client_order_id)


class StallableLease(SingleWriterLease):
    """Blocks the renewal thread inside lease I/O."""

    stall_renew = False

    def renew(self, *a, **k):
        if self.stall_renew and threading.current_thread().name == 'shata-lease-supervisor':
            time.sleep(30.0)
        return super().renew(*a, **k)


def make(base: Path, holder: str):
    ex = StallableExchange(base / 'exchange.db')
    lease = StallableLease(base / 'lease.db')
    eng = DemoExecutionEngine(
        ex,
        DeterministicRiskEngine(P),
        IdempotencyStore(base / 'idem.db'),
        HashChainedAuditLog(base / 'audit.jsonl'),
        ledger=TradeLedger(base / 'ledger.db'),
        lease=lease,
        holder_id=holder,
        activity=TradingActivityStore(base / 'activity.db'),
        lease_ttl_seconds=LEASE_TTL,
    )
    rt = TradingCoreRuntime(
        eng,
        protection_check_interval_seconds=INTERVAL,
        max_protection_age_seconds=MAX_AGE,
    )
    return eng, ex, lease, rt


def kill_thread(sup):
    """Simulate a supervisory thread dying: stop the loop, leave the object in place."""
    sup._stop.set()
    t = getattr(sup, '_thread', None)
    if t:
        t.join(timeout=1.0)


failures = []
faults_seen = Counter()
latencies = []
rng = random.Random(80820260)

for run in range(RUNS):
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        eng, ex, lease, rt = make(base, f'kill-run-{run}')
        try:
            rep = rt.start()
            if rep.unresolved != 0 or not rt.ready:
                failures.append((run, 'BOOT_NOT_READY'))
                continue

            # Open one position so there is a durable PROTECTED claim to police.
            sym = rng.choice(SYMBOLS)
            it = DeterministicDemoStrategy().create_intent(
                sym, Decimal('100'), Decimal(str(rng.choice([150, 200, 300]))), 1
            )
            rt.submit(it, PF())
            if not rt.ready:
                failures.append((run, 'NOT_READY_AFTER_CLEAN_SUBMIT'))
                continue

            fault = rng.choice(FAULTS)
            faults_seen[fault] += 1

            # ---- inject; from here on: no submit(), no verify_once() ----------
            if fault == 'kill_lease':
                kill_thread(rt.supervisor)
            elif fault == 'kill_protection':
                kill_thread(rt.protection_supervisor)
            elif fault == 'kill_watchdog':
                kill_thread(rt.safety_watchdog)
            elif fault == 'stall_protection':
                ex.stall_thread_name = 'shata-protection-supervisor'
            elif fault == 'stall_lease':
                lease.stall_renew = True
            elif fault == 'kill_all_but_one':
                survivors = [rt.supervisor, rt.protection_supervisor, rt.safety_watchdog]
                keep = rng.randrange(3)
                for i, s in enumerate(survivors):
                    if i != keep:
                        kill_thread(s)

            t0 = time.monotonic()
            deadline = t0 + DETECT_BUDGET
            while rt.ready and time.monotonic() < deadline:
                time.sleep(0.005)
            latency = time.monotonic() - t0

            if rt.ready:
                failures.append((run, f'READY_AFTER_{fault.upper()}:{latency:.3f}s'))
                continue
            latencies.append(latency)

            # A degraded runtime must also refuse new work.
            try:
                rt.submit(
                    DeterministicDemoStrategy().create_intent(sym, Decimal('100'), Decimal('150'), 1),
                    PF(),
                )
                failures.append((run, f'SUBMIT_ACCEPTED_AFTER_{fault.upper()}'))
                continue
            except Exception:
                pass

            # The execution gate itself must be shut, not just the readiness flag.
            # gate_open is what engine.process() actually consults.
            if eng.gate_open:
                failures.append((run, f'GATE_STILL_OPEN_AFTER_{fault.upper()}'))
                continue
        except Exception as exc:
            failures.append((run, f'UNCAUGHT:{type(exc).__name__}:{exc}'))
        finally:
            try:
                ex.stall_thread_name = None
                lease.stall_renew = False
                rt.stop(release_lease=False)
            except Exception:
                pass

print(f'SUPERVISOR KILL/STALL CHAOS RUNS: {RUNS}')
print(f'FAILURES: {len(failures)}')
print('FAULTS:', dict(faults_seen))
if latencies:
    print(
        f'DETECTION LATENCY: max={max(latencies):.3f}s '
        f'mean={sum(latencies)/len(latencies):.3f}s budget={DETECT_BUDGET}s'
    )
if failures:
    print('FIRST FAILURES:', failures[:20])
    raise SystemExit(1)
print(
    'RESULT: PASS - every supervisory death/stall degraded readiness within budget '
    'with no submit() and no manual verify_once()'
)

