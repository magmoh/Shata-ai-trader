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
