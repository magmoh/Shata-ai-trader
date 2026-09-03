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
