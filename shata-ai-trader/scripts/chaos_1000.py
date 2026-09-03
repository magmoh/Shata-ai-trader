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
