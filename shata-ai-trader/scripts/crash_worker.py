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
