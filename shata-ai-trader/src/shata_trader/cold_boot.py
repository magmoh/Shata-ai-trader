from __future__ import annotations
from dataclasses import dataclass
from .domain import TradeState
from .rate_governor import PriorityRateGovernor

@dataclass
class BootReport:
    inspected:int
    resolved:int
    unresolved:int
    states:dict[str,str]
    quarantined:int=0

class ColdBootCoordinator:
    """Reconcile every durable nonterminal row; one corrupt row cannot hide the rest."""
    def __init__(self,engine,rate_governor=None):
        self.engine=engine;self.ready=False;self.rate_governor=rate_governor or engine.rate_governor
    def reconcile_all(self)->BootReport:
        records=self.engine.ledger.nonterminal_records();states={};resolved=0;unresolved=0;quarantined=0
        for r in records:
            try:
                self.rate_governor.acquire(priority=1)
                intent=self.engine.ledger.intent_from_payload(r['payload'])
                if intent.trade_intent_id!=r['intent_id']:
                    raise ValueError('payload intent_id mismatch')
                sm=self.engine.recover_intent(intent)
                states[r['intent_id']]=sm.state.value
                if sm.state in {TradeState.CLOSED,TradeState.CANCELED,TradeState.EXPIRED,TradeState.REJECTED,TradeState.PROTECTED,TradeState.PARTIALLY_PROTECTED}:
                    resolved+=1
                else:
                    unresolved+=1
            except Exception as exc:
                quarantined+=1;unresolved+=1;states[r['intent_id']]=f'QUARANTINED:{type(exc).__name__}'
                try:self.engine.ledger.mark_error(r['intent_id'],f'COLD_BOOT_QUARANTINE:{type(exc).__name__}')
                except Exception:pass
        self.ready=(unresolved==0)
        return BootReport(len(records),resolved,unresolved,states,quarantined)
