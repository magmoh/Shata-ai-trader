from datetime import datetime, timezone
from decimal import Decimal

from .domain import PortfolioSnapshot, RiskDecision, RiskPolicy, Side, TradeIntent

ZERO = Decimal("0")


class DeterministicRiskEngine:
    def __init__(self, policy: RiskPolicy):
        self.policy = policy

    def evaluate(
        self,
        intent: TradeIntent,
        portfolio: PortfolioSnapshot,
        fresh_market_price: Decimal,
        now: datetime | None = None,
    ) -> RiskDecision:
        now = now or datetime.now(timezone.utc)

        if intent.risk_policy_version != self.policy.version:
            return RiskDecision(False, "Risk policy version mismatch", ZERO)

        if intent.side != Side.BUY:
            return RiskDecision(False, "Phase 0 supports BUY intents only", ZERO)

        if intent.is_expired(now):
            return RiskDecision(False, "Trade intent expired", ZERO)

        intent_age = (now - intent.created_at).total_seconds()
        if intent_age > self.policy.max_intent_age_seconds:
            return RiskDecision(False, "Trade intent exceeded maximum age", ZERO)

        if portfolio.reconciled_at is not None:
            age = (now - portfolio.reconciled_at).total_seconds()
            if age > self.policy.max_reconciliation_age_seconds:
                return RiskDecision(False, "Portfolio reconciliation state is stale", ZERO)

        if intent.quote_amount <= ZERO or portfolio.portfolio_value <= ZERO:
            return RiskDecision(False, "Invalid amount or portfolio value", ZERO)

        if not (intent.stop_price < intent.reference_entry_price < intent.take_profit_price):
            return RiskDecision(False, "Invalid stop/entry/target relationship", ZERO)

        deviation = abs(fresh_market_price - intent.reference_entry_price) / intent.reference_entry_price
        max_dev = min(intent.max_entry_deviation_pct, self.policy.max_entry_deviation_pct)
        if deviation > max_dev:
            return RiskDecision(False, "Fresh market price exceeded allowed deviation", ZERO)

        risk_per_unit = intent.reference_entry_price - intent.stop_price
        reward_per_unit = intent.take_profit_price - intent.reference_entry_price
        rr = reward_per_unit / risk_per_unit
        if rr < self.policy.min_risk_reward:
            return RiskDecision(False, "Risk/reward below policy minimum", ZERO)

        capital_cap = portfolio.portfolio_value * self.policy.max_position_allocation_pct
        exposure_room = (
            portfolio.portfolio_value * self.policy.max_portfolio_exposure_pct
            - portfolio.current_exposure
        )
        exposure_room = max(exposure_room, ZERO)

        risk_fraction_of_position = risk_per_unit / intent.reference_entry_price
        if risk_fraction_of_position <= ZERO:
            return RiskDecision(False, "Invalid risk fraction", ZERO)

        max_loss = portfolio.portfolio_value * self.policy.max_risk_per_trade_pct
        risk_based_cap = max_loss / risk_fraction_of_position

        max_quote = min(capital_cap, exposure_room, portfolio.quote_balance, risk_based_cap)
        if max_quote <= ZERO:
            return RiskDecision(False, "No available risk/exposure capacity", ZERO)

        if intent.quote_amount > max_quote:
            return RiskDecision(False, "Requested quote amount exceeds risk limits", max_quote)

        return RiskDecision(True, "PASS", max_quote)
