from .domain import TradeState


_ALLOWED = {
    TradeState.CREATED: {TradeState.RISK_APPROVED, TradeState.REJECTED, TradeState.HALTED},
    TradeState.RISK_APPROVED: {TradeState.SUBMITTED, TradeState.REJECTED, TradeState.HALTED},
    TradeState.SUBMITTED: {TradeState.ACKNOWLEDGED, TradeState.UNKNOWN, TradeState.REJECTED, TradeState.HALTED},
    TradeState.ACKNOWLEDGED: {
        TradeState.PARTIALLY_FILLED, TradeState.FILLED, TradeState.CANCELED,
        TradeState.EXPIRED, TradeState.UNKNOWN, TradeState.HALTED
    },
    TradeState.PARTIALLY_FILLED: {
        TradeState.PARTIAL_PROTECTION_PENDING, TradeState.UNKNOWN,
        TradeState.EMERGENCY_EXIT, TradeState.HALTED
    },
    TradeState.PARTIAL_PROTECTION_PENDING: {
        TradeState.PARTIALLY_PROTECTED, TradeState.UNDER_PROTECTED, TradeState.PROTECTION_FAILED,
        TradeState.UNKNOWN, TradeState.EMERGENCY_EXIT, TradeState.HALTED
    },
    TradeState.PARTIALLY_PROTECTED: {TradeState.HALTED, TradeState.EXIT_PENDING, TradeState.UNKNOWN, TradeState.UNDER_PROTECTED},
    TradeState.FILLED: {
        TradeState.PROTECTION_PENDING, TradeState.PROTECTION_FAILED,
        TradeState.EMERGENCY_EXIT, TradeState.HALTED
    },
    TradeState.PROTECTION_PENDING: {
        TradeState.PROTECTED, TradeState.UNDER_PROTECTED, TradeState.PROTECTION_FAILED,
        TradeState.UNKNOWN, TradeState.EMERGENCY_EXIT, TradeState.HALTED
    },
    TradeState.PROTECTED: {TradeState.EXIT_PENDING, TradeState.UNKNOWN, TradeState.UNDER_PROTECTED, TradeState.HALTED},
    TradeState.EXIT_PENDING: {TradeState.CLOSED, TradeState.UNKNOWN, TradeState.HALTED},
    TradeState.UNKNOWN: {TradeState.RECONCILING, TradeState.HALTED},
    TradeState.RECONCILING: {
        TradeState.ACKNOWLEDGED, TradeState.PARTIALLY_FILLED, TradeState.FILLED,
        TradeState.CANCELED, TradeState.EXPIRED,
        TradeState.PARTIAL_PROTECTION_PENDING, TradeState.PARTIALLY_PROTECTED,
        TradeState.PROTECTION_PENDING, TradeState.PROTECTED, TradeState.CLOSED,
        TradeState.PROTECTION_FAILED, TradeState.EMERGENCY_EXIT, TradeState.HALTED,
    },
    TradeState.PROTECTION_FAILED: {TradeState.EMERGENCY_EXIT, TradeState.UNKNOWN, TradeState.HALTED},
    TradeState.UNDER_PROTECTED: {TradeState.EMERGENCY_EXIT, TradeState.UNKNOWN, TradeState.HALTED},
    TradeState.EMERGENCY_EXIT: {TradeState.CLOSED, TradeState.UNKNOWN, TradeState.HALTED},
    TradeState.HALTED: {TradeState.RECONCILING},
    TradeState.REJECTED: set(),
    TradeState.EXPIRED: set(),
    TradeState.CANCELED: set(),
    TradeState.CLOSED: set(),
}


class InvalidTransition(RuntimeError):
    pass


class TradeStateMachine:
    def __init__(self, initial: TradeState = TradeState.CREATED, on_transition=None):
        self.state = initial
        self.history = [initial]
        self.on_transition = on_transition

    def transition(self, new_state: TradeState) -> None:
        if new_state not in _ALLOWED[self.state]:
            raise InvalidTransition(f"{self.state.value} -> {new_state.value} is not allowed")
        old = self.state
        self.state = new_state
        self.history.append(new_state)
        if self.on_transition:
            self.on_transition(old, new_state)
