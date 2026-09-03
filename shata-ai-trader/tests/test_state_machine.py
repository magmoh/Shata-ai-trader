import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.domain import TradeState
from shata_trader.state_machine import InvalidTransition, TradeStateMachine


class TestStateMachine(unittest.TestCase):
    def test_normal_prefix(self):
        sm = TradeStateMachine()
        sm.transition(TradeState.RISK_APPROVED)
        sm.transition(TradeState.SUBMITTED)
        sm.transition(TradeState.ACKNOWLEDGED)
        sm.transition(TradeState.FILLED)
        sm.transition(TradeState.PROTECTION_PENDING)
        sm.transition(TradeState.PROTECTED)
        self.assertEqual(sm.state, TradeState.PROTECTED)

    def test_invalid_transition_rejected(self):
        sm = TradeStateMachine()
        with self.assertRaises(InvalidTransition):
            sm.transition(TradeState.PROTECTED)


if __name__ == "__main__":
    unittest.main()
