import sys
from pathlib import Path
import json
import unittest
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


class TestDecimalRoundTrip(unittest.TestCase):
    def test_decimal_string_roundtrip_exact(self):
        original = Decimal("0.1234567890123456789012345678")
        wire = json.dumps({"value": str(original)})
        recovered = Decimal(json.loads(wire)["value"])
        self.assertEqual(original, recovered)


if __name__ == "__main__":
    unittest.main()
