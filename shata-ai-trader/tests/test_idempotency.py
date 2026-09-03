import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.idempotency import DuplicateIntent, IdempotencyStore


class TestIdempotency(unittest.TestCase):
    def test_duplicate_rejected(self):
        store = IdempotencyStore(":memory:")
        store.claim("abc")
        with self.assertRaises(DuplicateIntent):
            store.claim("abc")


if __name__ == "__main__":
    unittest.main()
