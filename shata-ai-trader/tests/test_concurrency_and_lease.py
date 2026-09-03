import sys
from pathlib import Path
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shata_trader.idempotency import DuplicateIntent, IdempotencyStore
from shata_trader.lease import LeaseUnavailable, SingleWriterLease


class TestConcurrencyAndLease(unittest.TestCase):
    def test_atomic_idempotency_eight_workers(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "idem.sqlite"

            # Initialize schema/WAL before racing workers.
            init = IdempotencyStore(db)
            init.close()

            wins = []
            errors = []
            lock = threading.Lock()
            barrier = threading.Barrier(8)

            def worker(worker_id):
                store = None
                try:
                    store = IdempotencyStore(db)
                    barrier.wait(timeout=5)
                    store.claim("same-intent")
                    with lock:
                        wins.append(worker_id)
                except DuplicateIntent:
                    pass
                except Exception as exc:
                    with lock:
                        errors.append((worker_id, type(exc).__name__, str(exc)))
                finally:
                    if store is not None:
                        store.close()

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)

            # Thread exceptions must fail the test; no false-pass.
            self.assertEqual(errors, [], f"Worker errors: {errors}")
            self.assertEqual(len(wins), 1, f"Expected exactly one winner, got {wins}")

    def test_single_writer_lease(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "lease.sqlite"
            a = SingleWriterLease(db)
            b = SingleWriterLease(db)
            a.acquire("core", "instance-a", ttl_seconds=30)
            with self.assertRaises(LeaseUnavailable):
                b.acquire("core", "instance-b", ttl_seconds=30)


if __name__ == "__main__":
    unittest.main()
