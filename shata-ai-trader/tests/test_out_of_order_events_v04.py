import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
import tempfile,unittest
from pathlib import Path
from shata_trader.events import OrderEventStore,ExchangeEvent

class TestOutOfOrderEventsV04(unittest.TestCase):
    def test_fill_before_ack_does_not_regress(self):
        with tempfile.TemporaryDirectory() as td:
            s=OrderEventStore(Path(td)/'e.db');self.assertTrue(s.ingest(ExchangeEvent('e-fill','cid','FILLED',200)));self.assertTrue(s.ingest(ExchangeEvent('e-ack','cid','ACKNOWLEDGED',100)));self.assertEqual(s.status('cid'),'FILLED')
    def test_duplicate_event_is_idempotent(self):
        s=OrderEventStore(':memory:');e=ExchangeEvent('same','cid','PARTIALLY_FILLED',100);self.assertTrue(s.ingest(e));self.assertFalse(s.ingest(e));self.assertEqual(s.status('cid'),'PARTIALLY_FILLED')
    def test_late_partial_cannot_regress_filled(self):
        s=OrderEventStore(':memory:');s.ingest(ExchangeEvent('1','cid','FILLED',100));s.ingest(ExchangeEvent('2','cid','PARTIALLY_FILLED',200));self.assertEqual(s.status('cid'),'FILLED')
if __name__=='__main__':unittest.main()
