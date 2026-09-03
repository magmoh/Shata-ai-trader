import sys,time,threading,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from shata_trader.rate_governor import PriorityRateGovernor

class TestRateGovernorPriorityV06(unittest.TestCase):
    def test_safety_priority_jumps_ahead_of_waiting_low_priority(self):
        g=PriorityRateGovernor(.04)
        g.acquire(priority=0)  # establish a pacing window
        order=[];lock=threading.Lock()
        def worker(name,priority):
            g.acquire(priority=priority)
            with lock:order.append(name)
        low=threading.Thread(target=worker,args=('market-data',5));low.start()
        time.sleep(.005)
        high=threading.Thread(target=worker,args=('protection',0));high.start()
        low.join();high.join()
        self.assertEqual(order[0],'protection',order)

if __name__=='__main__':unittest.main()
