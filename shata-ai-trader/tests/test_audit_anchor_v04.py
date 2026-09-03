import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
import tempfile,unittest,json
from pathlib import Path
from shata_trader.audit import HashChainedAuditLog
from shata_trader.audit_anchor import FileAuditAnchor

class TestAuditAnchorV04(unittest.TestCase):
    def test_external_anchor_matches_head_and_detects_rewritten_log(self):
        with tempfile.TemporaryDirectory() as td:
            logp=Path(td)/'core'/'audit.jsonl';anch=FileAuditAnchor(Path(td)/'external'/'anchor.json');log=HashChainedAuditLog(logp,anch)
            log.append('A',{'x':1});log.append('B',{'x':2});self.assertTrue(log.verify(verify_anchor=True))
            lines=logp.read_text().splitlines();rec=json.loads(lines[-1]);rec['payload']['x']=999;lines[-1]=json.dumps(rec,sort_keys=True);logp.write_text('\n'.join(lines)+'\n');self.assertFalse(log.verify(verify_anchor=True))
if __name__=='__main__':unittest.main()
