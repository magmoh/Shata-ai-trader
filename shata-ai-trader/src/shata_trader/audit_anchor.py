from __future__ import annotations
import json, os, tempfile
from datetime import datetime, timezone
from pathlib import Path

class FileAuditAnchor:
    """Prototype only. Production anchor must live in a separate trust domain/WORM service."""
    def __init__(self,path:str|Path): self.path=Path(path); self.path.parent.mkdir(parents=True,exist_ok=True)
    def publish(self,head_hash:str,height:int|None=None):
        # v0.8/N2: height makes truncation and witness rollback detectable without a
        # secret key. A witness that claims a height above the local chain is divergent.
        rec={'ts':datetime.now(timezone.utc).isoformat(),'head_hash':head_hash}
        if height is not None: rec['height']=int(height)
        fd,tmp=tempfile.mkstemp(prefix=self.path.name+'.',dir=str(self.path.parent))
        try:
            with os.fdopen(fd,'w') as f: json.dump(rec,f,sort_keys=True); f.flush(); os.fsync(f.fileno())
            os.replace(tmp,self.path)
            dfd=os.open(self.path.parent,os.O_DIRECTORY)
            try: os.fsync(dfd)
            finally: os.close(dfd)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
    def read(self): return json.loads(self.path.read_text()) if self.path.exists() else None
