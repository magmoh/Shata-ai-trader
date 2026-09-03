from __future__ import annotations
import fcntl, hashlib, json, os, threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class HashChainedAuditLog:
    """Local durable audit with guarded external witness publication.

    The local hash chain is an integrity structure, not a cryptographic proof against
    an attacker who can rewrite the entire host.  The external witness is therefore
    never overwritten when it diverges from the lineage of the local chain.
    Production must place the witness in an independent/WORM/signed trust domain.
    """

    def __init__(self, path: str | Path, anchor=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.anchor = anchor
        self.lock_path = self.path.with_suffix(self.path.suffix + '.lock')
        self.pending_anchor_path = self.path.with_suffix(self.path.suffix + '.anchor_pending')
        self._thread_lock = threading.RLock()
        self.anchor_degraded = False
        self.last_anchor_error = None

    def _last_hash_unlocked(self):
        if not self.path.exists() or self.path.stat().st_size == 0:
            return 'GENESIS'
        lines = self.path.read_text(encoding='utf-8', errors='strict').splitlines()
        if not lines:
            return 'GENESIS'
        try:
            return json.loads(lines[-1])['hash']
        except Exception:
            raise RuntimeError('Audit tail is corrupt; recovery required before append')

    def _records_unlocked(self):
        if not self.path.exists() or self.path.stat().st_size == 0:
            return []
        out = []
        for line in self.path.read_text(encoding='utf-8', errors='strict').splitlines():
            if not line:
                continue
            out.append(json.loads(line))
        return out

    def head(self):
        with self._thread_lock, self.lock_path.open('a+') as lf:
            fcntl.flock(lf.fileno(), fcntl.LOCK_SH)
            try:
                return self._last_hash_unlocked()
            finally:
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    def _local_height(self) -> int:
        try:
            return len(self._records_unlocked())
        except Exception:
            return -1

    def _height_relation(self, external, local_height: int) -> str:
        """v0.8/N2: compare the witnessed chain height against the local one.

        A witness recorded at a height greater than what the local chain now holds
        means local history was truncated or replaced. Hash-head comparison alone
        cannot see this, because a truncated prefix is itself a valid chain.
        """
        if not isinstance(external, dict):
            return 'unknown'
        h = external.get('height')
        if local_height < 0:
            return 'unknown'
        if h is None:
            # A witness written by this version always carries a height. A height-less
            # witness against a non-empty local chain is a downgrade attempt, not a
            # legacy record: treat it as shrunk rather than trusting it.
            return 'ok' if local_height == 0 else 'shrunk'
        try:
            h = int(h)
        except Exception:
            return 'unknown'
        if h > local_height:
            return 'shrunk'
        return 'ok'

    def _lineage_relation(self, external_head: str | None, candidate_head: str) -> str:
        """Return ancestor/equal/descendant/divergent relative to candidate_head.

        ancestor: external witness is behind candidate and can safely advance.
        descendant: external witness is already ahead; never roll it back.
        divergent: witness is not on the local lineage; do not overwrite it.
        """
        if external_head is None:
            return 'missing'
        if external_head == candidate_head:
            return 'equal'
        try:
            records = self._records_unlocked()
        except Exception:
            return 'divergent'
        prev_of = {r.get('hash'): r.get('prev_hash') for r in records if r.get('hash')}

        cur = candidate_head
        seen = set()
        while cur and cur not in seen:
            if cur == external_head:
                return 'ancestor'
            seen.add(cur)
            cur = prev_of.get(cur)
            if cur == 'GENESIS':
                if external_head == 'GENESIS':
                    return 'ancestor'
                break

        cur = external_head
        seen.clear()
        while cur and cur not in seen:
            if cur == candidate_head:
                return 'descendant'
            seen.add(cur)
            cur = prev_of.get(cur)
            if cur == 'GENESIS':
                break
        return 'divergent'

    def _mark_anchor_pending(self, digest, exc):
        self.anchor_degraded = True
        self.last_anchor_error = f'{type(exc).__name__}: {exc}'
        tmp = self.pending_anchor_path.with_suffix(self.pending_anchor_path.suffix + '.tmp')
        data = json.dumps(
            {'head_hash': digest, 'error': self.last_anchor_error, 'ts': datetime.now(timezone.utc).isoformat()},
            sort_keys=True,
        ).encode()
        fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self.pending_anchor_path)

    def _mark_lineage_mismatch(self, digest, external_head):
        self.anchor_degraded = True
        self.last_anchor_error = f'ANCHOR_LINEAGE_MISMATCH external={external_head} local={digest}'
        tmp = self.pending_anchor_path.with_suffix(self.pending_anchor_path.suffix + '.tmp')
        data = json.dumps(
            {'head_hash': digest, 'error': self.last_anchor_error, 'ts': datetime.now(timezone.utc).isoformat()},
            sort_keys=True,
        ).encode()
        fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self.pending_anchor_path)

    def _publish_best_effort(self, digest):
        if not self.anchor:
            return
        try:
            cur = self.anchor.read()
            external_head = cur.get('head_hash') if cur else None
            local_height = self._local_height()
            if self._height_relation(cur, local_height) == 'shrunk':
                self._mark_lineage_mismatch(digest, f"{external_head}@height{cur.get('height')}>local{local_height}")
                return
            relation = self._lineage_relation(external_head, digest)
            if relation == 'missing':
                # A witness may only be initialized at GENESIS or the first append.
                records = self._records_unlocked()
                if len(records) > 1:
                    self._mark_lineage_mismatch(digest, None)
                    return
            elif relation == 'divergent':
                self._mark_lineage_mismatch(digest, external_head)
                return
            elif relation in {'equal', 'descendant'}:
                # Already witnessed at this head or at a newer descendant; never roll back.
                self.anchor_degraded = False
                self.last_anchor_error = None
                if self.pending_anchor_path.exists():
                    self.pending_anchor_path.unlink()
                return

            self.anchor.publish(digest, self._local_height())
            self.anchor_degraded = False
            self.last_anchor_error = None
            if self.pending_anchor_path.exists():
                self.pending_anchor_path.unlink()
        except Exception as exc:
            self._mark_anchor_pending(digest, exc)

    def sync_anchor(self):
        if not self.anchor:
            return True
        digest = self.head()
        try:
            cur = self.anchor.read()
            external_head = cur.get('head_hash') if cur else None
            local_height = self._local_height()
            if self._height_relation(cur, local_height) == 'shrunk':
                self._mark_lineage_mismatch(digest, f"{external_head}@height{cur.get('height')}>local{local_height}")
                return False
            if external_head is None:
                if digest != 'GENESIS':
                    self._mark_lineage_mismatch(digest, None)
                    return False
                self.anchor.publish(digest, local_height)
            else:
                relation = self._lineage_relation(external_head, digest)
                if relation == 'divergent':
                    self._mark_lineage_mismatch(digest, external_head)
                    return False
                if relation == 'ancestor':
                    self.anchor.publish(digest, local_height)
                # equal/descendant: do not overwrite/roll back.
            self.anchor_degraded = False
            self.last_anchor_error = None
            if self.pending_anchor_path.exists():
                self.pending_anchor_path.unlink()
            return True
        except Exception as exc:
            self._mark_anchor_pending(digest, exc)
            return False

    def append(self, event_type: str, payload: dict[str, Any]) -> str:
        with self._thread_lock, self.lock_path.open('a+') as lf:
            fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
            try:
                prev = self._last_hash_unlocked()
                body = {
                    'ts': datetime.now(timezone.utc).isoformat(),
                    'event_type': event_type,
                    'payload': payload,
                    'prev_hash': prev,
                }
                canonical = json.dumps(body, sort_keys=True, separators=(',', ':'))
                digest = hashlib.sha256(canonical.encode()).hexdigest()
                data = (json.dumps({**body, 'hash': digest}, sort_keys=True) + '\n').encode()
                fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
                try:
                    off = 0
                    while off < len(data):
                        off += os.write(fd, data[off:])
                    os.fsync(fd)
                finally:
                    os.close(fd)
            finally:
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
        self._publish_best_effort(digest)
        return digest

    def verify(self, verify_anchor=False):
        if not self.path.exists():
            return True
        try:
            lines = self.path.read_text(encoding='utf-8', errors='strict').splitlines()
        except Exception:
            return False
        prev = 'GENESIS'
        for line in lines:
            try:
                rec = json.loads(line)
                claimed = rec.pop('hash')
            except Exception:
                return False
            if rec.get('prev_hash') != prev:
                return False
            if hashlib.sha256(json.dumps(rec, sort_keys=True, separators=(',', ':')).encode()).hexdigest() != claimed:
                return False
            prev = claimed
        if verify_anchor and self.anchor:
            try:
                a = self.anchor.read()
            except Exception:
                return False
            if not a or a.get('head_hash') != prev:
                return False
            # v0.8/N2: the head alone cannot see a truncated prefix — a truncated
            # chain is itself a valid chain. Height closes that.
            if self._height_relation(a, len(lines)) == 'shrunk':
                self.anchor_degraded = True
                self.last_anchor_error = (
                    f"ANCHOR_HEIGHT_MISMATCH external={a.get('height')} local={len(lines)}"
                )
                return False
        return True
