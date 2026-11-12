"""Incremental HNSW snapshots: writer publishes atomically, MCP memory-maps read-only."""
import asyncio
import hashlib
import os
import time
from datetime import datetime, timezone

import numpy as np
from usearch.index import Index

from collector.config import DATA, active_profile


def index_path(profile=None):
    profile = profile or active_profile()
    identity = os.getenv('MYSQL_DATABASE', 'tg_message_collector') + '\n' + profile.model
    return DATA / ('semantic-' + hashlib.sha256(identity.encode()).hexdigest()[:20] + '.usearch')


class IndexWriter:
    def __init__(self, profile=None):
        self.profile = profile or active_profile()
        self.index = None
        self.watermark = datetime(1970, 1, 1)
        self.dirty = False
        self.saved_at = 0

    async def refresh(self, store, force=False):
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        changed = False
        after = 0
        # Bound the SQL response itself: asyncmy can buffer an unlimited streaming result
        # while native HNSW insertion is running in another thread.
        while rows := await store.query(
            f'SELECT e.archive_id,e.vector FROM {self.profile.table} e '
            'JOIN messages m ON m.archive_id=e.archive_id '
            'WHERE e.model=%s AND e.text_hash=m.text_hash '
            'AND e.updated_at>=%s AND e.archive_id>%s ORDER BY e.archive_id LIMIT 1024',
            (self.profile.model, self.watermark, after),
        ):
            keys = np.asarray([r['archive_id'] for r in rows], dtype=np.uint64)
            vectors = np.stack([np.frombuffer(r['vector'], dtype='<f4') for r in rows])
            if vectors.shape[1] != self.profile.dimension or not np.isfinite(vectors).all():
                raise ValueError('Invalid vector dimension or values for index profile')
            if self.index is None:
                self.index = Index(ndim=vectors.shape[1], metric='cos', dtype='f32',
                                   connectivity=16, expansion_add=128, expansion_search=128)
            await asyncio.to_thread(self.index.remove, keys)
            await asyncio.to_thread(self.index.add, keys, vectors, threads=2)
            changed = True
            after = rows[-1]['archive_id']
        self.dirty = self.dirty or changed
        if self.dirty and (force or time.monotonic() - self.saved_at >= 30):
            path = index_path(self.profile)
            tmp = path.with_suffix('.tmp')
            await asyncio.to_thread(self.index.save, str(tmp))
            os.replace(tmp, path)
            self.saved_at = time.monotonic()
            self.dirty = False
        elif self.index is None and index_path(self.profile).exists():
            # Empty archive after a restore/reset must not expose a previous snapshot.
            index_path(self.profile).unlink()
        self.watermark = started


class IndexReader:
    def __init__(self, profile=None):
        self.profile = profile or active_profile()
        self.index = None
        self.stamp = None

    def candidates(self, vector, count):
        path = index_path(self.profile)
        try:
            stamp = path.stat().st_mtime_ns
        except FileNotFoundError:
            return None
        if self.stamp != stamp:
            self.index = Index.restore(str(path), view=True)
            self.stamp = stamp
        if self.index is None:
            return None
        matches = self.index.search(vector, count=min(count, len(self.index)), threads=2)
        return [int(k) for k in matches.keys], len(self.index), stamp
