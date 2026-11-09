"""Incremental HNSW snapshots: writer publishes atomically, MCP memory-maps read-only."""
import asyncio
import hashlib
import os
import time
from datetime import datetime, timezone

import numpy as np
from asyncmy.cursors import SSDictCursor
from usearch.index import Index

from collector.config import DATA, MODEL


def index_path():
    identity = os.getenv('MYSQL_DATABASE', 'tg_message_collector') + '\n' + MODEL
    return DATA / ('semantic-' + hashlib.sha256(identity.encode()).hexdigest()[:20] + '.usearch')


class IndexWriter:
    def __init__(self):
        self.index = None
        self.watermark = datetime(1970, 1, 1)
        self.dirty = False
        self.saved_at = 0

    async def refresh(self, store, force=False):
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        changed = False
        async with store.pool.acquire() as conn, conn.cursor(SSDictCursor) as cur:
            await cur.execute('SELECT e.archive_id,e.vector FROM message_embeddings e '
                              'JOIN messages m ON m.archive_id=e.archive_id '
                              'WHERE e.model=%s AND e.text_hash=m.text_hash '
                              'AND e.updated_at>=%s', (MODEL, self.watermark))
            while rows := await cur.fetchmany(1024):
                keys = np.asarray([r['archive_id'] for r in rows], dtype=np.uint64)
                vectors = np.stack([np.frombuffer(r['vector'], dtype='<f4') for r in rows])
                if self.index is None:
                    self.index = Index(ndim=vectors.shape[1], metric='cos', dtype='f32',
                                       connectivity=16, expansion_add=128, expansion_search=128)
                await asyncio.to_thread(self.index.remove, keys)
                await asyncio.to_thread(self.index.add, keys, vectors, threads=2)
                changed = True
        self.dirty = self.dirty or changed
        if self.dirty and (force or time.monotonic() - self.saved_at >= 30):
            path = index_path()
            tmp = path.with_suffix('.tmp')
            await asyncio.to_thread(self.index.save, str(tmp))
            os.replace(tmp, path)
            self.saved_at = time.monotonic()
            self.dirty = False
        elif self.index is None and index_path().exists():
            # Empty archive after a restore/reset must not expose a previous snapshot.
            index_path().unlink()
        self.watermark = started


class IndexReader:
    def __init__(self):
        self.index = None
        self.stamp = None

    def candidates(self, vector, count):
        path = index_path()
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
