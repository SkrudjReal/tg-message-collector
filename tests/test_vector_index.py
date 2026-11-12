"""Bounded index rebuilds without a production database or embedding model."""
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import numpy as np
from usearch.index import Index

from collector.config import EmbeddingProfile
from collector.vector_index import IndexWriter


class PagedStore:
    def __init__(self, count):
        self.rows = {}
        self.calls = []
        self.fail_after = None
        for key in range(1, count + 1):
            vector = np.zeros(384, dtype='<f4')
            vector[key % 384] = 1
            self.rows[key] = (datetime(2026, 1, 1), vector.tobytes())

    async def query(self, sql, params):
        assert 'ORDER BY e.archive_id LIMIT 1024' in sql
        model, watermark, after = params
        self.calls.append((watermark, after))
        if self.fail_after == after:
            self.fail_after = None
            raise ConnectionError('synthetic interrupted page')
        return [dict(archive_id=key, vector=vector)
                for key, (updated, vector) in sorted(self.rows.items())
                if key > after and updated >= watermark][:1024]


class IndexWriterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'test.usearch'
        patcher = patch('collector.vector_index.index_path', return_value=self.path)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.writer = IndexWriter(EmbeddingProfile('local'))

    async def test_rebuild_pages_and_incremental_edit_keep_f32(self):
        store = PagedStore(1030)
        await self.writer.refresh(store, force=True)
        self.assertEqual([after for _, after in store.calls], [0, 1024, 1030])
        published = Index.restore(str(self.path), view=True)
        self.assertEqual(len(published), 1030)
        self.assertEqual(published.ndim, 384)
        self.assertEqual(str(published.dtype), 'ScalarKind.F32')

        vector = np.zeros(384, dtype='<f4')
        vector[2] = 1
        store.rows[1] = (self.writer.watermark + timedelta(seconds=1), vector.tobytes())
        store.calls.clear()
        await self.writer.refresh(store, force=True)
        self.assertEqual([after for _, after in store.calls], [0, 1])
        published = Index.restore(str(self.path), view=True)
        self.assertEqual(len(published), 1030)
        np.testing.assert_array_equal(published.get(1), vector)

    async def test_failed_page_retries_without_publishing_partial_index(self):
        store = PagedStore(1030)
        store.fail_after = 1024
        with self.assertRaises(ConnectionError):
            await self.writer.refresh(store, force=True)
        self.assertFalse(self.path.exists())
        self.assertEqual(self.writer.watermark, datetime(1970, 1, 1))
        await self.writer.refresh(store, force=True)
        self.assertEqual(len(Index.restore(str(self.path), view=True)), 1030)

    async def test_invalid_vector_does_not_replace_published_snapshot(self):
        store = PagedStore(1)
        await self.writer.refresh(store, force=True)
        before = self.path.read_bytes()
        store.rows[1] = (self.writer.watermark, np.zeros(1024, dtype='<f4').tobytes())
        with self.assertRaises(ValueError):
            await self.writer.refresh(store, force=True)
        self.assertEqual(self.path.read_bytes(), before)
