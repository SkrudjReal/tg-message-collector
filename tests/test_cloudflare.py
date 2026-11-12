"""No real account traffic; mocked API plus isolated daily accounting."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import AsyncMock, mock_open

import httpx
import numpy as np

from collector.cloudflare import CloudflareEncoder, CloudflareError, UsageLedger
from collector.config import EmbeddingProfile


class CloudflareTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path_patch = patch('collector.cloudflare.DATA', Path(self.temp.name))
        self.path_patch.start()
        self.env = patch.dict(os.environ, dict(CF_ACCOUNT_ID='a' * 32, CF_API_TOKEN='test-secret',
            CF_ALLOW_REAL_MESSAGES='false', CF_BATCH_SIZE='2', CF_REQUESTS_PER_MINUTE='3000',
            CF_DAILY_NEURON_BUDGET='20'))
        self.env.start()

    async def asyncTearDown(self):
        self.env.stop()
        self.path_patch.stop()
        self.temp.cleanup()

    async def test_consent_prevents_any_http(self):
        def handler(request):
            self.fail('Must not send real text')
        with self.assertRaises(CloudflareError):
            await CloudflareEncoder(httpx.MockTransport(handler)).encode(['private archive text'])
        self.assertFalse(UsageLedger().path.exists())

    async def test_batching_retry_and_normalization(self):
        calls = []
        def handler(request):
            calls.append(json.loads(request.content)['text'])
            if len(calls) == 1:
                return httpx.Response(429, headers={'Retry-After': '0'})
            return httpx.Response(200, json={'success': True, 'result': {'data':
                [[2.0] + [0.0] * 1023 for _ in calls[-1]]}})
        vectors = await CloudflareEncoder(httpx.MockTransport(handler)).encode(['a', 'b', 'c'], synthetic=True)
        self.assertEqual([len(x) for x in calls], [2, 2, 1])
        np.testing.assert_equal(vectors.shape, (3, 1024))
        np.testing.assert_allclose(np.linalg.norm(vectors, axis=1), 1)
        self.assertEqual(json.loads(UsageLedger().path.read_text())['attempts'], 3)

    async def test_bad_dimensions_and_auth_are_not_retried_or_logged(self):
        for response in [httpx.Response(401, text='test-secret'),
                         httpx.Response(200, json={'success': True, 'result': {'data': [[1] * 384]}})]:
            calls = []
            def handler(request):
                calls.append(request)
                return response
            with self.assertRaises(CloudflareError) as caught:
                await CloudflareEncoder(httpx.MockTransport(handler)).encode(['synthetic'], synthetic=True)
            self.assertEqual(len(calls), 1)
            self.assertNotIn('test-secret', str(caught.exception))

    async def test_budget_is_persistent_and_fail_closed(self):
        with patch.dict(os.environ, CF_DAILY_NEURON_BUDGET='1'):
            UsageLedger().reserve(['synthetic'])
            with self.assertRaises(CloudflareError):
                UsageLedger().reserve(['synthetic'])

    async def test_billed_neurons_settle_once_and_unknown_attempts_stay_reserved(self):
        ledger = UsageLedger()
        _, reservation = ledger.reserve(['synthetic'])
        ledger.settle(reservation, 0.1)
        ledger.settle(reservation, 0.1)
        state = json.loads(ledger.path.read_text())
        self.assertAlmostEqual(state['reserved_neurons'], 0.1)
        self.assertAlmostEqual(state['actual_neurons'], 0.1)
        ledger.reserve(['uncertain network outcome'])
        self.assertAlmostEqual(json.loads(ledger.path.read_text())['reserved_neurons'], 1.1)

    async def test_long_retry_after_is_shared_without_sleeping(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(429, headers={'Retry-After': '300'})
        encoder = CloudflareEncoder(httpx.MockTransport(handler))
        with self.assertRaises(CloudflareError):
            await encoder.encode(['synthetic'], synthetic=True)
        with self.assertRaises(CloudflareError):
            await encoder.encode(['synthetic'], synthetic=True)
        self.assertEqual(len(calls), 1)

    def test_profile_dimensions_are_separate(self):
        self.assertEqual(EmbeddingProfile('local').dimension, 384)
        self.assertEqual(EmbeddingProfile('cloudflare').dimension, 1024)
        self.assertNotEqual(EmbeddingProfile('local').table, EmbeddingProfile('cloudflare').table)

    async def test_full_index_build_pauses_when_memory_is_insufficient(self):
        from collector.embedding_jobs import check_build_memory, IndexCapacityError
        store = AsyncMock()
        store.query.return_value = [{'n': 1000000}]
        memory = 'MemTotal: 4000000 kB\nMemAvailable: 100000 kB\nSwapFree: 100000 kB\n'
        with patch('builtins.open', mock_open(read_data=memory)):
            with self.assertRaises(IndexCapacityError):
                await check_build_memory(store, EmbeddingProfile('cloudflare'))


if __name__ == '__main__':
    unittest.main()
