import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from collector import mcp_server as server


class MCPLifespanTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_requests_share_process_resources(self):
        store = AsyncMock()
        with patch.object(server.Store, 'open', AsyncMock(return_value=store)) as opened, \
                patch.object(server, 'Search') as search:
            app = server.create_http_app()
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url=f'http://127.0.0.1:{server.port}',
                    headers={'Accept': 'application/json, text/event-stream'},
                ) as client:
                    responses = await asyncio.gather(*[
                        client.post('/mcp', json={
                            'jsonrpc': '2.0', 'id': i, 'method': 'initialize',
                            'params': {'protocolVersion': '2025-03-26', 'capabilities': {},
                                       'clientInfo': {'name': 'lifespan-test', 'version': '1'}},
                        }) for i in range(20)
                    ])
                self.assertTrue(all(r.status_code == 200 and 'result' in r.json() for r in responses))
                opened.assert_awaited_once_with(readonly=True)
                search.assert_called_once_with(store)
                store.close.assert_not_awaited()
                self.assertIs(server.store, store)
            store.close.assert_awaited_once()
            self.assertIsNone(server.store)
            self.assertIsNone(server.search)

    async def test_search_initialization_failure_closes_pool(self):
        store = AsyncMock()
        with patch.object(server.Store, 'open', AsyncMock(return_value=store)), \
                patch.object(server, 'Search', side_effect=RuntimeError('startup failed')):
            with self.assertRaises(RuntimeError):
                async with server.lifespan(None):
                    self.fail('startup should fail')
            store.close.assert_awaited_once()
            self.assertIsNone(server.store)


if __name__ == '__main__':
    unittest.main()
