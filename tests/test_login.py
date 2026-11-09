import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from collector.runtime import run


class LoginTests(unittest.IsolatedAsyncioTestCase):
    async def check_login(self, command, expected_qr):
        app = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(),
                              get_me=AsyncMock(return_value=SimpleNamespace(id=42)))
        with patch('collector.runtime.client', return_value=app) as factory:
            await run(SimpleNamespace(command=command))
        factory.assert_called_once_with(interactive=True)
        app.start.assert_awaited_once_with(use_qr=expected_qr)
        app.stop.assert_awaited_once()

    async def test_qr_login(self):
        await self.check_login('login-qr', True)

    async def test_phone_login_remains_available(self):
        await self.check_login('login', False)

    async def test_login_stops_client_if_identity_read_fails(self):
        app = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(),
                              get_me=AsyncMock(side_effect=RuntimeError('failed')))
        with patch('collector.runtime.client', return_value=app):
            with self.assertRaises(RuntimeError):
                await run(SimpleNamespace(command='login-qr'))
        app.stop.assert_awaited_once()
