import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from pyrogram import enums
from pyrogram.handlers import EditedMessageHandler, MessageHandler
from pyrogram.types import Chat, Message

from collector.runtime import history_loop, run


CHATS = [Chat(id=identifier, type=kind) for identifier, kind in (
    (-1, enums.ChatType.GROUP), (-1001, enums.ChatType.SUPERGROUP),
    (-1002, enums.ChatType.FORUM), (-1003, enums.ChatType.CHANNEL),
    (123, enums.ChatType.PRIVATE), (999, enums.ChatType.PRIVATE),
    (456, enums.ChatType.BOT), (-1004, enums.ChatType.DIRECT),
)]
ACCEPTED = {-1, -1001, -1002, -1003}


class ChatScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_new_and_edit_handlers_collect_groups_and_channels(self):
        handlers = []
        app = SimpleNamespace(add_handler=handlers.append, start=AsyncMock(), stop=AsyncMock(),
                              get_me=AsyncMock(return_value=SimpleNamespace(id=999)), is_connected=True)
        store = SimpleNamespace(save_message=AsyncMock(),
                                save_user=AsyncMock(), close=AsyncMock())
        messages = [Message(id=i + 1, chat=chat) for i, chat in enumerate(CHATS)]
        messages.append(Message(id=99))

        async def dispatch():
            for handler in handlers:
                for msg in messages:
                    if await handler.check(app, msg):
                        await handler.callback(app, msg)

        with patch('collector.runtime.client', return_value=app), \
             patch('collector.runtime.Store.open', new=AsyncMock(return_value=store)), \
             patch('collector.runtime.DATA') as data, \
             patch('collector.runtime.chat_ids', return_value=set()), \
             patch('collector.runtime.supervise', new=AsyncMock()), \
             patch('collector.runtime.idle', new=dispatch):
            data.__truediv__.return_value.exists.return_value = True
            await run(SimpleNamespace(command='run', history_limit=10))

        self.assertEqual(store.save_message.await_count, 8)
        self.assertEqual({call.args[0].chat.id for call in store.save_message.await_args_list}, ACCEPTED)
        self.assertEqual({type(handler) for handler in handlers}, {MessageHandler, EditedMessageHandler})

    async def check_history(self, allowed):
        async def dialogs():
            for chat in CHATS:
                yield SimpleNamespace(chat=chat)

        async def sleep(seconds):
            if seconds == 300:
                raise asyncio.CancelledError

        by_id = {chat.id: chat for chat in CHATS}
        app = SimpleNamespace(get_dialogs=dialogs, get_chat=AsyncMock(side_effect=lambda i: by_id[i]))
        store = SimpleNamespace(save_chat=AsyncMock())
        with patch('collector.runtime.chat_ids', return_value=allowed), \
             patch('collector.runtime.sync_chat', new=AsyncMock()) as sync, \
             patch('collector.runtime.asyncio.sleep', new=sleep):
            with self.assertRaises(asyncio.CancelledError):
                await history_loop(app, store, 999, 10)
        expected = ACCEPTED & allowed if allowed else ACCEPTED
        self.assertEqual({call.args[2] for call in sync.await_args_list}, expected)
        self.assertEqual({call.args[0].id for call in store.save_chat.await_args_list}, expected)

    async def test_dialog_discovery_excludes_private_and_saved_messages(self):
        await self.check_history(set())

    async def test_explicit_allowlist_cannot_enable_private_or_saved_messages(self):
        await self.check_history({-1, 123, 999, 456, -1004})
