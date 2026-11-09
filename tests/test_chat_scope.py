import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from pyrogram import enums, raw
from pyrogram.handlers import EditedMessageHandler, MessageHandler, RawUpdateHandler
from pyrogram.types import Chat, Message

from collector.runtime import fetch_bio, handle_raw_update, profile_loop, run


CHATS = [Chat(id=identifier, type=kind) for identifier, kind in (
    (-1, enums.ChatType.GROUP), (-1001, enums.ChatType.SUPERGROUP),
    (-1002, enums.ChatType.FORUM), (-1003, enums.ChatType.CHANNEL),
    (123, enums.ChatType.PRIVATE), (999, enums.ChatType.PRIVATE),
    (456, enums.ChatType.BOT), (-1004, enums.ChatType.DIRECT),
)]
ACCEPTED = {-1, -1001, -1002, -1003}


class ChatScopeTests(unittest.IsolatedAsyncioTestCase):
    async def check_run(self, allowed):
        handlers = []
        app = SimpleNamespace(add_handler=lambda handler, group=0: handlers.append(handler),
                              start=AsyncMock(), stop=AsyncMock(),
                              get_me=AsyncMock(return_value=SimpleNamespace(id=999)), is_connected=True,
                              get_dialogs=Mock(), get_chat_history=Mock(), get_messages=AsyncMock(),
                              get_chat=AsyncMock(), download_media=AsyncMock())
        store = SimpleNamespace(save_message=AsyncMock(return_value=None), save_user=AsyncMock(),
                                close=AsyncMock(), start_run=AsyncMock(), finish_run=AsyncMock(),
                                event_saved=AsyncMock(), record_error=AsyncMock())
        messages = [Message(id=i + 1, chat=chat) for i, chat in enumerate(CHATS)]
        messages.append(Message(id=99))

        async def dispatch():
            for handler in handlers:
                if isinstance(handler, RawUpdateHandler):
                    continue
                for msg in messages:
                    if await handler.check(app, msg):
                        await handler.callback(app, msg)

        with patch('collector.runtime.client', return_value=app), \
             patch('collector.runtime.Store.open', new=AsyncMock(return_value=store)), \
             patch('collector.runtime.DATA') as data, \
             patch('collector.runtime.chat_ids', return_value=allowed), \
             patch('collector.runtime.supervise', new=AsyncMock()) as supervisor, \
             patch('collector.runtime.idle', new=dispatch):
            data.__truediv__.return_value.exists.return_value = True
            await run(SimpleNamespace(command='run'))

        expected = ACCEPTED & allowed if allowed else ACCEPTED
        self.assertEqual(store.save_message.await_count, 2 * len(expected))
        self.assertEqual({call.args[0].chat.id for call in store.save_message.await_args_list}, expected)
        self.assertEqual({type(handler) for handler in handlers},
                         {MessageHandler, EditedMessageHandler, RawUpdateHandler})
        self.assertEqual({call.args[0] for call in supervisor.call_args_list}, {'profiles', 'embeddings', 'heartbeat'})
        for name in ('get_dialogs', 'get_chat_history', 'get_messages', 'get_chat', 'download_media'):
            getattr(app, name).assert_not_called()
        store.record_error.assert_not_called()
        store.finish_run.assert_awaited_once()

    async def test_only_handlers_no_history_dialog_scan_or_media_download(self):
        await self.check_run(set())

    async def test_explicit_allowlist_cannot_enable_private_or_saved_messages(self):
        await self.check_run({-1, 123, 999, 456, -1004})

    async def test_raw_counters_are_passive_and_require_archived_group_message(self):
        store = SimpleNamespace(query=AsyncMock(return_value=[{'type': 'channel'}]),
                                save_metrics=AsyncMock(), event_saved=AsyncMock())
        queue = Mock()
        update = raw.types.UpdateChannelMessageViews(channel_id=123, id=7, views=200)
        await handle_raw_update(store, update, queue, set())
        store.save_metrics.assert_awaited_once_with(-1000000000123, 7, views=200, source='views_update')
        queue.assert_not_called()
        store.save_metrics.reset_mock()
        store.query.return_value = []
        await handle_raw_update(store, update, queue, set())
        store.save_metrics.assert_not_called()
        update = raw.types.UpdateMessageReactions(peer=raw.types.PeerUser(user_id=123), msg_id=7,
                                                  reactions=raw.types.MessageReactions(results=[]))
        store.query.return_value = [{'type': 'private'}]
        await handle_raw_update(store, update, queue, set())
        store.save_metrics.assert_not_called()

    async def test_raw_profile_update_changes_known_user_without_polling(self):
        store = SimpleNamespace(query=AsyncMock(return_value=[{'user_id': 123}]),
                                save_profile=AsyncMock(), event_saved=AsyncMock())
        queue = Mock()
        update = raw.types.UpdateUserName(user_id=123, first_name='New', last_name='Name',
                                          usernames=[raw.types.Username(username='newname', active=True)])
        await handle_raw_update(store, update, queue, set())
        store.save_profile.assert_awaited_once_with(123, dict(username='newname', first_name='New', last_name='Name'),
                                                    source='telegram_update')
        queue.assert_called_once_with(123)
        queue.reset_mock()
        store.query.return_value = []
        await handle_raw_update(store, update, queue, set())
        queue.assert_not_called()

    async def test_profile_worker_does_not_poll_users_when_queue_is_empty(self):
        result = raw.types.users.UserFull(full_user=raw.types.UserFull(
            id=123, settings=raw.types.PeerSettings(), notify_settings=raw.types.PeerNotifySettings(),
            common_chats_count=0, about='observed bio', pinned_msg_id=77, personal_channel_id=456,
            personal_channel_message=88), users=[], chats=[])
        app = SimpleNamespace(resolve_peer=AsyncMock(return_value=raw.types.InputPeerUser(user_id=123, access_hash=1)),
                              invoke=AsyncMock(return_value=result), get_chat=AsyncMock(), get_messages=AsyncMock())
        store = SimpleNamespace(save_bio=AsyncMock())
        queue, pending = asyncio.Queue(), {123}
        task = asyncio.create_task(profile_loop(app, store, queue, pending))
        try:
            await asyncio.sleep(0)
            app.get_chat.assert_not_called()
            app.invoke.assert_not_called()
            queue.put_nowait(123)
            await queue.join()
            app.resolve_peer.assert_awaited_once_with(123)
            self.assertIsInstance(app.invoke.await_args.args[0], raw.functions.users.GetFullUser)
            app.get_chat.assert_not_called()
            app.get_messages.assert_not_called()
            store.save_bio.assert_awaited_once_with(123, 'observed bio')
            self.assertEqual(pending, set())
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_bad_profile_does_not_abort_the_next_profile_or_erase_bio(self):
        queue, pending = asyncio.Queue(), {123, 456}
        for user_id in (123, 456):
            queue.put_nowait(user_id)
        store = SimpleNamespace(save_bio=AsyncMock(), record_error=AsyncMock())

        async def no_delay(_):
            pass

        with patch('collector.runtime.fetch_bio', new=AsyncMock(side_effect=[AttributeError('bad'), 'good bio'])), \
             patch('collector.runtime.asyncio.sleep', new=no_delay):
            task = asyncio.create_task(profile_loop(None, store, queue, pending))
            try:
                await asyncio.wait_for(queue.join(), timeout=2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        store.save_bio.assert_awaited_once_with(456, 'good bio')
        self.assertEqual(store.record_error.await_args.args[0], 'profile')
        self.assertEqual(store.record_error.await_args.kwargs['user_id'], 123)
        self.assertEqual(pending, set())

    async def test_invalid_full_profile_is_not_treated_as_an_empty_bio(self):
        app = SimpleNamespace(resolve_peer=AsyncMock(return_value=raw.types.InputPeerUser(user_id=123, access_hash=1)),
                              invoke=AsyncMock(return_value=None))
        with self.assertRaises(ValueError):
            await fetch_bio(app, 123)

    async def test_raw_forward_and_reaction_updates_preserve_received_counts(self):
        store = SimpleNamespace(query=AsyncMock(return_value=[{'type': 'channel'}]),
                                save_metrics=AsyncMock(), event_saved=AsyncMock())
        queue = Mock()
        await handle_raw_update(store, raw.types.UpdateChannelMessageForwards(channel_id=123, id=7, forwards=0),
                                queue, set())
        store.save_metrics.assert_awaited_once_with(-1000000000123, 7, forwards=0, source='forwards_update')
        store.save_metrics.reset_mock()
        reactions = raw.types.MessageReactions(results=[raw.types.ReactionCount(
            reaction=raw.types.ReactionEmoji(emoticon='👍'), count=2)])
        await handle_raw_update(store, raw.types.UpdateMessageReactions(
            peer=raw.types.PeerChannel(channel_id=123), msg_id=7, reactions=reactions), queue, set())
        store.save_metrics.assert_awaited_once_with(-1000000000123, 7,
            reactions=[dict(emoji='👍', custom_emoji_id=None, paid=False, count=2)], source='reactions_update')
