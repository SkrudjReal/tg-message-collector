import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from pyrogram import enums
from pyrogram.types import Chat, Message

from collector.import_channels import ChannelImporter, channel_only, load_targets, parse_since
from collector.storage import message_row


class ImportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.chat = Chat(id=-100123, type=enums.ChatType.CHANNEL, title='Channel')
        self.store = SimpleNamespace(execute=AsyncMock(), save_chat=AsyncMock(), save_history_page=AsyncMock())
        self.app = SimpleNamespace(get_chat=AsyncMock(return_value=self.chat))
        self.importer = ChannelImporter(self.app, self.store, 7, targets=['test_channel'],
                                        since=parse_since('2025-01-01'), pause=0, page_size=2)
        self.importer.target_ids.add(self.chat.id)
        self.importer.channel_details = AsyncMock()
        self.importer.enrich = AsyncMock()

    def message(self, mid):
        return Message(id=mid, chat=self.chat, date=datetime(2026, 1, 1))

    async def test_channel_scope_excludes_all_group_and_private_types(self):
        for kind in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP, enums.ChatType.FORUM,
                     enums.ChatType.PRIVATE, enums.ChatType.BOT):
            self.assertFalse(channel_only(Chat(id=1, type=kind)))
        self.assertTrue(channel_only(self.chat))
        self.chat.raw = SimpleNamespace(left=True)
        self.assertTrue(channel_only(self.chat))  # Public history does not require joining.
        self.chat.raw = SimpleNamespace(kicked=True)
        self.assertFalse(channel_only(self.chat))

    async def test_group_requires_numeric_target_and_explicit_type(self):
        with self.assertRaises(ValueError):
            ChannelImporter(None, None, 7, targets=['deploycomments'], since=self.importer.since,
                            chat_type='supergroup')
        group = Chat(id=-10055, type=enums.ChatType.SUPERGROUP, title='Group')
        importer = ChannelImporter(None, None, 7, targets=['id:-10055'], since=self.importer.since,
                                   chat_type='supergroup')
        self.assertTrue(importer.accepts(group))
        self.assertFalse(self.importer.accepts(group))
        self.assertFalse(importer.accepts(self.chat))
        self.assertFalse(importer.accepts(Chat(id=55, type=enums.ChatType.PRIVATE)))

    async def test_numeric_group_discovery_resolves_only_requested_id(self):
        self.store.query = AsyncMock(return_value=[])
        group = Chat(id=-10055, type=enums.ChatType.SUPERGROUP, title='Group')
        self.app.get_chat.return_value = group
        self.app.get_dialogs = Mock(side_effect=AssertionError('Never scan dialogs'))

        async def newest(chat_id, limit):
            self.assertEqual(chat_id, group.id)
            yield Message(id=9, chat=group, date=datetime(2026, 1, 1))

        self.app.get_chat_history = newest
        importer = ChannelImporter(self.app, self.store, 7, targets=['id:-10055'], since=self.importer.since,
                                   chat_type='supergroup', pause=0)
        await importer.discover()
        self.app.get_chat.assert_awaited_once_with(-10055, force_full=False)
        self.app.get_dialogs.assert_not_called()
        self.assertEqual(importer.target_ids, {-10055})

    async def test_group_scope_differs_from_channel_scope_for_same_numeric_target(self):
        channel = ChannelImporter(None, None, 7, targets=['id:-10055'], since=self.importer.since)
        group = ChannelImporter(None, None, 7, targets=['id:-10055'], since=self.importer.since,
                                chat_type='supergroup')
        self.assertNotEqual(channel.scope_key, group.scope_key)

    async def test_old_messages_are_not_enriched_saved_or_followed_to_older_pages(self):
        boundary = self.importer.since
        recent = self.message(8)
        recent.date = boundary
        old = self.message(7)
        old.date = boundary - timedelta(seconds=1)
        calls = []

        async def history(*args, **kwargs):
            calls.append(kwargs)
            yield recent
            yield old

        self.app.get_chat_history = history
        await self.importer.import_channel(dict(chat_id=self.chat.id, next_max_id=8))
        self.assertEqual(len(calls), 1)
        self.importer.enrich.assert_awaited_once_with(recent)
        saved = self.store.save_history_page.await_args
        self.assertEqual(saved.args[0], [recent])
        self.assertEqual(saved.args[3], 0)
        self.assertEqual(saved.kwargs['scope_key'], self.importer.scope_key)

    async def test_unlisted_channel_rejected_before_any_telegram_request(self):
        with self.assertRaises(ValueError):
            await self.importer.import_channel(dict(chat_id=-100999, next_max_id=8))
        self.app.get_chat.assert_not_called()

    async def test_scope_identity_changes_with_targets_or_start_date(self):
        another = ChannelImporter(None, None, 7, targets=['other_channel'], since=self.importer.since)
        later = ChannelImporter(None, None, 7, targets=self.importer.targets, since=datetime(2026, 1, 1))
        self.assertNotEqual(another.scope_key, self.importer.scope_key)
        self.assertNotEqual(later.scope_key, self.importer.scope_key)

    async def test_discovery_reads_only_explicit_channel_without_scanning_dialogs(self):
        self.store.query = AsyncMock(return_value=[])
        self.chat.username = 'test_channel'
        self.app.get_dialogs = Mock(side_effect=AssertionError('Must never scan dialogs'))

        async def newest(chat_id, limit):
            self.assertEqual(chat_id, self.chat.id)
            self.assertEqual(limit, 1)
            yield self.message(10)

        self.app.get_chat_history = newest
        await self.importer.discover()
        self.app.get_chat.assert_awaited_once_with('test_channel', force_full=False)
        self.app.get_dialogs.assert_not_called()
        self.assertIn('INSERT INTO scoped_channel_imports', self.store.execute.await_args.args[0])

    async def test_discovery_does_not_read_history_when_link_resolves_to_group(self):
        self.store.query = AsyncMock(return_value=[])
        self.app.get_chat.return_value = Chat(id=-123, type=enums.ChatType.SUPERGROUP)
        self.app.get_chat_history = Mock(side_effect=AssertionError('Group history must not be read'))
        await self.importer.discover()
        self.app.get_chat_history.assert_not_called()
        self.assertEqual(self.importer.state['resolution_errors'][0]['target'], 'test_channel')

    async def test_allowlist_rejects_empty_invites_message_links_and_nontelegram_urls(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'targets.txt'
            for contents in ('', 'https://t.me/c/123/456', 'https://t.me/+secret', 'https://example.com/test'):
                path.write_text(contents)
                with self.assertRaises(ValueError):
                    load_targets(path)
            path.write_text('https://t.me/TestChannel\nhttps://t.me/testchannel\n')
            self.assertEqual(load_targets(path), ['testchannel'])

    async def test_year_boundary_uses_warsaw_and_is_stored_as_utc(self):
        self.assertEqual(parse_since('2025-01-01'), datetime(2024, 12, 31, 23))

    async def test_resume_inclusive_boundary_and_cursor_after_successful_page(self):
        calls = []

        async def history(chat_id, limit, max_id):
            calls.append((chat_id, limit, max_id))
            for mid in ({8: [8, 6], 5: [5], 4: []}[max_id]):
                yield self.message(mid)

        self.app.get_chat_history = history
        await self.importer.import_channel(dict(chat_id=self.chat.id, next_max_id=8))
        self.assertEqual([x[2] for x in calls], [8, 5, 4])
        self.assertEqual([x.args[0].id for x in self.importer.enrich.await_args_list], [8, 6, 5])
        self.assertEqual([c.args[3] for c in self.store.save_history_page.await_args_list], [5, 4])
        self.assertIn("status='complete'", self.store.execute.await_args.args[0])

    async def test_failed_write_does_not_advance_checkpoint_or_claim_completion(self):
        async def history(*args, **kwargs):
            yield self.message(8)
            yield self.message(7)

        self.app.get_chat_history = history
        self.store.save_history_page.side_effect = RuntimeError('database unavailable')
        with self.assertRaises(RuntimeError):
            await self.importer.import_channel(dict(chat_id=self.chat.id, next_max_id=8))
        writes = [x.args[0] for x in self.store.execute.await_args_list]
        self.assertFalse(any('processed=processed+' in x or "status='complete'" in x for x in writes))

    async def test_nonadvancing_api_page_is_rejected(self):
        async def history(*args, **kwargs):
            yield self.message(9)

        self.app.get_chat_history = history
        with self.assertRaises(ValueError):
            await self.importer.import_channel(dict(chat_id=self.chat.id, next_max_id=8))
        self.importer.enrich.assert_not_called()

    async def test_import_serialization_identifies_account_and_signature(self):
        msg = self.message(1)
        msg.author_signature = 'Editor'
        row = message_row(msg, 7, source='channel_history')
        self.assertEqual(row['source_account_id'], 7)
        self.assertEqual(row['ingest_source'], 'channel_history')
        self.assertEqual(row['author_signature'], 'Editor')


if __name__ == '__main__':
    unittest.main()
