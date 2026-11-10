"""Integration tests against an isolated MySQL database. Never use the real archive.

Run: .venv/bin/python tests/test_archive.py
Requires the local sudo mysql administration path used during provisioning.
"""
import hashlib
import asyncio
import os
from pathlib import Path
import subprocess
import sys
import unittest
import uuid
import socket
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TEST_DB = 'tg_message_collector_test_' + uuid.uuid4().hex[:12]
os.environ['MYSQL_DATABASE'] = TEST_DB

from collector.storage import Store, message_row, utc  # noqa: E402
from collector.search import Search  # noqa: E402
from collector.runtime import embedding_loop  # noqa: E402
from collector.vector_index import index_path  # noqa: E402
from pyrogram import enums  # noqa: E402
from pyrogram.types import Message, Chat, User  # noqa: E402


def admin(sql):
    subprocess.run(['sudo', '-n', 'mysql'], input=sql, text=True, check=True, capture_output=True)


def row(chat_id=-10001, message_id=1, text='Привет из архива', sender=123, edited=None, sent=None):
    return dict(chat_id=chat_id, message_id=message_id, sender_user_id=sender, sender_chat_id=None,
                recipient_user_id=None, sent_at=sent or datetime(2026, 1, 1), edited_at=edited,
                reply_to_chat_id=None, reply_to_message_id=None, topic_id=None,
                text=text, text_hash=hashlib.sha256(text.encode()).hexdigest(),
                media_type=None, media=None, entities=None, forward_info=None,
                media_group_id=None, service_type=None, outgoing=False)


class ArchiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # All destructive cleanup is strictly within a freshly generated test database.
        admin('USE `' + TEST_DB + '`;' + ''.join('TRUNCATE TABLE ' + name + ';' for name in (
            'messages', 'message_revisions', 'message_embeddings', 'users', 'chats', 'chat_sync', 'unresolved_deletions',
            'user_history', 'message_metrics', 'message_metric_history', 'collector_runs', 'collector_errors',
            'channel_imports', 'message_sources', 'scoped_channel_imports')))
        index_path().unlink(missing_ok=True)
        self.writer = await Store.open()
        self.reader = await Store.open(readonly=True)
        self.search = Search(self.reader)

    async def asyncTearDown(self):
        await self.writer.close()
        await self.reader.close()

    async def test_composite_identity_and_no_duplicates(self):
        await self.writer.save_row(row())
        await self.writer.save_row(row())
        await self.writer.save_row(row(chat_id=-10002))
        self.assertEqual((await self.reader.query('SELECT COUNT(*) AS n FROM messages'))[0]['n'], 2)

    async def test_new_edit_survives_old_history_and_has_revision(self):
        await self.writer.save_row(row(text='before'))
        await self.writer.save_row(row(text='after', edited=datetime(2026, 1, 2)))
        await self.writer.save_row(row(text='before'))
        self.assertEqual((await self.reader.query('SELECT text FROM messages'))[0]['text'], 'after')
        self.assertEqual((await self.reader.query('SELECT text FROM message_revisions'))[0]['text'], 'before')
        await self.writer.save_row(row(text='after', edited=datetime(2026, 1, 2)))
        self.assertEqual(len(await self.reader.query('SELECT * FROM message_revisions')), 1)

    async def test_reader_cannot_write_or_ddl(self):
        for sql in ('INSERT INTO users (user_id) VALUES (1)', 'UPDATE users SET bio=\'x\'',
                    'DELETE FROM users', 'CREATE TABLE forbidden (id INT)'):
            with self.assertRaises(Exception) as caught:
                await self.reader.execute(sql)
            self.assertEqual(caught.exception.args[0], 1142)

    async def test_history_same_version_preserves_live_media_and_source_account(self):
        live = dict(row(), ingest_source='live', source_account_id=111,
                    media_type='photo', media='{"file_id":"live-account-file"}')
        self.assertTrue(await self.writer.save_row(live))
        history = dict(live, ingest_source='channel_history', source_account_id=222,
                       media='{"file_id":"history-account-file"}')
        self.assertFalse(await self.writer.save_row(history))
        saved = (await self.reader.query('SELECT * FROM messages'))[0]
        self.assertEqual(saved['source_account_id'], 111)
        self.assertEqual(saved['file_id'], 'live-account-file')
        self.assertEqual(saved['ingest_source'], 'live')

    async def test_import_page_rollback_resume_provenance_and_metrics(self):
        await self.writer.execute('INSERT INTO channel_imports '
            '(account_id,chat_id,upper_message_id,next_max_id) VALUES (77,-10001,10,10)')
        channel = Chat(id=-10001, type=enums.ChatType.CHANNEL, title='Test')
        good = Message(id=10, chat=channel, date=datetime(2026, 1, 1), text='Post', views=123,
                       author_signature='Editor')
        bad = Message(id=9, chat=Chat(id=-10002, type=enums.ChatType.CHANNEL), date=datetime(2026, 1, 1))
        with self.assertRaises(ValueError):
            await self.writer.save_history_page([good, bad], 77, -10001, 8)
        self.assertEqual((await self.reader.query('SELECT COUNT(*) AS n FROM messages'))[0]['n'], 0)
        state = (await self.reader.query('SELECT * FROM channel_imports WHERE account_id=77'))[0]
        self.assertEqual(state['next_max_id'], 10)
        self.assertEqual(await self.writer.save_history_page([good], 77, -10001, 9), 1)
        self.assertEqual(await self.writer.save_history_page([good], 77, -10001, 9), 0)
        self.assertEqual((await self.reader.query('SELECT COUNT(*) AS n FROM messages'))[0]['n'], 1)
        self.assertEqual((await self.reader.query('SELECT * FROM message_sources WHERE account_id=77'))[0]['author_signature'], 'Editor')
        self.assertEqual((await self.reader.query('SELECT * FROM message_metrics'))[0]['views'], 123)

    async def test_scoped_import_rejects_old_posts_and_unlisted_channels_atomically(self):
        scope = 'a' * 64
        await self.writer.execute('INSERT INTO scoped_channel_imports '
            '(scope_key,account_id,chat_id,target_username,since_at,upper_message_id,next_max_id) '
            "VALUES (%s,77,-10001,'allowed','2025-01-01',10,10)", (scope,))
        channel = Chat(id=-10001, type=enums.ChatType.CHANNEL, title='Allowed')
        good = Message(id=10, chat=channel, date=datetime(2025, 1, 1), text='Allowed')
        old = Message(id=9, chat=channel, date=datetime(2024, 12, 31), text='Too old')
        with self.assertRaises(ValueError):
            await self.writer.save_history_page([good, old], 77, -10001, 8, scope_key=scope)
        self.assertEqual((await self.reader.query('SELECT COUNT(*) AS n FROM messages'))[0]['n'], 0)
        with self.assertRaises(ValueError):
            await self.writer.save_history_page([], 77, -10002, 0, scope_key=scope)
        self.assertEqual(await self.writer.save_history_page([good], 77, -10001, 9, scope_key=scope), 1)
        state = (await self.reader.query('SELECT * FROM scoped_channel_imports'))[0]
        self.assertEqual(state['processed'], 1)
        self.assertEqual(state['next_max_id'], 9)
        self.assertEqual((await self.reader.query('SELECT COUNT(*) AS n FROM channel_imports'))[0]['n'], 0)

    async def test_group_import_is_rejected_by_channel_scope_and_allowed_by_explicit_group_scope(self):
        scope = 'b' * 64
        await self.writer.execute('INSERT INTO scoped_channel_imports '
            '(scope_key,account_id,chat_id,target_username,since_at,upper_message_id,next_max_id) '
            "VALUES (%s,77,-10055,'id:-10055','2026-09-09',10,10)", (scope,))
        group = Chat(id=-10055, type=enums.ChatType.SUPERGROUP, title='Allowed group')
        msg = Message(id=10, chat=group, date=datetime(2026, 11, 9), text='Group post', reply_to_message_id=9)
        with self.assertRaises(ValueError):
            await self.writer.save_history_page([msg], 77, -10055, 9, scope_key=scope)
        await self.writer.execute("UPDATE scoped_channel_imports SET chat_type='supergroup' WHERE scope_key=%s", (scope,))
        self.assertEqual(await self.writer.save_history_page([msg], 77, -10055, 9, scope_key=scope), 1)
        stored = (await self.reader.query('SELECT * FROM messages'))[0]
        self.assertEqual(stored['ingest_source'], 'chat_history')
        self.assertEqual(stored['reply_to_message_id'], 9)

    async def test_filters_fulltext_and_injection_literal(self):
        await self.writer.save_row(row(text='ремонт велосипеда завтра'))
        await self.writer.save_row(row(chat_id=-10002, text='ремонт автомобиля', sender=456))
        found = await self.search.messages(query='велосипед*', chat_id=-10001, sender_user_id=123)
        self.assertEqual(len(found['messages']), 1)
        self.assertEqual((await self.search.messages(query="' OR 1=1 --", mode='substring'))['messages'], [])
        self.assertEqual((await self.search.messages(query='ремонт', sender_user_id=999))['messages'], [])
        self.assertEqual(len((await self.search.messages(query='ремонт'))['messages']), 2)

    async def test_literal_substring_escapes_wildcards(self):
        await self.writer.save_row(row(text='discount 20%_sale'))
        await self.writer.save_row(row(message_id=2, text='discount 20 percent'))
        result = await self.search.messages(query='20%_', mode='substring')
        self.assertEqual([r['message_id'] for r in result['messages']], [1])

    async def test_keyset_pagination_equal_dates_across_chats(self):
        for chat in (-10003, -10002, -10001):
            for msg in (1, 2):
                await self.writer.save_row(row(chat_id=chat, message_id=msg))
        seen, cursor = [], {}
        while True:
            page = await self.search.messages(limit=2, **cursor)
            seen.extend((x['chat_id'], x['message_id']) for x in page['messages'])
            cursor = page['next_cursor']
            if not cursor:
                break
        self.assertEqual(len(seen), 6)
        self.assertEqual(len(set(seen)), 6)

    async def test_legacy_deleted_marker_does_not_hide_archived_message(self):
        await self.writer.save_row(row())
        await self.writer.execute('UPDATE messages SET deleted_at=UTC_TIMESTAMP(6)')
        await self.writer.save_row(row())
        self.assertEqual(len((await self.search.messages())['messages']), 1)

    async def test_real_kurigram_serialization_and_private_recipient(self):
        user = User(id=123, first_name='Tester', username='tester')
        chat = Chat(id=123, type=enums.ChatType.PRIVATE, first_name='Tester')
        msg = Message(id=1, date=datetime(2026, 1, 1), chat=chat, from_user=user,
                      text='Hello', outgoing=False, reply_to_message_id=5)
        data = message_row(msg, own_id=999)
        self.assertEqual(data['recipient_user_id'], 999)
        self.assertEqual(data['reply_to_chat_id'], 123)
        await self.writer.save_message(msg, own_id=999)
        self.assertEqual(len(await self.reader.query('SELECT * FROM users')), 1)
        self.assertEqual(len(await self.reader.query('SELECT * FROM chats')), 1)
        channel = Chat(id=-10009, type=enums.ChatType.CHANNEL, title='Channel')
        msg.sender_chat = channel
        msg.from_user = None
        self.assertEqual(message_row(msg, 999)['sender_chat_id'], -10009)

    async def test_semantic_model_paraphrase_filter_staleness_and_truncation(self):
        await self.writer.save_row(row(text='Нужно починить велосипед, сломалось колесо.'))
        await self.writer.save_row(row(message_id=2, text='Банковский кредит и процентная ставка.'))
        await self.writer.save_row(row(chat_id=-10002, text='Сегодня идет дождь.'))
        # Telegram deletion/legacy marks must not remove an archived message from either index.
        await self.writer.execute('UPDATE messages SET deleted_at=UTC_TIMESTAMP(6) WHERE message_id=1 AND chat_id=-10001')
        await embedding_loop(self.writer, once=True)
        result = await self.search.semantic('ремонт двухколесного транспорта', chat_id=-10001)
        self.assertEqual(result['messages'][0]['message_id'], 1)
        self.assertFalse(result['truncated'])
        global_result = await self.search.semantic('ремонт двухколесного транспорта')
        self.assertTrue(global_result['approximate'])
        self.assertEqual(global_result['messages'][0]['chat_id'], -10001)
        self.assertEqual(global_result['messages'][0]['message_id'], 1)
        self.assertTrue((await self.search.semantic('дождь', candidate_limit=1))['truncated'])
        await self.writer.save_row(row(text='Измененный текст', edited=datetime(2026, 1, 2)))
        result = await self.search.semantic('велосипед', chat_id=-10001)
        self.assertNotIn(1, [r['message_id'] for r in result['messages']])
        global_result = await self.search.semantic('велосипед')
        self.assertNotIn(1, [r['message_id'] for r in global_result['messages'] if r['chat_id'] == -10001])

    async def test_reply_lookup_index_and_query_plans(self):
        await self.writer.save_row(row())
        plan = await self.reader.query('EXPLAIN SELECT * FROM messages WHERE chat_id=%s '
                                      'AND sender_user_id=%s ORDER BY sent_at DESC,message_id DESC LIMIT 10', (-10001, 123))
        self.assertTrue(plan[0]['possible_keys'])
        ft = await self.reader.query('EXPLAIN SELECT * FROM messages WHERE MATCH(text) AGAINST (%s IN BOOLEAN MODE)', ('ремонт',))
        self.assertEqual(ft[0]['type'], 'fulltext')

    async def test_utc_conversion(self):
        dt = datetime(2026, 1, 1, 12, tzinfo=timezone(timedelta(hours=2)))
        self.assertEqual(utc(dt), datetime(2026, 1, 1, 10))

    async def test_profile_history_retains_identity_and_bio_changes(self):
        user = User(id=123, first_name='First', last_name='Name', username='original')
        await self.writer.save_user(user)
        await self.writer.save_user(user)
        await self.writer.save_bio(123, 'old bio')
        await self.writer.save_bio(123, 'old bio')
        user.username = 'renamed'
        user.first_name = 'Second'
        await self.writer.save_user(user)
        await self.writer.save_bio(123, 'new bio')
        rows = await self.reader.query('SELECT * FROM user_history WHERE user_id=123 ORDER BY snapshot_id')
        self.assertEqual(len(rows), 4)
        self.assertEqual([r['username'] for r in rows], ['original', 'original', 'renamed', 'renamed'])
        self.assertEqual([r['bio'] for r in rows], [None, 'old bio', 'old bio', 'new bio'])
        self.assertEqual(rows[0]['first_name'], 'First')
        await self.writer.save_profile(999, dict(first_name='Unknown'), source='telegram_update')
        self.assertEqual(await self.reader.query('SELECT * FROM users WHERE user_id=999'), [])

    async def test_metrics_snapshots_merge_partial_updates_without_losing_zero(self):
        await self.writer.save_row(row())
        await self.writer.save_metrics(-10001, 1, views=100, forwards=0,
                                       reactions=[dict(emoji='❤', count=2)], source='message')
        await self.writer.save_metrics(-10001, 1, views=100)
        await self.writer.save_metrics(-10001, 1, views=120)
        await self.writer.save_metrics(-10001, 1, reactions=[])
        rows = await self.reader.query('SELECT * FROM message_metric_history ORDER BY snapshot_id')
        self.assertEqual(len(rows), 3)
        self.assertEqual([r['views'] for r in rows], [100, 120, 120])
        self.assertTrue(all(r['forwards'] == 0 for r in rows))
        self.assertEqual(rows[-1]['reactions'], [])
        self.assertEqual(rows[0]['reactions'][0]['count'], 2)

    async def test_all_file_media_ids_are_returned_without_download(self):
        from types import SimpleNamespace
        chat = Chat(id=-10001, type=enums.ChatType.SUPERGROUP, title='Test')
        for index, name in enumerate(('photo', 'animation', 'sticker', 'video', 'voice', 'video_note',
                                      'audio', 'document', 'live_photo'), 1):
            payload = SimpleNamespace(file_id=f'{name}-file-id', file_unique_id=f'{name}-unique')
            msg = Message(id=index, chat=chat, date=datetime(2026, 1, 1),
                          media=getattr(enums.MessageMediaType, name.upper()), **{name: payload})
            await self.writer.save_message(msg, 999)
        found = (await self.search.messages(chat_id=-10001))['messages']
        self.assertEqual(len(found), 9)
        for msg in found:
            self.assertEqual(msg['file_id'], msg['media_type'] + '-file-id')
            self.assertEqual(msg['media_file_ids'], [msg['file_id']])
        from collector.storage import media_metadata
        compound = media_metadata(SimpleNamespace(media=[SimpleNamespace(file_id='photo'),
                                                         SimpleNamespace(file_id='video')]))
        self.assertEqual([m['file_id'] for m in compound['files']], ['photo', 'video'])

    async def test_run_status_tracks_errors_without_exception_contents(self):
        await self.writer.start_run()
        await self.writer.event_saved()
        await self.writer.event_saved(metadata=True)
        await self.writer.record_error('message', ValueError('private-data-must-not-be-logged'), chat_id=-10001,
                                       message_id=55)
        await self.writer.heartbeat()
        state = (await self.reader.query('SELECT * FROM collector_runs'))[0]
        self.assertEqual(state['received_count'], 2)
        self.assertEqual(state['saved_count'], 1)
        self.assertEqual(state['message_errors'], 1)
        self.assertEqual(state['metadata_events'], 1)
        errors = await self.reader.query('SELECT * FROM collector_errors')
        self.assertEqual(errors[0]['error_type'], 'ValueError')
        self.assertNotIn('private-data', str(errors))
        await self.writer.finish_run()
        self.assertIsNotNone((await self.reader.query('SELECT stopped_at FROM collector_runs'))[0]['stopped_at'])

    async def test_http_mcp_tools_auth_host_and_real_data(self):
        import httpx
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        await self.writer.save_row(row(text='Тестовое сообщение для MCP'))
        await self.writer.save_row(dict(row(message_id=2, text='Фото'), media_type='photo',
                                        media='{"file_id":"test-photo-id"}'))
        await self.writer.save_user(User(id=123, first_name='Test', username='test'))
        await self.writer.save_metrics(-10001, 2, views=10, forwards=0)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        token = uuid.uuid4().hex
        env = dict(os.environ, MCP_PORT=str(port), MCP_TOKEN=token)
        with (ROOT / 'data/test-http.log').open('w') as logs:
            process = subprocess.Popen([sys.executable, '-m', 'collector.mcp_server'], cwd=ROOT,
                                       env=env, stdout=logs, stderr=logs)
            try:
                url = f'http://127.0.0.1:{port}/mcp'
                async with httpx.AsyncClient() as http:
                    for _ in range(100):
                        if process.poll() is not None:
                            self.fail('Test MCP server exited; see data/test-http.log')
                        try:
                            response = await http.post(url)
                            break
                        except httpx.ConnectError:
                            await asyncio.sleep(0.1)
                    else:
                        self.fail('MCP readiness timeout')
                    self.assertEqual(response.status_code, 401)
                    bad_host = await http.post(url, json={}, headers={'Authorization': 'Bearer ' + token,
                                                             'Host': 'attacker.invalid',
                                                             'Accept': 'application/json, text/event-stream'})
                    self.assertEqual(bad_host.status_code, 421)
                    bad_origin = await http.post(url, json={}, headers={'Authorization': 'Bearer ' + token,
                                                 'Origin': 'http://attacker.invalid',
                                                 'Accept': 'application/json, text/event-stream'})
                    self.assertEqual(bad_origin.status_code, 403)
                async with httpx.AsyncClient(headers={'Authorization': 'Bearer ' + token}) as http:
                    async with streamable_http_client(url, http_client=http) as (read, write, _):
                        async with ClientSession(read, write) as session:
                            await session.initialize()
                            tools = (await session.list_tools()).tools
                            self.assertEqual({t.name for t in tools}, {'search_messages', 'semantic_search',
                                             'get_message', 'list_users', 'list_chats', 'archive_status',
                                             'get_user_history', 'get_message_metrics', 'get_statistics', 'get_activity',
                                             'get_top_words', 'get_top_phrases', 'get_top_mentions', 'get_top_authors',
                                             'get_top_posts', 'get_reply_partners', 'get_message_history',
                                             'compare_periods', 'get_archive_coverage'})
                            self.assertTrue(all(t.annotations.readOnlyHint for t in tools))
                            result = await session.call_tool('search_messages', {'chat_id': -10001})
                            self.assertFalse(result.isError)
                            self.assertIn('Тестовое сообщение', result.content[0].text)
                            status = await session.call_tool('archive_status', {})
                            self.assertFalse(status.isError)
                            import json
                            self.assertFalse(json.loads(status.content[0].text)['backfill'])
                            media = await session.call_tool('get_message', {'chat_id': -10001, 'message_id': 2})
                            self.assertIn('test-photo-id', media.content[0].text)
                            profile = await session.call_tool('get_user_history', {'user_id': 123})
                            self.assertFalse(profile.isError)
                            metrics = await session.call_tool('get_message_metrics', {'chat_id': -10001, 'message_id': 2})
                            self.assertFalse(metrics.isError)
                            invalid = await session.call_tool('search_messages', {'limit': 5000})
                            self.assertTrue(invalid.isError)
            finally:
                process.terminate()
                await asyncio.to_thread(process.wait, timeout=10)


if __name__ == '__main__':
    schema = '\n'.join(p.read_text() for p in sorted((ROOT / 'sql').glob('*.sql')))
    admin(f'CREATE DATABASE `{TEST_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;'
          f"GRANT SELECT,INSERT,UPDATE ON `{TEST_DB}`.* TO 'tg_collector'@'127.0.0.1';"
          f"GRANT SELECT ON `{TEST_DB}`.* TO 'tg_reader'@'127.0.0.1';"
          f'USE `{TEST_DB}`;' + schema)
    try:
        program = unittest.main(exit=False)
    finally:
        index_path().unlink(missing_ok=True)
        admin(f'DROP DATABASE `{TEST_DB}`;'
              f"REVOKE ALL PRIVILEGES ON `{TEST_DB}`.* FROM 'tg_collector'@'127.0.0.1';"
              f"REVOKE ALL PRIVILEGES ON `{TEST_DB}`.* FROM 'tg_reader'@'127.0.0.1';")
    sys.exit(0 if program.result.wasSuccessful() else 1)
