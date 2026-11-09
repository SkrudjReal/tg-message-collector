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
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TEST_DB = 'tg_message_collector_test_' + uuid.uuid4().hex[:12]
os.environ['MYSQL_DATABASE'] = TEST_DB

from collector.storage import Store, message_row, utc  # noqa: E402
from collector.search import Search  # noqa: E402
from collector.runtime import embedding_loop, sync_chat  # noqa: E402
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
            'messages', 'message_revisions', 'message_embeddings', 'users', 'chats', 'chat_sync', 'unresolved_deletions')))
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

    async def test_history_catchup_and_backfill_checkpoints(self):
        saved = []

        class FakeApp:
            ids = [5, 4, 3, 2, 1]

            async def get_chat_history(self, chat_id, min_id=0, max_id=0, limit=0):
                ids = [i for i in self.ids if i >= min_id and (not max_id or i <= max_id)]
                for i in ids[:limit or len(ids)]:
                    yield SimpleNamespace(id=i)

        original = self.writer.save_message

        async def save(msg, own_id):
            saved.append(msg.id)

        self.writer.save_message = save
        app = FakeApp()
        await sync_chat(app, self.writer, -1, 999, 2)
        app.ids = [8, 7, 6, 5, 4, 3, 2, 1]
        await sync_chat(app, self.writer, -1, 999, 2)
        await sync_chat(app, self.writer, -1, 999, 2)
        state = (await self.reader.query('SELECT * FROM chat_sync'))[0]
        self.assertEqual(state['latest_message_id'], 8)
        self.assertTrue(state['history_complete'])
        self.assertEqual(set(saved), set(range(1, 9)))
        self.writer.save_message = original

    async def test_utc_conversion(self):
        dt = datetime(2026, 1, 1, 12, tzinfo=timezone(timedelta(hours=2)))
        self.assertEqual(utc(dt), datetime(2026, 1, 1, 10))

    async def test_http_mcp_tools_auth_host_and_real_data(self):
        import httpx
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        await self.writer.save_row(row(text='Тестовое сообщение для MCP'))
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
                                             'get_message', 'list_users', 'list_chats', 'archive_status'})
                            self.assertTrue(all(t.annotations.readOnlyHint for t in tools))
                            result = await session.call_tool('search_messages', {'chat_id': -10001})
                            self.assertFalse(result.isError)
                            self.assertIn('Тестовое сообщение', result.content[0].text)
                            status = await session.call_tool('archive_status', {})
                            self.assertFalse(status.isError)
                            invalid = await session.call_tool('search_messages', {'limit': 5000})
                            self.assertTrue(invalid.isError)
            finally:
                process.terminate()
                await asyncio.to_thread(process.wait, timeout=10)


if __name__ == '__main__':
    schema = (ROOT / 'sql/001_archive.sql').read_text()
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
