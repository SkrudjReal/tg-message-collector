"""Run directly; SQL tests use a new disposable database, never the real archive."""
from datetime import datetime
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from test_archive import TEST_DB, ROOT, Store, admin, row  # noqa: E402
from collector.analytics import Analytics, FrequencyCounter, MessageScope  # noqa: E402


def sample(text, message_id=1):
    return dict(text=text, chat_id=-10011, message_id=message_id, sent_at=datetime(2026, 1, 1))


class FrequencyTests(unittest.TestCase):
    def test_counts_documents_boundaries_links_and_stable_ties(self):
        c = FrequencyCounter(n=2)
        c.add_batch([sample('Claude code claude code. кот\nсобака кот https://example.com собака @testname зверь'),
                     sample('CLAUDE CODE', 2)])
        self.assertEqual(c.top(1, 1)[0]['occurrences'], 3)
        self.assertEqual(c.top(1, 1)[0]['messages'], 2)
        self.assertNotIn('кот собака', c.counts)
        self.assertNotIn('собака зверь', c.counts)
        self.assertEqual(len(c.top(1, 1)[0]['examples']), 2)

    def test_russian_forms_and_mentions(self):
        c = FrequencyCounter(normalization='lemma_ru')
        c.add_batch([sample('модель модели моделью Claude')])
        self.assertEqual(c.counts['модель'], 3)
        self.assertEqual(c.counts['claude'], 1)
        m = FrequencyCounter(mentions=True)
        m.add_batch([sample('@Notkisa @notkisa test@example.com https://t.me/@ignored')])
        self.assertEqual(m.counts, {'@notkisa': 2})

    def test_character_budget_and_validation(self):
        c = FrequencyCounter()
        c.chars = 7_999_999
        self.assertFalse(c.add_batch([sample('too long')]))
        self.assertEqual(c.processed, 0)
        with self.assertRaises(ValueError):
            MessageScope(since=datetime(2026, 2, 1), until=datetime(2026, 1, 1))
        with self.assertRaises(ValueError):
            MessageScope(unknown='bad')


class AnalyticsSQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        admin(f'USE `{TEST_DB}`;' + ''.join('DELETE FROM ' + t + ';' for t in (
            'messages', 'users', 'chats', 'message_metrics', 'message_revisions', 'user_history')))
        self.writer = await Store.open()
        self.reader = await Store.open(readonly=True)
        self.a = Analytics(self.reader)
        for mid, sender, parent, text in [(1, 101, None, 'Модель модели. Claude code @Notkisa'),
            (2, 202, 1, 'Ответ первый'), (3, 101, 2, 'Claude code @notkisa'),
            (4, 101, 1, 'Ответ себе'), (5, 101, 999, 'Пропущенный ответ'),
            (6, 101, None, 'Пересланные слова'), (7, None, None, 'Без автора'),
            (8, 101, 7, 'Ответ неизвестному')]:
            r = row(chat_id=-10011, message_id=mid, sender=sender, text=text)
            r['reply_to_message_id'] = parent
            if parent:
                r['reply_to_chat_id'] = -10011
            if mid == 6:
                r['forward_info'] = json.dumps({'sender_name': 'someone'})
            await self.writer.save_row(r)
        await self.writer.save_row(row(chat_id=-10022, message_id=2, sender=999, text='Другой чат'))
        await self.writer.execute('INSERT INTO message_metrics (chat_id,message_id,views,forwards,reactions) '
                                  'VALUES (%s,%s,%s,%s,%s)', (-10011, 1, 100, 20, json.dumps([{'count': 3}])))
        await self.writer.execute('INSERT INTO message_metrics (chat_id,message_id,views,reactions) '
                                  'VALUES (%s,%s,%s,%s)', (-10011, 2, None, '[]'))

    async def asyncTearDown(self):
        await self.reader.close()
        await self.writer.close()

    async def test_scope_summary_and_complete_and_truncated_frequencies(self):
        scope = MessageScope(chat_id=-10011, sender_user_id=101, include_forwarded=False)
        result = await self.a.summary(scope)
        self.assertEqual(result['messages'], 5)
        self.assertEqual(result['replies'], 4)
        freq = await self.a.frequencies(scope, n=2)
        self.assertFalse(freq['metadata']['truncated'])
        self.assertEqual(freq['top'][0]['term'], 'claude code')
        self.assertEqual(freq['top'][0]['messages'], 2)
        short = await self.a.frequencies(scope, max_messages=1)
        self.assertTrue(short['metadata']['truncated'])
        self.assertEqual(short['metadata']['processed_messages'], 1)
        empty = await self.a.summary(MessageScope(chat_id=123))
        self.assertEqual(empty['messages'], 0)
        self.assertIsNone(empty['reply_percent'])

    async def test_reply_direction_composite_identity_missing_and_self(self):
        outgoing = await self.a.partners(101, MessageScope(chat_id=-10011), 'outgoing', False, 10)
        self.assertEqual(outgoing['partners'][0]['user_id'], 202)
        self.assertEqual(outgoing['partners'][0]['replies'], 1)
        self.assertEqual(outgoing['totals']['missing_targets'], 1)
        self.assertEqual(outgoing['totals']['target_without_author'], 1)
        self.assertEqual(outgoing['totals']['self_replies'], 1)
        incoming = await self.a.partners(101, MessageScope(chat_id=-10011), 'incoming', False, 10)
        self.assertEqual(incoming['partners'][0]['user_id'], 202)
        self.assertEqual(len(incoming['partners']), 1)
        with self.assertRaises(ValueError):
            await self.a.partners(101, MessageScope(sender_user_id=202), 'outgoing', False, 10)

    async def test_reaction_ranking_and_missing_metrics(self):
        result = await self.a.posts(MessageScope(chat_id=-10011), 'reactions', 10)
        self.assertEqual([int(r['score']) for r in result['posts']], [3, 0])
        result = await self.a.posts(MessageScope(chat_id=-10011), 'views', 10)
        self.assertEqual(len(result['posts']), 1)
        self.assertEqual(result['posts'][0]['url'], 'https://t.me/c/11/1')

    async def test_activity_timezone_coverage_and_authors(self):
        activity = await self.a.activity(MessageScope(chat_id=-10011), 'hour', 'Asia/Kolkata', 10)
        self.assertEqual(activity['buckets'], [{'bucket': '2026-01-01T05:00:00+05:30', 'messages': 8}])
        # Repeated local hour during DST must remain two distinct buckets.
        for mid, hour in [(20, 0), (21, 1)]:
            await self.writer.save_row(row(chat_id=-10033, message_id=mid, sent=datetime(2026, 10, 25, hour, 30)))
        dst = await self.a.activity(MessageScope(chat_id=-10033), 'hour', 'Europe/Warsaw', 10)
        self.assertEqual(len(dst['buckets']), 2)
        self.assertEqual({b['messages'] for b in dst['buckets']}, {1})
        coverage = await self.a.coverage(MessageScope(chat_id=-10011), 10)
        self.assertEqual(coverage['replies']['missing_reply_targets'], 1)
        authors = await self.a.authors(MessageScope(chat_id=-10011), 10)
        self.assertEqual(authors['authors'][0]['sender_user_id'], 101)
        self.assertEqual(authors['authors'][0]['percent'], 75)

    async def test_profile_pagination_changes_and_comparison(self):
        import collector.mcp_server as server
        server.store = self.reader
        await self.writer.execute('INSERT INTO users (user_id,username,bio) VALUES (101,%s,%s)', ('new', 'new bio'))
        for username, bio in [('old', 'old bio'), ('new', 'new bio')]:
            await self.writer.execute('INSERT INTO user_history (user_id,username,bio,source) VALUES (101,%s,%s,%s)',
                                      (username, bio, 'message'))
        one = await server.get_user_history(101, limit=1)
        self.assertEqual(one['current']['username'], 'new')
        self.assertTrue(one['snapshots'][0]['baseline'])
        two = await server.get_user_history(101, after_snapshot_id=one['next_snapshot_id'], limit=1)
        self.assertIsNone(two['next_snapshot_id'])
        self.assertEqual(two['snapshots'][0]['changes']['bio'], {'before': 'old bio', 'after': 'new bio'})
        from collector.analytics_tools import register_analytics
        class Registry:
            def __init__(self):
                self.tools = {}
            def tool(self, **kwargs):
                def add(fn):
                    self.tools[fn.__name__] = fn
                    return fn
                return add
        registry = Registry()
        register_analytics(registry, lambda: self.reader, {})
        comparison = await registry.tools['compare_periods'](datetime(2025, 1, 1), datetime(2025, 2, 1),
            datetime(2026, 1, 1), datetime(2026, 2, 1), MessageScope(chat_id=-10011))
        self.assertEqual(comparison['changes']['messages'], {'absolute': 8, 'percent': None})
        history = await registry.tools['get_message_history'](-10011, 1)
        self.assertEqual(history['current']['message_id'], 1)
        self.assertEqual(history['revisions'], [])
        for text in ('old text', 'intermediate text'):
            await self.writer.execute('INSERT INTO message_revisions (chat_id,message_id,text,text_hash) '
                                      'VALUES (%s,%s,%s,%s)', (-10011, 1, text, 'a' * 64))
        history = await registry.tools['get_message_history'](-10011, 1, limit=1)
        self.assertEqual(history['revisions'][0]['text'], 'old text')
        self.assertEqual(history['total_revisions'], 2)
        history = await registry.tools['get_message_history'](-10011, 1,
            after_revision_id=history['next_revision_id'], limit=1)
        self.assertEqual(history['revisions'][0]['text'], 'intermediate text')
        self.assertIsNone(history['next_revision_id'])
        with self.assertRaises(Exception) as caught:
            await self.reader.execute('UPDATE users SET bio=bio WHERE 1=0')
        self.assertEqual(caught.exception.args[0], 1142)


if __name__ == '__main__':
    schema = '\n'.join(p.read_text() for p in sorted((ROOT / 'sql').glob('*.sql')))
    admin(f'CREATE DATABASE `{TEST_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;'
          f"GRANT SELECT,INSERT,UPDATE ON `{TEST_DB}`.* TO 'tg_collector'@'127.0.0.1';"
          f"GRANT SELECT ON `{TEST_DB}`.* TO 'tg_reader'@'127.0.0.1';" f'USE `{TEST_DB}`;' + schema)
    try:
        program = unittest.main(exit=False)
    finally:
        admin(f'DROP DATABASE `{TEST_DB}`;'
              f"REVOKE ALL PRIVILEGES ON `{TEST_DB}`.* FROM 'tg_collector'@'127.0.0.1';"
              f"REVOKE ALL PRIVILEGES ON `{TEST_DB}`.* FROM 'tg_reader'@'127.0.0.1';")
    sys.exit(0 if program.result.wasSuccessful() else 1)
