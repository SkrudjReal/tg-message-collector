import asyncio
import unittest
from unittest.mock import AsyncMock

from collector.search import Search, filters


class HybridSearchTests(unittest.IsolatedAsyncioTestCase):
    async def test_exclude_chat_applies_to_both_search_sources(self):
        clauses, params = filters(sender_user_id=42, exclude_chat_id=-1001, public_only=True)
        self.assertIn('m.chat_id<>%s', clauses)
        self.assertEqual(params, [-1001, 42])
        store = AsyncMock()
        store.query.return_value = []
        search = Search(store)
        search.semantic = AsyncMock(return_value=dict(messages=[]))
        await search.hybrid('тема', sender_user_id=42, exclude_chat_id=-1001, public_only=True)
        sql = store.query.call_args.args[0]
        self.assertLess(sql.index('m.chat_id<>%s'), sql.index('ORDER BY'))
        self.assertEqual(search.semantic.call_args.kwargs['exclude_chat_id'], -1001)
        await search.messages(sender_user_id=42, exclude_chat_id=-1001)
        self.assertIn('m.chat_id<>%s', store.query.call_args.args[0])

    async def test_public_scope_filters_before_lexical_limit_and_semantic_scoring(self):
        clauses, params = filters(public_only=True)
        self.assertIn("m.chat_id<0", clauses[0])
        self.assertIn("c.type IN ('group','supergroup','channel')", clauses[0])
        self.assertEqual(params, [])
        store = AsyncMock()
        store.query.return_value = []
        search = Search(store)
        search.semantic = AsyncMock(return_value=dict(messages=[]))
        await search.hybrid('новости ИИ', public_only=True)
        sql = store.query.call_args.args[0]
        self.assertIn(clauses[0], sql)
        self.assertLess(sql.index('m.chat_id<0'), sql.index('ORDER BY'))
        self.assertTrue(search.semantic.call_args.kwargs['public_only'])

    async def test_fusion_deduplicates_and_preserves_filters(self):
        store = AsyncMock()
        store.query.return_value = [dict(archive_id=1, text='one', lexical_score=8),
                                    dict(archive_id=2, text='two', lexical_score=2)]
        search = Search(store)
        search.semantic = AsyncMock(return_value=dict(messages=[dict(archive_id=2, similarity=.9),
                                                               dict(archive_id=3, text='three', similarity=.8)],
                                                      truncated=True, scanned=50))
        result = await search.hybrid('some + words?', chat_id=-10001, topic_id=7, limit=3)
        self.assertEqual([r['archive_id'] for r in result['messages']], [2, 1, 3])
        self.assertEqual(result['messages'][0]['matched_by'], ['fulltext', 'semantic'])
        self.assertEqual(result['messages'][0]['similarity'], .9)
        self.assertTrue(result['truncated'])
        search.semantic.assert_awaited_once_with('some + words?', limit=100, candidate_limit=50000,
                                                 chat_id=-10001, topic_id=7)
        sql, params = store.query.call_args.args
        self.assertIn('NATURAL LANGUAGE MODE', sql)
        self.assertIn('m.chat_id=%s', sql)
        self.assertIn('m.topic_id=%s', sql)
        self.assertEqual(params, ('some + words?', -10001, 7, 'some + words?', 100))

    async def test_semantic_failure_returns_keywords_without_exception_details(self):
        store = AsyncMock()
        store.query.return_value = [dict(archive_id=1)]
        search = Search(store)
        search.semantic = AsyncMock(side_effect=RuntimeError('private-error-detail'))
        result = await search.hybrid('test')
        self.assertEqual(len(result['messages']), 1)
        self.assertEqual(result['warnings'], [dict(component='semantic', error_type='RuntimeError')])
        self.assertNotIn('private-error-detail', str(result))

    async def test_validation_and_cancellation(self):
        search = Search(AsyncMock())
        for query, kwargs in [('', {}), ('x' * 2001, {}), ('x', {'limit': 101}),
                              ('x', {'candidate_limit': 0})]:
            with self.assertRaises(ValueError):
                await search.hybrid(query, **kwargs)
        search.store.query.return_value = []
        search.semantic = AsyncMock(side_effect=asyncio.CancelledError)
        with self.assertRaises(asyncio.CancelledError):
            await search.hybrid('test')
