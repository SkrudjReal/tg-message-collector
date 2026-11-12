import asyncio
import heapq
import threading

import numpy as np
from asyncmy.cursors import SSDictCursor

from collector.config import DATA, MODEL, active_profile, EmbeddingProfile
from collector.vector_index import IndexReader

_MODEL_REGISTRATION_LOCK = threading.Lock()


def embedding_model():
    from fastembed import TextEmbedding
    from fastembed.common.model_description import ModelSource, PoolingType

    name = MODEL
    if MODEL == 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2':
        name = 'tg-message-collector/minilm-mean-v1'
        with _MODEL_REGISTRATION_LOCK:
            if not any(m['model'] == name for m in TextEmbedding.list_supported_models()):
                TextEmbedding.add_custom_model(
                    model=name, pooling=PoolingType.MEAN, normalization=True,
                    sources=ModelSource(hf='qdrant/paraphrase-multilingual-MiniLM-L12-v2-onnx-Q'),
                    dim=384, model_file='model_optimized.onnx',
                )
    return TextEmbedding(name, cache_dir=str(DATA / 'models'), threads=2)


class Encoder:
    def __init__(self, profile=None):
        self.profile = profile or EmbeddingProfile()
        self.model = None
        self.lock = asyncio.Lock()

    def _encode(self, texts):
        if self.model is None:
            self.model = embedding_model()
        vectors = np.asarray(list(self.model.embed(texts, batch_size=8)), dtype='<f4')
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        return vectors / np.maximum(norms, 1e-12)

    async def encode(self, texts, *, synthetic=False):
        async with self.lock:
            if self.profile.provider == 'cloudflare':
                from collector.cloudflare import CloudflareEncoder
                return await CloudflareEncoder().encode(texts, synthetic=synthetic)
            return await asyncio.to_thread(self._encode, texts)


def filters(chat_id=None, sender_user_id=None, sender_chat_id=None, since=None, until=None, topic_id=None):
    clauses, params = [], []
    for name, value, op in [('chat_id', chat_id, '='), ('sender_user_id', sender_user_id, '='),
                            ('sender_chat_id', sender_chat_id, '='), ('sent_at', since, '>='),
                            ('sent_at', until, '<'), ('topic_id', topic_id, '=')]:
        if value is not None:
            clauses.append(f'm.{name}{op}%s')
            params.append(value)
    return clauses, params


def bounded_limit(limit, maximum=200):
    if not 1 <= limit <= maximum:
        raise ValueError(f'limit must be between 1 and {maximum}')
    return limit


class Search:
    def __init__(self, store, profile=None):
        self.store = store
        self.fixed = profile is not None
        self.profile = profile or active_profile()
        self.encoder = Encoder(self.profile)
        self.index = IndexReader(self.profile)
        self.current = None

    async def hybrid(self, query, limit=20, candidate_limit=50000, **kwargs):
        bounded_limit(limit, 100)
        bounded_limit(candidate_limit, 200000)
        if not query.strip() or len(query) > 2000:
            raise ValueError('query must contain 1..2000 characters')
        clauses, params = filters(**kwargs)
        clauses.append('MATCH(m.text) AGAINST (%s IN NATURAL LANGUAGE MODE)')
        # Rank lexical candidates by relevance, rather than message date.
        lexical = await self.store.query(
            'SELECT m.*,MATCH(m.text) AGAINST (%s IN NATURAL LANGUAGE MODE) AS lexical_score '
            'FROM messages m WHERE ' + ' AND '.join(clauses) +
            ' ORDER BY lexical_score DESC,m.sent_at DESC,m.archive_id DESC LIMIT %s',
            (query, *params, query, 100),
        )
        warnings = []
        try:
            semantic = await asyncio.wait_for(
                self.semantic(query, limit=100, candidate_limit=candidate_limit, **kwargs), timeout=60,
            )
        except Exception as exc:
            # Keyword search remains usable while a model/provider is unavailable.
            semantic = dict(messages=[], truncated=True)
            warnings.append(dict(component='semantic', error_type=type(exc).__name__))
        merged = {}
        for source, rows in [('fulltext', lexical), ('semantic', semantic['messages'])]:
            seen = set()
            for rank, row in enumerate(rows, 1):
                key = row['archive_id']
                if key in seen:
                    continue
                seen.add(key)
                if key not in merged:
                    merged[key] = dict(row, matched_by=[], hybrid_score=0.0)
                item = merged[key]
                if 'similarity' in row:
                    item['similarity'] = row['similarity']
                item['matched_by'].append(source)
                # Reciprocal rank fusion avoids mixing incompatible score scales.
                item['hybrid_score'] += 1 / (60 + rank)
        ranked = sorted(merged.values(), key=lambda row: (-row['hybrid_score'], row['archive_id']))
        return dict(messages=ranked[:limit], mode='hybrid', ranking='reciprocal_rank_fusion',
                    candidates=dict(fulltext=len(lexical), semantic=len(semantic['messages'])),
                    truncated=bool(semantic.get('truncated')) or len(lexical) == 100 or len(ranked) > limit,
                    semantic_coverage={key: semantic[key] for key in
                                       ('model', 'scanned', 'indexed_vectors', 'approximate', 'note') if key in semantic},
                    warnings=warnings, note='hybrid_score is a ranking score, not a probability.')

    async def messages(self, query=None, mode='fulltext', limit=50, before_date=None,
                       before_chat_id=None, before_message_id=None, **kwargs):
        bounded_limit(limit)
        clauses, params = filters(**kwargs)
        if mode not in ('fulltext', 'substring'):
            raise ValueError('mode must be fulltext or substring')
        if query:
            if len(query) > 2000:
                raise ValueError('Query is too long')
            if mode == 'fulltext':
                clauses.append('MATCH(m.text) AGAINST (%s IN BOOLEAN MODE)')
                params.append(query)
            else:
                # Explicit literal substring mode for short words / inflections; may scan rows.
                clauses.append("m.text LIKE %s ESCAPE '='")
                params.append('%' + query.replace('=', '==').replace('%', '=%').replace('_', '=_') + '%')
        if before_date is not None:
            if before_chat_id is None or before_message_id is None:
                raise ValueError('All three cursor fields are required')
            clauses.append('(m.sent_at<%s OR (m.sent_at=%s AND m.chat_id<%s) OR '
                           '(m.sent_at=%s AND m.chat_id=%s AND m.message_id<%s))')
            params += [before_date, before_date, before_chat_id, before_date, before_chat_id, before_message_id]
        rows = await self.store.query('SELECT m.* FROM messages m WHERE ' + (' AND '.join(clauses) or '1') +
                                      ' ORDER BY m.sent_at DESC,m.chat_id DESC,m.message_id DESC LIMIT %s',
                                      tuple(params) + (limit,))
        last = rows[-1] if rows else None
        return dict(messages=rows, next_cursor=dict(before_date=last['sent_at'], before_chat_id=last['chat_id'],
                    before_message_id=last['message_id']) if last and len(rows) == limit else None)

    async def semantic(self, query, limit=20, candidate_limit=50000, **kwargs):
        if not self.fixed:
            profile = active_profile()
            if self.current is None or self.current.profile != profile:
                self.current = Search(self.store, profile)
            return await self.current.semantic(query, limit, candidate_limit, **kwargs)
        bounded_limit(limit, 100)
        bounded_limit(candidate_limit, 200000)
        if not query.strip() or len(query) > 2000:
            raise ValueError('query must contain 1..2000 characters')
        vector = (await self.encoder.encode([query]))[0]
        if not any(v is not None and v is not False for v in kwargs.values()):
            ann = await self.semantic_ann(vector, limit, candidate_limit)
            if ann is not None:
                return ann
        clauses, params = filters(**kwargs)
        clauses += ['e.model=%s', 'e.text_hash=m.text_hash']
        params.append(self.profile.model)
        sql = (f'SELECT m.archive_id,e.vector FROM messages m JOIN {self.profile.table} e '
               f'ON e.archive_id=m.archive_id WHERE ' + ' AND '.join(clauses) +
               ' ORDER BY m.sent_at DESC,m.chat_id DESC,m.message_id DESC LIMIT %s')
        heap, scanned = [], 0
        async with self.store.pool.acquire() as conn, conn.cursor(SSDictCursor) as cur:
            await cur.execute(sql, tuple(params) + (candidate_limit + 1,))
            while rows := await cur.fetchmany(512):
                for row in rows:
                    scanned += 1
                    if scanned > candidate_limit:
                        continue
                    v = np.frombuffer(row['vector'], dtype='<f4')
                    if v.shape != vector.shape:
                        raise ValueError('Embedding dimension mismatch; reindex with configured model')
                    score = float(v @ vector)
                    item = (score, row['archive_id'])
                    if len(heap) < limit:
                        heapq.heappush(heap, item)
                    elif item > heap[0]:
                        heapq.heapreplace(heap, item)
        ranked = sorted(heap, reverse=True)
        if not ranked:
            return dict(messages=[], scanned=0, truncated=False,
                        note='No current embeddings match. Run the embedding worker and check archive_status.')
        ids = [item[1] for item in ranked]
        # Recheck currentness after scoring in case a live edit happened concurrently.
        rows = await self.store.query(f'SELECT m.* FROM messages m JOIN {self.profile.table} e '
                                      'ON e.archive_id=m.archive_id WHERE e.text_hash=m.text_hash AND e.model=%s '
                                      'AND m.archive_id IN (' +
                                      ','.join(['%s'] * len(ids)) + ')', (self.profile.model, *ids))
        by_id = {row['archive_id']: row for row in rows}
        return dict(messages=[dict(by_id[i], similarity=score) for score, i in ranked if i in by_id],
                    scanned=min(scanned, candidate_limit), truncated=scanned > candidate_limit,
                    candidate_order='newest first; filter by chat/user/date for larger archives',
                    approximate=False, model=self.profile.model)

    async def semantic_ann(self, vector, limit, candidate_limit):
        count = min(max(limit * 8, 100), candidate_limit)
        while True:
            result = await asyncio.to_thread(self.index.candidates, vector, count)
            if result is None:
                return None
            ids, size, stamp = result
            if not ids:
                return dict(messages=[], approximate=True, indexed_vectors=size, truncated=False)
            rows = await self.store.query(f'SELECT m.*,e.vector FROM messages m JOIN {self.profile.table} e '
                                          f'ON e.archive_id=m.archive_id WHERE '
                                          'e.text_hash=m.text_hash AND e.model=%s AND m.archive_id IN (' +
                                          ','.join(['%s'] * len(ids)) + ')', (self.profile.model, *ids))
            # Score using current SQL vectors, even if an edit occurred since the ANN snapshot.
            for row in rows:
                row['similarity'] = float(np.frombuffer(row.pop('vector'), dtype='<f4') @ vector)
            rows.sort(key=lambda r: r['similarity'], reverse=True)
            if len(rows) >= limit or count >= min(size, candidate_limit):
                return dict(messages=rows[:limit], approximate=True, indexed_vectors=size,
                            candidates=len(ids), truncated=len(rows) < limit and count < size,
                            snapshot_mtime_ns=stamp, model=self.profile.model)
            count = min(count * 2, size, candidate_limit)
