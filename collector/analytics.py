"""Bounded read-only analytics over the observed archive, never Telegram RPCs."""
import asyncio
from collections import Counter, defaultdict
from datetime import datetime, timezone
from functools import lru_cache
import re
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, model_validator

from collector.storage import utc

SELECT = 'SELECT /*+ MAX_EXECUTION_TIME(25000) */ '
STOPWORDS_VERSION = 'ru_en_v1'
STOPWORDS = frozenset(('''и в во не что он на я с со как а то все она так его но да ты к у же вы за бы по
только ее мне было вот от меня еще нет о из ему теперь когда даже ну ли если уже или ни быть был него
до вас опять уж вам ведь там потом себя ничего ей может они тут где есть надо ней для мы тебя их чем
была сам чтоб без будто чего тоже себе под будет тогда кто этот того потому этого какой ним здесь
этом один мой тем чтобы нее сейчас были куда зачем всех можно при два об другой после над больше тот
через эти нас про всего них какая эту моя свою этой перед иногда лучше том нельзя такой им более всегда
всю между это просто очень вообще типа щас тебе поэтому который которые могу буду самый самое самых
такие тому свой свои нужно много the a an and or of to in on at for with is are was were be been it
this that these those you your i we they he she my our their as by from not but if then''').split())
TOKEN = re.compile(r'[а-яa-z]+(?:-[а-яa-z]+)*|\d+')
URL_MENTION = re.compile(r'https?://\S+|www\.\S+|(?<!\w)@[a-z0-9_]+', re.I)
MENTION = re.compile(r'(?<![\w@])@([a-z][a-z0-9_]{3,31})(?![\w])', re.I)
MORPH = None


class MessageScope(BaseModel):
    model_config = ConfigDict(extra='forbid')
    chat_id: int | None = None
    sender_user_id: int | None = None
    sender_chat_id: int | None = None
    topic_id: int | None = None
    since: datetime | None = None
    until: datetime | None = None
    media_type: str | None = Field(default=None, max_length=32)
    chat_type: Literal['group', 'supergroup', 'forum', 'channel'] | None = None
    include_forwarded: bool = True

    @model_validator(mode='after')
    def validate_period(self):
        self.since, self.until = utc(self.since), utc(self.until)
        if self.since and self.until and self.since >= self.until:
            raise ValueError('since must be earlier than until (exclusive)')
        return self

    def where(self, alias='m'):
        clauses, params = [], []
        for field, op in [('chat_id', '='), ('sender_user_id', '='), ('sender_chat_id', '='),
                          ('topic_id', '='), ('media_type', '='), ('since', '>='), ('until', '<')]:
            value = getattr(self, field)
            if value is not None:
                column = 'sent_at' if field in ('since', 'until') else field
                clauses.append(f'{alias}.{column}{op}%s')
                params.append(value)
        if not self.include_forwarded:
            clauses.append(f'{alias}.forward_info IS NULL')
        if self.chat_type:
            clauses.append(f'EXISTS (SELECT 1 FROM chats c WHERE c.chat_id={alias}.chat_id AND c.type=%s)')
            params.append(self.chat_type)
        return ' AND '.join(clauses) or '1', tuple(params)


def link(row):
    chat, message = row['chat_id'], row['message_id']
    return f'https://t.me/c/{str(chat)[4:]}/{message}' if str(chat).startswith('-100') else None


def metadata(scope, **kwargs):
    return dict(scope=scope.model_dump(mode='json'), calculated_at=datetime.now(timezone.utc).isoformat(),
                archive_only=True, truncated=False, **kwargs)


@lru_cache(maxsize=30000)
def lemma(word):
    global MORPH
    if not re.fullmatch('[а-я-]+', word):
        return word
    if MORPH is None:
        from pymorphy3 import MorphAnalyzer
        MORPH = MorphAnalyzer()
    return MORPH.parse(word)[0].normal_form.replace('ё', 'е')


class FrequencyCounter:
    def __init__(self, n=1, normalization='literal', remove_stopwords=True, extra_stopwords=(), mentions=False):
        if normalization not in ('literal', 'lemma_ru'):
            raise ValueError('normalization must be literal or lemma_ru')
        if not 1 <= n <= 5:
            raise ValueError('n must be 1..5')
        if len(extra_stopwords) > 500 or any(len(w) > 100 for w in extra_stopwords):
            raise ValueError('At most 500 extra stopwords of at most 100 characters')
        self.n, self.normalization, self.mentions = n, normalization, mentions
        self.stop = set(STOPWORDS if remove_stopwords else ())
        self.stop.update(w.lower().replace('ё', 'е') for w in extra_stopwords)
        if normalization == 'lemma_ru':
            self.stop = {lemma(w) for w in self.stop}
        self.counts, self.documents = Counter(), Counter()
        self.examples = defaultdict(list)
        self.chars = self.processed = self.text_messages = 0
        self.first = self.last = None
        self.stop_reason = None

    def add_batch(self, rows):
        for row in rows:
            text = row['text'] or ''
            if self.chars + len(text) > 8_000_000:
                self.stop_reason = 'character_budget'
                return False
            found = []
            if self.mentions:
                clean = re.sub(r'https?://\S+|www\.\S+', ' ', text)
                found = ['@' + m.lower() for m in MENTION.findall(clean)]
            else:
                clean = URL_MENTION.sub(' . ', text.lower()).replace('ё', 'е')
                for segment in re.split(r'[\n.!?;:…]+', clean):
                    tokens = TOKEN.findall(segment)
                    if self.normalization == 'lemma_ru':
                        tokens = [lemma(t) for t in tokens]
                    for i in range(len(tokens) - self.n + 1):
                        phrase = tokens[i:i + self.n]
                        if any(w.isdigit() for w in phrase):
                            continue
                        if phrase[0] in self.stop or phrase[-1] in self.stop:
                            continue
                        # Do not bridge numbers or short words for unigrams/bigrams.
                        if sum(len(w) >= 3 and not w.isdigit() and w not in self.stop for w in phrase) < min(2, self.n):
                            continue
                        found.append(' '.join(phrase))
            terms = set(found)
            if len(self.counts) + sum(t not in self.counts for t in terms) > 100000:
                self.stop_reason = 'distinct_term_budget'
                return False
            self.chars += len(text)
            self.processed += 1
            self.text_messages += bool(text)
            stamp = row['sent_at']
            self.first = min(self.first, stamp) if self.first else stamp
            self.last = max(self.last, stamp) if self.last else stamp
            self.counts.update(found)
            self.documents.update(terms)
            for term in terms:
                if len(self.examples[term]) < 3:
                    self.examples[term].append(dict(chat_id=row['chat_id'], message_id=row['message_id'],
                                                   url=link(row)))
        return True

    def top(self, limit, min_messages):
        ranked = sorted((p for p in self.counts if self.documents[p] >= min_messages),
                        key=lambda p: (-self.counts[p], -self.documents[p], p))[:limit]
        return [dict(term=p, occurrences=self.counts[p], messages=self.documents[p], examples=self.examples[p])
                for p in ranked]


class Analytics:
    def __init__(self, store):
        self.store = store
        self.heavy = asyncio.Semaphore(1)

    async def summary(self, scope):
        where, params = scope.where()
        rows = await self.store.query(SELECT + 'COUNT(*) AS messages,COUNT(DISTINCT m.sender_user_id) AS users,'
            'COUNT(DISTINCT m.sender_chat_id) AS sender_chats,COUNT(DISTINCT m.chat_id) AS chats,'
            'MIN(m.sent_at) AS first_at,MAX(m.sent_at) AS last_at,COUNT(DISTINCT DATE(m.sent_at)) AS active_days_utc,'
            'COALESCE(SUM(m.reply_to_message_id IS NOT NULL),0) AS replies,'
            'COALESCE(SUM(m.edited_at IS NOT NULL),0) AS edited,'
            'COALESCE(SUM(m.media_type IS NOT NULL),0) AS with_media,'
            'COALESCE(SUM(m.forward_info IS NOT NULL),0) AS forwarded,'
            'COALESCE(SUM(CHAR_LENGTH(COALESCE(m.text,\'\'))),0) AS text_characters,'
            'COALESCE(SUM(COALESCE(m.text,\'\')<>\'\'),0) AS text_messages '
            'FROM messages m WHERE ' + where, params)
        result = rows[0]
        n = int(result['messages'])
        result['avg_text_length_all_messages'] = round(int(result['text_characters']) / n, 2) if n else None
        result['reply_percent'] = round(int(result['replies']) * 100 / n, 2) if n else None
        result['edited_percent'] = round(int(result['edited']) * 100 / n, 2) if n else None
        return result

    async def frequencies(self, scope, n=1, normalization='literal', remove_stopwords=True,
                          extra_stopwords=(), limit=10, min_messages=1, max_messages=50000, mentions=False,
                          return_counter=False):
        if not 1 <= limit <= 100 or not 1 <= min_messages <= 200000 or not 1 <= max_messages <= 200000:
            raise ValueError('limit: 1..100; min_messages/max_messages: 1..200000')
        from asyncmy.cursors import SSDictCursor
        where, params = scope.where()
        async with self.heavy:
            counter = await asyncio.to_thread(FrequencyCounter, n, normalization, remove_stopwords,
                                              extra_stopwords or (), mentions)
            truncated = False
            async with self.store.pool.acquire() as conn, conn.cursor(SSDictCursor) as cur:
                await cur.execute(SELECT + 'm.chat_id,m.message_id,m.sent_at,LEFT(m.text,8000001) AS text '
                    'FROM messages m WHERE ' + where +
                    ' ORDER BY m.sent_at DESC,m.chat_id DESC,m.message_id DESC LIMIT %s', params + (max_messages + 1,))
                while rows := await cur.fetchmany(128):
                    available = max_messages - counter.processed
                    batch = rows[:max(0, available)]
                    complete = await asyncio.to_thread(counter.add_batch, batch)
                    if not complete or len(rows) > available:
                        truncated = True
                        break
            top = await asyncio.to_thread(counter.top, limit, min_messages)
        meta = metadata(scope, processed_messages=counter.processed, text_messages=counter.text_messages,
            processed_characters=counter.chars, first_processed_at=counter.first, last_processed_at=counter.last,
            order='newest first', max_messages=max_messages, max_characters=8000000,
            normalization=normalization, stopwords_version=STOPWORDS_VERSION if remove_stopwords else None,
            extra_stopwords=list(extra_stopwords or ()), n=n)
        meta['truncated'] = truncated
        meta['truncation_reason'] = counter.stop_reason or ('message_limit' if truncated else None)
        meta['matching_messages'] = None if truncated else counter.processed
        meta['max_distinct_terms'] = 100000
        result = dict(top=top, metadata=meta)
        return (result, counter) if return_counter else result

    async def vocabulary_change(self, first, second, limit, max_messages):
        one, c1 = await self.frequencies(first, limit=limit, max_messages=max_messages, return_counter=True)
        two, c2 = await self.frequencies(second, limit=limit, max_messages=max_messages, return_counter=True)
        terms = set(c1.counts) | set(c2.counts)
        ranked = sorted(terms, key=lambda t: (-abs(c2.counts[t] - c1.counts[t]), t))[:limit]
        changes = []
        for term in ranked:
            before, after = c1.counts[term], c2.counts[term]
            n1, n2 = c1.text_messages, c2.text_messages
            changes.append(dict(term=term, first_occurrences=before, second_occurrences=after,
                absolute=after - before, percent=round((after - before) * 100 / before, 2) if before else None,
                first_per_1000_text_messages=round(before * 1000 / n1, 2) if n1 else None,
                second_per_1000_text_messages=round(after * 1000 / n2, 2) if n2 else None))
        return dict(changes=changes, first_metadata=one['metadata'], second_metadata=two['metadata'],
                    note='Largest absolute count changes; rates per 1000 text messages aid comparison of different volumes.')

    async def grouped(self, scope, column):
        if column not in ('media_type', 'ingest_source'):
            raise ValueError('Unsupported group')
        where, params = scope.where()
        return await self.store.query(SELECT + f'm.{column},COUNT(*) AS messages FROM messages m WHERE ' +
                                      where + f' GROUP BY m.{column} ORDER BY messages DESC', params)

    async def activity(self, scope, bucket, timezone_name, limit):
        if bucket not in ('hour', 'day', 'week', 'month') or not 1 <= limit <= 1000:
            raise ValueError('bucket: hour/day/week/month; limit: 1..1000')
        zone = ZoneInfo(timezone_name)
        where, params = scope.where()
        # UTC minute aggregates preserve local hour/day boundaries even in half-hour zones and DST.
        rows = await self.store.query(SELECT + "DATE_FORMAT(m.sent_at,'%%Y-%%m-%%d %%H:%%i') AS minute,"
            'COUNT(*) AS messages FROM messages m WHERE ' + where + ' GROUP BY minute ORDER BY minute DESC LIMIT 200001',
            params)
        counts, bucket_order = Counter(), {}
        for r in rows[:200000]:
            stamp = datetime.strptime(r['minute'], '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc).astimezone(zone)
            if bucket == 'hour':
                key = stamp.replace(minute=0, second=0, microsecond=0).isoformat()
            elif bucket == 'week':
                from datetime import timedelta
                key = (stamp.date() - timedelta(days=stamp.weekday())).isoformat()
            elif bucket == 'month':
                key = stamp.strftime('%Y-%m')
            else:
                key = stamp.date().isoformat()
            counts[key] += r['messages']
            bucket_order[key] = max(bucket_order.get(key, float('-inf')), stamp.timestamp())
        keys = sorted(counts, key=bucket_order.__getitem__, reverse=True)[:limit]
        meta = metadata(scope, timezone=timezone_name, bucket=bucket,
                        processed_messages=sum(r['messages'] for r in rows[:200000]),
                        zero_buckets_omitted=True, order='newest bucket first', available_buckets=len(counts))
        meta['truncated'] = len(rows) > 200000 or len(counts) > limit
        return dict(buckets=[dict(bucket=k, messages=counts[k]) for k in keys], metadata=meta)

    async def authors(self, scope, limit):
        where, params = scope.where()
        total = await self.summary(scope)
        rows = await self.store.query(SELECT + 'm.sender_user_id,m.sender_chat_id,COUNT(*) AS messages '
            'FROM messages m WHERE ' + where + ' GROUP BY m.sender_user_id,m.sender_chat_id '
            'ORDER BY messages DESC,m.sender_user_id,m.sender_chat_id LIMIT %s', params + (limit,))
        for r in rows:
            r['percent'] = round(r['messages'] * 100 / total['messages'], 2)
            profiles = await self.store.query('SELECT user_id,username,first_name,last_name FROM users WHERE user_id=%s',
                                             (r['sender_user_id'],)) if r['sender_user_id'] else []
            r['profile'] = profiles[0] if profiles else None
        return dict(authors=rows, metadata=metadata(scope, matching_messages=total['messages'],
                                                   ranking_limited=len(rows) == limit))

    async def partners(self, user_id, scope, direction, include_self, limit):
        if scope.sender_user_id is not None or scope.sender_chat_id is not None:
            raise ValueError('Use user_id for the participant; scope sender filters are not applicable')
        where, params = scope.where()
        predicate = 'm.sender_user_id=%s' if direction == 'outgoing' else 'p.sender_user_id=%s'
        target = 'p' if direction == 'outgoing' else 'm'
        base = ('FROM messages m LEFT JOIN messages p ON p.chat_id=COALESCE(m.reply_to_chat_id,m.chat_id) '
                'AND p.message_id=m.reply_to_message_id WHERE ' + where +
                ' AND m.reply_to_message_id IS NOT NULL AND ' + predicate)
        params += (user_id,)
        totals = (await self.store.query(SELECT + 'COUNT(*) AS replies,'
            'COALESCE(SUM(p.archive_id IS NULL),0) AS missing_targets,'
            'COALESCE(SUM(p.archive_id IS NOT NULL AND p.sender_user_id IS NULL AND p.sender_chat_id IS NULL),0) '
            'AS target_without_author,COALESCE(SUM(m.sender_user_id=p.sender_user_id),0) AS self_replies ' + base,
            params))[0]
        exclude = '' if include_self else ' AND (m.sender_user_id IS NULL OR p.sender_user_id IS NULL OR '
        if not include_self:
            exclude += 'm.sender_user_id<>p.sender_user_id)'
        rows = await self.store.query(SELECT + f'{target}.sender_user_id AS user_id,'
            f'{target}.sender_chat_id AS sender_chat_id,COUNT(*) AS replies,MIN(m.sent_at) AS first_reply_at,'
            'MAX(m.sent_at) AS last_reply_at ' + base +
            f' AND p.archive_id IS NOT NULL AND ({target}.sender_user_id IS NOT NULL OR '
            f'{target}.sender_chat_id IS NOT NULL)' + exclude +
            ' GROUP BY user_id,sender_chat_id ORDER BY replies DESC,user_id,sender_chat_id LIMIT %s', params + (limit,))
        for r in rows:
            profiles = await self.store.query('SELECT user_id,username,first_name,last_name FROM users WHERE user_id=%s',
                                             (r['user_id'],)) if r['user_id'] else []
            r['profile'] = profiles[0] if profiles else None
        return dict(partners=rows, totals=totals, metadata=metadata(scope, user_id=user_id, direction=direction,
            include_self=include_self, ranking_limited=len(rows) == limit,
            note='Explicit replies only; missing incoming reply targets cannot be attributed to this user.'))

    async def posts(self, scope, metric, limit):
        if metric not in ('views', 'forwards', 'reactions'):
            raise ValueError('metric must be views, forwards or reactions')
        where, params = scope.where()
        # Reaction JSON is an array of observed aggregate counts, not identities.
        score = ('CASE WHEN x.reactions IS NULL THEN NULL ELSE COALESCE((SELECT SUM(j.n) '
                 'FROM JSON_TABLE(x.reactions,\'$[*]\' COLUMNS(n BIGINT PATH \'$.count\')) j),0) END'
                 ) if metric == 'reactions' else f'x.{metric}'
        rows = await self.store.query(SELECT + 'm.chat_id,m.message_id,m.sender_user_id,m.sender_chat_id,m.sent_at,'
            'LEFT(m.text,500) AS text,m.media_type,m.media,x.views,x.forwards,x.observed_at,' + score + ' AS score '
            'FROM messages m JOIN message_metrics x ON x.chat_id=m.chat_id AND x.message_id=m.message_id '
            'WHERE ' + where + ' HAVING score IS NOT NULL '
            'ORDER BY score DESC,m.sent_at DESC,m.chat_id,m.message_id LIMIT %s', params + (limit,))
        for r in rows:
            r['url'] = link(r)
        return dict(posts=rows, metadata=metadata(scope, metric=metric, ranking_limited=len(rows) == limit,
            note='Latest observed counters, not current live metrics or unique viewers; missing values excluded.'))

    async def coverage(self, scope, limit):
        where, params = scope.where()
        rows = await self.store.query(SELECT + 'm.chat_id,COUNT(*) AS messages,MIN(m.sent_at) AS first_at,'
            'MAX(m.sent_at) AS last_at,MIN(m.collected_at) AS first_collected_at,'
            'MAX(m.collected_at) AS last_collected_at FROM messages m WHERE ' + where +
            ' GROUP BY m.chat_id ORDER BY m.chat_id LIMIT %s', params + (limit + 1,))
        missing = (await self.store.query(SELECT + 'COUNT(*) AS replies,'
            'COALESCE(SUM(p.archive_id IS NULL),0) AS missing_reply_targets '
            'FROM messages m LEFT JOIN messages p ON p.chat_id=COALESCE(m.reply_to_chat_id,m.chat_id) '
            'AND p.message_id=m.reply_to_message_id WHERE ' + where + ' AND m.reply_to_message_id IS NOT NULL',
            params))[0]
        meta = metadata(scope, note='Observed bounds do not prove complete history; missing replies may predate scope.')
        meta['truncated'] = len(rows) > limit
        return dict(chats=rows[:limit], sources=await self.grouped(scope, 'ingest_source'), replies=missing, metadata=meta)
