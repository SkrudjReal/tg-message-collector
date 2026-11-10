"""MCP schemas and short descriptions for the analytics service."""
from datetime import datetime
from typing import Literal

from collector.analytics import Analytics, MessageScope, metadata
from collector.search import bounded_limit
from collector.storage import utc


def register_analytics(mcp, get_store, annotations):
    instance = None

    def service():
        nonlocal instance
        if instance is None or instance.store is not get_store():
            instance = Analytics(get_store())
        return instance

    @mcp.tool(annotations=annotations)
    async def get_statistics(scope: MessageScope | None = None) -> dict:
        """Сообщения, авторы, реплаи, правки, медиа и длина текста за выбранный период. Только архив."""
        scope = scope or MessageScope()
        a = service()
        counts = await a.summary(scope)
        return dict(counts=counts, media=await a.grouped(scope, 'media_type'),
                    metadata=metadata(scope, matching_messages=counts['messages']))

    @mcp.tool(annotations=annotations)
    async def get_activity(scope: MessageScope | None = None,
                           bucket: Literal['hour', 'day', 'week', 'month'] = 'day',
                           timezone_name: str = 'UTC', limit: int = 365) -> dict:
        """Активность по часам, дням, неделям или месяцам; IANA-пояс, например Europe/Warsaw. Новые интервалы первыми."""
        return await service().activity(scope or MessageScope(), bucket, timezone_name, limit)

    @mcp.tool(annotations=annotations)
    async def get_top_words(scope: MessageScope | None = None, limit: int = 10,
                            normalization: Literal['literal', 'lemma_ru'] = 'literal',
                            remove_stopwords: bool = True, extra_stopwords: list[str] | None = None,
                            min_messages: int = 1, max_messages: int = 50000) -> dict:
        """Топ слов: употребления, число сообщений и примеры. Русские леммы опциональны; проверяйте metadata.truncated."""
        return await service().frequencies(scope or MessageScope(), normalization=normalization,
            remove_stopwords=remove_stopwords, extra_stopwords=extra_stopwords, limit=limit,
            min_messages=min_messages, max_messages=max_messages)

    @mcp.tool(annotations=annotations)
    async def get_top_phrases(scope: MessageScope | None = None, n: int = 2, limit: int = 10,
                              normalization: Literal['literal', 'lemma_ru'] = 'literal',
                              remove_stopwords: bool = True, extra_stopwords: list[str] | None = None,
                              min_messages: int = 2, max_messages: int = 50000) -> dict:
        """Топ соседних сочетаний из 2–5 слов с частотой и примерами; не склеивает предложения и ссылки."""
        if not 2 <= n <= 5:
            raise ValueError('n must be 2..5')
        return await service().frequencies(scope or MessageScope(), n=n, normalization=normalization,
            remove_stopwords=remove_stopwords, extra_stopwords=extra_stopwords, limit=limit,
            min_messages=min_messages, max_messages=max_messages)

    @mcp.tool(annotations=annotations)
    async def get_top_mentions(scope: MessageScope | None = None, limit: int = 10,
                               min_messages: int = 1, max_messages: int = 50000) -> dict:
        """Топ буквальных @username в тексте с частотой и примерами; не устанавливает личность адресата."""
        return await service().frequencies(scope or MessageScope(), mentions=True, limit=limit,
                                           min_messages=min_messages, max_messages=max_messages)

    @mcp.tool(annotations=annotations)
    async def get_top_authors(scope: MessageScope | None = None, limit: int = 10) -> dict:
        """Авторы с наибольшим числом сообщений и их доля в выборке; пользователи и отправители-каналы отдельно."""
        bounded_limit(limit, 100)
        return await service().authors(scope or MessageScope(), limit)

    @mcp.tool(annotations=annotations)
    async def get_top_posts(scope: MessageScope | None = None,
                            metric: Literal['views', 'forwards', 'reactions'] = 'views', limit: int = 10) -> dict:
        """Топ публикаций по последним сохранённым просмотрам, пересылкам или реакциям; снимки могут быть устаревшими."""
        bounded_limit(limit, 100)
        return await service().posts(scope or MessageScope(), metric, limit)

    @mcp.tool(annotations=annotations)
    async def get_reply_partners(user_id: int, scope: MessageScope | None = None,
                                 direction: Literal['outgoing', 'incoming'] = 'outgoing',
                                 include_self: bool = False, limit: int = 10) -> dict:
        """Кому пользователь чаще отвечает или кто отвечает ему. Только явные reply; отдельно показаны пропущенные цели."""
        bounded_limit(limit, 100)
        return await service().partners(user_id, scope or MessageScope(), direction, include_self, limit)

    @mcp.tool(annotations=annotations)
    async def get_message_history(chat_id: int, message_id: int,
                                   after_revision_id: int = 0, limit: int = 50) -> dict:
        """Текущее сообщение и сохранённые предыдущие версии текста с датами; пагинация по revision_id."""
        bounded_limit(limit)
        if after_revision_id < 0:
            raise ValueError('after_revision_id must be nonnegative')
        store = get_store()
        current = await store.query('SELECT * FROM messages WHERE chat_id=%s AND message_id=%s', (chat_id, message_id))
        rows = await store.query('SELECT * FROM message_revisions WHERE chat_id=%s AND message_id=%s '
            'AND revision_id>%s ORDER BY revision_id LIMIT %s', (chat_id, message_id, after_revision_id, limit + 1))
        total = (await store.query('SELECT COUNT(*) AS n FROM message_revisions WHERE chat_id=%s AND message_id=%s',
                                  (chat_id, message_id)))[0]['n']
        return dict(current=current[0] if current else None, revisions=rows[:limit], total_revisions=total,
                    next_revision_id=rows[limit - 1]['revision_id'] if len(rows) > limit else None,
                    note='Observed text versions only; earlier media and formatting are not reconstructed.')

    @mcp.tool(annotations=annotations)
    async def compare_periods(first_since: datetime, first_until: datetime,
                              second_since: datetime, second_until: datetime,
                              scope: MessageScope | None = None, include_vocabulary: bool = False,
                              word_limit: int = 10, max_messages: int = 50000) -> dict:
        """Сравнение статистики двух периодов; include_vocabulary добавляет изменения частоты слов и нормированные частоты."""
        bounded_limit(word_limit, 100)
        if not 1 <= max_messages <= 200000:
            raise ValueError('max_messages must be 1..200000')
        scope = scope or MessageScope()
        if scope.since or scope.until:
            raise ValueError('Pass dates in first/second_since/until, not scope')
        a = service()
        first = MessageScope(**(scope.model_dump() | dict(since=utc(first_since), until=utc(first_until))))
        second = MessageScope(**(scope.model_dump() | dict(since=utc(second_since), until=utc(second_until))))
        one, two = await a.summary(first), await a.summary(second)
        changes = {}
        for k in ('messages', 'users', 'chats', 'replies', 'edited', 'with_media', 'text_characters'):
            before, after = int(one[k]), int(two[k])
            changes[k] = dict(absolute=after - before,
                              percent=round((after - before) * 100 / before, 2) if before else None)
        result = dict(first=dict(counts=one, metadata=metadata(first)),
                      second=dict(counts=two, metadata=metadata(second)), changes=changes,
                      note='Percent is null for a zero baseline. Count totals are not normalized by period length.')
        if include_vocabulary:
            result['vocabulary'] = await a.vocabulary_change(first, second, word_limit, max_messages)
        return result

    @mcp.tool(annotations=annotations)
    async def get_archive_coverage(scope: MessageScope | None = None, limit: int = 100) -> dict:
        """Границы сохранённых периодов, источники загрузки и отсутствующие reply targets. Не доказывает полноту истории."""
        bounded_limit(limit, 1000)
        return await service().coverage(scope or MessageScope(), limit)
