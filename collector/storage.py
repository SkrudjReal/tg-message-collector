import hashlib
import json
from datetime import datetime, timezone

import asyncmy
from asyncmy.cursors import DictCursor

from collector.config import db_options


def utc(value):
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def kind(value):
    return value.value if hasattr(value, 'value') else str(value) if value else None


def json_text(value):
    return json.dumps(value, ensure_ascii=False, default=str) if value is not None else None


def media_metadata(media):
    """File identifiers from the received object only; never download/resolve media."""
    if media is None:
        return None
    files, visited = [], set()

    def visit(obj):
        if obj is None or id(obj) in visited:
            return
        visited.add(id(obj))
        if isinstance(obj, (list, tuple)):
            for item in obj:
                visit(item)
        elif getattr(obj, 'file_id', None):
            files.append({k: getattr(obj, k, None) for k in (
                'file_id', 'file_unique_id', 'file_size', 'file_name', 'mime_type',
                'duration', 'width', 'height',
            )})
        else:
            for field in ('media', 'photo', 'video', 'animation', 'audio', 'document', 'sticker', 'live_photo'):
                visit(getattr(obj, field, None))

    visit(media)
    return dict(file_id=files[0]['file_id'] if files else None, files=files,
                latitude=getattr(media, 'latitude', None), longitude=getattr(media, 'longitude', None))


def reaction_counts(value):
    if value is None:
        return None
    result = []
    for item in getattr(value, 'results', getattr(value, 'reactions', [])):
        reaction = getattr(item, 'reaction', item)
        result.append(dict(emoji=getattr(reaction, 'emoticon', getattr(reaction, 'emoji', None)),
                           custom_emoji_id=str(getattr(reaction, 'document_id', None) or
                                               getattr(reaction, 'custom_emoji_id', '') or '') or None,
                           paid=type(reaction).__name__ == 'ReactionPaid' or bool(getattr(reaction, 'is_paid', False)),
                           count=item.count))
    return sorted(result, key=lambda r: (r['emoji'] or '', r['custom_emoji_id'] or '', r['paid']))


def message_row(message, own_id):
    user = message.from_user
    sender_chat = message.sender_chat
    text = str(message.text or message.caption or '')
    media_type = kind(message.media)
    media = getattr(message, media_type, None) if media_type else None
    metadata = media_metadata(media)
    entities = message.entities or message.caption_entities
    entity_data = [dict(type=kind(e.type), offset=e.offset, length=e.length,
                        url=e.url, user_id=e.user.id if e.user else None,
                        custom_emoji_id=getattr(e, 'custom_emoji_id', None)) for e in (entities or [])]
    origin = getattr(message, 'forward_origin', None)
    forward = dict(
        type=kind(getattr(origin, 'type', None)),
        from_user_id=getattr(getattr(origin, 'sender_user', None), 'id', None),
        from_chat_id=getattr(getattr(origin, 'chat', None) or getattr(origin, 'sender_chat', None), 'id', None),
        message_id=getattr(origin, 'message_id', None),
        sender_name=getattr(origin, 'sender_user_name', None),
        date=getattr(origin, 'date', None),
    )
    private = kind(message.chat.type) == 'private'
    reply_id = message.reply_to_message_id
    return dict(
        chat_id=message.chat.id, message_id=message.id,
        sender_user_id=user.id if user else None,
        sender_chat_id=sender_chat.id if sender_chat else None,
        recipient_user_id=(message.chat.id if message.outgoing else own_id) if private else None,
        sent_at=utc(message.date), edited_at=utc(message.edit_date),
        reply_to_chat_id=(getattr(message, 'reply_to_chat_id', None) or message.chat.id) if reply_id else None,
        reply_to_message_id=reply_id, topic_id=getattr(message, 'message_thread_id', None),
        text=text, text_hash=hashlib.sha256(text.encode()).hexdigest(),
        media_type=media_type, media=json_text(metadata), entities=json_text(entity_data),
        forward_info=json_text(forward) if any(forward.values()) else None,
        media_group_id=str(message.media_group_id) if message.media_group_id else None,
        service_type=kind(message.service), outgoing=bool(message.outgoing),
        ingest_source='live',
    )


class Store:
    def __init__(self, pool):
        self.pool = pool
        self.run_id = None

    @classmethod
    async def open(cls, readonly=False):
        return cls(await asyncmy.create_pool(**db_options(readonly)))

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()

    async def query(self, sql, params=()):
        async with self.pool.acquire() as conn, conn.cursor(DictCursor) as cur:
            await cur.execute(sql, params)
            rows = list(await cur.fetchall())
            for row in rows:
                for field in ('media', 'entities', 'forward_info', 'reactions'):
                    if isinstance(row.get(field), str):
                        row[field] = json.loads(row[field])
                if 'media_type' in row:
                    media = row.get('media') or {}
                    row['file_id'] = media.get('file_id')
                    row['media_file_ids'] = list(dict.fromkeys(
                        [item['file_id'] for item in media.get('files', []) if item.get('file_id')] or
                        ([media['file_id']] if media.get('file_id') else [])))
            return rows

    async def execute(self, sql, params=()):
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            return cur.rowcount

    async def save_user(self, user):
        if not user:
            return False
        return await self.save_profile(user.id, dict(username=user.username, first_name=user.first_name,
                                      last_name=user.last_name, is_bot=bool(user.is_bot),
                                      is_deleted=bool(user.is_deleted)), source='message', create=True)

    @staticmethod
    async def snapshot_user(cur, user_id, source):
        await cur.execute('INSERT INTO user_history '
                          '(user_id,username,first_name,last_name,bio,bio_checked_at,source) '
                          'SELECT user_id,username,first_name,last_name,bio,bio_checked_at,%s '
                          'FROM users WHERE user_id=%s', (source, user_id))

    async def save_profile(self, user_id, fields, source, create=False):
        if not fields.keys() <= {'username', 'first_name', 'last_name', 'bio', 'bio_checked_at', 'is_bot', 'is_deleted'}:
            raise ValueError('Unsupported profile fields')
        async with self.pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor(DictCursor) as cur:
                    inserted = False
                    if create:
                        await cur.execute('INSERT INTO users (user_id) VALUES (%s) '
                                          'ON DUPLICATE KEY UPDATE user_id=user_id', (user_id,))
                        inserted = cur.rowcount == 1
                    await cur.execute('SELECT * FROM users WHERE user_id=%s FOR UPDATE', (user_id,))
                    old = await cur.fetchone()
                    if old is None:
                        await conn.commit()
                        return False
                    if not inserted:
                        await cur.execute('SELECT snapshot_id FROM user_history WHERE user_id=%s LIMIT 1', (user_id,))
                        if not await cur.fetchone():
                            await self.snapshot_user(cur, user_id, 'baseline')
                    changed = any(old[key] != value for key, value in fields.items()
                                  if key in ('username', 'first_name', 'last_name', 'bio'))
                    await cur.execute('UPDATE users SET ' + ','.join(key + '=%s' for key in fields) +
                                      ',updated_at=UTC_TIMESTAMP(6) WHERE user_id=%s', (*fields.values(), user_id))
                    if inserted or changed:
                        await self.snapshot_user(cur, user_id, source)
                    needs_bio = fields.get('bio_checked_at', old['bio_checked_at']) is None
                await conn.commit()
                return needs_bio
            except BaseException:
                await conn.rollback()
                raise

    async def save_bio(self, user_id, bio):
        await self.save_profile(user_id, dict(bio=bio, bio_checked_at=datetime.now(timezone.utc).replace(tzinfo=None)),
                                source='profile_lookup')

    async def save_chat(self, chat, details=False):
        await self.execute('''INSERT INTO chats
            (chat_id,type,title,username,description,members_count,is_forum,linked_chat_id,details_checked_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) AS incoming ON DUPLICATE KEY UPDATE
            type=incoming.type,title=incoming.title,username=incoming.username,
            description=IF(incoming.details_checked_at IS NOT NULL,incoming.description,chats.description),
            members_count=COALESCE(incoming.members_count,chats.members_count),
            is_forum=IF(incoming.details_checked_at IS NOT NULL,incoming.is_forum,chats.is_forum OR incoming.is_forum),
            linked_chat_id=IF(incoming.details_checked_at IS NOT NULL,incoming.linked_chat_id,chats.linked_chat_id),
            details_checked_at=COALESCE(incoming.details_checked_at,chats.details_checked_at),updated_at=UTC_TIMESTAMP(6)''',
            (chat.id, kind(chat.type), chat.title or getattr(chat, 'first_name', None), chat.username,
             getattr(chat, 'description', None), getattr(chat, 'members_count', None),
             bool(getattr(chat, 'is_forum', False)), getattr(getattr(chat, 'linked_chat', None), 'id', None),
             datetime.now(timezone.utc).replace(tzinfo=None) if details else None))

    async def save_message(self, message, own_id):
        if not message.chat or message.empty:
            return
        needs_bio = await self.save_user(message.from_user)
        await self.save_chat(message.chat)
        if message.sender_chat:
            await self.save_chat(message.sender_chat)
        await self.save_row(message_row(message, own_id))
        await self.save_metrics(message.chat.id, message.id, views=message.views, forwards=message.forwards,
                                reactions=reaction_counts(message.reactions), source='message')
        return message.from_user.id if message.from_user and needs_bio else None

    async def save_row(self, row):
        """Atomic upsert. Old history must never overwrite a newer live edit."""
        cols = list(row)
        async with self.pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute('INSERT INTO messages (' + ','.join(cols) + ') VALUES (' +
                                      ','.join(['%s'] * len(cols)) +
                                      ') ON DUPLICATE KEY UPDATE archive_id=archive_id', tuple(row.values()))
                    await cur.execute('SELECT text,text_hash,edited_at,sent_at FROM messages '
                                      'WHERE chat_id=%s AND message_id=%s FOR UPDATE',
                                      (row['chat_id'], row['message_id']))
                    old = await cur.fetchone()
                    incoming_version = row['edited_at'] or row['sent_at']
                    old_version = old['edited_at'] or old['sent_at']
                    if incoming_version >= old_version:
                        if old['text_hash'] != row['text_hash']:
                            await cur.execute('INSERT INTO message_revisions '
                                              '(chat_id,message_id,text,text_hash,edited_at) VALUES (%s,%s,%s,%s,%s)',
                                              (row['chat_id'], row['message_id'], old['text'], old['text_hash'], old['edited_at']))
                        mutable = [k for k in cols if k not in ('chat_id', 'message_id', 'ingest_source')]
                        await cur.execute('UPDATE messages SET ' + ','.join(k + '=%s' for k in mutable) +
                                          ' WHERE chat_id=%s AND message_id=%s',
                                          tuple(row[k] for k in mutable) + (row['chat_id'], row['message_id']))
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def save_metrics(self, chat_id, message_id, views=None, forwards=None, reactions=None, source='update'):
        incoming = {key: value for key, value in dict(views=views, forwards=forwards,
                    reactions=json_text(reactions)).items() if value is not None}
        if not incoming:
            return
        async with self.pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute('INSERT INTO message_metrics (chat_id,message_id) VALUES (%s,%s) '
                                      'ON DUPLICATE KEY UPDATE chat_id=chat_id', (chat_id, message_id))
                    await cur.execute('SELECT * FROM message_metrics WHERE chat_id=%s AND message_id=%s FOR UPDATE',
                                      (chat_id, message_id))
                    old = await cur.fetchone()
                    changed = any((json.loads(old[k]) if k == 'reactions' and isinstance(old[k], str) else old[k]) !=
                                  (json.loads(v) if k == 'reactions' else v) for k, v in incoming.items())
                    await cur.execute('UPDATE message_metrics SET ' + ','.join(k + '=%s' for k in incoming) +
                                      ',observed_at=UTC_TIMESTAMP(6) WHERE chat_id=%s AND message_id=%s',
                                      (*incoming.values(), chat_id, message_id))
                    if changed:
                        await cur.execute('INSERT INTO message_metric_history '
                                          '(chat_id,message_id,views,forwards,reactions,source) '
                                          'SELECT chat_id,message_id,views,forwards,reactions,%s FROM message_metrics '
                                          'WHERE chat_id=%s AND message_id=%s', (source, chat_id, message_id))
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def start_run(self):
        import os
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute('INSERT INTO collector_runs (pid) VALUES (%s)', (os.getpid(),))
            self.run_id = cur.lastrowid

    async def heartbeat(self):
        await self.execute('UPDATE collector_runs SET heartbeat_at=UTC_TIMESTAMP(6) WHERE run_id=%s', (self.run_id,))

    async def finish_run(self):
        await self.execute('UPDATE collector_runs SET stopped_at=UTC_TIMESTAMP(6),heartbeat_at=UTC_TIMESTAMP(6) '
                           'WHERE run_id=%s', (self.run_id,))

    async def event_saved(self, metadata=False):
        changes = ('metadata_events=metadata_events+1' if metadata else
                   'received_count=received_count+1,saved_count=saved_count+1,last_message_at=UTC_TIMESTAMP(6)')
        await self.execute('UPDATE collector_runs SET ' + changes + ' WHERE run_id=%s', (self.run_id,))

    async def record_error(self, stage, exc, chat_id=None, message_id=None, user_id=None):
        await self.execute('INSERT INTO collector_errors (run_id,stage,error_type,chat_id,message_id,user_id) '
                           'VALUES (%s,%s,%s,%s,%s,%s)',
                           (self.run_id, stage, type(exc).__name__, chat_id, message_id, user_id))
        increment = ',received_count=received_count+1,message_errors=message_errors+1' if stage == 'message' else ''
        await self.execute('UPDATE collector_runs SET error_count=error_count+1' + increment + ' WHERE run_id=%s',
                           (self.run_id,))
