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


def message_row(message, own_id):
    user = message.from_user
    sender_chat = message.sender_chat
    text = str(message.text or message.caption or '')
    media_type = kind(message.media)
    media = getattr(message, media_type, None) if media_type else None
    metadata = None
    if media is not None:
        metadata = {k: getattr(media, k, None) for k in (
            'file_id', 'file_unique_id', 'file_size', 'file_name', 'mime_type',
            'duration', 'width', 'height', 'latitude', 'longitude',
        )}
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
    )


class Store:
    def __init__(self, pool):
        self.pool = pool

    @classmethod
    async def open(cls, readonly=False):
        return cls(await asyncmy.create_pool(**db_options(readonly)))

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()

    async def query(self, sql, params=()):
        async with self.pool.acquire() as conn, conn.cursor(DictCursor) as cur:
            await cur.execute(sql, params)
            return list(await cur.fetchall())

    async def execute(self, sql, params=()):
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            return cur.rowcount

    async def save_user(self, user):
        if not user:
            return
        await self.execute('''INSERT INTO users
            (user_id, username, first_name, last_name, is_bot, is_deleted)
            VALUES (%s,%s,%s,%s,%s,%s) AS incoming ON DUPLICATE KEY UPDATE
            username=incoming.username,first_name=incoming.first_name,last_name=incoming.last_name,
            is_bot=incoming.is_bot,is_deleted=incoming.is_deleted,updated_at=UTC_TIMESTAMP(6)''',
            (user.id, user.username, user.first_name, user.last_name, bool(user.is_bot), bool(user.is_deleted)))

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
        await self.save_user(message.from_user)
        await self.save_chat(message.chat)
        if message.sender_chat:
            await self.save_chat(message.sender_chat)
        await self.save_row(message_row(message, own_id))

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
                        mutable = [k for k in cols if k not in ('chat_id', 'message_id')]
                        await cur.execute('UPDATE messages SET ' + ','.join(k + '=%s' for k in mutable) +
                                          ' WHERE chat_id=%s AND message_id=%s',
                                          tuple(row[k] for k in mutable) + (row['chat_id'], row['message_id']))
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def mark_deleted(self, messages, allowed):
        for message in messages:
            chat = message.chat
            if chat and (not allowed or chat.id in allowed):
                await self.execute('UPDATE messages SET deleted_at=COALESCE(deleted_at,UTC_TIMESTAMP(6)) '
                                   'WHERE chat_id=%s AND message_id=%s', (chat.id, message.id))
            elif not chat:
                # Private/basic group deletion updates can lack peer information. Never guess a chat.
                await self.execute('INSERT IGNORE INTO unresolved_deletions (message_id) VALUES (%s)', (message.id,))
