import argparse
import asyncio
import logging
import os

from pyrogram import Client, enums, filters, idle
from pyrogram.errors import FloodWait, RPCError
from pyrogram.handlers import MessageHandler, EditedMessageHandler

from collector.config import DATA, MODEL, chat_ids
from collector.storage import Store
from collector.search import Encoder
from collector.vector_index import IndexWriter

log = logging.getLogger('collector')


def archive_chat(chat):
    return bool(chat and chat.type in {
        enums.ChatType.GROUP, enums.ChatType.SUPERGROUP, enums.ChatType.FORUM, enums.ChatType.CHANNEL,
    })


class AuthorizedClient(Client):
    async def authorize(self):
        raise RuntimeError('Telegram authorization is required; run app.py login in an interactive terminal')


def client(interactive=False):
    cls = Client if interactive else AuthorizedClient
    return cls('collector', workdir=str(DATA), api_id=int(os.environ['API_ID']),
                  api_hash=os.environ['API_HASH'], phone_number=os.environ['PHONE_NUMBER'],
                  workers=1, sleep_threshold=60, hide_password=True,
                  fetch_replies=False, fetch_topics=False, fetch_stories=False, fetch_stickers=False)


async def sync_chat(app, store, chat_id, own_id, history_limit):
    await store.execute('INSERT IGNORE INTO chat_sync (chat_id) VALUES (%s)', (chat_id,))
    state = (await store.query('SELECT * FROM chat_sync WHERE chat_id=%s', (chat_id,)))[0]
    latest = state['latest_message_id']
    highest = latest
    if latest:
        # Catch-up is independent of the historical batch limit.
        async for msg in app.get_chat_history(chat_id, min_id=latest + 1):
            await store.save_message(msg, own_id)
            highest = max(highest, msg.id)
        await store.execute('UPDATE chat_sync SET latest_message_id=%s,updated_at=UTC_TIMESTAMP(6) '
                            'WHERE chat_id=%s', (highest, chat_id))
    if state['history_complete']:
        return
    count = 0
    oldest = state['oldest_message_id']
    async for msg in app.get_chat_history(chat_id, max_id=(oldest - 1) if oldest else 0, limit=history_limit):
        await store.save_message(msg, own_id)
        highest = max(highest, msg.id)
        count += 1
        await store.execute('UPDATE chat_sync SET oldest_message_id=LEAST(COALESCE(oldest_message_id,%s),%s),'
                            'updated_at=UTC_TIMESTAMP(6) WHERE chat_id=%s', (msg.id, msg.id, chat_id))
    complete = history_limit == 0 or count < history_limit
    await store.execute('UPDATE chat_sync SET latest_message_id=GREATEST(latest_message_id,%s),'
                        'history_complete=%s,updated_at=UTC_TIMESTAMP(6) WHERE chat_id=%s',
                        (highest, complete, chat_id))
    log.info('History batch: chat=%s messages=%s complete=%s', chat_id, count, complete)


async def history_loop(app, store, own_id, history_limit):
    allowed = chat_ids()
    while True:
        try:
            if allowed:
                ids = sorted(allowed)
            else:
                ids = []
                async for dialog in app.get_dialogs():
                    if not archive_chat(dialog.chat):
                        continue
                    await store.save_chat(dialog.chat)
                    ids.append(dialog.chat.id)
            for chat_id in ids:
                try:
                    chat = await app.get_chat(chat_id)
                    if not archive_chat(chat):
                        continue
                    await store.save_chat(chat, details=True)
                    await sync_chat(app, store, chat_id, own_id, history_limit)
                except FloodWait as exc:
                    log.warning('History FloodWait: %ss', exc.value)
                    await asyncio.sleep(exc.value + 1)
                except RPCError as exc:
                    log.warning('Chat %s unavailable (%s)', chat_id, type(exc).__name__)
                await asyncio.sleep(1)
        except FloodWait as exc:
            await asyncio.sleep(exc.value + 1)
        await asyncio.sleep(300)


async def bios_loop(app, store):
    while True:
        users = await store.query('SELECT user_id FROM users WHERE is_deleted=0 AND '
                                  '(bio_checked_at IS NULL OR bio_checked_at<UTC_TIMESTAMP()-INTERVAL 1 DAY) '
                                  'ORDER BY bio_checked_at LIMIT 50')
        for user in users:
            try:
                full = await app.get_chat(user['user_id'])
                await store.execute('UPDATE users SET bio=%s,bio_checked_at=UTC_TIMESTAMP(6) WHERE user_id=%s',
                                    (full.bio, user['user_id']))
            except FloodWait as exc:
                await asyncio.sleep(exc.value + 1)
                break
            except RPCError:
                await store.execute('UPDATE users SET bio_checked_at=UTC_TIMESTAMP(6) WHERE user_id=%s',
                                    (user['user_id'],))
            await asyncio.sleep(1)
        await asyncio.sleep(30)


async def embedding_loop(store, once=False):
    encoder = Encoder()
    index = IndexWriter()
    await index.refresh(store)
    while True:
        rows = await store.query('SELECT m.archive_id,m.text,m.text_hash FROM messages m '
                                 'LEFT JOIN message_embeddings e ON e.archive_id=m.archive_id '
                                 "WHERE m.text<>'' "
                                 'AND (e.archive_id IS NULL OR e.text_hash<>m.text_hash OR e.model<>%s) '
                                 'ORDER BY m.archive_id LIMIT 64', (MODEL,))
        if not rows:
            await index.refresh(store, force=True)
            if once:
                return
            await asyncio.sleep(10)
            continue
        vectors = await encoder.encode([r['text'] for r in rows])
        for row, vector in zip(rows, vectors):
            await store.execute('INSERT INTO message_embeddings (archive_id,model,text_hash,vector) '
                                'VALUES (%s,%s,%s,%s) AS incoming ON DUPLICATE KEY UPDATE model=incoming.model,'
                                'text_hash=incoming.text_hash,vector=incoming.vector,updated_at=UTC_TIMESTAMP(6)',
                                (row['archive_id'], MODEL, row['text_hash'], vector.astype('<f4').tobytes()))
        log.info('Embedded %s messages locally', len(rows))
        await index.refresh(store)


async def supervise(name, factory):
    while True:
        try:
            await factory()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error('%s failed (%s); retrying in 10 seconds', name, type(exc).__name__)
            await asyncio.sleep(10)


async def run(args):
    os.umask(0o077)
    if args.command == 'embed':
        store = await Store.open()
        try:
            await embedding_loop(store, once=args.once)
        finally:
            await store.close()
        return
    app = client(interactive=args.command in ('login', 'login-qr'))
    if args.command in ('login', 'login-qr'):
        await app.start(use_qr=args.command == 'login-qr')
        try:
            me = await app.get_me()
            log.info('Telegram session authorized for user %s', me.id)
        finally:
            await app.stop()
        return
    if not (DATA / 'collector.session').exists():
        raise RuntimeError('Run .venv/bin/python app.py login in an interactive terminal first')
    store = await Store.open()
    allowed = chat_ids()
    own_id = None
    ready = asyncio.Event()

    async def on_message(_, msg):
        await ready.wait()
        if archive_chat(msg.chat) and (not allowed or msg.chat.id in allowed):
            try:
                await store.save_message(msg, own_id)
            except Exception as exc:
                log.error('Message archive failed (%s); history sync will retry persisted messages', type(exc).__name__)

    scope = filters.group | filters.channel
    app.add_handler(MessageHandler(on_message, scope))
    app.add_handler(EditedMessageHandler(on_message, scope))
    tasks = []
    try:
        await app.start()
        me = await app.get_me()
        own_id = me.id
        ready.set()
        await store.save_user(me)
        tasks = [asyncio.create_task(supervise('history', lambda: history_loop(app, store, own_id, args.history_limit))),
                 asyncio.create_task(supervise('bios', lambda: bios_loop(app, store))),
                 asyncio.create_task(supervise('embeddings', lambda: embedding_loop(store)))]
        log.info('Collector running for user %s', own_id)
        await idle()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if app.is_connected:
            await app.stop()
        await store.close()


def main():
    import fcntl
    lock = (DATA / 'collector.lock').open('w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit('Another collector/login/embedding process is already running') from None
    parser = argparse.ArgumentParser(description='Telegram message collector')
    parser.add_argument('command', choices=['login', 'login-qr', 'run', 'embed'], nargs='?', default='run')
    parser.add_argument('--history-limit', type=int, default=int(os.getenv('HISTORY_LIMIT', '1000')),
                        help='Older messages per chat per 5-minute cycle; 0 means all')
    parser.add_argument('--once', action='store_true', help='Drain the embedding backlog and exit')
    args = parser.parse_args()
    if args.history_limit < 0:
        parser.error('--history-limit must be >=0')
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    logging.getLogger('asyncmy').setLevel(logging.ERROR)
    asyncio.run(run(args))
