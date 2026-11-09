import argparse
import asyncio
import logging
import os

from pyrogram import Client, enums, filters, idle, raw, utils
from pyrogram.errors import FloodWait, RPCError
from pyrogram.handlers import MessageHandler, EditedMessageHandler, RawUpdateHandler

from collector.config import DATA, MODEL, chat_ids
from collector.storage import Store, reaction_counts
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
                  workers=1, sleep_threshold=60, hide_password=True, skip_updates=True,
                  fetch_replies=False, fetch_topics=False, fetch_stories=False, fetch_stickers=False)


async def profile_loop(app, store, queue, pending):
    # Event-driven enrichment only: no dialog scan or daily profile polling.
    while True:
        user_id = await queue.get()
        retry = False
        try:
            full = await app.get_chat(user_id)
            await store.save_bio(user_id, full.bio)
        except FloodWait as exc:
            await report_error(store, 'profile', exc, user_id=user_id)
            await asyncio.sleep(exc.value + 1)
            retry = True
        except RPCError as exc:
            await store.execute('UPDATE users SET bio_checked_at=UTC_TIMESTAMP(6) WHERE user_id=%s', (user_id,))
            await report_error(store, 'profile', exc, user_id=user_id)
        finally:
            queue.task_done()
            if retry:
                queue.put_nowait(user_id)
            else:
                pending.discard(user_id)
        await asyncio.sleep(1)


async def heartbeat_loop(store):
    while True:
        await store.heartbeat()
        await asyncio.sleep(15)


async def report_error(store, stage, exc, **context):
    log.error('%s failed (%s)', stage, type(exc).__name__)
    try:
        await store.record_error(stage, exc, **context)
    except Exception as telemetry_error:
        log.error('Error telemetry unavailable (%s)', type(telemetry_error).__name__)


async def handle_raw_update(store, update, queue_profile, allowed):
    if isinstance(update, (raw.types.UpdateUserName, raw.types.UpdateUser)):
        known = await store.query('SELECT user_id FROM users WHERE user_id=%s', (update.user_id,))
        if not known:
            return
        if isinstance(update, raw.types.UpdateUserName):
            username = next((item.username for item in update.usernames if item.active), None)
            await store.save_profile(update.user_id, dict(username=username, first_name=update.first_name,
                                     last_name=update.last_name), source='telegram_update')
        queue_profile(update.user_id)
        await store.event_saved(metadata=True)
        return
    if isinstance(update, raw.types.UpdateChannelMessageViews):
        chat_id, message_id = utils.get_peer_id(raw.types.PeerChannel(channel_id=update.channel_id)), update.id
        metrics = dict(views=update.views, source='views_update')
    elif isinstance(update, raw.types.UpdateChannelMessageForwards):
        chat_id, message_id = utils.get_peer_id(raw.types.PeerChannel(channel_id=update.channel_id)), update.id
        metrics = dict(forwards=update.forwards, source='forwards_update')
    elif isinstance(update, raw.types.UpdateMessageReactions):
        chat_id, message_id = utils.get_peer_id(update.peer), update.msg_id
        metrics = dict(reactions=reaction_counts(update.reactions), source='reactions_update')
    else:
        return
    if allowed and chat_id not in allowed:
        return
    rows = await store.query('SELECT c.type FROM messages m JOIN chats c ON c.chat_id=m.chat_id '
                             'WHERE m.chat_id=%s AND m.message_id=%s', (chat_id, message_id))
    if not rows or rows[0]['type'] not in ('group', 'supergroup', 'forum', 'channel'):
        return
    await store.save_metrics(chat_id, message_id, **metrics)
    await store.event_saved(metadata=True)


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


async def supervise(name, factory, store):
    while True:
        try:
            await factory()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await report_error(store, name, exc)
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
    profile_queue = asyncio.Queue()
    pending_profiles = set()

    def queue_profile(user_id):
        if user_id not in pending_profiles:
            pending_profiles.add(user_id)
            profile_queue.put_nowait(user_id)

    async def on_message(_, msg):
        await ready.wait()
        if archive_chat(msg.chat) and (not allowed or msg.chat.id in allowed):
            try:
                needs_bio = await store.save_message(msg, own_id)
                if needs_bio:
                    queue_profile(needs_bio)
                await store.event_saved()
            except Exception as exc:
                await report_error(store, 'message', exc, chat_id=msg.chat.id, message_id=msg.id)

    async def on_raw(_, update, users, chats):
        await ready.wait()
        try:
            await handle_raw_update(store, update, queue_profile, allowed)
        except Exception as exc:
            await report_error(store, 'metadata', exc)

    scope = filters.group | filters.channel
    app.add_handler(MessageHandler(on_message, scope))
    app.add_handler(EditedMessageHandler(on_message, scope))
    app.add_handler(RawUpdateHandler(on_raw), group=1)
    tasks = []
    try:
        await store.start_run()
        await app.start()
        me = await app.get_me()
        own_id = me.id
        await store.save_user(me)
        ready.set()
        tasks = [asyncio.create_task(supervise('profiles', lambda: profile_loop(
                     app, store, profile_queue, pending_profiles), store)),
                 asyncio.create_task(supervise('embeddings', lambda: embedding_loop(store), store)),
                 asyncio.create_task(supervise('heartbeat', lambda: heartbeat_loop(store), store))]
        log.info('Collector running for user %s', own_id)
        await idle()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if app.is_connected:
            await app.stop()
        await store.finish_run()
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
    parser.add_argument('--once', action='store_true', help='Drain the embedding backlog and exit')
    args = parser.parse_args()
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    logging.getLogger('asyncmy').setLevel(logging.ERROR)
    asyncio.run(run(args))
