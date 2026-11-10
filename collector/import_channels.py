"""Explicit, resumable channel history import. Never installed in the live handlers."""
import argparse
import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import signal
import subprocess
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from pyrogram import enums, raw, utils
from pyrogram.errors import FloodWait, RPCError

from collector.config import DATA
from collector.runtime import AuthorizedClient, fetch_bio
from collector.storage import Store, utc


def parse_since(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        result = result.replace(tzinfo=ZoneInfo('Europe/Warsaw'))
    return utc(result)


def load_targets(path):
    targets = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('@'):
            username = line[1:]
        else:
            url = urlparse(line)
            if url.scheme != 'https' or url.netloc.lower() != 't.me' or url.query or url.fragment:
                raise ValueError('Targets must be public https://t.me/username channel links')
            username = url.path.strip('/')
        if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{3,31}', username):
            raise ValueError('Invalid public channel username')
        if username.lower() not in targets:
            targets.append(username.lower())
    if not targets:
        raise ValueError('An empty target list is not allowed')
    return targets


def channel_only(chat):
    return bool(chat and chat.type == enums.ChatType.CHANNEL and not chat.is_banned and
                not any(getattr(chat.raw, flag, False) for flag in ('kicked', 'deactivated')))


@contextmanager
def session_lock(path):
    """Cooperative lock plus detection of non-cooperating clients before opening SQLite."""
    if not path.is_file() or path.suffix != '.session':
        raise ValueError('An existing .session file is required')
    with Path(str(path) + '.import.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = subprocess.run(['fuser', str(path)], capture_output=True, check=False)
        if result.returncode not in (0, 1) or result.stdout.strip():
            raise RuntimeError('Session is in use; stop its Telegram client before importing')
        yield


class ChannelImporter:
    def __init__(self, app, store, account_id, *, targets, since, chat_type='channel',
                 page_size=100, pause=1, progress_seconds=300):
        if not targets or since is None:
            raise ValueError('Explicit targets and a start date are required')
        self.app, self.store, self.account_id = app, store, account_id
        self.targets, self.since = targets, utc(since)
        if chat_type not in ('channel', 'supergroup', 'forum'):
            raise ValueError('Unsupported chat type')
        if chat_type != 'channel' and any(not re.fullmatch(r'id:-[1-9]\d*', t) for t in targets):
            raise ValueError('Groups require explicit numeric chat IDs')
        self.chat_type = chat_type
        identity = [sorted(targets), self.since.isoformat()]
        if chat_type != 'channel':
            identity.append(chat_type)
        self.scope_key = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
        self.target_ids = set()
        self.page_size, self.pause, self.progress_seconds = page_size, pause, progress_seconds
        self.users = set()
        self.saved_chats = set()
        self.state = dict(account_id=account_id, pid=os.getpid(), phase='starting', scope_key=self.scope_key,
                          targets=targets, chat_type=chat_type, since_utc=self.since.isoformat(), resolution_errors=[],
                          current_chat_id=None, waiting_until=None, profile_errors=0)
        self.status_path = DATA / f'channel-import-{account_id}.json'

    def accepts(self, chat):
        return bool(chat and chat.type.value == self.chat_type and not chat.is_banned and
                    not any(getattr(chat.raw, flag, False) for flag in ('kicked', 'deactivated')))

    async def report(self, emit=False):
        rows = await self.store.query('SELECT * FROM scoped_channel_imports '
            'WHERE scope_key=%s AND account_id=%s ORDER BY chat_id', (self.scope_key, self.account_id))
        payload = dict(self.state, updated_at=datetime.now(timezone.utc).isoformat(), channels=rows,
                       total_channels=len(self.targets), resolved_channels=len(rows),
                       completed=sum(r['status'] == 'complete' for r in rows),
                       failed=sum(r['status'] == 'error' for r in rows),
                       processed=sum(r['processed'] for r in rows), inserted=sum(r['inserted'] for r in rows))
        temporary = self.status_path.with_suffix('.tmp')
        temporary.write_text(json.dumps(payload, ensure_ascii=False, default=str, indent=2))
        temporary.replace(self.status_path)
        if emit:
            print(json.dumps({k: v for k, v in payload.items() if k != 'channels'}, ensure_ascii=False), flush=True)
        return payload

    async def progress_loop(self):
        elapsed = 0
        while True:
            await asyncio.sleep(10)
            elapsed += 10
            await self.report(emit=elapsed >= self.progress_seconds)
            if elapsed >= self.progress_seconds:
                elapsed = 0

    async def read(self, operation):
        """All retried operations are Telegram reads. Database failures are not swallowed."""
        retries = 0
        while True:
            try:
                return await operation()
            except FloodWait as exc:
                self.state['waiting_until'] = datetime.fromtimestamp(
                    datetime.now(timezone.utc).timestamp() + exc.value + 1, timezone.utc).isoformat()
                await self.report(emit=True)
                await asyncio.sleep(exc.value + 1)
                self.state['waiting_until'] = None
            except (OSError, TimeoutError):
                retries += 1
                if retries > 5:
                    raise
                await asyncio.sleep(min(60, 2 ** retries))

    async def discover(self):
        # No get_dialogs: only explicitly supplied targets may be resolved/read.
        self.state['phase'] = 'discovery'
        for username in self.targets:
            existing = await self.store.query('SELECT chat_id FROM scoped_channel_imports '
                'WHERE scope_key=%s AND account_id=%s AND target_username=%s',
                (self.scope_key, self.account_id, username))
            if existing:
                if username.startswith('id:') and existing[0]['chat_id'] != int(username[3:]):
                    raise ValueError('Checkpoint differs from the explicitly requested chat ID')
                self.target_ids.add(existing[0]['chat_id'])
                continue
            try:
                peer = int(username[3:]) if username.startswith('id:') else username
                chat = await self.read(lambda: self.app.get_chat(peer, force_full=False))
                if not self.accepts(chat):
                    raise ValueError('Target does not match the explicitly allowed chat type')
                names = {chat.username.lower()} if chat.username else set()
                names.update(u.username.lower() for u in (getattr(chat, 'usernames', None) or []) if u.active)
                if isinstance(peer, int) and chat.id != peer:
                    raise ValueError('Resolved chat does not match the requested ID')
                if isinstance(peer, str) and username not in names:
                    raise ValueError('Resolved channel does not match the requested username')

                async def newest():
                    return [m async for m in self.app.get_chat_history(chat.id, limit=1)]
                latest = await self.read(newest)
                upper = latest[0].id if latest else 0
                await self.store.save_chat(chat)
                await self.store.execute('INSERT INTO scoped_channel_imports '
                    '(scope_key,account_id,chat_id,target_username,since_at,title,upper_message_id,next_max_id,chat_type) '
                    'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) AS incoming ON DUPLICATE KEY UPDATE title=incoming.title',
                    (self.scope_key, self.account_id, chat.id, username, self.since, chat.title, upper, upper, self.chat_type))
                self.target_ids.add(chat.id)
            except (RPCError, ValueError, AttributeError) as exc:
                self.state['resolution_errors'].append(dict(target=username, error_type=type(exc).__name__))
            await asyncio.sleep(self.pause)

    async def channel_details(self, chat):
        # Read only ChannelFull metadata; do not invoke Chat._parse_full (pinned posts/linked groups).
        peer = await self.read(lambda: self.app.resolve_peer(chat.id))
        result = await self.read(lambda: self.app.invoke(raw.functions.channels.GetFullChannel(
            channel=raw.types.InputChannel(channel_id=peer.channel_id, access_hash=peer.access_hash))))
        full = result.full_chat
        chat.description = getattr(full, 'about', None)
        chat.members_count = getattr(full, 'participants_count', None)
        chat.linked_chat = None
        await self.store.save_chat(chat, details=True)
        linked = getattr(full, 'linked_chat_id', None)
        if linked:
            await self.store.execute('UPDATE chats SET linked_chat_id=%s WHERE chat_id=%s',
                                     (utils.get_channel_id(linked), chat.id))

    async def enrich(self, msg):
        if not msg.chat or msg.empty:
            return False
        if not self.accepts(msg.chat):
            raise ValueError('History yielded a message of a different chat type')
        user = msg.from_user
        if user and user.id not in self.users:
            needs_bio = await self.store.save_user(user)
            if needs_bio:
                try:
                    bio = await self.read(lambda: fetch_bio(self.app, user.id))
                except (RPCError, ValueError, AttributeError) as exc:
                    self.state['profile_errors'] += 1
                    await self.store.record_error('import_profile', exc, user_id=user.id)
                else:
                    await self.store.save_bio(user.id, bio)
                await asyncio.sleep(self.pause)
            self.users.add(user.id)
        if msg.sender_chat and msg.sender_chat.id not in self.saved_chats:
            await self.store.save_chat(msg.sender_chat)
            self.saved_chats.add(msg.sender_chat.id)

    async def import_channel(self, state):
        chat_id = state['chat_id']
        if chat_id not in self.target_ids:
            raise ValueError('Channel is outside the explicit target list')
        chat = await self.read(lambda: self.app.get_chat(chat_id, force_full=False))
        if not self.accepts(chat):
            raise ValueError('Channel membership/type changed')
        try:
            await self.channel_details(chat)
        except (RPCError, AttributeError, ValueError) as exc:
            await self.store.execute('UPDATE scoped_channel_imports SET details_error=%s '
                                      'WHERE scope_key=%s AND account_id=%s AND chat_id=%s',
                                      (type(exc).__name__, self.scope_key, self.account_id, chat_id))
        await self.store.execute("UPDATE scoped_channel_imports SET status='running',error_type=NULL "
            'WHERE scope_key=%s AND account_id=%s AND chat_id=%s', (self.scope_key, self.account_id, chat_id))
        cursor = state['next_max_id']
        while cursor > 0:
            async def page():
                return [m async for m in self.app.get_chat_history(chat_id, limit=self.page_size, max_id=cursor)]
            messages = await self.read(page)
            if not messages:
                break
            next_cursor = min(m.id for m in messages) - 1
            if next_cursor >= cursor or any(m.id > cursor for m in messages):
                raise ValueError('History cursor did not advance')
            if any(not m.empty and m.date is None for m in messages):
                raise ValueError('Message date is missing')
            reached_start = any(not m.empty and utc(m.date) < self.since for m in messages)
            eligible = [m for m in messages if not m.empty and utc(m.date) >= self.since]
            if any(not m.chat or m.chat.id != chat_id or not self.accepts(m.chat) for m in eligible):
                raise ValueError('History returned messages outside the requested chat/type')
            for msg in eligible:
                await self.enrich(msg)
            await self.store.save_history_page(eligible, self.account_id, chat_id,
                0 if reached_start else next_cursor, scope_key=self.scope_key)
            if reached_start:
                break
            cursor = next_cursor
            await asyncio.sleep(self.pause)
        await self.store.execute("UPDATE scoped_channel_imports SET status='complete',next_max_id=0,"
            'updated_at=UTC_TIMESTAMP(6) WHERE scope_key=%s AND account_id=%s AND chat_id=%s',
            (self.scope_key, self.account_id, chat_id))

    async def run(self):
        reporter = asyncio.create_task(self.progress_loop())
        try:
            await self.discover()
            self.state['phase'] = 'history'
            await self.report(emit=True)
            rows = await self.store.query("SELECT * FROM scoped_channel_imports WHERE scope_key=%s AND account_id=%s "
                "AND status<>'complete' ORDER BY chat_id DESC", (self.scope_key, self.account_id))
            for row in rows:
                self.state['current_chat_id'] = row['chat_id']
                try:
                    await self.import_channel(row)
                except (RPCError, ValueError, AttributeError) as exc:
                    await self.store.execute("UPDATE scoped_channel_imports SET status='error',error_type=%s,"
                        'updated_at=UTC_TIMESTAMP(6) WHERE scope_key=%s AND account_id=%s AND chat_id=%s',
                        (type(exc).__name__, self.scope_key, self.account_id, row['chat_id']))
                    await self.store.record_error('import_channel', exc, chat_id=row['chat_id'])
                await self.report()
            self.state.update(phase='finished', current_chat_id=None)
        except asyncio.CancelledError:
            self.state['phase'] = 'stopped'
            raise
        except Exception:
            self.state['phase'] = 'failed'
            raise
        finally:
            reporter.cancel()
            await asyncio.gather(reporter, return_exceptions=True)
            await self.report(emit=True)


async def run(args):
    if args.status:
        path = DATA / f'channel-import-{args.expected_user_id}.json'
        print(path.read_text() if path.exists() else '{}')
        return
    targets = load_targets(args.channels_file) if args.channels_file else ['id:' + str(x) for x in args.chat_id]
    store = await Store.open()
    try:
        settings = dotenv_values(args.settings)
        app = AuthorizedClient(args.session.stem, workdir=str(args.session.parent),
            api_id=int(settings['API_ID']), api_hash=settings['API_HASH'], no_updates=True,
            sleep_threshold=60, fetch_replies=False, fetch_topics=False, fetch_stories=False, fetch_stickers=False)
        try:
            await app.start()
            me = await app.get_me()
            if me.id != args.expected_user_id:
                raise ValueError('Session account differs from --expected-user-id')
            importer = ChannelImporter(app, store, me.id, targets=targets, since=args.since, chat_type=args.chat_type)
            await importer.run()
        finally:
            if app.is_connected:
                await app.stop()
    finally:
        await store.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session', type=lambda s: Path(s).expanduser().resolve())
    parser.add_argument('--settings', type=Path)
    parser.add_argument('--expected-user-id', type=int, required=True)
    target_args = parser.add_mutually_exclusive_group()
    target_args.add_argument('--channels-file', type=Path, help='Allowlist of public channel links')
    target_args.add_argument('--chat-id', type=int, action='append', help='Exact negative chat ID; repeat to allow more')
    parser.add_argument('--chat-type', choices=['channel', 'supergroup', 'forum'], default='channel')
    parser.add_argument('--since', type=parse_since, help='Inclusive start date; naive dates use Europe/Warsaw')
    parser.add_argument('--status', action='store_true')
    args = parser.parse_args()
    if not args.status:
        if not args.session or not args.settings or not (args.channels_file or args.chat_id) or args.since is None:
            parser.error('--session, --settings, an explicit target list and --since are required for import')
        if args.chat_type != 'channel' and not args.chat_id:
            parser.error('Groups require --chat-id; a channel links file cannot enable group history')
        if args.chat_id and any(x >= 0 for x in args.chat_id):
            parser.error('--chat-id must be negative; private user chats are not supported')
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(name)s: %(message)s')

    async def managed():
        task = asyncio.current_task()
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        try:
            await run(args)
        finally:
            loop.remove_signal_handler(signal.SIGTERM)

    try:
        if args.status:
            asyncio.run(managed())
        else:
            with session_lock(args.session):
                asyncio.run(managed())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except Exception as exc:
        # Never print RPC/request bodies, API credentials or message contents.
        print(f'Channel import stopped: {type(exc).__name__}', flush=True)
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
