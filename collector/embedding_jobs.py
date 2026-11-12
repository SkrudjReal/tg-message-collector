"""Resumable embedding generation and validated, atomic provider activation."""
import argparse
import asyncio
import fcntl
import json
import os
import uuid
import time
from contextlib import contextmanager

import numpy as np
from usearch.index import Index

from collector.config import EmbeddingProfile, active_profile, active_profile_path
from collector.search import Encoder
from collector.storage import Store
from collector.vector_index import IndexWriter, index_path


class IndexCapacityError(RuntimeError):
    pass


async def check_build_memory(store, profile):
    if profile.provider != 'cloudflare':
        return
    count = (await store.query(f'SELECT COUNT(*) AS n FROM {profile.table}'))[0]['n']
    required = count * (profile.dimension * 4 + 256) * 1.25 + 64 * 1024 ** 2
    memory = {}
    with open('/proc/meminfo') as source:
        for line in source:
            key, value, *_ = line.split()
            memory[key.rstrip(':')] = int(value) * 1024
    available = memory['MemAvailable'] + min(memory['SwapFree'], memory['MemTotal'] / 4)
    if required > available:
        raise IndexCapacityError(f'Index build needs approximately {required / 1024**2:.0f} MiB; '
                                 f'available budget {available / 1024**2:.0f} MiB. Job remains resumable.')


@contextmanager
def provider_lock(profile):
    path = index_path(profile).with_suffix('.writer.lock')
    with path.open('a+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('An embedding writer already owns this provider') from None
        yield


async def missing_rows(store, profile, upper=None, after=0, limit=64):
    bounds = ' AND m.archive_id<=%s' if upper is not None else ''
    params = (profile.model, after) + ((upper,) if upper is not None else ()) + (limit,)
    return await store.query(f'SELECT m.archive_id,m.text,m.text_hash FROM messages m '
        f'LEFT JOIN {profile.table} e ON e.archive_id=m.archive_id '
        "WHERE m.text<>'' AND (e.archive_id IS NULL OR e.model<>%s OR e.text_hash<>m.text_hash) "
        f'AND m.archive_id>%s{bounds} ORDER BY m.archive_id LIMIT %s', params)


async def save_vectors(store, profile, rows, vectors):
    if vectors.shape != (len(rows), profile.dimension) or not np.isfinite(vectors).all():
        raise ValueError('Embedding dimension or values do not match provider')
    for row, vector in zip(rows, vectors):
        # Recheck the text hash: a live edit during the API call must not become current.
        await store.execute(f'INSERT INTO {profile.table} (archive_id,model,text_hash,vector) '
            'SELECT * FROM (SELECT archive_id,%s AS model,text_hash,%s AS vector FROM messages '
            'WHERE archive_id=%s AND text_hash=%s) AS incoming '
            'ON DUPLICATE KEY UPDATE model=incoming.model,text_hash=incoming.text_hash,'
            'vector=incoming.vector,updated_at=UTC_TIMESTAMP(6)',
            (profile.model, vector.astype('<f4').tobytes(), row['archive_id'], row['text_hash']))


async def validate_and_activate(store, profile, upper, activate=True):
    if profile.provider == 'cloudflare':
        from collector.cloudflare import require_consent
        require_consent()
    if await missing_rows(store, profile, upper=upper, limit=1):
        raise RuntimeError('Cannot activate: current texts are not fully indexed')
    rows = await store.query(f'SELECT COUNT(*) AS n FROM {profile.table} e JOIN messages m '
        'ON m.archive_id=e.archive_id WHERE e.model=%s AND e.text_hash=m.text_hash', (profile.model,))
    path = index_path(profile)
    if not path.exists():
        raise RuntimeError('Cannot activate: index is missing')
    idx = await asyncio.to_thread(Index.restore, str(path), view=True)
    if not rows[0]['n'] or len(idx) != rows[0]['n'] or idx.ndim != profile.dimension:
        raise RuntimeError('Cannot activate: index size or dimension check failed')
    # Recheck after the index inspection, including edits committed during validation.
    if await missing_rows(store, profile, upper=upper, limit=1):
        raise RuntimeError('Cannot activate: archive changed during validation; resume reindex')
    manifest = dict(provider=profile.provider, model=profile.model, dimension=profile.dimension,
                    upper_archive_id=upper, indexed=len(idx))
    if not activate:
        return manifest
    target = active_profile_path()
    tmp = target.with_suffix('.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('w') as out:
        json.dump(manifest, out)
        out.flush()
        os.fsync(out.fileno())
    tmp.chmod(0o600)
    os.replace(tmp, target)
    directory = os.open(target.parent, os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return manifest


async def reindex(store, profile, activate=False, max_batches=None):
    if profile.provider == 'cloudflare':
        from collector.cloudflare import require_consent
        require_consent()
    upper = (await store.query('SELECT COALESCE(MAX(archive_id),0) AS n FROM messages'))[0]['n']
    await store.execute('INSERT INTO embedding_jobs (provider,upper_archive_id) VALUES (%s,%s) '
                        'ON DUPLICATE KEY UPDATE provider=embedding_jobs.provider',
                        (profile.provider, upper))
    job = (await store.query('SELECT * FROM embedding_jobs WHERE provider=%s', (profile.provider,)))[0]
    upper, cursor = job['upper_archive_id'], job['last_archive_id']
    encoder = Encoder(profile)
    batches = 0
    last_report = time.monotonic()
    while True:
        rows = await missing_rows(store, profile, upper=upper, after=cursor)
        if not rows:
            # Catch edits to IDs behind the saved cursor; never rely on cursor alone for completeness.
            rows = await missing_rows(store, profile, upper=upper)
            if not rows:
                break
        vectors = await encoder.encode([r['text'] for r in rows])
        await save_vectors(store, profile, rows, vectors)
        cursor = rows[-1]['archive_id']
        await store.execute('UPDATE embedding_jobs SET last_archive_id=%s,processed=processed+%s,'
            "status='running',updated_at=UTC_TIMESTAMP(6) WHERE provider=%s", (cursor, len(rows), profile.provider))
        batches += 1
        if time.monotonic() - last_report >= 60:
            print(json.dumps(dict(provider=profile.provider, status='running', last_archive_id=cursor,
                                  upper_archive_id=upper)), flush=True)
            last_report = time.monotonic()
        if max_batches is not None and batches >= max_batches:
            return dict(status='paused', last_archive_id=cursor)
    await check_build_memory(store, profile)
    await IndexWriter(profile).refresh(store, force=True)
    manifest = await validate_and_activate(store, profile, upper, activate=activate)
    await store.execute("UPDATE embedding_jobs SET status='verified',updated_at=UTC_TIMESTAMP(6) WHERE provider=%s",
                        (profile.provider,))
    result = dict(status='verified', provider=profile.provider, upper_archive_id=upper)
    result.update(manifest)
    return result


async def run(args):
    profile = EmbeddingProfile(args.provider)
    if profile == active_profile() and not args.activate_only:
        raise RuntimeError('Reindex only an inactive provider; the collector maintains the active one')
    with provider_lock(profile):
        store = await Store.open()
        try:
            if args.activate_only:
                # Rollback can include messages collected since the previous provider was active.
                rows = await store.query('SELECT COALESCE(MAX(archive_id),0) AS n FROM messages')
                await check_build_memory(store, profile)
                await IndexWriter(profile).refresh(store, force=True)
                result = await validate_and_activate(store, profile, rows[0]['n'])
            else:
                result = await reindex(store, profile, args.activate, args.max_batches)
            print(json.dumps(result))
        except Exception:
            await store.execute("UPDATE embedding_jobs SET status='paused_error',updated_at=UTC_TIMESTAMP(6) "
                                'WHERE provider=%s', (profile.provider,))
            raise
        finally:
            await store.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provider', choices=['local', 'cloudflare'], required=True)
    parser.add_argument('--activate', action='store_true')
    parser.add_argument('--activate-only', action='store_true')
    parser.add_argument('--max-batches', type=int)
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except Exception as exc:
        # Never echo HTTP requests, response bodies, SQL rows or credentials.
        from collector.cloudflare import CloudflareError
        detail = str(exc) if isinstance(exc, (CloudflareError, IndexCapacityError)) else type(exc).__name__
        raise SystemExit('Embedding job paused: ' + detail) from None


if __name__ == '__main__':
    main()
