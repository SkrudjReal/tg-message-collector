"""Workers AI client. No response bodies, inputs or authorization values are logged."""
import asyncio
import fcntl
import hashlib
import json
import math
import os
import random
import re
import time
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx
import numpy as np

from collector.config import DATA, CF_MODEL


class CloudflareError(RuntimeError):
    pass


class CloudflareLimitError(CloudflareError):
    pass


def require_consent():
    if os.getenv('CF_ALLOW_REAL_MESSAGES', 'false').lower() != 'true':
        raise CloudflareError('External text transfer is disabled (CF_ALLOW_REAL_MESSAGES)')


class UsageLedger:
    """Shared process-safe estimate, not an account-wide Cloudflare billing meter."""
    def __init__(self):
        account = os.getenv('CF_ACCOUNT_ID', '')
        self.path = DATA / ('cf-usage-' + hashlib.sha256(account.encode()).hexdigest()[:16] + '.json')
        self.lock_path = self.path.with_suffix('.lock')

    def reserve(self, texts):
        # UTF-8 byte count plus per-input overhead is a conservative planning proxy, not a tokenizer.
        tokens = sum(len(t.encode('utf-8')) + 32 for t in texts)
        neurons = math.ceil(tokens * float(os.getenv('CF_NEURONS_PER_MILLION_TOKENS', '1075')) / 1e6)
        day = datetime.now(timezone.utc).date().isoformat()
        with self.lock_path.open('a+') as lock:
            os.chmod(self.lock_path, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX)
            state = json.loads(self.path.read_text()) if self.path.exists() else {}
            if state.get('day') != day:
                state = dict(day=day, reserved_neurons=0, actual_neurons=0, attempts=0,
                             next_request_at=0, pending={})
            limit = float(os.getenv('CF_DAILY_NEURON_BUDGET', '9000'))
            if limit <= 0 or state['reserved_neurons'] + neurons > limit:
                raise CloudflareLimitError('Local daily neuron budget exhausted')
            rpm = float(os.getenv('CF_REQUESTS_PER_MINUTE', '60'))
            if not 0 < rpm <= 3000:
                raise ValueError('CF_REQUESTS_PER_MINUTE must be 0..3000')
            now = time.time()
            slot = max(now, state.get('next_request_at', 0))
            if slot - now > 60:
                raise CloudflareLimitError('Workers AI cooldown is active; retry job later')
            state.update(reserved_neurons=state['reserved_neurons'] + neurons,
                         attempts=state['attempts'] + 1, next_request_at=slot + 60 / rpm)
            reservation = uuid.uuid4().hex
            state.setdefault('pending', {})[reservation] = neurons
            tmp = self.path.with_suffix('.tmp')
            with tmp.open('w') as out:
                json.dump(state, out)
                out.flush()
                os.fsync(out.fileno())
            tmp.chmod(0o600)
            os.replace(tmp, self.path)
            return max(0, slot - now), reservation

    def settle(self, reservation, actual):
        if not isinstance(actual, (int, float)) or not math.isfinite(actual) or actual < 0:
            return
        with self.lock_path.open('a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            state = json.loads(self.path.read_text())
            estimated = state.setdefault('pending', {}).pop(reservation, None)
            if estimated is None:
                return
            state['reserved_neurons'] = max(0, state['reserved_neurons'] - estimated + actual)
            state['actual_neurons'] = state.get('actual_neurons', 0) + actual
            tmp = self.path.with_suffix('.tmp')
            with tmp.open('w') as out:
                json.dump(state, out)
                out.flush()
                os.fsync(out.fileno())
            tmp.chmod(0o600)
            os.replace(tmp, self.path)

    def cooldown(self, seconds):
        with self.lock_path.open('a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            state = json.loads(self.path.read_text()) if self.path.exists() else {}
            state['next_request_at'] = max(state.get('next_request_at', 0), time.time() + seconds)
            tmp = self.path.with_suffix('.tmp')
            tmp.write_text(json.dumps(state))
            tmp.chmod(0o600)
            os.replace(tmp, self.path)


def retry_delay(value, attempt):
    if value:
        try:
            delay = float(value)
        except ValueError:
            try:
                delay = parsedate_to_datetime(value).timestamp() - time.time()
            except (ValueError, TypeError, OverflowError):
                delay = 0
        return max(0, delay)
    return min(30, 2 ** attempt) + random.random()


class CloudflareEncoder:
    def __init__(self, transport=None):
        self.transport = transport
        self.ledger = UsageLedger()

    async def encode(self, texts, *, synthetic=False):
        if not synthetic:
            require_consent()
        account = os.getenv('CF_ACCOUNT_ID', '')
        token = os.getenv('CF_API_TOKEN', '')
        if not re.fullmatch('[a-fA-F0-9]{32}', account) or not token:
            raise CloudflareError('CF_ACCOUNT_ID / CF_API_TOKEN are missing or invalid')
        batch_size = int(os.getenv('CF_BATCH_SIZE', '16'))
        if not 1 <= batch_size <= 64:
            raise ValueError('CF_BATCH_SIZE must be 1..64')
        if any(not isinstance(t, str) or not t.strip() or len(t.encode('utf-8')) > 24000 for t in texts):
            raise CloudflareError('Cloudflare inputs must be nonempty and at most 24000 UTF-8 bytes')
        vectors = []
        url = f'https://api.cloudflare.com/client/v4/accounts/{account}/ai/run/{CF_MODEL}'
        async with httpx.AsyncClient(timeout=60, transport=self.transport, follow_redirects=False) as client:
            for offset in range(0, len(texts), batch_size):
                batch = texts[offset:offset + batch_size]
                for attempt in range(5):
                    wait, reservation = await asyncio.to_thread(self.ledger.reserve, batch)
                    await asyncio.sleep(wait)
                    try:
                        response = await client.post(url, headers={'Authorization': 'Bearer ' + token},
                                                     json={'text': batch})
                    except httpx.TransportError:
                        if attempt == 4:
                            raise CloudflareError('Workers AI transport failed after retries') from None
                        await asyncio.sleep(retry_delay(None, attempt))
                        continue
                    if response.status_code == 429 or response.status_code >= 500:
                        if attempt == 4:
                            if response.status_code == 429:
                                raise CloudflareLimitError('Workers AI rate limit retries exhausted')
                            raise CloudflareError('Workers AI rate limit or availability retries exhausted')
                        delay = retry_delay(response.headers.get('Retry-After'), attempt)
                        await asyncio.to_thread(self.ledger.cooldown, delay)
                        if delay > 60:
                            raise CloudflareLimitError('Workers AI requested a long cooldown; retry job later')
                        await asyncio.sleep(delay)
                        continue
                    if not response.is_success:
                        raise CloudflareError(f'Workers AI rejected request (HTTP {response.status_code})')
                    try:
                        body = response.json()
                        if body.get('success') is not True:
                            raise ValueError()
                        meta = body['result'].get('meta') or {}
                        await asyncio.to_thread(self.ledger.settle, reservation, meta.get('neurons'))
                        array = np.asarray(body['result']['data'], dtype='<f4')
                        norms = np.linalg.norm(array, axis=1, keepdims=True)
                        if (array.shape != (len(batch), 1024) or not np.isfinite(array).all()
                                or not np.isfinite(norms).all() or np.any(norms < 1e-12)):
                            raise ValueError()
                    except (ValueError, KeyError, TypeError, np.exceptions.AxisError):
                        raise CloudflareError('Workers AI returned invalid embedding data') from None
                    vectors.extend(array / norms)
                    break
        return np.asarray(vectors, dtype='<f4').reshape((-1, 1024))
