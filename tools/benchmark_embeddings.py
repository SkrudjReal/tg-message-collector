"""Synthetic-only, isolated provider benchmark; never reads the Telegram archive."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
from usearch.index import Index  # noqa: E402
from collector.config import EmbeddingProfile  # noqa: E402
from collector.search import Encoder  # noqa: E402

# Explicit relevance labels; all texts are invented, not extracted from messages.
CASES = [
 ('не хватает оперативки', 'Приложение исчерпало доступную оперативную память.', 'Процессу нужно больше RAM.'),
 ('велосипед больше не едет', 'У велосипеда сломалась цепь, нужен ремонт.', 'The bicycle has a broken wheel.'),
 ('как подготовиться к собесу', 'Перед техническим интервью решайте задачи и повторяйте алгоритмы.', 'Practice coding before the job interview.'),
 ('потерял доступ к аккаунту', 'Восстановить вход можно через резервные коды.', 'Use account recovery if you cannot sign in.'),
 ('модель выдумывает факты', 'Нейросеть галлюцинирует и придумывает источники.', 'The language model invents unsupported information.'),
 ('где лежат мои документы', 'Сохранённые файлы находятся в папке Downloads.', 'Your documents are stored in the home directory.'),
 ('сервер отвечает слишком долго', 'Высокая задержка запросов вызвана перегрузкой сервера.', 'The backend is slow under heavy traffic.'),
 ('перевод денег не дошёл', 'Банковский платёж завис в обработке.', 'The recipient has not received the bank transfer.'),
 ('в телефоне села батарея', 'Смартфон разрядился, подключите зарядное устройство.', 'The phone battery is empty and needs charging.'),
 ('код падает при запуске', 'Программа завершается с исключением во время старта.', 'The application crashes on startup.'),
]


def memory():
    return {line.split(':')[0] + '_MiB': round(int(line.split()[1]) / 1024, 2)
            for line in Path('/proc/self/smaps_rollup').read_text().splitlines()
            if line.startswith(('Rss:', 'Pss:', 'Swap:'))}


async def main(provider):
    encoder = Encoder(EmbeddingProfile(provider))
    docs = [text for case in CASES for text in case[1:]]
    queries = [case[0] for case in CASES]
    baseline = memory()
    started = time.perf_counter()
    vectors = await encoder.encode(docs, synthetic=True)
    build_seconds = time.perf_counter() - started
    index = Index(ndim=vectors.shape[1], metric='cos', dtype='f32', connectivity=16, expansion_search=128)
    index.add(np.arange(len(vectors), dtype=np.uint64), vectors, threads=2)
    latency = []
    reciprocal = []
    recall = []
    for number, query in enumerate(queries):
        started = time.perf_counter()
        vector = (await encoder.encode([query], synthetic=True))[0]
        result = index.search(vector, count=5, threads=2).keys.tolist()
        latency.append(time.perf_counter() - started)
        relevant = {number * 2, number * 2 + 1}
        reciprocal.append(next((1 / (i + 1) for i, k in enumerate(result) if k in relevant), 0))
        recall.append(len(set(result) & relevant) / 2)
    print(json.dumps(dict(provider=provider, model=encoder.profile.model, dimensions=vectors.shape[1],
        documents=len(docs), queries=len(queries), corpus_encode_seconds=build_seconds,
        median_query_seconds=statistics.median(latency), p95_query_seconds=float(np.percentile(latency,95)),
        mrr_at_5=statistics.mean(reciprocal), recall_at_5=statistics.mean(recall),
        baseline=baseline, after=memory(), synthetic_only=True, index_dtype='f32'), indent=2))


if __name__ == '__main__':
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provider', choices=['local', 'cloudflare'], required=True)
    args = parser.parse_args()
    asyncio.run(main(args.provider))
