"""Measure a complete USearch index in a fresh process, with no embedding API calls."""
import argparse
import gc
import json
from pathlib import Path
import resource
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
from usearch.index import Index  # noqa: E402
from collector.config import EmbeddingProfile  # noqa: E402
from collector.vector_index import index_path  # noqa: E402


def memory():
    return {line.split(':')[0] + '_MiB': round(int(line.split()[1]) / 1024, 2)
            for line in Path('/proc/self/smaps_rollup').read_text().splitlines()
            if line.startswith(('Rss:', 'Pss:', 'Swap:'))}


def measure(provider, mode):
    profile = EmbeddingProfile(provider)
    path = index_path(profile)
    metadata = Index.restore(str(path), view=True)
    count = len(metadata)
    if metadata.ndim != profile.dimension or not count:
        raise ValueError('Index dimension or size is invalid')
    del metadata
    gc.collect()
    available = {line.split()[0].rstrip(':'): int(line.split()[1]) * 1024
                 for line in Path('/proc/meminfo').read_text().splitlines()}
    required = count * (profile.dimension * 4 + 256) * 1.25 + 64 * 1024**2
    if required > available['MemAvailable'] + min(available['SwapFree'], available['MemTotal'] / 4):
        raise RuntimeError('Insufficient memory for full index probe; production search was not switched')
    before = memory()
    started = time.perf_counter()
    index = Index.restore(str(path), view=mode == 'reader')
    load_seconds = time.perf_counter() - started
    keys = np.asarray(index.keys)
    peak = memory()
    # Traverse EVERY vector; measuring a cold mmap header alone understates resident memory.
    for offset in range(0, count, 256):
        vectors = index.get(keys[offset:offset + 256])
        if not np.isfinite(vectors).all():
            raise ValueError('Nonfinite vectors in candidate index')
        del vectors
        if offset % 4096 == 0:
            current = memory()
            peak = {k: max(peak[k], current[k]) for k in peak}
    timings = []
    for key in keys[::max(1, count // 20)][:20]:
        query = index.get(int(key))
        began = time.perf_counter()
        result = index.search(query, count=min(20, count), threads=2)
        if not len(result.keys):
            raise ValueError('Candidate returned no neighbors')
        timings.append((time.perf_counter() - began) * 1000)
    after = memory()
    return dict(provider=provider, mode=mode, dimension=profile.dimension, vectors=count,
                index_bytes=path.stat().st_size, all_vectors_visited=True,
                baseline=before, after=after, sampled_peak=peak,
                peak_rss_MiB=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
                load_seconds=load_seconds, median_search_ms=float(np.median(timings)),
                production_switched=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provider', choices=['local', 'cloudflare'], required=True)
    parser.add_argument('--mode', choices=['writer', 'reader'], required=True)
    args = parser.parse_args()
    print(json.dumps(measure(args.provider, args.mode), indent=2))
