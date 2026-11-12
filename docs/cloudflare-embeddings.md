# Cloudflare embeddings

`local` uses MiniLM (384 dimensions); `cloudflare` uses Workers AI
`@cf/baai/bge-m3` (1024 dimensions). Both use **USearch f32**. MySQL FULLTEXT,
message storage and ordinary statistics are unchanged. Real text transfer is
**disabled by default**, including semantic query text. There is no automatic
fallback that could mix vectors from different models.

The current deployment uses **local MiniLM**. The earlier Cloudflare migration
trial is stopped; installing this code does not enable external text transfer.

## Setup and consent

Apply `sql/005_cloudflare_embeddings.sql` to the archive database. It only adds
`cloudflare_embeddings` and `embedding_jobs`; it does not modify existing
`message_embeddings` or messages. The existing database reader needs SELECT on
these new tables. Restart deployed processes after installing this version.

Put `CF_ACCOUNT_ID` and `CF_API_TOKEN` in the private `.env`. The token needs Workers AI
inference permission. Leave `EMBEDDING_PROVIDER=local`; an environment value alone
cannot activate an unverified Cloudflare generation. `CF_ALLOW_REAL_MESSAGES=false`
blocks both reindexing real messages and encoding real semantic queries before HTTP.
Only the hardcoded synthetic benchmark bypasses this gate.

When the owner decides to allow the archive text to leave this host, explicitly set:

```dotenv
CF_ALLOW_REAL_MESSAGES=true
CF_BATCH_SIZE=16
CF_REQUESTS_PER_MINUTE=60
CF_DAILY_NEURON_BUDGET=9000
```

Restart the collector/MCP so both see the changed consent setting. No production
consent was enabled by this integration's tests.

## Background reindex and activation

Use the project working directory and environment. Run as a separate process:

```bash
uv run python -m collector.embedding_jobs --provider cloudflare --activate
```

For PM2, start this Python module with the same working directory and `.env`.
Use no autorestart after successful completion, or run it manually again to resume.
No Telegram session is opened by the embedding job. Collection stays online.
An optional `--max-batches 10` stops after ten batches without activating.

The job freezes an upper archive ID, saves its cursor and processed count in
`embedding_jobs`, and reconciles edits behind that cursor before completion.
Vectors are committed before progress, so an interruption can repeat work but
cannot skip uncommitted rows. Input text hashes are checked again before saving.
Progress is printed every minute, without text or credentials. Errors pause the
job; rerun the same command after fixing the cause or quota reset.

The candidate index is separate from MiniLM and published by atomic rename.
Activation verifies coverage through the frozen upper ID, dimensions, finite
vector values, index count, and current text hashes, then atomically replaces a
small per-database active-provider manifest. Each search request pins one generation;
the collector notices a switch on its next embedding cycle. Newer messages after
the frozen upper ID are processed by the active collector afterward. The live
archive can change immediately after verification; MySQL hash revalidation still
excludes stale results. This is not a lock over all Telegram writes.

`archive_status` reports `embedding_provider`, `dimension`, and `embedding_jobs`.
Existing local snapshots remain for rollback. Model/index objects held by in-flight
requests finish normally; for predictable immediate memory release restart processes
after activation. Never delete the local generation as part of cutover.

## Rollback

```bash
uv run python -m collector.embedding_jobs --provider local --activate
```

This resumes/rebuilds missing local vectors and catches edits, validates them, and
switches back atomically. If the local generation is already complete, use
`--provider local --activate-only` to validate and switch without model inference.
The active collector owns a per-provider writer lock; do not run a second writer
for its active provider. Neither rollback nor reindex deletes messages or the other
provider's vectors. Separate files include database and model identity.

## Limits and consumption

Each HTTP batch holds up to 16 inputs by default (configurable 1–64). Inputs must
be nonempty and at most 24,000 UTF-8 bytes; oversized input pauses the job, with no
silent truncation or activation. HTTP timeouts and 429/5xx receive bounded retries,
exponential backoff/jitter and Retry-After handling. Long cooldowns are persisted
and the job pauses. Auth/validation errors are not retried; response bodies, text,
and tokens are never included in errors. HTTP redirects are disabled.

A private, locked ledger under `data/cf-usage-*.json` reserves estimated neurons
**before each attempted request**, including retries. It uses UTF-8 bytes plus
per-text overhead as a conservative token proxy and the configurable published
rate of 1075 neurons per million tokens. Estimates round up per attempt; uncertain or failed
attempt reservations are not refunded. Successful responses with
`result.meta.neurons` replace that attempt's estimate with the reported consumption,
idempotently; `actual_neurons` tracks the reported sum. No account token is stored in the ledger.
The estimate enforces a local daily cap, resets at midnight UTC, and survives
process restarts. It is **not actual billing or an account-wide quota**: other hosts,
applications, pricing changes and tokenizer differences are outside this ledger.
Check actual usage in the Cloudflare dashboard. Do not enable paid overages as part
of this setup. The default 9000 is deliberately below the documented 10,000 free
neurons/day; it is not a guarantee that an account has no other usage.

## Reproducible synthetic benchmark

```bash
uv run python tools/benchmark_embeddings.py --provider local
CF_REQUESTS_PER_MINUTE=3000 uv run python tools/benchmark_embeddings.py --provider cloudflare
```

Each command runs in a fresh process with the same 20 invented RU/EN documents,
10 labeled queries and USearch f32. It measures recall@5/MRR@5, inference+search
latency, corpus embedding time and process RSS/PSS. Cloudflare calls are real;
no MySQL messages are read. This small fixture does not establish production
relevance, full-index RAM, concurrent-load performance or a throughput SLA.
At equal message count, 1024-dimensional f32 vectors require 2.67 times the raw
vector memory of 384 dimensions. Offloading MiniLM does not guarantee lower total
collector RAM with a large resident HNSW index.

Sources: [model/API](https://developers.cloudflare.com/workers-ai/models/bge-m3/),
[pricing](https://developers.cloudflare.com/workers-ai/platform/pricing/),
[rate limits](https://developers.cloudflare.com/workers-ai/platform/limits/).

[Measured synthetic comparison and deployment checks →](embedding-benchmark.md)

## Low-memory server migration

`EMBEDDING_WORKER_ENABLED=false` disables only the collector's embedding/index
worker. Telegram handlers and MySQL writes remain active; existing local search
continues against the previous index, and fresh messages remain available through
FULLTEXT. They join semantic search after the cloud generation catches up.
Before building the full cloud index, the job checks estimated f32 index memory
against available RAM plus a bounded allowance of free swap. Insufficient headroom
pauses the resumable job rather than activating a partial generation.

The earlier server trial used a private PM2 `tg-cloudflare-migration` operator
script with hourly retries and **without `--activate`**. That trial is stopped.
For a future candidate comparison, run
`tools/measure_full_index.py` in separate writer and reader processes, loading the
complete index and visiting every vector. The trial saved RSS/PSS, swap, sampled
peak, index size and search timings to private server files:

- `data/deployment/cf-full-index-writer.json`
- `data/deployment/cf-full-index-reader.json`

Keep the production provider local during measurement. Do not automatically
activate a candidate or restart workers. The original MiniLM snapshot was copied under
`data/deployment/minilm-rollback/` and its SHA-256 was verified identical.

Cloudflare documents 10,000 free neurons/day; the local cap remains 9000. Account-wide
Analytics was queried but the token lacks permission (`not authorized for that
account`). Therefore API-reported request costs and the local ledger are verified,
but other applications' account usage is not. Initial extrapolation from indexed
text bytes suggested roughly 110,000 neurons for the archive: about 13 daily
9000-neuron budgets. This is a rough, changing estimate, not a billing guarantee.

[Measured synthetic comparison and deployment checks →](embedding-benchmark.md)
