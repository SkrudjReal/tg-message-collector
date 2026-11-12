# Embedding provider comparison

Measured locally on 2026-11-11 in WSL, in separate fresh Python processes.
Both providers used the same 20 invented RU/EN documents, 10 manually labeled
queries and USearch **f32**. No archived Telegram text was sent to Cloudflare.
The reproducible fixture and measurement code are in
[`tools/benchmark_embeddings.py`](../tools/benchmark_embeddings.py).

| Metric | Local MiniLM | Cloudflare BGE-M3 |
| :--- | ---: | ---: |
| Dimensions | 384 | 1024 |
| Corpus embedding time, cold start included | 3.397 s | 0.490 s |
| Median query embedding + search | 12.2 ms | 136.0 ms |
| p95 query embedding + search | 14.8 ms | 164.8 ms |
| Recall@5 (two relevant documents per query) | 0.90 | 1.00 |
| MRR@5 | 0.95 | 1.00 |
| Process RSS after workload | 679.64 MiB | 78.50 MiB |
| Process PSS after workload | 649.87 MiB | 59.71 MiB |
| Process swap | 0 MiB | 0 MiB |

The Cloudflare measurement used `CF_REQUESTS_PER_MINUTE=3000` so the client-side
one-request-per-second production default did not dominate network latency. Calls
were sequential; this does not measure 3000 RPM throughput. The ordinary default
remains 60 RPM. Timing is a single run with 10 queries, not a latency guarantee.
All API requests count toward the local estimated-neuron ledger, including retries.
The benchmark did not collect account billing. Subsequent API inspection found
`result.meta.neurons`; the runtime now settles successful request reservations
using this reported consumption. The dashboard remains authoritative for total
account usage, including other clients.

**This is a small synthetic relevance check, not validation on real chat searches.**
It omits the full production index, concurrent collector load and data-dependent
sequence-length peaks. A 1024-dimensional f32 vector uses 4096 bytes versus 1536
for MiniLM: 2.67 times the raw payload. Per million vectors, raw storage is about
3.81 versus 1.43 GiB before HNSW graph overhead. Thus the roughly 590 MiB PSS
reduction here must not be extrapolated to the full server collector.

## Validation and deployment state

- Six mocked API tests passed: consent before HTTP, batching/retry, response
  validation, persistent daily budget, shared cooldown and separate profiles.
- Twenty-one isolated MySQL/HTTP integration tests passed, including FULLTEXT,
  semantic filters/staleness/truncation, resumable generation, edits behind the
  cursor, candidate validation, atomic switch and local rollback.
- Eight chat-scope tests and fifteen explicit-import tests passed.
- Local encoder parity test passed; Ruff and whitespace checks passed.
- Live Cloudflare API returned 1024-dimensional vectors for synthetic fixtures.
- Server schema additions applied; collector and MCP updated. Production remains
  on **local + USearch f32**, with `CF_ALLOW_REAL_MESSAGES=false`.
- Live authenticated MCP `archive_status` reported local/384, a running collector,
  and no cloud embedding jobs. New MySQL writes were verified after restart.
- A server reindex attempt with consent disabled was blocked before encoding;
  `cloudflare_embeddings` remained empty. No recent embedding-worker errors found.

A real archive reindex and production Cloudflare memory/load comparison have not
been run, because external archive transfer is disabled. No production cutover
is claimed. Enable consent deliberately and follow the
[reindex/cutover/rollback procedure](cloudflare-embeddings.md) when ready.

## Subsequent owner-authorized migration

After the initial synthetic-only validation, the owner requested continuation.
Real-text transfer was then explicitly enabled on the server for a resumable PM2
migration trial. The trial was subsequently stopped at the owner's request and
local indexing restored, with external text transfer disabled again. Existing
local vectors remain intact. The preceding bullets describe historical checks;
consult `archive_status` and PM2 for current counters rather than this report.

## Local index rebuild memory fix — 2026-11-12

The server is back on local MiniLM with USearch **f32**. An unlimited SQL response
could accumulate inside asyncmy's receive buffer while HNSW insertion ran in a
thread, despite using an unbuffered cursor. IndexWriter now issues separate,
keyset-paginated queries of at most 1024 vectors, bounding the response itself.
The existing model, index precision, archive tables and atomic publication remain.

Before the fix, PM2 repeatedly restarted the collector above its 2200 MiB limit
(one observed RSS was 2233 MiB). After a complete rebuild of 519,253 vectors,
loading MiniLM and resuming incremental indexing, observed RSS was 1665–1689 MiB
and PSS 1656–1680 MiB. The published index then contained 519,381 vectors with
384 dimensions and f32 precision. New messages, embedding timestamps and the
collector heartbeat advanced; the PID remained unchanged throughout the rebuild.
These are live operational observations, not a controlled long-term load test.

Three focused paging/failure/publication tests passed, as did isolated MySQL
integration tests for semantic search and provider cutover/rollback. Ruff and
whitespace checks passed. No external embedding API calls were needed.
