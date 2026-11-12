# Unified message search

Use the read-only MCP tool `search` as the default for finding messages. It combines
MySQL FULLTEXT (natural-language relevance) with the active semantic provider,
merges up to 100 candidates from each source using reciprocal rank fusion
(`1 / (60 + rank)`), and deduplicates by `archive_id`. Embeddings remain f32;
the existing archive and index format are unchanged.

Parameters: `query` (1–2000 nonblank characters), `limit` (1–100), optional
`chat_id`, `sender_user_id`, `sender_chat_id`, UTC `since`/`until`, and `topic_id`.
`candidate_limit` bounds the semantic scan (default 50,000, maximum 200,000).

Results include `matched_by`, `hybrid_score`, optional cosine `similarity`,
source candidate counts, semantic coverage metadata, `truncated`, and `warnings`.
Scores describe ranking, not probabilities. Check these fields before claiming
exhaustive coverage: semantic filtering scans a bounded recent sample, while
FULLTEXT excludes stopwords and tokens below the MySQL minimum token length.

If semantic retrieval fails or exceeds 60 seconds, the tool returns keyword
results with an explicit warning containing only the exception type. SQL errors
still fail the request. Local encoding sends no text outside the server;
Cloudflare requires the existing explicit external-transfer setting.

`search_messages` remains available for Boolean FULLTEXT, literal substring,
chronological browsing and pagination. `semantic_search` remains available for
semantic-only queries. Existing clients and analytics tools keep working.
