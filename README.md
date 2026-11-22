<div align="center">

# ✦ TG Message Collector

Local Telegram archive · MySQL 8 · lexical and semantic search · read-only MCP

[![Issues](https://img.shields.io/github/issues/SkrudjReal/tg-message-collector?style=for-the-badge&color=f0abfc&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/issues)
[![Pull requests](https://img.shields.io/github/issues-pr/SkrudjReal/tg-message-collector?style=for-the-badge&color=a5b4fc&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/pulls)
[![Stars](https://img.shields.io/github/stars/SkrudjReal/tg-message-collector?style=for-the-badge&color=c4b5fd&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/stargazers)
[![Forks](https://img.shields.io/github/forks/SkrudjReal/tg-message-collector?style=for-the-badge&color=a5b4fc&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/forks)

[Getting started](#getting-started) · [Features](#features) · [MCP tools](#mcp-tools) · [Documentation](docs/operations.md) · [Report an issue](https://github.com/SkrudjReal/tg-message-collector/issues/new)

</div>

## Features

| Component | Implementation |
| :--- | :--- |
| Telegram | Kurigram / MTProto, a separate session, only new events from handlers, no history fetching |
| Archive | Groups and channels, users and bios, authors, replies, topics, forwards, media metadata |
| Analytics | [Statistics, activity, top words and phrases, reply partners, profile and edit history](docs/analytics.md) |
| Changes | Previous versions of edited text, deduplication; deletion in Telegram does not affect the archive |
| One-time import | Explicit channel links or supergroup IDs, a start date, checkpoints, and import-scope verification in MySQL |
| Search | Chat/author/date indexes, MySQL FULLTEXT, literal substrings, local multilingual embeddings |
| Semantics | USearch f32, local MiniLM or [Cloudflare BGE-M3](docs/cloudflare-embeddings.md); separate generations and verifiable switching |
| MCP | `http://127.0.0.1:8765/mcp`, Bearer token, a separate MySQL user with `SELECT` |

## MCP tools

The server provides **20 read-only tools**. They query the stored
MySQL archive without requesting data from Telegram. Default address:
`http://127.0.0.1:8765/mcp`, authentication: `Authorization: Bearer <MCP_TOKEN>`.

For message search, start with [`search`](docs/search.md): it combines lexical
and semantic search. Statistics and history remain available through separate tools.

| Tool | Purpose |
| :--- | :--- |
| `search` | Main search: FULLTEXT + semantics, deduplicated merging, filters, and explicit warnings |
| `search_messages` | Search by words or substring, chat, author, dates, and topic; pagination |
| `semantic_search` | Semantic search with filters and index coverage metrics |
| `list_users` | Current profiles: ID, username, name, and bio |
| `list_chats` | List and metadata of archived groups and channels |
| `get_message` | Message, surrounding context, reply, media file_id, and sources |
| `get_user_history` | Current profile and combined history of names, usernames, and bios |
| `get_message_history` | Current message text and saved edit versions |
| `get_message_metrics` | History of observed views, forwards, and reactions |
| `get_statistics` | Message, author, reply, edit, and media counts, and text length |
| `get_activity` | Hourly, daily, weekly, and monthly activity with timezone support |
| `get_top_words` | Top words: occurrences, message counts, and examples; optional Russian lemmatization |
| `get_top_phrases` | Top phrases of 2–5 consecutive words |
| `get_top_mentions` | Frequent literal @mentions in text |
| `get_top_authors` | Active authors and their share of messages in the sample |
| `get_top_posts` | Posts ranked by saved views, forwards, or reactions |
| `get_reply_partners` | Whom a user replies to most often and who replies to them |
| `compare_periods` | Compare metrics for two periods and, optionally, word frequencies |
| `get_archive_coverage` | Available periods, data sources, and missing reply targets |
| `archive_status` | Collector status, errors, archive size, indexing queue, and import |

[Analytics parameters, filters, and limitations →](docs/analytics.md) ·
[MCP connection and access setup →](docs/operations.md#mcp-только-для-чтения)

Statistics describe the available archive. When analyzing it, check the period, `truncated`,
and coverage; profile and metric snapshots reflect only observed states.

## Getting started

Requires Python 3.12–3.14, uv, and MySQL 8. Prepare the database and separate accounts
for writing and reading, apply the migrations in `sql/`, then fill in the local `.env`.
For initial authentication in an interactive terminal:

```bash
git clone https://github.com/SkrudjReal/tg-message-collector.git
cd tg-message-collector
uv sync --frozen
cp .env.example .env
# Fill in .env before authenticating; do not publish it.
uv run python3 app.py login-qr
uv run python3 app.py run
```

Scan the QR code in Telegram on your phone: Settings → Devices → Link Desktop Device.
If 2FA is enabled, enter the password only in the terminal. Standard code-based login: `app.py login`.
Only groups, supergroups/forums, and channels are collected; private chats and Saved Messages are excluded.
`CHAT_IDS` in `.env` restricts this list. The regular collector does not fetch old messages
or conversations from downtime. A separate one-time import command requires explicit
targets and a start date — [startup procedure](docs/operations.md#разовый-импорт-истории-каналов).
Media is not downloaded: MCP returns saved file IDs.
Previously recorded data remains in the archive; chat exports will be added separately by the user.

[MCP, indexes, limitations, tests, and installation on another host →](docs/operations.md)

Based on: [SkrudjReal/single-pyrouserbot-example](https://github.com/SkrudjReal/single-pyrouserbot-example),
commit `549dc7b`. Unused demo ping / Redis / scheduler components were removed;
Kurigram, asynchronous MySQL, and the `app.py` entry point were retained. The original remote is kept as `template`.
Sessions, credentials, MySQL data, and personal channel lists remain local and are excluded from Git.
