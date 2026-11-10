<div align="center">

# ✦ TG Message Collector

Локальный архив Telegram · MySQL 8 · поиск по словам и смыслу · MCP только для чтения

[![Issues](https://img.shields.io/github/issues/SkrudjReal/tg-message-collector?style=for-the-badge&color=f0abfc&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/issues)
[![Pull requests](https://img.shields.io/github/issues-pr/SkrudjReal/tg-message-collector?style=for-the-badge&color=a5b4fc&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/pulls)
[![Stars](https://img.shields.io/github/stars/SkrudjReal/tg-message-collector?style=for-the-badge&color=c4b5fd&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/stargazers)
[![Forks](https://img.shields.io/github/forks/SkrudjReal/tg-message-collector?style=for-the-badge&color=a5b4fc&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/forks)

[Запуск](#запуск) · [Возможности](#возможности) · [MCP-инструменты](#mcp-инструменты) · [Документация](docs/operations.md) · [Сообщить о проблеме](https://github.com/SkrudjReal/tg-message-collector/issues/new)

</div>

## Возможности

| Часть | Реализация |
| :--- | :--- |
| Telegram | Kurigram / MTProto, отдельная сессия, только новые события из хендлеров, без загрузки истории |
| Архив | Группы и каналы, пользователи и био, авторы, replies, темы, пересылки, метаданные медиа |
| Аналитика | [Статистика, активность, топы слов и сочетаний, собеседники по reply, история профилей и правок](docs/analytics.md) |
| Изменения | Предыдущий текст правок, защита от дублей; удаление в Telegram не влияет на архив |
| Разовый импорт | Явные ссылки на каналы или ID супергруппы, нижняя граница даты, контрольные точки и проверка области импорта в MySQL |
| Поиск | Индексы чата/автора/даты, MySQL FULLTEXT, буквальная подстрока, локальные multilingual embeddings |
| Семантика | HNSW по всему индексу; точный cosine по ограниченной выборке при фильтрах |
| MCP | `http://127.0.0.1:8765/mcp`, Bearer token, отдельный MySQL-пользователь с `SELECT` |

## MCP-инструменты

Сервер предоставляет **19 инструментов только для чтения**. Они работают с сохранённым
архивом MySQL и не запрашивают Telegram. Адрес по умолчанию:
`http://127.0.0.1:8765/mcp`, авторизация — `Authorization: Bearer <MCP_TOKEN>`.

| Инструмент | Назначение |
| :--- | :--- |
| `search_messages` | Поиск по словам или подстроке, чату, автору, датам и теме; пагинация |
| `semantic_search` | Поиск по смыслу с фильтрами и показателями покрытия индекса |
| `list_users` | Текущие профили: ID, username, имя и био |
| `list_chats` | Список и метаданные сохранённых групп и каналов |
| `get_message` | Сообщение, соседний контекст, reply, file_id медиа и источники |
| `get_user_history` | Текущий профиль и общая история имени, username и био |
| `get_message_history` | Текущий текст сообщения и сохранённые версии правок |
| `get_message_metrics` | История наблюдённых просмотров, пересылок и реакций |
| `get_statistics` | Число сообщений, авторов, replies, правок, медиа и длина текста |
| `get_activity` | Активность по часам, дням, неделям и месяцам с часовым поясом |
| `get_top_words` | Топ слов: употребления, число сообщений и примеры; русские леммы опциональны |
| `get_top_phrases` | Топ словосочетаний из 2–5 последовательных слов |
| `get_top_mentions` | Частые буквальные @упоминания в тексте |
| `get_top_authors` | Активные авторы и их доля сообщений в выборке |
| `get_top_posts` | Публикации по сохранённым просмотрам, пересылкам или реакциям |
| `get_reply_partners` | Кому пользователь чаще отвечает и кто отвечает ему |
| `compare_periods` | Сравнение показателей двух периодов и, опционально, частоты слов |
| `get_archive_coverage` | Доступные периоды, источники данных и отсутствующие reply targets |
| `archive_status` | Состояние сборщика, ошибки, объём архива, очередь индексации и импорт |

[Параметры аналитики, фильтры и ограничения →](docs/analytics.md) ·
[Подключение MCP и настройка доступа →](docs/operations.md#mcp-только-для-чтения)

Статистика относится к доступному архиву. При анализе проверяй период, `truncated`
и покрытие; снимки профиля и метрик отражают только наблюдённые состояния.

## Запуск

Нужны Python 3.12–3.14, uv и MySQL 8. Подготовь базу и отдельные учётные записи
для записи и чтения, примени миграции из `sql/`, затем заполни локальный `.env`.
Для первой авторизации в интерактивном терминале:

```bash
git clone https://github.com/SkrudjReal/tg-message-collector.git
cd tg-message-collector
uv sync --frozen
cp .env.example .env
# Заполни .env перед авторизацией; не публикуй его.
uv run python3 app.py login-qr
uv run python3 app.py run
```

QR сканируется в Telegram на телефоне: Настройки → Устройства → Подключить устройство.
Если включена 2FA, пароль вводится только в терминале. Обычный вход по коду: `app.py login`.
Собираются только группы, супергруппы/форумы и каналы; личные чаты и Saved Messages исключены.
`CHAT_IDS` в `.env` ограничивает их список. Старые сообщения и переписка за время простоя
не запрашиваются обычным сборщиком. Отдельная команда разового импорта требует явных
целей и даты начала — [порядок запуска](docs/operations.md#разовый-импорт-истории-каналов).
Медиа не скачиваются: MCP возвращает сохранённые file IDs.
Ранее записанные данные остаются в архиве; экспорты чатов будут добавлены пользователем отдельно.

[MCP, индексы, ограничения, тесты и установка на другом хосте →](docs/operations.md)

Основа: [SkrudjReal/single-pyrouserbot-example](https://github.com/SkrudjReal/single-pyrouserbot-example),
commit `549dc7b`. Удалены неиспользуемые demo ping / Redis / scheduler;
сохранены Kurigram, асинхронный MySQL и точка входа `app.py`. Исходный remote сохранён как `template`.
Сессии, credentials, данные MySQL и личные списки каналов остаются локальными и исключены из Git.
