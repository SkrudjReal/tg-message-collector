<div align="center">

# ✦ TG Message Collector

Локальный архив Telegram · MySQL 8 · поиск по словам и смыслу · MCP только для чтения

[![Issues](https://img.shields.io/github/issues/SkrudjReal/tg-message-collector?style=for-the-badge&color=f0abfc&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/issues)
[![Pull requests](https://img.shields.io/github/issues-pr/SkrudjReal/tg-message-collector?style=for-the-badge&color=a5b4fc&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/pulls)
[![Stars](https://img.shields.io/github/stars/SkrudjReal/tg-message-collector?style=for-the-badge&color=c4b5fd&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/stargazers)
[![Forks](https://img.shields.io/github/forks/SkrudjReal/tg-message-collector?style=for-the-badge&color=a5b4fc&labelColor=19152e)](https://github.com/SkrudjReal/tg-message-collector/forks)

[Запуск](#запуск) · [Возможности](#возможности) · [Документация](docs/operations.md) · [Сообщить о проблеме](https://github.com/SkrudjReal/tg-message-collector/issues/new)

</div>

## Возможности

| Часть | Реализация |
| :--- | :--- |
| Telegram | Kurigram / MTProto, отдельная сессия, только новые события из хендлеров, без загрузки истории |
| Архив | Группы и каналы, пользователи и био, авторы, replies, темы, пересылки, метаданные медиа |
| Аналитика | История имён/username/био, снимки просмотров/реакций/пересылок, ошибки и очередь индексации |
| Изменения | Предыдущий текст правок, защита от дублей; удаление в Telegram не влияет на архив |
| Разовый импорт | Явные ссылки на каналы или ID супергруппы, нижняя граница даты, контрольные точки и проверка области импорта в MySQL |
| Поиск | Индексы чата/автора/даты, MySQL FULLTEXT, буквальная подстрока, локальные multilingual embeddings |
| Семантика | HNSW по всему индексу; точный cosine по ограниченной выборке при фильтрах |
| MCP | `http://127.0.0.1:8765/mcp`, Bearer token, отдельный MySQL-пользователь с `SELECT` |

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
