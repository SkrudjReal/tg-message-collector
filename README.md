<div align="center">

# ✦ TG Message Collector

Локальный архив Telegram · MySQL 8 · поиск по словам и смыслу · MCP только для чтения

[Запуск](#запуск) · [Возможности](#возможности) · [Документация](docs/operations.md)

</div>

## Возможности

| Часть | Реализация |
| :--- | :--- |
| Telegram | Kurigram / MTProto, отдельная сессия, новые сообщения и постепенная загрузка истории |
| Архив | Пользователи и био, чаты, авторы, адресаты личных сообщений, replies, темы, пересылки, метаданные медиа |
| Изменения | Предыдущий текст правок, отметки наблюдавшихся удалений, защита от дублей |
| Поиск | Индексы чата/автора/даты, MySQL FULLTEXT, буквальная подстрока, локальные multilingual embeddings |
| Семантика | HNSW по всему индексу; точный cosine по ограниченной выборке при фильтрах |
| MCP | `http://127.0.0.1:8765/mcp`, Bearer token, отдельный MySQL-пользователь с `SELECT` |

## Запуск

На этой машине база, `.env`, зависимости, модель и пользовательские systemd units уже подготовлены.
Для первой авторизации в интерактивном терминале:

```bash
cd /home/velunae/projects/mainstream/tg-message-collector
.venv/bin/python app.py login-qr
systemctl --user start tg-message-collector
systemctl --user status tg-message-collector tg-message-collector-mcp
```

QR сканируется в Telegram на телефоне: Настройки → Устройства → Подключить устройство.
Если включена 2FA, пароль вводится только в терминале. Обычный вход по коду: `app.py login`.
По умолчанию собираются все доступные диалоги;
`CHAT_IDS` в `.env` ограничивает их список. За цикл загружается до 1000 старых сообщений на чат,
каждые пять минут процесс продолжает загрузку глубже. Новые сообщения принимаются одновременно.

[MCP, индексы, ограничения, тесты и установка на другом хосте →](docs/operations.md)

Основа: [SkrudjReal/single-pyrouserbot-example](https://github.com/SkrudjReal/single-pyrouserbot-example),
commit `549dc7b`. Удалены неиспользуемые demo ping / Redis / scheduler;
сохранены Kurigram, асинхронный MySQL и точка входа `app.py`. Исходный remote сохранён как `template`.
Новый GitHub-репозиторий пока не опубликован.
