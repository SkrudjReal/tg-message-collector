"""Offline systemd condition; never prompts or starts Telegram authorization."""
import sqlite3
import sys

from collector.config import DATA


def ready():
    path = DATA / 'collector.session'
    if not path.is_file():
        return False
    try:
        with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as conn:
            return bool(conn.execute('SELECT user_id FROM sessions LIMIT 1').fetchone()[0])
    except (sqlite3.Error, TypeError, IndexError):
        return False


if __name__ == '__main__':
    sys.exit(0 if ready() else 1)
