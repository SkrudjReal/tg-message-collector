import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / '.env', override=False)
DATA = ROOT / 'data'
DATA.mkdir(mode=0o700, exist_ok=True)
MODEL = os.getenv('EMBEDDING_MODEL', 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')


def db_options(readonly=False):
    prefix = 'MYSQL_READ_' if readonly else 'MYSQL_'
    return dict(
        host=os.getenv('MYSQL_HOST', '127.0.0.1'),
        port=int(os.getenv('MYSQL_PORT', '3306')),
        db=os.getenv('MYSQL_DATABASE', 'tg_message_collector'),
        user=os.environ[prefix + 'USER'], password=os.environ[prefix + 'PASSWORD'],
        charset='utf8mb4', autocommit=True, minsize=1, maxsize=5,
        init_command="SET time_zone = '+00:00'", connect_timeout=10,
    )


def chat_ids():
    return {int(x.strip()) for x in os.getenv('CHAT_IDS', '').split(',') if x.strip()}
