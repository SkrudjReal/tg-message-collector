import os
import json
import hashlib
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / '.env', override=False)
DATA = ROOT / 'data'
DATA.mkdir(mode=0o700, exist_ok=True)
MODEL = os.getenv('EMBEDDING_MODEL', 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
CF_MODEL = '@cf/baai/bge-m3'


@dataclass(frozen=True)
class EmbeddingProfile:
    provider: str = 'local'

    def __post_init__(self):
        if self.provider not in ('local', 'cloudflare'):
            raise ValueError('Embedding provider must be local or cloudflare')

    @property
    def model(self):
        return MODEL if self.provider == 'local' else CF_MODEL

    @property
    def dimension(self):
        return int(os.getenv('LOCAL_EMBEDDING_DIM', '384')) if self.provider == 'local' else 1024

    @property
    def table(self):
        return 'message_embeddings' if self.provider == 'local' else 'cloudflare_embeddings'


def active_profile_path():
    identity = os.getenv('MYSQL_DATABASE', 'tg_message_collector')
    return DATA / ('embedding-active-' + hashlib.sha256(identity.encode()).hexdigest()[:20] + '.json')


def active_profile():
    path = active_profile_path()
    if path.exists():
        manifest = json.loads(path.read_text())
        profile = EmbeddingProfile(manifest['provider'])
        if manifest['model'] != profile.model or manifest['dimension'] != profile.dimension:
            raise ValueError('Active embedding profile does not match configuration')
        return profile
    # Environment cannot bypass the verified cutover workflow for a new cloud generation.
    if os.getenv('EMBEDDING_PROVIDER', 'local') != 'local':
        raise ValueError('Build and activate the Cloudflare generation before selecting it')
    return EmbeddingProfile()


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
