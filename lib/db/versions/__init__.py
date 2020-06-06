from typing import Tuple, Callable, Any, Dict

import redis

from ..utils import ENCODING
from ..names import VERSION

from . import migrate_0_to_0_1

migrations: Dict[Tuple[str, str], Callable[[redis.Redis], Any]] = {
    ('0', '0.1'): migrate_0_to_0_1.migrate
}

CURRENT_VERSION = '0'


def initialize(conn: redis.Redis):
    v = conn.get(VERSION)
    if v is not None and v.decode(ENCODING) != CURRENT_VERSION:
        raise ValueError(f"Database versions not match: expected {CURRENT_VERSION}, got {v}")


def get_db_version(conn: redis.Redis):
    v = conn.get(VERSION)
    if v is None:
        return None
    else:
        return v.decode(ENCODING)


def migrate_db(conn: redis.Redis, from_ver: str, to_ver: str):
    ver_key = (from_ver, to_ver)
    if ver_key not in migrations:
        conn.close()
        raise ValueError(f"Can not find migration for: {from_ver} -> {to_ver}")
    f = migrations[ver_key]
    print(f'Running database migration: {from_ver} -> {to_ver} ({f})')
    f(conn)
