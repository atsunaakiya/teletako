from functools import reduce
from operator import or_
from typing import AnyStr, NamedTuple, Optional, Iterable, List, Callable, Any, Dict, Tuple

import redis

from lib.config import RedisConfig
from lib.utils import UMessage, MessageStatus, MessageType

ENCODING = 'utf-8'
VERSION = 'stbot.version'
DOWNLOAD_QUEUE = 'stbot.queue.download'
POST_QUEUE = 'stbot.queue.post'
FAILURE_STATUS_PREFIX = 'stbot.failure.status'
SUCCESS_QUEUE = 'stbot.queue.success'
CLEANED_QUEUE = 'stbot.queue.clean'
FAILED_QUEUE = 'stbot.queue.failed'
BACKUP_QUEUE = 'stbot.queue.backup'
RETRY_COUNT_PREFIX = 'stbot.retry'
DATA_PREFIX = 'stbot.data'
STATUS_PREFIX = 'stbot.status'
MONITOR_PREFIX = 'stbot.monitor'
URL_TO_FILE = 'stbot.url2file'
RELATION_PREFIX = 'stbot.relation'
RELATION_ID_PREFIX = 'stbot.relation.id'


def _retry_count_key(uid: AnyStr):
    return f'{RETRY_COUNT_PREFIX}:{uid}'


def _data_key(uid: AnyStr):
    return f'{DATA_PREFIX}:{uid}'


def _status_key(uid: AnyStr):
    return f'{STATUS_PREFIX}:{uid}'


def _get_failure_status(uid: AnyStr) -> str:
    return f'{FAILURE_STATUS_PREFIX}:{uid}'


def _monitor_key(type_: MessageType) -> str:
    return f"{MONITOR_PREFIX}:{type_.value}"


def _relation_key(type_: MessageType) -> str:
    return f"{RELATION_PREFIX}:{type_.value}"


def _relation_id_key(type_: MessageType) -> str:
    return f"{RELATION_ID_PREFIX}:{type_.value}"


def _get_uid_from_key(key: str) -> str:
    k_prefix, uid = key.split(":")
    return uid


def _merge_rel_key(src: str, dst: str) -> str:
    return f"{src}:{dst}"


def _split_rel_key(key: str) -> Tuple[str, str]:
    src, dst = key.split(":")
    return src, dst


class RBQueue(NamedTuple):
    conn: redis.Redis
    queue_key: AnyStr

    def push(self, uid: str):
        self.conn.lpush(self.queue_key, uid.encode(ENCODING))

    def pop(self) -> Optional[str]:
        res = self.conn.rpop(self.queue_key)
        if res is not None:
            return res.decode(ENCODING)

    def size(self):
        return self.conn.llen(self.queue_key)

    def empty(self) -> bool:
        return self.size() == 0

    def iter_pop(self, limit: Optional[int] = None) -> Iterable[str]:
        if limit is None:
            limit = -1
        d = self.pop()
        while d is not None and limit != 0:
            yield d
            d = self.pop()
            limit -= 1

    def list(self) -> List[str]:
        return [
            b.decode(ENCODING)
            for b in self.conn.lrange(self.queue_key, 0, -1)
        ]


class UDB:
    version = '0'
    conn: redis.Redis
    download_queue: RBQueue
    post_queue: RBQueue
    success_queue: RBQueue
    cleaned_queue: RBQueue
    failed_queue: RBQueue
    _status_to_queue: Dict[MessageStatus, RBQueue]

    def __init__(self, config: RedisConfig):
        self.conn = redis.StrictRedis(host=config.host, port=config.port, db=config.db)
        current_version = self.get_version()
        if current_version is None:
            initialize(self.conn, self.version)
        elif current_version != self.version:
            raise RuntimeError(f"Unsupported version: {current_version}, requires {self.version}")
        self.download_queue = RBQueue(self.conn, DOWNLOAD_QUEUE)
        self.post_queue = RBQueue(self.conn, POST_QUEUE)
        self.success_queue = RBQueue(self.conn, SUCCESS_QUEUE)
        self.cleaned_queue = RBQueue(self.conn, CLEANED_QUEUE)
        self.failed_queue = RBQueue(self.conn, FAILED_QUEUE)
        self._status_to_queue = {
            MessageStatus.Downloading: self.download_queue,
            MessageStatus.Posting: self.post_queue,
            MessageStatus.Failed: self.failed_queue,
            MessageStatus.Success: self.success_queue,
            MessageStatus.Cleaned: self.cleaned_queue
        }

    def download_add(self, data: UMessage):
        self.put_data(data)
        self.set_status(data.uid, MessageStatus.Downloading)
        self.download_queue.push(data.uid)

    def download_poll(self) -> Optional[UMessage]:
        uid = self.download_queue.pop()
        if uid is not None:
            return self.get_data(uid)

    def download_iter_poll(self, limit: Optional[int] = None) -> Iterable[UMessage]:
        return map(self.get_data, self.download_queue.iter_pop(limit))

    def download_count(self) -> int:
        return self.download_queue.size()

    def download_retry(self, uid: AnyStr):
        self.set_status(uid, MessageStatus.Downloading)
        self.download_queue.push(uid)
        self.inc_retry(uid)

    def post_add(self, uid: AnyStr):
        self.assert_status(uid, MessageStatus.Downloading)
        self.set_status(uid, MessageStatus.Posting)
        self.post_queue.push(uid)

    def post_poll(self) -> Optional[UMessage]:
        uid = self.post_queue.pop()
        if uid is not None:
            return self.get_data(uid)

    def post_iter_poll(self, limit: Optional[int] = None) -> Iterable[UMessage]:
        return map(self.get_data, self.post_queue.iter_pop(limit))

    def post_count(self):
        return self.post_queue.size()

    def post_retry(self, uid: AnyStr):
        self.assert_status(uid, MessageStatus.Posting)
        self.post_queue.push(uid)
        self.inc_retry(uid)

    def add_success(self, uid: AnyStr):
        self.assert_status(uid, MessageStatus.Posting)
        self.set_status(uid, MessageStatus.Success)
        self.success_queue.push(uid)

    def success_poll(self) -> Optional[UMessage]:
        uid = self.success_queue.pop()
        if uid is not None:
            return self.get_data(uid)

    def success_count(self):
        return self.success_queue.size()

    def success_iter_poll(self, limit: Optional[int] = None) -> Iterable[UMessage]:
        return map(self.get_data, self.success_queue.iter_pop(limit))

    def clean(self, uid: AnyStr):
        self.assert_status(uid, MessageStatus.Success)
        self.set_status(uid, MessageStatus.Cleaned)
        self.cleaned_queue.push(uid)

    def clean_count(self):
        return self.cleaned_queue.size()

    def clean_retry(self, uid: str):
        self.set_status(uid, MessageStatus.Success)
        self.success_queue.push(uid)
        self.inc_retry(uid)

    def fail(self, uid: AnyStr):
        current_status = self.get_status(uid)
        self.set_status(uid, MessageStatus.Failed)
        self.failed_queue.push(uid)
        self.set_failure_status(uid, current_status)

    def set_failure_status(self, uid: AnyStr, status: MessageStatus):
        self.conn[_get_failure_status(uid)] = status.value.encode(ENCODING)

    def get_failure_status(self, uid: AnyStr) -> MessageStatus:
        return MessageStatus(self.conn[_get_failure_status(uid)].decode(ENCODING))

    def failed_count(self):
        return self.failed_queue.size()

    def set_status(self, uid: AnyStr, status: MessageStatus):
        self.conn[_status_key(uid)] = status.value.encode(ENCODING)

    def get_status(self, uid) -> MessageStatus:
        return MessageStatus(self.conn[_status_key(uid)].decode(ENCODING))

    def put_data(self, msg: UMessage):
        self.conn[_data_key(msg.uid)] = msg.stringify().encode(ENCODING)

    def get_data(self, uid: AnyStr) -> UMessage:
        d = self.conn[_data_key(uid)]
        return UMessage.parse(d.decode(ENCODING))

    def data_exists(self, uid: AnyStr) -> bool:
        return _data_key(uid) in self.conn

    def monitor_add(self, type_: MessageType, name: str):
        k = _monitor_key(type_)
        self.conn.sadd(k, name.encode(ENCODING))

    def monitor_list(self, type_: MessageType) -> List[str]:
        k = _monitor_key(type_)
        return [
            s.decode(ENCODING)
            for s in self.conn.smembers(k)
        ]

    def monitor_remove(self, type_: MessageType, name: str):
        k = _monitor_key(type_)
        self.conn.srem(k, name.encode(ENCODING))

    def add_file(self, url: str, path: str):
        self.conn.hset(URL_TO_FILE, url.encode(ENCODING), str(path).encode(ENCODING))

    def get_file(self, url: str) -> Optional[str]:
        d = self.conn.hget(URL_TO_FILE, url.encode(ENCODING))
        if d is not None:
            return d.decode(ENCODING)

    def remove_file(self, url: str):
        self.conn.hdel(URL_TO_FILE, url.encode(ENCODING))

    def inc_retry(self, uid: AnyStr):
        k = _retry_count_key(uid)
        c = self.conn.get(k) or b'0'
        c = int(c.decode(ENCODING))
        self.conn[k] = c + 1

    def retry_or_fail(self, uid: AnyStr, retry_func: Callable[[AnyStr], Any], limit: int):
        if self.get_retry(uid) < limit:
            retry_func(uid)
        else:
            self.fail(uid)

    def get_retry(self, uid: AnyStr) -> int:
        k = _retry_count_key(uid)
        c = self.conn.get(k) or b'0'
        c = int(c.decode(ENCODING))
        return c

    def assert_status(self, uid: AnyStr, expected: MessageStatus):
        status = self.get_status(uid)
        if status == MessageStatus.Failed and expected != MessageStatus.Failed:
            status = self.get_failure_status(uid)
        if status != expected:
            raise RuntimeError(f"Invalid status for uid={uid}, expected: {expected.value}, actual: {status.value}")

    def get_version(self) -> Optional[str]:
        v = self.conn.get(VERSION)
        if v is not None:
            return v.decode(ENCODING)

    def close(self):
        self.conn.close()

    def recover(self):
        error_keys = set(_get_uid_from_key(k.decode(ENCODING)) for k in self.conn.keys(_status_key("*"))) - \
                     reduce(or_, (set(x.list()) for x in self._status_to_queue.values()))
        for k in error_keys:
            s = self.get_status(k)
            print(k, ">>>", s.value)
            self._status_to_queue[s].push(k)

    def restart_failed_tasks(self):
        for uid in self.failed_queue.iter_pop():
            old_status = self.get_failure_status(uid)
            self.clean_retry(uid)
            self._status_to_queue[old_status].push(uid)
            self.set_status(uid, old_status)

    def relation_add(self, type_: MessageType, src: str, dst: str, status_id: str) -> int:
        name = _relation_key(type_)
        key = _merge_rel_key(src, dst)
        s = self.conn.hget(name, key) or b'0'
        c = int(s.decode(ENCODING))
        id_name = _relation_id_key(type_)
        bid = status_id.encode(ENCODING)
        if not self.conn.sismember(id_name, bid):
            self.conn.sadd(id_name, bid)
            c += 1
            s = str(c).encode(ENCODING)
            self.conn.hset(name, key, s)
        return c

    def relation_query(self, type_: MessageType) -> Dict[Tuple[str, str], int]:
        name = _relation_key(type_)
        return {
            _split_rel_key(k.decode(ENCODING)): int(v.decode(ENCODING))
            for k, v in self.conn.hscan_iter(name)
        }

    def __enter__(self) -> 'UDB':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def initialize(conn: redis.Redis, version: str):
    conn[VERSION] = version.encode(ENCODING)
