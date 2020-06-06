from functools import reduce
from operator import or_
from typing import AnyStr, Optional, Iterable, List, Callable, Any, Dict

import redis

from lib.config import RedisConfig
from lib.utils import UMessage, MessageStatus
from .names import *
from .utils import ENCODING, RBQueue
from .versions import initialize


def connect_db(config: RedisConfig) -> redis.Redis:
    return redis.StrictRedis(host=config.host, port=config.port, db=config.db)


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
        self.conn = connect_db(config)
        initialize(self.conn)
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
        self.conn[get_failure_status(uid)] = status.value.encode(ENCODING)

    def get_failure_status(self, uid: AnyStr) -> MessageStatus:
        return MessageStatus(self.conn[get_failure_status(uid)].decode(ENCODING))

    def failed_count(self):
        return self.failed_queue.size()

    def set_status(self, uid: AnyStr, status: MessageStatus):
        self.conn[status_key(uid)] = status.value.encode(ENCODING)

    def get_status(self, uid) -> MessageStatus:
        return MessageStatus(self.conn[status_key(uid)].decode(ENCODING))

    def put_data(self, msg: UMessage):
        self.conn[data_key(msg.uid)] = msg.stringify().encode(ENCODING)

    def get_data(self, uid: AnyStr) -> UMessage:
        d = self.conn[data_key(uid)]
        return UMessage.parse(d.decode(ENCODING))

    def data_exists(self, uid: AnyStr) -> bool:
        return data_key(uid) in self.conn

    def monitor_add(self, type_: MessageType, name: str):
        k = monitor_key(type_)
        self.conn.sadd(k, name.encode(ENCODING))

    def monitor_list(self, type_: MessageType) -> List[str]:
        k = monitor_key(type_)
        return [
            s.decode(ENCODING)
            for s in self.conn.smembers(k)
        ]

    def monitor_remove(self, type_: MessageType, name: str):
        k = monitor_key(type_)
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
        k = retry_count_key(uid)
        c = self.conn.get(k) or b'0'
        c = int(c.decode(ENCODING))
        self.conn[k] = c + 1

    def retry_or_fail(self, uid: AnyStr, retry_func: Callable[[AnyStr], Any], limit: int):
        if self.get_retry(uid) < limit:
            retry_func(uid)
        else:
            self.fail(uid)

    def get_retry(self, uid: AnyStr) -> int:
        k = retry_count_key(uid)
        c = self.conn.get(k) or b'0'
        c = int(c.decode(ENCODING))
        return c

    def assert_status(self, uid: AnyStr, expected: MessageStatus):
        status = self.get_status(uid)
        if status == MessageStatus.Failed and expected != MessageStatus.Failed:
            status = self.get_failure_status(uid)
        if status != expected:
            raise RuntimeError(f"Invalid status for uid={uid}, expected: {expected.value}, actual: {status.value}")

    def close(self):
        self.conn.close()

    def recover(self):
        error_keys = set(get_uid_from_key(k.decode(ENCODING)) for k in self.conn.keys(status_key("*"))) - \
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
        name = relation_key(type_)
        key = merge_rel_key(src, dst)
        s = self.conn.hget(name, key) or b'0'
        c = int(s.decode(ENCODING))
        id_name = relation_id_key(type_)
        bid = status_id.encode(ENCODING)
        if not self.conn.sismember(id_name, bid):
            self.conn.sadd(id_name, bid)
            c += 1
            s = str(c).encode(ENCODING)
            self.conn.hset(name, key, s)
        return c

    def relation_query(self, type_: MessageType) -> Dict[Tuple[str, str], int]:
        name = relation_key(type_)
        return {
            split_rel_key(k.decode(ENCODING)): int(v.decode(ENCODING))
            for k, v in self.conn.hscan_iter(name)
        }

    def reversed_index_add(self, type_: TargetType, tid, uid):
        self.conn.hset(reversed_index_key(type_), tid.encode(ENCODING), uid.encode(ENCODING))

    def reversed_index_get(self, type_: TargetType, tid) -> Optional[UMessage]:
        uid = self.conn.hget(reversed_index_key(type_), tid.encode(ENCODING))
        if uid is None:
            return None
        uid = uid.decode(ENCODING)
        return self.get_data(uid)

    def __enter__(self) -> 'UDB':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
