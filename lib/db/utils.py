from typing import Optional, NamedTuple, List, Iterable

import redis

ENCODING = 'utf-8'


class RBQueue(NamedTuple):
    conn: redis.Redis
    queue_key: str

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