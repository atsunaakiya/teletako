import json

import redis

from lib.db.utils import ENCODING


def migrate(conn: redis.Redis):
    pass
    # for key in conn.keys("stbot.data:*"):
    #     data = json.loads(conn[key].decode(ENCODING))
    #     data['tags'] = ['default']
    #     conn[key] = json.dumps(data)
