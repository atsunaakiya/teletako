from lib.config import parse
from lib.db import UDB

if __name__ == '__main__':
    config = parse(open('config.toml'))
    with UDB(config.redis) as db:
        db.recover()
