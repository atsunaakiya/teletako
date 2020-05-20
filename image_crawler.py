import time
import traceback
from io import BytesIO

import requests

from lib.cache import add_cache
from lib.config import parse
from lib.db import UDB


def save_image(url: str) -> str:
    res = requests.get(url)
    bio = BytesIO(res.content)
    id_ = add_cache(bio)
    return id_


def main():
    with open('config.toml') as cf:
        config = parse(cf)

    with UDB(config.redis) as db:
        for msg in db.download_iter_poll(config.crawler.download_limit):
            try:
                for u in msg.media_list:
                    fp = save_image(u)
                    print(u, '=>', fp)
                    time.sleep(config.crawler.cool_down_time)
                    db.add_file(u, fp)
            except Exception as err:
                db.retry_or_fail(msg.uid, db.download_retry, config.crawler.retry_limit)
                traceback.print_exc()
            else:
                db.post_add(msg.uid)


if __name__ == '__main__':
    main()
