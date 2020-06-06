import os
import traceback
from pathlib import Path
from typing import Dict, Set, List

from webdav3.client import Client

from lib.cache import cache_path
from lib.config import parse, WebDavConfig, RoutingConfig
from lib.db import UDB
from lib.utils import UMessage, MessageType


def _target_dir(config: WebDavConfig, msg: UMessage) -> str:
    return f"{config.root_dir}/{msg.type.value}/{msg.monitor}"

def get_client(config: WebDavConfig) -> Client:
    protocol = 'https' if config.use_https else 'http'
    client = Client(dict(
        webdav_hostname=f"{protocol}://{config.host}:{config.port}{config.path}",
        webdav_login=config.username,
        webdav_password=config.password,
    ))
    client.verify = config.use_https
    return client


def update_files(routings: List[RoutingConfig], db: UDB, client: Client, root_dir: Path, retry_limit: int):
    tags = [r.tag for r in routings]
    for d in set(tags) - set(client.list(str(root_dir))):
        client.mkdir(str(root_dir / d))
    for tag in tags:
        tag_dir = root_dir / tag
        services_dir = set([t.value for t in MessageType])
        for s in set(services_dir) - set(client.list(str(tag_dir))):
            client.mkdir(str(tag_dir / s))
        authors: Dict[MessageType, Set[str]] = {
            t: set(client.list(str(tag_dir / t.value)))
            for t in MessageType
        }
        for msg in db.success_iter_poll():
            try:
                if msg.monitor not in authors[msg.type]:
                    client.mkdir(str(tag_dir / msg.type.value / msg.monitor))
                    authors[msg.type].add(msg.monitor)
                local_files = [
                    cache_path(db.get_file(u))
                    for u in msg.media_list
                ]
                for i, local_path in enumerate(local_files):
                    remote_path = str(tag_dir / msg.type.value / msg.monitor / f"{msg.id}_{i}.jpg")
                    print(local_path, ">>>", remote_path)
                    client.upload(remote_path, local_path)
                for f in local_files:
                    os.remove(f)
            except Exception as err:
                traceback.print_exc()
                db.retry_or_fail(msg.uid, db.clean_retry, retry_limit)
            else:
                db.clean(msg.uid)


def main():
    with open("config.toml") as cf:
        config = parse(cf)

    client = get_client(config.webdav)
    with UDB(config.redis) as db:
        update_files(db, client, Path(config.webdav.root_dir), config.crawler.retry_limit)


if __name__ == '__main__':
    main()
