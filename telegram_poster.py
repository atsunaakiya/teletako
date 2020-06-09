import traceback
from itertools import chain
from typing import List, TypeVar, Iterable

from telegram import InputMediaPhoto, Bot
from telegram.ext import Updater

from lib.cache import read_cache
from lib.config import parse, TelegramConfig
from lib.db import UDB
from lib.utils import TargetType

T = TypeVar("T")

def chunk(src: Iterable[T], n: int) -> Iterable[List[T]]:
    ck: List[T] = []
    for it in src:
        ck.append(it)
        if len(ck) == n:
            yield ck
            ck = []
    if ck:
        yield ck


def get_updater(config: TelegramConfig) -> Updater:
    return Updater(config.token, use_context=False)


def main():
    with open('config.toml') as cf:
        config = parse(cf)
    updater = get_updater(config.telegram)
    with UDB(config.redis) as db:
        posts = list(db.post_iter_poll(config.crawler.post_limit))
        for post in posts:
            try:
                images: 'chain[str]' = post.media_list
                for urls in chunk(images, config.telegram.media_group_limit):
                    files = map(db.get_file, urls)
                    media = [
                        InputMediaPhoto(read_cache(f))
                        for f in files
                    ]
                    for ch in config.telegram.channels:
                        res = updater.bot.send_media_group(f"@{ch}", media=media)
                        for r in res:
                            message_id = r['message_id']
                            db.reversed_index_add(TargetType.Telegram, f'{ch}/{message_id}', post.uid)
                    if config.telegram.private_channels:
                        for ch in config.telegram.private_channels:
                            updater.bot.send_media_group(ch, media=media)
            except Exception as err:
                traceback.print_exc()
                db.retry_or_fail(post.uid, db.post_retry, config.crawler.retry_limit)
            else:
                print("DONE:", post.uid)
                db.add_success(post.uid)



if __name__ == '__main__':
    main()
