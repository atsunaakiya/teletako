from typing import Iterable

import tweepy
from lib.config import TwitterConfig, parse
from lib.db import UDB
from lib.utils import UMessage, MessageType


def start_authorization(config: TwitterConfig) -> tweepy.API:
    auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_key, config.access_secret)
    api = tweepy.API(auth)
    return api


def get_twitter_medias(api: tweepy.API, username: str) -> Iterable[UMessage]:
    tl = api.user_timeline(username)
    for t in tl:
        id_ = str(t.id)
        author = t.author.screen_name
        content: str = t.text
        if t.retweeted or content.startswith("RT"):
            continue
        if hasattr(t, 'extended_entities'):
            media = t.extended_entities.get('media') or []
        elif hasattr(t, 'entities'):
            media = t.entities.get('media') or []
        else:
            media = []
        images = [
            m['media_url_https']
            for m in media
            if m['type'] == 'photo'
        ]
        if not images:
            continue
        yield UMessage(
            type=MessageType.Twitter,
            id=id_,
            author=author,
            content=content,
            monitor=username,
            media_list=images,
            source=f'https://twitter.com/{author}/status/{id_}'
        )


def main():
    with open('config.toml') as cf:
        config = parse(cf)
    with UDB(config.redis) as db:
        api = start_authorization(config.twitter)
        for mu in db.monitor_list(MessageType.Twitter):
            for msg in get_twitter_medias(api, mu):
                if db.data_exists(msg.uid):
                    continue
                db.download_add(msg)
                print(msg)


if __name__ == '__main__':
    main()
