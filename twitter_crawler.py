from collections import deque
import re
from typing import Iterable, NamedTuple, List, Optional, Set, Tuple, Deque

import tweepy
from lib.config import TwitterConfig, parse
from lib.db import UDB
from lib.utils import UMessage, MessageType


class RelatedStatusRef(NamedTuple):
    username: str
    id: str


class WrappedMessage(NamedTuple):
    msg: UMessage
    related_id: List[Optional[RelatedStatusRef]]


def _get_retweet_name(s: str) -> Optional[str]:
    res = re.search(r"^RT @(\w+):", s)
    if res is not None:
        return res.groups()[0]


def start_authorization(config: TwitterConfig) -> tweepy.API:
    auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_key, config.access_secret)
    api = tweepy.API(auth)
    return api


def _split_twitter_status_url(s: str) -> Optional[RelatedStatusRef]:
    res = re.search(r'^https?://twitter\.com/(\w+)/status/(\d+)$', s)
    if res is not None:
        user, id_ = res.groups()
        return RelatedStatusRef(user, id_)


def _get_message_from_status(status: tweepy.models.Status) -> WrappedMessage:
    id_ = str(status.id)
    author = status.author.screen_name
    content: str = status.text
    if hasattr(status, 'extended_entities'):
        media = status.extended_entities.get('media') or []
    elif hasattr(status, 'entities'):
        media = status.entities.get('media') or []
    else:
        media = []
    images = [
        m['media_url_https']
        for m in media
        if m['type'] == 'photo'
    ]
    umsg = UMessage(
        type=MessageType.Twitter,
        id=id_,
        author=author,
        content=content,
        monitor=author,
        media_list=images,
        source=f'https://twitter.com/{author}/status/{id_}'
    )
    related_urls = status.entities.get('urls') or []
    related_id = [
        _split_twitter_status_url(u['expanded_url'])
        for u in related_urls
        if 'expanded_url' in u
    ]
    return WrappedMessage(umsg, related_id)


def _walk_status(api: tweepy.API, status: tweepy.models.Status, max_depth: int) -> Iterable[UMessage]:
    q: Deque[Tuple[tweepy.models.Status, int]] = deque([(status, 0)])
    while q:
        status, depth = q.popleft()
        msg = _get_message_from_status(status)
        yield msg.msg
        if depth < max_depth:
            for r in msg.related_id:
                if r is None:
                    continue
                rs = api.get_status(r.id)
                q.append((rs, depth + 1))


def get_twitter_medias(api: tweepy.API, db: UDB, username: str) -> Iterable[UMessage]:
    tl = api.user_timeline(username)
    for status in tl:
        uid = f"{MessageType.Twitter.value}_{status.id}"
        if db.data_exists(uid):
            continue
        yield from _walk_status(api, status, 2)


def main():
    with open('config.toml') as cf:
        config = parse(cf)
    api = start_authorization(config.twitter)
    with UDB(config.redis) as db:
        monitors = set(db.monitor_list(MessageType.Twitter))
        for mu in monitors:
            for msg in get_twitter_medias(api, db, mu):
                if db.data_exists(msg.uid):
                    continue
                if msg.author not in monitors:
                    r = db.relation_add(MessageType.Twitter, mu, msg.author, msg.id)
                    print(f"Rel Add: {mu} => {msg.author} [{r}]")
                    continue
                retweet_user = _get_retweet_name(msg.content)
                if retweet_user is not None:
                    r = db.relation_add(MessageType.Twitter, msg.author, retweet_user, msg.id)
                    print(f"Rel Add: {msg.author} => {retweet_user} [{r}]")
                    continue
                if not msg.media_list:
                    continue
                db.download_add(msg)
                print(msg)


if __name__ == '__main__':
    main()
