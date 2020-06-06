from typing import NamedTuple, List, IO
import toml


class WebDavConfig(NamedTuple):
    host: str
    port: int
    path: str
    use_https: bool
    username: str
    password: str
    root_dir: str


class ManageConfig(NamedTuple):
    host: str
    port: int
    debug: bool
    root_url: str


class CrawlerConfig(NamedTuple):
    retry_limit: int
    cool_down_time: int
    download_limit: int
    post_limit: int


class RedisConfig(NamedTuple):
    host: str
    port: int
    db: int


class TwitterConfig(NamedTuple):
    consumer_key: str
    consumer_secret: str
    access_key: str
    access_secret: str


class TelegramConfig(NamedTuple):
    token: str
    media_group_limit: int


class RoutingConfig(NamedTuple):
    tag: str
    telegram_channels: List[str]


class UConfig(NamedTuple):
    webdav: WebDavConfig
    twitter: TwitterConfig
    telegram: TelegramConfig
    redis: RedisConfig
    crawler: CrawlerConfig
    manage: ManageConfig
    routings: List[RoutingConfig]




def parse(f: IO) -> UConfig:
    d = toml.load(f)
    return UConfig(
        twitter=TwitterConfig(**d['twitter']),
        telegram=TelegramConfig(**d['telegram']),
        redis=RedisConfig(**d['redis']),
        crawler=CrawlerConfig(**d['crawler']),
        manage=ManageConfig(**d['manage']),
        webdav=WebDavConfig(**d['webdav']),
        routings=[
            RoutingConfig(**r)
            for r in d['routing']
        ]
    )
