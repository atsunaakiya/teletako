import json
from enum import Enum
from typing import NamedTuple, AnyStr, List, Dict


class MessageType(Enum):
    Twitter = 'twitter'
    # Pixiv = 'pixiv'


class TargetType(Enum):
    Telegram = 'telegram'


_user_base_url: Dict[MessageType, str] = {
    MessageType.Twitter: 'https://twitter.com/{username}'
}


def get_user_home_page_url(type_: MessageType, author: str):
    return _user_base_url[type_].format(username=author)


class UMessage(NamedTuple):
    id: AnyStr
    type: MessageType
    monitor: AnyStr
    source: AnyStr
    content: AnyStr
    author: AnyStr
    media_list: List[AnyStr]

    @property
    def uid(self):
        return f"{self.type.value}_{self.id}"

    def stringify(self):
        return json.dumps(dict(
            id=self.id,
            monitor=self.monitor,
            type=self.type.value,
            source=self.source,
            content=self.content,
            author=self.author,
            media_list=self.media_list
        ))

    @classmethod
    def parse(cls, s: AnyStr) -> 'UMessage':
        d = json.loads(s)
        return UMessage(
            id=d['id'],
            type=MessageType(d['type']),
            source=d['source'],
            content=d['content'],
            author=d['author'],
            media_list=d['media_list'],
            monitor=d['monitor']
        )


class MessageStatus(Enum):
    Downloading = 'downloading'
    Posting = 'posting'
    Success = 'success'
    Cleaned = 'cleaned'
    Failed = 'failed'
