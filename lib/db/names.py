from typing import Tuple

from lib.utils import MessageType, TargetType

VERSION = 'stbot.version'
DOWNLOAD_QUEUE = 'stbot.queue.download'
POST_QUEUE = 'stbot.queue.post'
FAILURE_STATUS_PREFIX = 'stbot.failure.status'
SUCCESS_QUEUE = 'stbot.queue.success'
CLEANED_QUEUE = 'stbot.queue.clean'
FAILED_QUEUE = 'stbot.queue.failed'
BACKUP_QUEUE = 'stbot.queue.backup'
RETRY_COUNT_PREFIX = 'stbot.retry'
DATA_PREFIX = 'stbot.data'
STATUS_PREFIX = 'stbot.status'
MONITOR_PREFIX = 'stbot.monitor'
URL_TO_FILE = 'stbot.url2file'
RELATION_PREFIX = 'stbot.relation'
RELATION_ID_PREFIX = 'stbot.relation.id'
REVERSED_INDEX_PREFIX = 'stbot.reversed.index'


def retry_count_key(uid: str):
    return f'{RETRY_COUNT_PREFIX}:{uid}'


def data_key(uid: str):
    return f'{DATA_PREFIX}:{uid}'


def status_key(uid: str):
    return f'{STATUS_PREFIX}:{uid}'


def get_failure_status(uid: str) -> str:
    return f'{FAILURE_STATUS_PREFIX}:{uid}'


def monitor_key(type_: MessageType) -> str:
    return f"{MONITOR_PREFIX}:{type_.value}"


def relation_key(type_: MessageType) -> str:
    return f"{RELATION_PREFIX}:{type_.value}"


def relation_id_key(type_: MessageType) -> str:
    return f"{RELATION_ID_PREFIX}:{type_.value}"


def get_uid_from_key(key: str) -> str:
    k_prefix, uid = key.split(":")
    return uid


def merge_rel_key(src: str, dst: str) -> str:
    return f"{src}:{dst}"


def split_rel_key(key: str) -> Tuple[str, str]:
    src, dst = key.split(":")
    return src, dst


def reversed_index_key(type_: TargetType) -> str:
    return f"{REVERSED_INDEX_PREFIX}:{type_.value}"
