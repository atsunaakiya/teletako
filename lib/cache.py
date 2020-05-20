import os
import shutil
import uuid
from io import BytesIO
from pathlib import Path
from typing import IO
from PIL import Image

cache_root = Path('cache')


def _get_name_from_id(id_: str) -> str:
    return str(cache_root / f"{id_}.jpg")


def add_cache(io: IO) -> str:
    id_ = str(uuid.uuid4())
    img: Image.Image = Image.open(io)
    img = img.convert("RGB")
    fp = _get_name_from_id(id_)
    img.save(fp, format='JPEG')
    return id_


def read_cache(id_: str) -> IO:
    return open(_get_name_from_id(id_), 'rb')

def cache_path(id_: str) -> str:
    return _get_name_from_id(id_)

def remove_cache(id_: str):
    os.remove(_get_name_from_id(id_))
