import json
import sys

from lib.config import parse
from lib.db import UDB
from lib.utils import MessageType


def main():
    with open('config.toml') as cf:
        config = parse(cf)
    if len(sys.argv) != 3:
        raise ValueError('Usage: "author.py import <filename>" or author.py export <filename>')
    _, action, fp = sys.argv
    if not fp.endswith(".backup.json"):
        raise ValueError("extname of target file should be '.backup.json'")
    with UDB(config.redis) as db:
        if action == 'import':
            with open(fp) as f:
                data = json.load(f)
            for k, v in data.items():
                t = MessageType(k)
                for m in v:
                    db.monitor_add(t, m)
        elif action == 'export':
            data = {
                t.value: db.monitor_list(t)
                for t in MessageType
            }
            with open(fp, 'w') as f:
                json.dump(data, f)
        else:
            raise ValueError("Unknown action: " + action)


if __name__ == '__main__':
    main()
