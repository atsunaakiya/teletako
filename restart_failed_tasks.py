from lib.config import parse
from lib.db import UDB


def main():
    with open("config.toml") as cf:
        config = parse(cf)
    with UDB(config.redis) as db:
        print(db.failed_queue.list())
        db.restart_failed_tasks()

if __name__ == '__main__':
    main()
