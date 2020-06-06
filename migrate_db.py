from lib.config import parse
from lib.db import connect_db, migrate_db, get_db_version, CURRENT_VERSION
from lib.db.names import VERSION

def main():
    with open('config.toml') as cf:
        config = parse(cf)
    conn = connect_db(config.redis)
    real_version = get_db_version(conn)
    if real_version == CURRENT_VERSION:
        print("DB Version up-to-date:", real_version)
        return
    migrate_db(conn, real_version, CURRENT_VERSION)
    # conn[VERSION] = VERSION


if __name__ == '__main__':
    main()
