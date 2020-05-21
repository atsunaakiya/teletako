from functools import partial
from operator import itemgetter
from typing import Optional, List, Dict, Tuple, Set

import flask

from lib.config import parse
from lib.db import UDB
from lib.utils import MessageType, get_user_home_page_url

app = flask.Flask(__name__)
db: Optional[UDB] = None


def get_page():
    services = [
        (t.value, db.monitor_list(t), partial(get_user_home_page_url, t))
        for t in MessageType
    ]
    download_n = db.download_count()
    post_n = db.post_count()
    success_n = db.success_count()
    failed_n = db.failed_count()
    cleaned_n = db.clean_count()
    return flask.render_template('index.html',
                                 services=services,
                                 download_n=download_n,
                                 post_n=post_n,
                                 success_n=success_n,
                                 failed_n=failed_n,
                                 cleaned_n=cleaned_n
                                 )


@app.route("/", methods=["GET"])
def home_page():
    return get_page()


@app.route("/rels/<type_>", methods=["GET"])
def rel_page(type_):
    type_ = MessageType(type_)
    rels = db.relation_query(type_)
    rec_c = {}
    rec_v = {}
    monitors = db.monitor_list(type_)
    home_url = partial(get_user_home_page_url, type_)
    for (src, dst), ctr in rels.items():
        if dst in monitors:
            continue
        if dst not in rec_c:
            rec_c[dst] = 0
            rec_v[dst] = set()
        rec_c[dst] += ctr
        rec_v[dst].add(src)
    recommends: List[Tuple[str, int, Set[str]]] = [
        (k, rec_c[k], rec_v[k])
        for k in rec_c.keys()
    ]
    recommends.sort(key=itemgetter(1), reverse=True)
    return flask.render_template('relation.html', service=type_.value, recommends=recommends, home_url=home_url)

@app.route("/add", methods=["POST"])
def add_monitor():
    service = flask.request.form['type']
    name = flask.request.form['name']
    type_ = MessageType(service)
    db.monitor_add(type_, name)
    print(type_, name)
    return flask.redirect(flask.request.url_root)


@app.route("/delete", methods=["POST"])
def delete_monitor():
    service = flask.request.form['type']
    name = flask.request.form['name']
    type_ = MessageType(service)
    db.monitor_remove(type_, name)
    return flask.redirect(flask.request.url_root)


if __name__ == '__main__':
    with open('config.toml') as cf:
        config = parse(cf)
    with UDB(config.redis) as db:
        app.config["APPLICATION_ROOT"] = config.manage.root_url
        app.run(host=config.manage.host, port=config.manage.port, debug=config.manage.debug)
