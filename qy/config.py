# -- 数据迁移配置文件
import os

BASE_FILE = os.path.abspath(os.path.dirname(__file__))

STATIC_FILE = os.path.join(BASE_FILE, 'static')

Postgresql_DATABASE = {
    'database': 'ox',
    'user': 'postgres',
    'password': '123456',
    'host': '192.168.1.222',
    'port': '5432'
}

SQLServer_DATABASE = {
    'database': 'www1',
    'user': 'sa',
    'password': '123456',
    'host': '127.0.0.1',
}

SET_Maxid_sql = "SELECT setval('{tablename}_id_seq', max(id)) FROM {tablename};"

SELECT_KML_sql = "SELECT {fieldname}.STAsText() FROM {tablename}"

INSERT_sql = "INSERT INTO {tablename} values{val};"

SELECT_sql = "SELECT * FROM {tablename};"

SELECT_ID_sql = "SELECT * FROM {tablename} WHERE id={id};"

SELECT_MAXID_sql = "SELECT max(id) FROM {tablename};"

DELETE_sql = "DELETE FROM {tablename} WHERE id={id}"

INSERT_COLUMNS_sql = "INSERT INTO {tablename}{fieldtitle} values{val}"

Replace_str = {
    '\\u202a': '',
    '\\u3000': '',
    '\'': "\""
}


def regex_list():
    global Replace_str
    _l1 = [i for i, v in Replace_str.items()]
    _l2 = [v for i, v in Replace_str.items()]
    return [_l1, _l2]


FRAME_MAP = {

}
