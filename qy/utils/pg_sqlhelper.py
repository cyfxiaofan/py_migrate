# postgresql 连接文件

import psycopg2
from config import Postgresql_DATABASE


class SqlHelper(object):
    def __init__(self):
        self.conn = psycopg2.connect(**Postgresql_DATABASE)
        self.cursor = self.conn.cursor()

    def execute_modify_sql(self, sql):
        self.cursor.execute(sql)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def __del__(self):
        self.cursor.close()
        self.conn.close()


