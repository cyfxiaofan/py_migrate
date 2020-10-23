# sqlserver 连接文件

import pymssql
from config import SQLServer_DATABASE


class SqlHelper(object):
    def __init__(self):
        self.conn = pymssql.connect(**SQLServer_DATABASE)
        # 创建指针
        self.cursor = self.conn.cursor()

    def execute_modify_sql(self, sql):
        self.cursor.execute(sql)

    def rollback(self):
        self.conn.rollback()

    def commit(self):
        self.conn.commit()

    def __del__(self):
        self.cursor.close()
        self.conn.close()
