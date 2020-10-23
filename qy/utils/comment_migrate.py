# 通用迁移模块

import os
import datetime
from geoalchemy2 import Geometry, WKTElement
from config import SELECT_sql, STATIC_FILE, INSERT_COLUMNS_sql, SELECT_KML_sql, SET_Maxid_sql, regex_list, \
    SELECT_MAXID_sql
import pandas as pd
from utils import pg_sqlhelper, sr_sqlhelper
from utils.comment_exception import CustomError


class MigrateInterface(object):

    # 初始化连接 sqlserver 和 postgresql
    def __init__(self, tablename):
        self.df = None
        self.pg = pg_sqlhelper.SqlHelper()
        self.sr = sr_sqlhelper.SqlHelper()
        self.tablename = tablename
        self.kml_df = None
        self.errorinfo = {}
        self.test = ''

    def make_idrelation(self):
        """
        :return: 所有数据ID列表
        """
        return self.df['Id'].values.tolist()

    def get_data(self):
        """
            获取实例表数据
        """
        self.df = pd.read_sql(SELECT_sql.format(tablename=self.tablename), con=self.sr.conn)

    def get_sqldata(self, sql):
        """
        执行一句sql 返回结果 list
        :param sql: 执行的sql
        :return: sql执行结果list
        """
        return pd.read_sql(sql, con=self.pg.conn).replace(regex_list()[0], regex_list()[1], regex=True).values.tolist()

    def get_sqldf(self, sql):
        """
        执行一句sql 返回结果 dataframe
        :param sql: 执行的sql
        :return: sql执行结果 dataframe
        """
        return pd.read_sql(sql, con=self.pg.conn)

    def save_infos(self, kw, data):
        """
        :param kw: 需要保存的类型
        :param data: 需要保存的数据
        :return:
        """
        try:
            with open('{}/{}/{}.txt'.format(STATIC_FILE, self.tablename, self.tablename + '__%s' % kw), 'w') as f:
                f.write(str(data))
        except FileNotFoundError:
            os.makedirs('{}/{}/'.format(STATIC_FILE, self.tablename))
            with open('{}/{}/{}.txt'.format(STATIC_FILE, self.tablename, self.tablename + '__%s' % kw), 'w') as f:
                f.write(str(data))
        print('%s 数据已保存' % kw)

    def get_field(self, field):
        """
        获取指定字段列表
        :param field: 字段名
        :return: 指定的字段列表
        """
        return self.df[field].values.tolist()

    def get_columns(self, index):
        """
        通过索引获取列数据
        :param index: 所列索引
        :return: 列数据list
        """
        return self.df[index:index + 1].replace(regex_list()[0], regex_list()[1], regex=True).values.tolist()[0]

    def change_titles_all(self, field_list):
        # 修改列名，将列名全部替换
        try:
            self.df.columns = field_list
        except Exception as e:
            raise CustomError('修改Dataframe列名错误：%s' % e)

    def change_titles(self, field_dict):
        """
        局部替换列名，只替换指定字段
        :param field_dict: 旧新字段名组成的字典
        """
        try:
            self.df.rename(columns=field_dict, inplace=True)
        except Exception as e:
            raise CustomError('修改Dataframe列名错误：%s' % e)

    def drop_columns(self, field_list):
        """
        :param field_list: 删除的字段组成的列表
        :return: None
        """
        for i in field_list:
            self.df = self.df.drop(i, axis=1, inplace=False)

    def check_file(self, filepath, filename):
        """
        :param filename: 文件名
        :param filepath: 文件路径
        :return: True or False
        """
        try:
            assert os.path.exists(os.path.join(STATIC_FILE, filepath + '/' + filename))
            return self.read_file(filepath, filename)
        except AssertionError:
            return False

    def map_fields(self, map_dict, oldfield, newfield):
        """
        :param map_dict: 一个映射字典或是自定义函数
        :param oldfield: 需要映射匹配的字段
        :param newfield: 添加的新字段
        :return: None
        """
        _li = eval('self.df.{}.map'.format(oldfield))(map_dict)
        self.df[newfield] = _li

    def add_columns(self, field, data):
        """
        添加一列
        :param field: 列名
        :param data: 列数据
        :return: None
        """
        try:
            self.df[field] = data
        except Exception as e:
            raise CustomError('增加列错误：%s' % e)

    def get_title(self):
        """
        :return: 返回DataFrame的title列表
        """
        return self.df.columns.to_list()

    def get_index(self, field):
        """
        :param field: 获取索引的行名
        :return: index
        """
        return self.get_title().index(field)

    def get_data_list(self):
        """
        :return: 将dataframe 去除 nan和 config的字符 之后返回 list
        """
        return self.df.replace(pd.np.nan, None).replace(regex_list()[0], regex_list()[1], regex=True).values.tolist()

    def save_id_ralation(self):
        """
        :return: ID关系保存
        """
        self.get_data()
        self.save_infos('id', self.make_idrelation())
        print('id关系已保存')

    def insert(self, index, field, data_list=None, default=None):
        """
        :param default: 若没有指定data_list，则取默认值。
        :param index: 插入的索引位置
        :param field: 插入的指定字段名称
        :param data_list: 插入的数据
        """
        if not data_list:
            data_list = [default for _ in range(self.count())]
        try:
            self.df.insert(index, field, data_list)
        except Exception as e:
            raise CustomError('插入列数据错误：%s' % e)

    def start_migrate(self, tablename, data_list=None, title_list=None, ignore=False):
        """
        :param title_list: 自定义插入表头
        :param data_list: 是否自定义datalist
        :param tablename: 表名称
        :param ignore: 是否忽略重复键
        """
        if not data_list:
            data_list = self.get_data_list()
            for index, columns in enumerate(data_list):
                _sql = INSERT_COLUMNS_sql.format(
                    tablename=tablename,
                    fieldtitle=str(tuple(self.get_title())).replace('\'', '"'),
                    val=tuple(columns)
                )
                try:
                    self.pg.execute_modify_sql(_sql)
                    self.pg.commit()
                    print('第%s行写入成功！' % str(index + 1))
                except Exception as e:
                    self.pg.rollback()
                    if ignore:
                        if '已经存在' not in str(e):
                            raise CustomError('第%d行写入失败！失败原因：%s. 执行的sql语句为: %s' % (index + 1, e, _sql))
                        else:
                            self.errorinfo[index] = columns
                    else:
                        raise CustomError('第%d行写入失败！失败原因：%s . 执行的sql语句为: %s' % (index + 1, e, _sql))

            self.pg.commit()
            print('%s表 ===> %s表 迁移成功' % (self.tablename, tablename))
            return True
        else:
            _sql = INSERT_COLUMNS_sql.format(
                tablename=tablename,
                fieldtitle=str(tuple(title_list)).replace('\'', '"'),
                val=tuple(data_list)
            )
            try:
                self.pg.execute_modify_sql(_sql)
                self.pg.commit()
                print('%s表 ===> 新增数据插入成功！' % tablename)
                return True
            except Exception as e:
                self.pg.rollback()
                if ignore:
                    if '已经存在' not in str(e):
                        raise CustomError('写入失败！失败原因：%s. 执行的sql语句为: %s' % (e, _sql))
                    else:
                        return True
                else:
                    raise CustomError('写入失败！失败原因：%s. 执行的sql语句为: %s' % (e, _sql))

    def set_maxid_seq(self, tablename):
        """
        将id索引设置成最大 此方法在数据迁完时候  ***必须要执行*****
        :param tablename: pg 的表名
        """
        self.pg.execute_modify_sql(SET_Maxid_sql.format(tablename=tablename))
        self.pg.commit()

    def get_kml_data(self, fieldname):
        """
        处理 kml
        :param fieldname:
        :return:
        """
        self.kml_df = pd.read_sql(SELECT_KML_sql.format(fieldname=fieldname, tablename=self.tablename),
                                  con=self.sr.conn)
        return [
            i[0].replace('POINT (', '').replace(')', '').replace('POLYGON ((', '').replace('LINESTRING (', '').replace(
                '))', '')
            if i[0] is not None else '' for i in self.show_kml()]

    @classmethod
    def read_file(cls, filepath, filename):
        """
        :param filepath: static目录下的文件夹名
        :param filename: 完整文件名
        :return:
        """
        with open('{}/{}/{}.txt'.format(STATIC_FILE, filepath, filename), 'r') as f:
            r = f.read()
        return eval(r)

    def get_maxid(self, tablename):
        """
        获取 pg 中 该表的maxid
        :param tablename: 表名
        :return:
        """
        self.pg.execute_modify_sql(SELECT_MAXID_sql.format(tablename=tablename))
        rows = self.pg.cursor.fetchone()
        max_id, = rows
        return max_id + 1

    def make_timetostr(self, field, default=None):
        """
        将Timestape转化为 str
        :param default: 若转化为 NAT 的时候 默认转化为 default
        :param field: 时间戳对应字段
        """
        self.add_columns(field, [_t if _t != 'NaT' else default for _t in [str(i) for i in self.df[field]]])

    def make_nantostr(self, field, default=''):
        """
        将某列数据的None转化为default
        :param field: 要转化的列名
        :param default: 设置的默认值
        """
        _fieldlist = self.get_field(field)
        self.add_columns(field, [_f if _f is not None else default for _f in _fieldlist])

    def make_nulltodefault(self, field, default=None):
        """
        将空字符串转化为default
        :param field: 转化的字段
        :param default: 默认值
        """
        _field = self.get_field(field)
        self.add_columns(field, [_f if _f != '' else default for _f in _field])

    def make_typetodefault(self, field, types, default=None):
        """
        将指定字段类型转化为default
        :param types: 指定的type
        :param field: 转化的字段
        :param default: 默认值
        """
        check_type = eval(types)
        _field = self.get_field(field)
        _newfield = []
        for _ev in _field:
            if _ev is not None:
                try:
                    check_type(_ev)
                except ValueError:
                    _newfield.append(default)
                else:
                    _newfield.append(_ev)
            else:
                _newfield.append(_ev)
        self.add_columns(field, [_f if _f != '' else default for _f in _newfield])

    def repare_field(self, field, length, default, special=False, replace=None):
        """
        修理字段
        :param replace: 特殊情况所替换的 str
        :param special: 默认不检测特殊情况
        :param length: check的field长度
        :param default: 默认增加的str
        :param field: 字段
        """
        if not special:
            _field = self.get_field(field)
        else:
            _field = self.get_field(field)
            special_list = []
            for index, _i in enumerate(_field):
                if _i in special:
                    special_list.append(index)
            for _v in special_list:
                _field[_v] = replace
        self.add_columns(field, [_t if _t is None or len(_t) > length else _t + default for _t in _field])

    def get_row(self, field, val):
        """
        获取某列
        :param field: 指定字段
        :param val: 指定该字段的值
        :return: 该行
        """
        try:
            assert self.df.loc[self.df[field] == val].values.tolist() != []
            return self.df.loc[self.df[field] == val].values.tolist()[0]
        except AssertionError:
            return []

    def makeid_to_str(self, default='Id'):
        """
        将 uuid 转化为 大写的str
        :param default: 指定的字段名
        """
        try:
            self.add_columns(default, [str(i).upper() for i in self.df[default]])
        except TypeError:
            raise CustomError('%s 转换 str 失败! 请为该方法指定 default' % default)

    def show_kml(self):
        return self.kml_df.values.tolist()

    def show(self):
        print(self.df)
        print(self.df.columns.to_list())

    def show_headers(self):
        ls = self.df[:5].values.tolist()
        for i in ls:
            print(i)

    def show_titles(self):
        print(self.df.columns.to_list())

    def count(self):
        print('共有%d条数据。' % self.df.__len__())
        return self.df.__len__()
