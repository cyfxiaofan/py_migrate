# 通用迁移模块

import os
import datetime
from geoalchemy2 import Geometry, WKTElement
from config import SELECT_sql, STATIC_FILE, INSERT_COLUMNS_sql, SELECT_KML_sql, SET_Maxid_sql, regex_list, \
    SELECT_MAXID_sql, UPDATE_sql, UPDATEALL_sql
import pandas as pd
import numpy as np
from utils import pg_sqlhelper, sr_sqlhelper
from utils.comment_exception import CustomError
import copy


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

    def get_data_pg(self):
        """
        获取 pg 的 Dataframe
        :return: sql执行结果 dataframe
        """
        self.df = pd.read_sql(SELECT_sql.format(tablename=self.tablename), con=self.pg.conn)

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
        try:
            return self.df[field].values.tolist()
        except FutureWarning:
            return self.df[field].values.tolist()

    def get_field_replace(self, field, replace=None, default=None):
        """
        获取指定字段列表 并处理
        :param replace: 默认不开启特殊替换
        :param default: 默认替换为空
        :param field: 字段名
        :return: 指定的字段列表
        """
        if replace is None:
            return self.df[field].replace(np.nan, '').values.tolist()
        else:
            return self.df[field].replace(np.nan, '').replace(replace, default, regex=True).values.tolist()

    def format_time(self, _field, special=None):
        """
        格式化时间
        :param special: 是否开启特殊匹配 指定spacial字典 格式： {'特殊字符':'替换的字符'}
        :param _field: 需要格式化的时间字段
        """
        if special is not None:
            _Time = self.get_field(_field)
            for index, _ev in enumerate(_Time):
                if _ev in special:
                    _Time[index] = special[_ev]
            self.add_columns(_field, _Time)
        _Time = self.get_field_replace(_field, replace=['/', r'\.', '年', '月', '日', '号'],
                                       default=['-', '-', '-', '-', '', ''])
        new_A = []
        for _i in _Time:
            if not self.check_none(_i):
                if _i[-1] == '-':
                    new_A.append(_i[:-1])
                else:
                    new_A.append(_i)
            else:
                new_A.append(_i)
        _new_T = []
        for i in new_A:
            if not self.check_none(i):
                if len(i) < 5:
                    _new_T.append(i + '-1-1')
                elif 5 < len(i) < 8:
                    _new_T.append(i + '-1')
                else:
                    _new_T.append(i)
            else:
                _new_T.append(i)
        self.add_columns(_field, _new_T)

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

    def map_fields(self, map_dict, oldfield, newfield, drop=False):
        """
        :param drop: 默认不删oldfield
        :param map_dict: 一个映射字典或是自定义函数
        :param oldfield: 需要映射匹配的字段
        :param newfield: 添加的新字段
        :return: None
        """
        _li = eval('self.df.{}.map'.format(oldfield))(map_dict)
        self.df[newfield] = _li
        if drop:
            self.drop_columns([oldfield])

    def add_columns(self, field, data, drop=None, flag=False):
        """
        添加一列
        :param flag: 是否删除多条
        :param drop: 是否指定删除某字段 默认不删
        :param field: 列名
        :param data: 列数据
        """
        try:
            self.df[field] = data
        except Exception as e:
            raise CustomError('增加列错误：%s' % e)

        if drop is not None:
            try:
                self.drop_columns([drop] if not flag else [drop])
            except Exception as e:
                raise CustomError('删除指定字段出错，错误信息：%s' % e)

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
        return self.df.replace(np.nan, None).replace(regex_list()[0], regex_list()[1], regex=True).values.tolist()

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

    def update_field(self, field, val, _id=None, many=False, index=None):
        """
        执行更新sql
        :param index: 开启计数
        :param many: 是否为批量更新
        :param field: 更新的字段名
        :param val: 更新的值
        :param _id: 更新的的id
        :return:
        """
        update = UPDATEALL_sql if many else UPDATE_sql
        try:
            self.pg.execute_modify_sql(
                update.format(
                    tablename=self.tablename,
                    fieldname=field,
                    val=val,
                    id=_id
                )
            )
            self.pg.commit()
            print('更新成功') if not index else print('第%d条数据更新成功！' % index)
            return True
        except Exception as e:
            self.pg.rollback()
            raise CustomError('更新失败， 错误原因：%s' % e)

    def set_maxid_seq(self, tablename):
        """
        将id索引设置成最大 此方法在数据迁完时候  *****必须要执行*****
        :param tablename: pg 的表名
        """
        self.pg.execute_modify_sql(SET_Maxid_sql.format(tablename=tablename))
        self.pg.commit()
        print('%s 表 索引已更新' % tablename)

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
        repare字段
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

    @classmethod
    def check_none(cls, _str):
        """
        检验 _str 是否为空 或 None
        :param _str:
        :return: bool
        """
        if _str is None or _str.isspace() or _str == '':
            return True
        return False

    def get_row_field(self, index, field, val=None, update=False):
        """
        获取指定列指定字段的值
        :param val: 更新的指定值
        :param update: 是否更新该字段
        :param index: 索引
        :param field: 字段
        :return: 值
        """
        _index = self.get_title().index(field)
        if not update:
            try:
                return self.get_columns(index)[_index]
            except Exception as e:
                raise CustomError('获取指定数据错误。错误信息：%s' % e)
        else:
            try:
                self.df.loc[index, field] = val
                return self.get_columns(index)[_index]
            except Exception as e:
                raise CustomError('更新指定位置数据错误。错误信息：%s' % e)

    @classmethod
    def check_type(cls, _str, _type):
        """
        检测字符是否符合类型
        :param _str: 检测字符串
        :param _type: 检测类型
        :return: True or False
        """
        _type = eval(_type)
        try:
            _type(_str)
            return True
        except ValueError:
            return False

    def make_reationtouser(self, field, default='system_users'):
        """
        与 user表建立映射返回map_dict
        :param default: 若未指定则默认用户表
        :param field: 获取映射关系的字段
        :return: map_dict
        """
        try:
            sql_df = self.get_sqldf(SELECT_sql.format(
                tablename=default
            ))
            assert sql_df is not None
            new_userid_list = sql_df['id'].values.tolist()
            new_field_list = sql_df[field].values.tolist()
            assert new_field_list is not None
            map_dict = {}
            for _index in range(len(new_userid_list)):
                map_dict[str(new_field_list[_index])] = new_userid_list[_index]
            return map_dict
        except AssertionError as e:
            raise CustomError('与User表建立映射关系出错。错误：%s' % e)

    def change_id(self):
        self.add_columns('id', [i for i in range(1, self.count() + 1)], drop='Id')

    @classmethod
    def copy(cls, _list):
        return copy.copy(_list)

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
