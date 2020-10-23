# 项目表 迁移
import copy
import os

from config import SELECT_ID_sql, SELECT_sql, STATIC_FILE
from utils import comment_migrate
from utils.comment_exception import CustomError

mt = comment_migrate.MigrateInterface('Projects')

mt.save_id_ralation()


new_field = ['Id', 'item_name', 'item_number', 'responsible_dept',
             'item_content', 'item_place', 'begin_time', 'end_time',
             'itemleader_phone', 'itemleader_email', 'CustomerPhone',
             'CustomerEmail', 'ContractNumber', 'ContractName',
             'sj_contract_money', 'get_bill', 'Quality', 'Backup',
             'Handover', 'Recorder', 'recordtime', 'UpdateTime',
             'kml_path', 'ProjectLeaderName', 'CustomerName', 'item_type',
             'item_status', 'zhengli_status', 'company_qualification',
             'MarketPeople', 'ZhiDing', 'item_money', 'End_Submit_People',
             'End_Summarize', 'End_Zhixing', 'End_Quality', 'End_Qingdan',
             'Submit_Time', 'Submit_Department', 'KeHuDanWei', 'KeHuSex',
             'KeHuZhiWu', 'IsSignContract', 'RequsetTime', 'expect_signed_date', 'SubClass']

mt.change_titles_all(new_field)

drop_list = ['Quality', 'Backup', 'Handover',
             'UpdateTime', 'ZhiDing', 'End_Submit_People',
             'End_Summarize', 'End_Zhixing', 'End_Quality', 'End_Qingdan',
             'Submit_Time', 'Submit_Department', 'RequsetTime', 'SubClass']
mt.drop_columns(drop_list)

# 市场人员处理
MarkerPeople_list = mt.get_field('MarketPeople')
new_MarkerPeople_list = copy.copy(MarkerPeople_list)
# 校验无效用户
mr = comment_migrate.MigrateInterface('Users')
mr.get_data()
ID_list = mr.get_field('Id')
ID_list = [str(i).upper() for i in ID_list]
GH_index = mr.get_title().index('GongHao')
# 将Id_list 映射到新的数据上去
mr.add_columns('Id', ID_list)

wx_user = 0
wx_user_list = []
for index, i in enumerate(MarkerPeople_list):
    if i is not None and len(i) > 6:
        if i not in ID_list:
            wx_user += 1
            wx_user_list.append(i)
            raise CustomError('项目表中存在市场人员ID与现存用户表不匹配！')
        else:
            this_user = mr.df.loc[mr.df['Id'] == i].values.tolist()[0]
            this_user = this_user[GH_index]
            new_MarkerPeople_list[index] = this_user

print('一表转员工号 ---共有无效用户%d个' % wx_user)

PG_SQL_df = mt.get_sqldf(SELECT_sql.format(
    tablename='system_users'
))
U_index = PG_SQL_df.columns.values.tolist().index('id')

# 将员工号转化为对应ID
wx_user = 0
real_userlist = []
miss_userlist = []
insert_MarkerPeople_list = []
for i in new_MarkerPeople_list:
    if i is not None:
        real_userlist.append(i)
        this_user = PG_SQL_df.loc[PG_SQL_df['serial'] == i].values.tolist()
        if not this_user:
            wx_user += 1
            miss_userlist.append(i)
            this_user = None
        else:

            this_user = str((this_user[0][U_index]))
        insert_MarkerPeople_list.append(this_user)
        # print(this_user)
    else:
        insert_MarkerPeople_list.append(None)

print('项目表市场人员数据一共%d个' % len(real_userlist))
print('二表转员工号 ---共无效用户%d个' % wx_user, miss_userlist)

# 保存 无效员工号信息
mt.save_infos('errorYGH', list(set(miss_userlist)))

if (len(insert_MarkerPeople_list)) == len(new_MarkerPeople_list):
    print('转换MarkPeople校验通过')
else:
    raise CustomError('校验markPeople失败')

# 插入市场人员 并删除原数据
mt.add_columns('marketperson', insert_MarkerPeople_list)
mt.drop_columns(['MarketPeople'])
print('/*        市场人员已处理        */')

# 处理客户信息
phone_list = mt.read_file('OneMapKeHus', 'OneMapKeHus__phone')

# 无效信息统计
wx = 0
wx_list = []
bc = 0

# 客户信息获取索引
P_titlelist = mt.get_title()
P_x = [
    P_titlelist.index('CustomerName'),
    P_titlelist.index('CustomerPhone'),
    P_titlelist.index('CustomerEmail'),
    P_titlelist.index('KeHuSex'),
    P_titlelist.index('KeHuZhiWu'),
    P_titlelist.index('KeHuDanWei'),
    P_titlelist.index('item_name'),
    P_titlelist.index('marketperson'),
    P_titlelist.index('recordtime'),
]
# 新写入的phone 与其 pg id 映射关系
if os.path.exists(os.path.join(STATIC_FILE, 'Projects/Projects__newphone.txt')):
    reation_writed_customer = mt.read_file('Projects', 'Projects__newphone')
else:
    reation_writed_customer = {}


def check_none(phone):
    if phone is None or phone.isspace() or phone == '':
        return True
    return False


def get_id(index, phone):
    global wx, wx_list, bc
    try:
        if check_none(phone):
            raise ValueError
        # 校验手机号 ,校验存在该手机号的客户其迁移之后的ID是否与其索引值匹配
        r = check_index(phone_list.index(phone) + 1)
        if r[5] == phone:
            return phone_list.index(phone) + 1
        # 校验失败直接raise
        raise CustomError('校验手机号 %s 失败 **********' % phone)
    except ValueError:
        # 拿到无效的客户手机号，取出该条项目的行信息
        tr = mt.get_columns(index)
        # 先匹配映射关系
        if check_none(phone):
            if check_none(tr[P_x[0]]):
                return None
            else:
                # 更新无效信息
                wx += 1
                wx_list.append(phone)
                exist_id = reation_writed_customer.get(tr[P_x[0]], '')
                if exist_id:
                    bc += 1
                    # print('%s 映射关系匹配成功 ---> %s' % (str(tr[P_x[0]]), str(exist_id)))
                    return exist_id
        else:
            # 更新无效信息
            wx += 1
            wx_list.append(phone)
            exist_id = reation_writed_customer.get(phone, '')
            if exist_id:
                bc += 1
                # print('%s 映射关系匹配成功 ---> %s' % (str(phone), str(exist_id)))
                return exist_id

        # 客户表头
        curstomer_title_list = ['id', 'name', 'mobile', 'email', 'sex', 'position', 'place', 'through_item',
                                'through_user', 'addtime', 'is_delete']
        # 获取客户表maxid
        max_id = mt.get_maxid('classify_customer')
        # 从该项目信息中提取客户数据
        add_curstomer_val_list = [max_id,
                                  tr[P_x[0]],
                                  tr[P_x[1]] if not check_none(tr[P_x[1]]) else tr[P_x[0]],
                                  tr[P_x[2]] if tr[P_x[2]] is not None else '',
                                  tr[P_x[3]],
                                  tr[P_x[4]] if tr[P_x[4]] is not None else '',
                                  tr[P_x[5]] if tr[P_x[5]] is not None else '',
                                  tr[P_x[6]] if tr[P_x[6]] is not None else '',
                                  tr[P_x[7]] if tr[P_x[7]] is not None else '',
                                  str(tr[P_x[8]]) if str(tr[P_x[8]]) is not 'NaT' else '2020/1/1 00:00:00',
                                  False
                                  ]

        check_insert = mt.start_migrate(tablename='classify_customer',
                                        data_list=add_curstomer_val_list,
                                        title_list=curstomer_title_list,
                                        # 成功插入后就不需要第二次了，开启忽略，更新customer的id映射关系
                                        ignore=True
                                        )
        # 若插入成功 更新映射关系
        if check_insert:
            bc += 1
            if check_none(phone):
                reation_writed_customer[tr[P_x[0]]] = max_id
            else:
                reation_writed_customer[phone] = max_id
        return max_id


def check_index(ids):
    val = mt.get_sqldata(sql=SELECT_ID_sql.format(
        tablename='classify_customer',
        id=ids,
    ))
    return val[0]


CustomerPhone_list = mt.get_field('CustomerPhone')

new_CustomerPhone_list = [
    get_id(index, k) for index, k in enumerate(CustomerPhone_list)
]

# 保存所更新phone映射关系 此处必须在 客户信息补充完毕且上方忽略关系关闭的时候执行
mt.save_infos('newphone', reation_writed_customer)

print('共%d个无效手机号，重复项%d个,已补充客户信息%d个,剩余%d个' % (
    wx, len(wx_list) - len(set(wx_list)), bc - (len(wx_list) - len(set(wx_list))),
    wx - bc))

# 新增 customer 同时删除原客户
mt.add_columns('customer_id', new_CustomerPhone_list)
mt.drop_columns(['CustomerPhone', 'CustomerEmail', 'KeHuDanWei', 'KeHuSex', 'KeHuZhiWu', 'CustomerName'])
print('/*        客户信息已处理        */')

# 处理责任部门
mt.show()
response_dept_list = mt.get_field('responsible_dept')
new_dept_list = []
mdept = comment_migrate.MigrateInterface('Organizes')
mdept.get_data()
mdept.show()
mdept.makeid_to_str()

for ev_i in response_dept_list:
    if '-' in ev_i:
        if ',' not in ev_i:
            tr = mdept.get_row('Id', ev_i.upper())
            new_dept_list.append(tr[2])
        else:
            this_field_list = []
            for e_i in ev_i.split(','):
                tr = mdept.get_row('Id', e_i.upper())
                this_field_list.append(tr[2])
            new_dept_list.append(','.join(this_field_list))
    else:
        new_dept_list.append(ev_i)

mt.add_columns('responsible_dept', new_dept_list)

# 处理合同信息
"""
wx_ht = 0
wx_ht_list = []
wx_htmoney = 0
wx_htmoney_list = []
title_list = mt.get_title()
money_index = title_list.index('sj_contract_money')
HT_list = mt.get_field('ContractNumber')
print(HT_list)
haveHT_list = mt.read_file('OneMapHeTongs', 'OneMapHeTongs__htnum')
haveMoney_list = mt.read_file('OneMapHeTongs', 'OneMapHeTongs__htmoney')
print(haveHT_list)
pass_list = [None, '否', '无', '内部', '']

for index, i in enumerate(HT_list):
    if i not in haveHT_list and i not in pass_list:
        wx_ht += 1
        wx_ht_list.append(i)
    else:
        if i not in pass_list:
            this_money = mt.get_columns(index)[money_index]
            R_index = haveHT_list.index(i)
            print(this_money, haveMoney_list[R_index])
            if this_money != haveMoney_list[R_index]:
                wx_htmoney += 1
                wx_htmoney_list.append(i)

new_list = []
for i in HT_list:
    if i not in pass_list:
        new_list.append(i)


def check_same(ls):
    ls2 = []
    same_list = []
    for i in ls:
        if i not in ls2:
            ls2.append(i)
        else:
            same_list.append(i)
    return same_list


print('一个合同对多个项目的合同有%d个' % (len(new_list) - (len(set(new_list)))))
print('合同编号为：', check_same(new_list))

print('无效合同信息%d个，其中重复的合同%d个' % (wx_ht, wx_ht - len(list(set(wx_ht_list)))))
print('无效的合同编号：', wx_ht_list)

print('项目中合同额与合同管理合同额不对应信息%d个' % wx_htmoney)
print('无效的合同额其合同编号：', wx_htmoney_list)
# 处理合同额信息
"""


# 处理映射关系方法
def make_reation(field):
    """
    :param field: 获取映射关系的字段
    :return: map_dict
    """
    try:
        sql_df = mt.get_sqldf(SELECT_sql.format(
            tablename='system_users'
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
        raise CustomError('%s' % e)


# 处理项目负责人 外键
# 通过手机号映射 关联 其 Userid
Itemleader_map = make_reation('tel')
mt.map_fields(Itemleader_map, 'itemleader_phone', 'itemleader_id')
mt.drop_columns(['ProjectLeaderName'])

# 处理 Recorder
Recoder_map = make_reation('name')
mt.map_fields(Recoder_map, 'Recorder', 'record_user_id')
mt.drop_columns(['Recorder'])

# 处理 IsSignContract
IsSignContract_map = {
    0: '未签',
    1: '已签'
}
mt.map_fields(IsSignContract_map, 'IsSignContract', 'iscontract')
mt.drop_columns(['IsSignContract'])

# 清除合同信息
mt.drop_columns(['ContractNumber', 'ContractName', 'sj_contract_money'])

# 把实际合同额全部重设为 0
mt.insert(0, 'sj_contract_money', default=0)

# 处理kml
mt.add_columns('item_place', mt.get_kml_data('ProjectLocation'))

# 删除Id
mt.add_columns('id', [i for i in range(1, mt.count() + 1)])
mt.drop_columns(['Id'])

# 处理Timestape
mt.make_timetostr('recordtime')
mt.make_timetostr('expect_signed_date')

# 处理None
mt.make_nantostr('itemleader_email')

# 处理null
mt.make_nulltodefault('end_time')
mt.make_nulltodefault('begin_time')

# 修理时间
mt.repare_field('begin_time', 4, '-01-01')
mt.repare_field('end_time', 4, '-01-01', special=['至今', '未结束'])

# 处理到账额
mt.make_typetodefault('get_bill', 'float')

# 补充必填
mt.insert(0, 'is_delete', default=False)

# 设置项目状态为 已立项
mt.insert(0, 'status', default=4)

mt.start_migrate('item_item', ignore=True)
