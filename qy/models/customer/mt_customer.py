# 客户表 迁移

from utils import comment_migrate

# 实例化传参 ---> 表名称
mt = comment_migrate.MigrateInterface('OneMapKeHus')
# 保存 id 关联关系
mt.save_id_ralation()
# 保存手机号关联关系
mt.save_infos('phone', mt.df['LianXiFangShi'].values.tolist())
# 共有数据多少条
mt.count()
# 打印列名
mt.get_title()
# 修改列名
new_field = ['Id', 'name', 'position', 'place', 'mobile', 'through_item',
             'address', 'through_user', 'KmlUrl', 'cursour_position', 'wechat', 'SuoZaiWeiZhi', 'Gender', 'addtime',
             'ImgUrl', 'KeHuType']
mt.change_titles_all(new_field)
drop_list = ['KmlUrl', 'ImgUrl', 'KeHuType', 'SuoZaiWeiZhi']
mt.drop_columns(drop_list)
# 增加列 sex
map_dict = {'男': '0', '女': '1', None: None}
mt.map_fields(map_dict, 'Gender', 'sex')
# 删除旧字段所在列
mt.drop_columns(['Gender'])

# 添加默认时间
map_dict = {None: '2020/1/1 00:00:00'}
mt.map_fields(map_dict, 'addtime', 'addtime')
mt.show()

# 插入 kml
mt.add_columns('cursour_position', mt.get_kml_data('LngLat'))

# 添加新表所需数据列和ID
mt.insert(0, 'is_delete', default=False)
mt.insert(0, 'email', default='')
mt.insert(0, 'kml_path', default='')
mt.insert(0, 'id', data_list=[i for i in range(1, mt.count() + 1)])
mt.drop_columns(['Id'])
mt.show()

mt.show_headers()  # 显示前5条

mt.start_migrate('classify_customer', ignore=True)

mt.set_maxid_seq('classify_customer')

mt.save_infos('errordict', mt.errorinfo)
