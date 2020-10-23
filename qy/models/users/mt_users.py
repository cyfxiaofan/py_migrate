# 用户表迁移
from utils import comment_migrate

mt = comment_migrate.MigrateInterface('Users')
mt.save_id_ralation()
"""
['Id', 'Name', 'BirthDate', 'IsZhuanZheng', 'ZhuanZhengDate', 'IdCard', 
'IdCardAddress', 'IdCardCertificate', 'CurrentAddress', 'FamilyPthone', 
'IsLabour', 'EducationLevel', 'GraduateSchool', 'Email', 'GraduationCertificate', 
'BankCard', 'Entrydate', 'IsQuit', 'QuitDate', 'Password', 'Telephone', 'RealName', 
'GongHao', 'DiQu', 'DangAnBianHao', 'KaoQinHao', 'ELEELaoWuHeTong', 'BaoMiXieYi', 
'JiuYeXieYi', 'SuoXueZhuanYe', 'BiYeShiJian', 'SuoZaiGongSi', 'Enabled', 'IsDelete', 
'ZhuZhiKml', 'JiGuanKml', 'ZhuZhiLngLat', 'JiGuanLngLat', 'BanShenPic', 'QuanShenPic', 
'Sex', 'Nationality', 'marriage', 'Emergencye', 'EmergencyPhone', 'EdStartTime', 
'EdStartTime1', 'EdStartTime2', 'EdEndTime', 'EdEndTime1', 'EdEndTime2', 'JiuDuSchool', 
'JiuDuSchool1', 'JiuDuSchool2', 'SuoXueZhuanYe1', 'SuoXueZhuanYe2', 'jobStartTime', 
'jobStartTime1', 'jobStartTime2', 'jobEndTime', 'jobEndTime1', 'jobEndTime2', 'DanWeiName', 
'DanWeiName1', 'DanWeiName2', 'DanRenZhiWu', 'DanRenZhiWu1', 'DanRenZhiWu2', 'FName', 
'FName1', 'FName2', 'ChengWei', 'ChengWei1', 'ChengWei2', 'FZhiWu', 'FZhiWu1', 
'FZhiWu2', 'FPhone', 'FPhone1', 'FPhone2', 'SheBaoGongSi', 'YiWaiXianGongSi']
"""

new_field = ['Id', 'username', 'birthday', 'regular', 'regular_date', 'identity_card', 'native_place',
             'card_image', 'now_address', 'FamilyPthone', 'IsLabour', 'max_education', 'graduate_sc',
             'email', 'graduate_image', 'bank_card', 'date_joined', 'IsQuit', 'dimission_date', 'Password', 'tel',
             'name', 'serial', 'DiQu', 'DangAnBianHao', 'KaoQinHao', 'service_contract', 'nda', 'employment_deal',
             'major', 'date_graduate', 'SuoZaiGongSi', 'Enabled', 'IsDelete', 'ZhuZhiKml', 'JiGuanKml',
             'ZhuZhiLngLat', 'JiGuanLngLat', 'half_image', 'all_image', 'Sex', 'nation', 'matrimony',
             'emergency_contact', 'contact_tel', 'EdStartTime', 'EdStartTime1', 'EdStartTime2', 'EdEndTime',
             'EdEndTime1',
             'EdEndTime2', 'JiuDuSchool', 'JiuDuSchool1', 'JiuDuSchool2', 'SuoXueZhuanYe1', 'SuoXueZhuanYe2',
             'jobStartTime', 'jobStartTime1', 'jobStartTime2', 'jobEndTime', 'jobEndTime1', 'jobEndTime2', 'DanWeiName',
             'DanWeiName1', 'DanWeiName2', 'DanRenZhiWu', 'DanRenZhiWu1', 'DanRenZhiWu2', 'FName', 'FName1', 'FName2',
             'ChengWei', 'ChengWei1', 'ChengWei2', 'FZhiWu', 'FZhiWu1', 'FZhiWu2', 'FPhone', 'FPhone1', 'FPhone2',
             'social_company', 'insurance_company', 'QuitNote']

mt.change_titles_all(new_field)
mt.drop_columns(['FamilyPthone', 'EdStartTime', 'EdStartTime1', 'EdStartTime2', 'EdEndTime',
                 'EdEndTime1', 'DiQu', 'DangAnBianHao', 'KaoQinHao', 'IsLabour',
                 'EdEndTime2', 'JiuDuSchool', 'JiuDuSchool1', 'JiuDuSchool2', 'SuoXueZhuanYe1', 'SuoXueZhuanYe2',
                 'jobStartTime', 'jobStartTime1', 'jobStartTime2', 'jobEndTime', 'jobEndTime1', 'jobEndTime2',
                 'DanWeiName',
                 'DanWeiName1', 'DanWeiName2', 'DanRenZhiWu', 'DanRenZhiWu1', 'DanRenZhiWu2', 'FName', 'FName1',
                 'FName2',
                 'ChengWei', 'ChengWei1', 'ChengWei2', 'FZhiWu', 'FZhiWu1', 'FZhiWu2', 'FPhone', 'FPhone1', 'FPhone2'])

# 映射修改isdelete
map_isdelete_dict = {
    0: 0,
    1: 2,
    2: 1
}

mt.map_fields(map_isdelete_dict, 'IsDelete', 'IsDelete')

Quit_list = mt.get_field('IsQuit')
Dele_list = mt.get_field('IsDelete')

# 员工状态  在职0，请假1，离职2
status_list = [2 if Quit_list[i] is True or Dele_list[i] == 2 else Dele_list[i] for i in range(len(Quit_list))]

mt.add_columns('status', status_list)
mt.drop_columns(['IsQuit', 'IsDelete'])
mt.makeid_to_str()
mt.show()
mt.show_headers()

frame_mt = comment_migrate.MigrateInterface('Organizes')
frame_mt.get_data()
frame_mt.show()
