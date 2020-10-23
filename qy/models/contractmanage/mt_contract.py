# 合同表 迁移

from utils import comment_migrate

mt = comment_migrate.MigrateInterface('OneMapHeTongs')

mt.save_id_ralation()


new_field = ['Id', 'contract_no', 'contract_type', 'contract_name', 'contract_info', 'account_money', 'JinZhangShiJian',
             'ShengYuE', 'contract_num', 'acompany', 'acontant_person', 'bcompany',
             'bitem_leader', 'CunDangRen', 'note', 'signed_time', 'electronic_time',
             'SaoMiaoCunDangDate', 'KaiPiaoDate', 'KaiPiaoE', 'HeTongE', 'ZhiZhiCunDangDate', 'DianZiBan',
             'SaoMiaoJian', 'RecorderTime', 'ProjectName', 'ProjectNumber', 'Recorder', 'IsSignContract',
             'MarketPeople', 'Paperfile_loca', 'Paperfile_Peop', 'Paperfile_Date', 'scanfile_Date', 'scanfile_loca',
             'scanfile_Peop']

mt.change_titles_all(new_field)
mt.show()

HT_NUMBER = mt.get_field('contract_no')
print(HT_NUMBER)
mt.save_infos('htnum', HT_NUMBER)

HT_MONEY = mt.get_field('HeTongE')
print(HT_MONEY)
mt.save_infos('htmoney', HT_MONEY)
