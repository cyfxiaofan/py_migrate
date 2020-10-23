import datetime
import time
import pickle

from config import SELECT_KML_sql
from utils.comment_migrate import MigrateInterface

# s = pickle.load(open(r'C:\Users\Public\Nwt\cache\recv\葛梦飞\user_map.txt', 'rb'))
# print(s)

print('sdsadada'[:-2])

mt = MigrateInterface('Projects')

mt.sr.execute_modify_sql(SELECT_KML_sql.format(fieldname='ProjectLocation', tablename=mt.tablename))
s = mt.sr.cursor.fetchall()
# s1 = mt.sr.cursor.fetchone()
for i in s:
    print(i[:140])
# print(s1)
