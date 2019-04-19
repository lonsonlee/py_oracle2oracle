import sys,os
# ReportSumLuncher.py
# add by lixiaoqing 2019-4-17
# 主启动类
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from DBOperator import DBOperator as DBDeal
import ConfigGlobal as config
import GlobalCommand as dbcmd
print("rootparh="+rootPath)
print(config.dbUserName+"**"+config.dbUserPassword+"**"+config.dbName+"**"+config.dbHostAddress+"**"+config.dbHostPort)
deal=DBDeal()
conn = deal.conn_ora(config.dbUserName,config.dbUserPassword,config.dbName,config.dbHostAddress,config.dbHostPort)
cursor= deal.open_cursor()

all_data = deal.db_query(cursor,dbcmd.querySql)
if config.logLevel == 'ALL':
    print(all_data)
if config.logLevel == 'DEBUG':
    for item in  all_data:
         print(item)
# NUM=deal.db_insert_batch(cursor,dbcmd.batchSql,all_data)
# print(NUM)

for item in  all_data:
    print(item)
    deal.db_insert_single(cursor,dbcmd.batchSql,item)
DBDeal.dis_cursor(deal,cursor)
DBDeal.dis_conn(deal)

