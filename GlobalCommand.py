# GlobalCommand.py
# add by lixiaoqing 2019-4-18
# 主要定义对数据库的操作
#
querySql="select * from query.T_LXQ_TEST_RATE_PRICE"

batchSql='''insert into bill.T_LXQ_TEST_RATE_PRICE  
(mdn, local_net, roam_type, fee_item, item_fee_code, comments, list, stand_price ) 
values ( :mdn, :local_net, :roam_type, :fee_item, :item_fee_code, :comments, :list, :stand_price )'''
