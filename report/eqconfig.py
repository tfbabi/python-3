# coding:utf-8
from sqlalchemy import create_engine
# 日志对象
from utility import logger

#数据库连接
DB_CONN_STRING = 'oracle://m1:m2@10.10.10.10:1521/h2'

import datetime
days = 1
#CONF_ORDER_TIME = "to_date('2016-10-01','yyyy-mm-dd')+1 - 1"
CONF_ORDER_TIME = "sysdate -%s" % days

#设备状态
CONF_EQ_STATUS_DICT={'00':'可用(已注册)','01':'不可用(未注册)','02':'挂失','03':'注销','04':'作废'}

#活动类型
CONF_ACTIVITY_TYPE_DICT={'00':'采购','01':'0元赠机','02':'激活返机'}

#证件类型
CONF_CERT_TYPE_DICT={'1':'居民身份证','01':'居民身份证','2':'护照','3':'军官证','4':'回乡证'}

#身份证地区信息
CONF_REGION_DICT={ '11':'北京','12':'天津','13':'河北','14':'山西',
     '15':'内蒙古','21':'辽宁','22':'吉林','23':'黑龙江','31':'上海',
     '32':'江苏','33':'浙江','34':'安徽','35':'福建','36':'江西',
     '37':'山东','41':'河南','42':'湖北','43':'湖南','44':'广东',
     '45':'广西','46':'海南','50':'重庆','51':'四川','52':'贵州',
     '53':'云南','54':'西藏','61':'陕西','62':'甘肃','63':'青海',
     '64':'宁夏','65':'新疆','71':'台湾','81':'香港','82':'澳门',
     '91':'国外'}

#设备明细字典
CONF_EQDETAIL_DICT={'CSN':'设备CSN号',#hpay.terminal_info.csn
                    'SUPPLIER':'供应商', # dsi.SUPPLIER hpay.device_supplier_info dsi,hpay.terminal_info ti where dsi.supplier_code=ti.factory_no
                    'PERSONLIZED_TIME':'生产日期', #
                    'SALES_AREA':'销售区域',#暂不处理
                    'PROJECT_NUMBER':'项目编号', #hpay.t_sell_order tso ,hpay.terminal_info ti where tso.stock_number=ti.stock_number
                    'SELL_NUMBER':'销售单号', #hpay.t_sell_order
                    'PURCHASE_NUMBER':'采购单号', #hpay.t_sell_order
                    'STOCK_NUMBER':'个人化单号', #
                    'PROPOSER':'申请人',#暂不处理
                    'BUY_NAME':'买方名字',#暂不处理
                    'NAME':'产品名称',#select cd.name from hpay.constant_define cd where cd.code = (select t.product_name from hpay.t_sell_order t) and cd.projecttype = 'PRODUCTNAME'
                    'TYPE':'产品型号',
                    'PRODUCT_COLOUR':'产品颜色',#暂不处理
                    'ORDER_PRICE':'订单单价',#暂不处理
                    'SALES_PRICE':'销售价',#暂不处理
                    'ACTIVITY_TYPE':'活动类型',#采购；激活返机；0元购机；内部领用；客户送礼 00 - 采购
                    					#01 - 0元赠机
                    					#02 - 激活返机hpay.t_activity_terminal_info.securyity_code=hpay.terminal_info.securyity_code(+)
                    'DELIVERY_TIME':'发货时间',#暂不处理
                    'RECIVER':'收货人',#暂不处理
                    'RECIVER_PHONE':'收货人手机号',#暂不处理
                    'RECIVER_ADRESS':'收货地址',#暂不处理
                    'CREATE_DATE':'设备注册日期',#hpay.t_merchant_csn	tmc,hpay.terminal_info ti where ti.csn=tmc.csn(+)
                    'MINTIME':'设备第一次交易时间',#hpay.payment_transaction_log.min(paymenttransdate) csn做关联
                    'STATUS':'设备状态',#枚举值：00-可用(已注册) 01-不可用(未注册)，02-挂失，03-注销，04-作废，且显示中文hpay.terminal_info
                    'SECURITY_CODE':'银联防伪码',#
                    'AGENT_M':'监管代理商',#如果为空，取一级代理商信息select tai.id, tai.corp_full_name from hpay.t_agent_info tai where tai.parent_path = {一级代理商ID} and tai.status = '00'
                    'AGENT_1':'一级代理商',#
                    'AGENT_2':'二级代理商',#
                    'AGENT_3':'三级代理商',#
                    'AGENT_4':'四级代理商',#
                    'USER_REG_DATE':'用户注册日期',  # 
                    'USER_REG_YEAR':'用户注册日期-年',  # 
                    'USER_REG_MONTH':'用户注册日期-月',  # 
                    'USER_REAL_NAME':'用户姓名',  # 
                    'USER_MOBILE':'用户手机',  # 
                    'USER_CERT_TYPE':'证件类型',  # 
                    'USER_CERT_NUMBER':'证件号',  # 
                    'USER_REGION':'身份证地区',  # 到省份
                    'ACTIVE_TIME':'激活日期',
                    'DAY_TRADE_COUNT':'每日交易笔数',
                    'DAY_TRADE_AMOUNT':'每日交易金额',
                    'MONTH_TRADE_COUNT':'月交易笔数',
                    'MONTH_TRADE_AMOUNT':'月交易金额',
                    'QUARTER_TRADE_COUNT':'季度交易笔数',
                    'QUARTER_TRADE_AMOUNT':'季度交易金额',
                    'YEAR_TRADE_COUNT':'年交易笔数',
                    'YEAR_TRADE_AMOUNT':'年交易金额'}
