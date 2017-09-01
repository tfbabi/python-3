# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 14:36:05 2016
以terminal_info表为基础来做扩展
@author: trsenzhang
"""

import os 
import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
#import tushare as ts   
import sys
import gc
from mail import Mail
import time

#from matplotlib.pyplot import axis 
import datetime
from pip import status_codes
from cert import CERT_ZONE 
from eqconfig import CONF_ORDER_TIME,DB_CONN_STRING
from eqconfig import CONF_EQ_STATUS_DICT,CONF_REGION_DICT,CONF_EQDETAIL_DICT,CONF_CERT_TYPE_DICT,CONF_ACTIVITY_TYPE_DICT
from utility import logger,AESCipher
from pandas import Series, DataFrame
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


reload(sys)
sys.setdefaultencoding('utf-8') 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
CUR_PATH = os.path.dirname(os.path.realpath(__file__))

#生成execl文件或者csv文件
def saveToExcel(df):
    df_cols = []
    import re
    now = datetime.datetime.now()
    m=re.match('.*(\d+)',CONF_ORDER_TIME)
    days = 1
    if m:
        days = int(m.groups()[0])
    oneday = datetime.timedelta(days=days)
    target_date = now - oneday
    target_date = target_date.strftime("%Y%m%d")
    if not os.path.exists('%s/report' % CUR_PATH):
        os.mkdir('%s/report' % CUR_PATH)
    fileName = '%s/report/eqdetail-%s.xlsx' % (CUR_PATH,target_date)
    report_col_dict = {} 
    for k, v in CONF_EQDETAIL_DICT.items():
        report_col_dict[k.lower()] = v    
    for k in df.columns:
        if report_col_dict.has_key(k):
            df_cols.append(report_col_dict[k])
        else:
            #df_cols.append('unTitled')
            df_cols.append('unTitled-%s' % k)
            logger.error('异常,标题字段不存在,字段名称:%s' % k)
    df.columns = df_cols
    excel = df
    excel.columns = df_cols
    # print fileName
    if os.path.exists(fileName):
        os.remove(fileName)
    excel.to_excel(fileName)
    #df.to_csv(fileName)
    

#供应商名称
SUPPLIER_DICT = {}
def GetSupplierDict():
    sql = '''select SUPPLIER,SUPPLIER_CODE from hpay.device_supplier_info'''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql,engine)
    pd_result.index = pd_result.supplier_code
    pd_result['supplier'] = pd_result['supplier'].replace('^merAbbr=','',regex=True)
    pd_result = DataFrame.to_dict(pd_result)
    return pd_result['supplier']

def GetSupplier(supplier_code):
    if SUPPLIER_DICT.has_key(supplier_code):
        return SUPPLIER_DICT[supplier_code]
    #无法在device_supplier_info找到supplier_code信息，此表99%的supplier为空
    return '供应商不存在'

#单号信息::::项目编号\销售单号\采购单号
def getSellOrder():
    sql = '''select stock_number as stock_number1
        ,sell_number as sell_number1
        ,project_number as project_number1
        ,purchase_number as purchase_number1
            from hpay.t_sell_order
                where stock_number is not null'''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql,engine)
    return pd_result

#获取设备状态
def getStatus(status):
    if CONF_EQ_STATUS_DICT.has_key(status):
        return CONF_EQ_STATUS_DICT[status]
    #无法获取设备状态在CONF_EQ_STATUS_DICT字典里
    return '设备状态不存在'

#获取证件类型
def getCertType(u_cert_type):
    if CONF_CERT_TYPE_DICT.has_key(u_cert_type):
        return CONF_CERT_TYPE_DICT[u_cert_type]
    #无法获取设备状态在CONF_EQ_STATUS_DICT字典里
    return '证件类型不存在'
    
#获取活动类型
def getActivityType(act_type):
    if CONF_ACTIVITY_TYPE_DICT.has_key(act_type):
        return CONF_ACTIVITY_TYPE_DICT[act_type]
    #无法获取CONF_ACTIVITY_TYPE_DICT字典里
    return '-'
    
#获取产品名称
def getProductName():
    sql = '''
    select t.stock_number as stock_number1, c.name as name1
        from hpay.t_sell_order t, hpay.constant_define c where stock_number is not null
            and c.code(+) = t.product_name
                and c.projecttype = 'PRODUCTNAME'
            '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool) 
    return pd.read_sql_query(sql,engine)

#获取用户信息
def getUserInfo():
    #这里排除了在t_merchant_csn里不存在的记录
    sql = '''   
    select
       createtime as u_create_time,
       replace(replace(realname, CHR(10), ''), chr(13), '') as u_realname,
       mobile as u_mobile,
       CERTIFICATETYPE as u_cert_type,
       CERTIFICATENUMBER as u_cert_num,
       merchantcode as u_merch_code
  from hpay.user_info
 where realname is not null
   and merchantcode not in
       ('T0001', 'Z00CMBC011304000041', 'H0001', 'Z00HPAY011304000003')   
    '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool) 
    return pd.read_sql_query(sql,engine)

#获取注册时间 
def getMerCsn():
    #这里对下面四个测试的csn进行排除，因为一个csn有多条记录
    sql = '''
        select csn,merchantcode,create_date  as c_create_date from hpay.t_merchant_csn 
        where csn not in('07975D677643402F0100',
        '074C8C3210E5E9310100',
        'C164CECB0237450E0100',
        '92A108D55743E6490100')'''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)
      
#Architecture function
def getEqdetail():
    '''获取设备明细信息'''
    sql = """
      select CSN,
        factory_no SUPPLIER,
        PERSONLIZED_TIME,
        '-' SALES_AREA,
        '' PROJECT_NUMBER,
        '' SELL_NUMBER,
        '' PURCHASE_NUMBER,
        STOCK_NUMBER,
        '-' PROPOSER,
        '-' BUY_NAME,
        '' NAME,
        TYPE,
        '-' PRODUCT_COLOUR,
        '-' ORDER_PRICE,
        '-' SALES_PRICE,
        '' ACTIVITY_TYPE,
        '-' DELIVERY_TIME,
        '-' RECIVER,
        '-' RECIVER_PHONE,
        '-' RECIVER_ADRESS,
        '' CREATE_DATE,
        '' MINTIME,
        STATUS,
        SECURITY_CODE,
        AGENT_INFO,
        '' AGENT_M,
        '' AGENT_1,
        '' AGENT_2,
        '' AGENT_3,
        '' AGENT_4,
        '' USER_REG_DATE,
        '' USER_REG_YEAR,
        '' USER_REG_MONTH,
        '' USER_REAL_NAME,
        '' USER_MOBILE,
        '' USER_CERT_TYPE,
        '' USER_CERT_NUMBER, 
        '' USER_REGION,
        '' ACTIVE_TIME,
        '' DAY_TRADE_COUNT,
        '' DAY_TRADE_AMOUNT,
        '' MONTH_TRADE_COUNT,
        '' MONTH_TRADE_AMOUNT,
        '' QUARTER_TRADE_COUNT,
        '' QUARTER_TRADE_AMOUNT,
        '' YEAR_TRADE_COUNT,
        '' YEAR_TRADE_AMOUNT from hpay.terminal_info
        --where rownum<=120000
        --and security_code like 'Q8NL01%'
        """
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql, engine)

def getSubDate(date_time, date_type):
    '''日期切割''' 
    if not date_time:
        return '-'
    if str(date_time) == 'nan':
        return '-'
    try:        
        if date_type == 'year':
            return date_time[:4]
        if date_type == 'month':
            return date_time[4:6]
        if date_type == 'day':
            return date_time[6:8]
        if date_type == 'hour':
            return date_time[8:10]
    except Exception as e:
        logger.error('异常 格式化时间失败,异常信息: %s' % str(e))
        logger.error('输入参数 date_time:%s,date_type:%s' % (date_time, date_type))
        return '无效日期,无法切割'        
    return date_time

def getCerAddr(cer,cer_type):
    '''身份正切割''' 
    if not cer:
        return '-'
    if str(cer) == 'nan':
        return '-'
    try:        
        if cer_type == 'pro':
            try:
                if CONF_REGION_DICT.has_key(cer[:2]):
                    cer1 = cer[:2]
                    return CONF_REGION_DICT[cer1]
            except Exception as e:
                logger.error('身份证数据字典无相关信息: %s' % str(e))
                logger.error('输入参数cer[:2]:%s' % cer[:2])
                return '身份证地区信息不存在'
    except Exception as e:
        logger.error('异常 身份证号,异常信息: %s' % str(e))
        logger.error('输入参数 cer:%s,date_type:%s' % (cer, cer_type))
        return '无效身份证号,无法切割'        
    return cer

def getSVAgent():
    # 获取监管代理商信息
    sql = '''
    select a.id,
       a.corp_full_name,
       (select b.id from hpay.t_agent_info b where b.id = a.parent_path) as sv_id,
       (select b.corp_full_name
         from hpay.t_agent_info b
         where b.id = a.parent_path) as sv_name
    from hpay.t_agent_info a
    where type = 'L1'
          and sv_status = '00'
    '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql, engine)

    
def loadAgentInfo():
    sql = '''
    select tai.id,replace(tai.corp_full_name,',','|') as corp_full_name from hpay.t_agent_info tai
    '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql, engine)

def getAgentNameByID(agentid):
    if agentid == '-':
        return '-'
    if not agentid:
        return '-'
    if str(agentid) == 'nan':
        return '-'
    aid = int(agentid)
    agent = AGENTINFO[(AGENTINFO.id == aid)]
    if agent.empty:        
        return '无对应代理商信息:%s' % (aid)
    return agent['corp_full_name'].values[0]
    
def getAgentByLevel(info, level):
    # 根据字符串获取对应级别的代理商代码
    if not info:
        return '-'
    if info[0] == '|':
        info = info[1:]
    agents = info.split('|')
    if len(agents) < level:
        return '-'
    agent = '-'
    try:
        agent = int(agents[level - 1])
    except Exception as e:
        logger.error('异常:无法获取代理商')
        logger.error('异常信息:%s' % str(e))
        logger.error("参数 info:%s level:%s" % (info, level))
    return agent  

def getSVAgentName(agentID):
    # 获取agentID对应的监管代理商
    if not agentID:
        return '-'
    if agentID == '-':
        return '-'   
    agentName = '-'
    try:
        svAgent = SV_AGENT[SV_AGENT.id == agentID]
        if svAgent.empty:
            return getAgentNameByID(agentID)
        agentName = svAgent.sv_name.values[0]
    except Exception as e:
        return agentName
    return agentName

def getActiveType():
    sql = '''
    select security_code as sec_code1,activity_type as activity_type1 from hpay.t_activity_terminal_info
    '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql, engine)

def getPlogDayCountSum(flat):
    sql = '''
    select accounttranstime,csn,paymenttransseq,paymentamount
        from hpay.payment_transaction_log
            where accounttranstime = to_char(sysdate-1,'yyyymmdd') and csn is not null and csn <>'null'
    '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql,engine)
    pd1 = pd_result['paymenttransseq'].groupby(pd_result['csn']).count()
    pd2 = pd_result['paymentamount'].groupby(pd_result['csn']).sum()
    if flat == 1:
        return pd1
    else:
        return pd2


def getCsn():
    sql = '''
    select csn as p_csn,min(paymenttransdate) as p_min_time
    from hpay.payment_transaction_log where 
    csn is not null  group by csn
    '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql, engine)
    return pd_result

    
if __name__ == '__main__':
    gc.collect()
    aes = AESCipher()
    
    
    logger.info('开始')
    #获取设备信息
    logger.info('设备信息加载 开始')
    eqdetail = getEqdetail()
    eqdetail['stock_number'] = eqdetail['stock_number'].fillna('-')
    logger.info('设备信息加载 完成')    
    
    #供应商处理
    logger.info('供应商处理 开始')
    SUPPLIER_DICT = GetSupplierDict()
    eqdetail['supplier'] = eqdetail.apply(lambda row: GetSupplier(row['supplier']), axis=1)
    eqdetail['supplier'] = eqdetail['supplier'].fillna('-')
    logger.info('供应商处理 完成')
    
    #单号信息::::项目编号\销售单号\采购单号
    logger.info('单号信息加载 开始')
    sellorder = getSellOrder()
    sellorder.index = sellorder.stock_number1
    logger.info('单号信息加载 完成')
    
    logger.info('单号信息处理 开始')
    #因为terminal_info表的stock_number为null，默认为null没有做相应处理
    eqdetail = pd.merge(eqdetail,sellorder,left_on='stock_number',right_on='stock_number1',how='left')
    eqdetail['sell_number'] = eqdetail['sell_number1'].fillna('-')
    eqdetail['project_number'] = eqdetail['project_number1'].fillna('-')
    eqdetail['purchase_number'] = eqdetail['purchase_number1'].fillna('-')
    del eqdetail['stock_number1']
    del eqdetail['sell_number1']
    del eqdetail['project_number1']
    del eqdetail['purchase_number1']
    del sellorder
    gc.collect()
    logger.info('单号信息处理 完成')
    
    logger.info('设备状态加工 开始')
    eqdetail['status'] = eqdetail.apply(lambda row: getStatus(row['status']), axis=1)
    logger.info('设备状态加工 完成')

    logger.info('用户信息加载 开始')
    userinfo = getUserInfo()
    userinfo.index = userinfo.u_merch_code
    logger.info('用户信息加载完成 完成')
    
    logger.info('注册设备日期加载 开始')
    mercsn = getMerCsn()
    mercsn.index = mercsn.csn
    logger.info('注册设备日期加载 完成')
    
    logger.info('用户信息机注册设备日期处理1 开始')
    mercsn = pd.merge(mercsn,userinfo,left_on='merchantcode',right_on='u_merch_code',how='left')
    del mercsn['merchantcode']
    logger.info('用户信息机注册设备日期处理2 开始')
    eqdetail = pd.merge(eqdetail,mercsn,on='csn',how='left')
    eqdetail['user_reg_date'] = eqdetail['u_create_time'].fillna('-')
    eqdetail['user_reg_year'] = eqdetail.apply(lambda row:getSubDate(row['u_create_time'],'year'), axis=1)
    eqdetail['user_reg_month'] = eqdetail.apply(lambda row:getSubDate(row['u_create_time'],'month'), axis=1)
    eqdetail['user_real_name'] = eqdetail.apply(lambda row:aes.decrypt(row['u_realname']),axis=1)
    eqdetail['user_mobile'] = eqdetail.apply(lambda row:aes.decrypt(row['u_mobile']),axis=1)
    eqdetail['user_cert_type'] = eqdetail.apply(lambda row:getCertType(row['u_cert_type']), axis=1)
    eqdetail['user_cert_number'] = eqdetail.apply(lambda row:aes.decrypt(row['u_cert_num']),axis=1)
    eqdetail['user_region'] = eqdetail.apply(lambda row:getCerAddr(row['user_cert_number'],'pro'),axis=1)
    eqdetail['create_date'] = eqdetail['c_create_date'].fillna('-')
    eqdetail['user_reg_year'] =  eqdetail['user_reg_year'].fillna('-')
    eqdetail['user_reg_month'] = eqdetail['user_reg_month'].fillna('-') 
    eqdetail['user_real_name'] = eqdetail['user_real_name'].fillna('-')
    eqdetail['user_mobile'] = eqdetail['user_mobile'].fillna('-')
    eqdetail['user_cert_type']  = eqdetail['user_cert_type'].fillna('-')
    eqdetail['user_cert_number'] = eqdetail['user_cert_number'].fillna('-')
    
    del eqdetail['u_create_time']
    del eqdetail['u_realname']
    del eqdetail['u_mobile']
    del eqdetail['u_cert_type']
    del eqdetail['u_cert_num']
    del eqdetail['u_merch_code']
    del eqdetail['c_create_date']
    gc.collect()
    logger.info('用户信息机注册设备日期处理 完成')
    
    logger.info('加载监管代理商  开始')
    SV_AGENT = getSVAgent()
    logger.info('加载监管代理商  完成') 
    
    logger.info('加载代理商基础信息数据 ')
    AGENTINFO = loadAgentInfo()
    logger.info('加载代理商基础信息数据  完成')
    
    logger.info('计算一级代理商')
    eqdetail['agent_1'] = eqdetail.apply(lambda row:getAgentByLevel(row['agent_info'], 1), axis=1)
    logger.info('计算二级代理商')
    eqdetail['agent_2'] = eqdetail.apply(lambda row:getAgentByLevel(row['agent_info'], 2), axis=1)
    logger.info('计算三级代理商')
    eqdetail['agent_3'] = eqdetail.apply(lambda row:getAgentByLevel(row['agent_info'], 3), axis=1)
    logger.info('计算四级代理商')
    eqdetail['agent_4'] = eqdetail.apply(lambda row:getAgentByLevel(row['agent_info'], 4), axis=1)
    logger.info('计算监管代理商')
    eqdetail['agent_m'] = eqdetail.apply(lambda row:getSVAgentName(row['agent_1']), axis=1)
    
    del eqdetail['agent_info']
    
    logger.info('代理商转义 开始')
    eqdetail['agent_1'] = eqdetail.apply(lambda row:getAgentNameByID(row['agent_1']), axis=1)
    eqdetail['agent_2'] = eqdetail.apply(lambda row:getAgentNameByID(row['agent_2']), axis=1)
    eqdetail['agent_3'] = eqdetail.apply(lambda row:getAgentNameByID(row['agent_3']), axis=1)
    eqdetail['agent_4'] = eqdetail.apply(lambda row:getAgentNameByID(row['agent_4']), axis=1)    
    logger.info('代理商转义 完成')
    
    gc.collect()
    
    #银联防伪码处理
    eqdetail['security_code'] = eqdetail['security_code'].fillna('-')
    
    #活动类型处理 
    actype=getActiveType()
    actype.index = actype.sec_code1
    logger.info('活动类型处理 开始')
    eqdetail = pd.merge(eqdetail,actype,left_on='security_code',right_on='sec_code1',how='left')
    eqdetail['activity_type'] = eqdetail['activity_type1'].fillna('-')
    eqdetail['activity_type'] = eqdetail.apply(lambda row:getActivityType(row['activity_type']), axis=1)
    del eqdetail['sec_code1']
    del eqdetail['activity_type1']
    logger.info('活动类型处理 完成')
    
    #获取产品名称
    prodname=getProductName()
    prodname.index = prodname.stock_number1
    logger.info('产品名称 开始')
    eqdetail = pd.merge(eqdetail,prodname,left_on='stock_number',right_on='stock_number1',how='left')
    eqdetail['name'] = eqdetail['name1'].fillna('-')
    del eqdetail['name1']
    del eqdetail['stock_number1']
    gc.collect()
    logger.info('产品名称 完成')
    
    
    #这种方式无法实现，会卡死的
    #设备第一交易时间
    logger.info('设备第一次交易时间  开始')
    #if not os.path.exists('product_min_transdate.csv'):
    #    logger.error('product_min_transdate.csv 文件不存在,无法加载产品数据,请检查')
    #    sys.exit(1)
    #mintrans = pd.read_csv('product_min_transdate.csv')
    #mintrans.columns = ('p_csn', 'p_min_time')
    #mintrans.index = mintrans.p_csn
    
    #获取不存在csv文件里的csn
    #pd1 = pd.merge(eqdetail,mintrans,left_on='csn',right_on='p_csn',how='left')
    #pd3 = pd1[ (pd1['p_csn'].isnull()) ]['csn']
    pd4 = getCsn()
    pd4.index= pd4.p_csn
    #if not os.path.exists('product_min_transdate.csv'):
    #    logger.error('product_min_transdate.csv 文件不存在,无法加载产品数据,请检查')
    #    sys.exit(1)
    #pd4.to_csv('product_min_transdate.csv',index=False,mode='a',header=False,quotechar='"')   
    #gc.collect()
    #利用最新的csv里的csn信息来更新报表信息
    #mintrans1 = pd.read_csv('product_min_transdate.csv')
    #mintrans1.columns = ('p_csn', 'p_min_time')
    #mintrans1.index = mintrans1.p_csn
    eqdetail = pd.merge(eqdetail,pd4,left_on='csn',right_on='p_csn',how='left')
    eqdetail['mintime'] = eqdetail['p_min_time'].fillna('-')
    del eqdetail['p_csn']
    del eqdetail['p_min_time']
    gc.collect()
    logger.info('设备第一次交易时间  结束')
    
    
    #获取加工后的数据
    saveToExcel(eqdetail)
    
    #单号信息处理

    #产品名称处理