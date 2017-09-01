# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 13:03:09 2016

@author: trsenzhang
"""
import os 
import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
import tushare as ts   
import sys
import gc
from mail import Mail
import time
import re
from datetime import datetime as dt
from datetime import timedelta

#from matplotlib.pyplot import axis 
import datetime
from pip import status_codes
from config import CONF_ORDER_TIME, CONF_TRANS_TYPE, CONF_TRANS_STATUS, CONF_BANK_CARD_TYPE
from config import CONF_CHANNEL_NAME, CONF_TRAN_PAYMENT_CHANNEL, CONF_REPORT_COLS_DICT
from config import CONF_TRANS_CONFIRM_STATUS, CONF_BANK_CARD_CLASS
from config import CONFIG_MERCH_STATUS, CONF_ASSET_STATUS,CONF_PRODUCT_TRANS_TYPE
from config import BIZCTRL_AUDIT_STATUS, AUDIT_STATUS_DICT, SETTLE_STATUS_DICT, CONFIG_CANCEL_STATUS
from cert import CERT_ZONE 
from config import DB_CONN_STRING #engine
from utility import logger,AESCipher
from pandas import Series, DataFrame
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


reload(sys)
sys.setdefaultencoding('utf-8') 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
CUR_PATH = os.path.dirname(os.path.realpath(__file__))

# 加解密工具
AES = None
# 临时变量,调试使用
v = None
# 手续费规则表
PAYMENT_CHARGE_CONFIG_RULE = None
# 转账记录表
TRANFER_LOG = None
# 小微实名登记表
MERCHNAMEDICT = {}
MCA_LIST = {}
BANKDICT = {}
MCA_DATAFRAME = None
# 产品清单
PRODUCT = None
# 代理商信息
AGENTINFO = None
# 监管商信息
SV_AGENT = None

def getProductTransType(backendid):
    # 根据bacnendid返回产品交易类型
    if CONF_PRODUCT_TRANS_TYPE.has_key(backendid):
        return CONF_PRODUCT_TRANS_TYPE[backendid]
    return backendid

def getUserRegion(cert_num):
    # 根据身份证号码获取地区信息
    #logger.error(cert_num)
    #logger.error(cert_num.__class__)
    # np.nan
    if not cert_num:
        return '-'
    if str(cert_num) == '-':
        return '-'
    try:
        cert = aes.decrypt(cert_num)   
        cnum = cert[:6]
        if CERT_ZONE.has_key(cnum):
            return CERT_ZONE[cnum]
        return '%s 无法获取身份证信息' % cert_num
    except Exception as e:
        #logger.error(cert_num.__class__)
        return '%s 无法获取身份证信息' % cert_num

def getUserInfo():
    # 获取用户信息
    sql = '''   
    select userid            as u_userid,
           createtime        as u_create_time,
           replace(replace(realname,CHR(10),''),chr(13),'')         as u_realname,
           mobile            as u_mobile,
           CERTIFICATETYPE   as u_cert_type,
           CERTIFICATENUMBER as u_cert_num,
           userinfo.merchantcode as u_merch_code
      from hpay.user_info userinfo
     where userinfo.merchantcode in (select l.merchantcode
                        from hpay.payment_transaction_log l
                       where l.paymenttransdate between
                             to_char(%s, 'yyyymmdd') || '000000' and
                             to_char(%s, 'yyyymmdd') || '235959'
                         --and l.BACKENDTRANSSEQ is not null
                         --and rownum <= 2000
                         )
    '''    % (CONF_ORDER_TIME,CONF_ORDER_TIME)

    engine = create_engine(DB_CONN_STRING,poolclass=NullPool) 
    return pd.read_sql_query(sql,engine)

def getAssetTypeName(status_code):
    # 获取设备状态名称
    if not CONF_ASSET_STATUS.has_key(status_code):
        return '未知状态'
    return CONF_ASSET_STATUS[status_code]
    

def getOrderUnionPaySecurityCode():
    # 获取银联设备状态码
    sql = '''
select ti.csn,
       ti.security_code t_scode,
       PERSONLIZED_TIME as t_ptime,
       replace(address,',','|')          as t_addr,
       status as t_status
  from hpay.terminal_info ti
 where csn in (select csn
                 from hpay.payment_transaction_log l
                where l.paymenttransdate between
                      to_char(%s, 'yyyymmdd') || '000000' and
                      to_char(%s, 'yyyymmdd') || '235959'
                  --and l.BACKENDTRANSSEQ is not null
                  --and rownum <= 2000
                  )
   and csn is not null

''' % (CONF_ORDER_TIME,CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql, engine)

def getBankCardTypeName(card_code):
    # 获取银行卡种类
    if not card_code:
        return '-'
    if CONF_BANK_CARD_CLASS.has_key(card_code):
        return CONF_BANK_CARD_CLASS[card_code]
    else:
        # logger.info('异常,获取银行卡类型失败,银行卡代码:%s' % card_code)
        return '无效银行卡代码'

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
    select tai.id,replace(tai.corp_full_name,',','|') as corp_full_name  from hpay.t_agent_info tai
    '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql, engine)

def getAgentByLevel(info, level):
    # 根据字符串获取对应级别的代理商代码
    if not info:
        return '-'
    if info[0] == '|':
        info = info[1:]
    agents = info.split('|')
    if len(agents) < level:
        # logger.error('异常:无法获取到对应代理商')
        # logger.error('info:%s,level:%s' %(info,level))
        return '-'
    agent = '-'
    try:
        agent = int(agents[level - 1])
    except Exception as e:
        logger.error('异常:无法获取代理商')
        logger.error('异常信息:%s' % str(e))
        logger.error("参数 info:%s level:%s" % (info, level))
    # logger.debug("info:%s,level:%s agent:%s"  % (info,level,agent))
    return agent
        

def getAgentNameByID(agentid, orderno):
    if agentid == '-':
        return '-'
    if not agentid:
        return '-'
    # if np.isnan(agentid):
    #    return '-'
    
    # logger.info(AGENTINFO.id.__class__) 
    # logger.info(agentid.__class__) 
    # mask = (AGENTINFO.id == agentid)
    if str(agentid) == 'nan':
        return '-' 
    # logger.error(agentid)
    # logger.info('异常,agentid class:%s' % agentid.__class__)
    aid = int(agentid)
    agent = AGENTINFO[(AGENTINFO.id == aid)]
    if agent.empty: 
        # logger.error('异常,agentid class:%s' % id.__class__)
        # logger.error('异常,无法获取代理商信息,代理商编号:%s,订单编号:%s' % (id, orderno))        
        return '无对应代理商信息:%s' % (aid)
    return agent['corp_full_name'].values[0]
    
def loadOrderAgentInfo():
    # 代理商信息
    sql = '''
    select plog.paymenttransseq order_no, ti.agent_info info
  from hpay.terminal_info ti, hpay.payment_transaction_log plog
 where plog.csn = ti.csn
   and plog.paymenttransdate between
       to_char(%s, 'yyyymmdd') || '000000' and
       to_char(%s, 'yyyymmdd') || '235959'
   and plog.paymenttransseq in
       (select paymenttransseq
          from hpay.payment_transaction_log l
         where l.paymenttransdate between
                to_char(%s, 'yyyymmdd') || '000000' and
       to_char(%s, 'yyyymmdd') || '235959' 
           -- and rownum <= 2000
           )
    ''' % (CONF_ORDER_TIME,CONF_ORDER_TIME,CONF_ORDER_TIME,CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    df = pd.read_sql_query(sql, engine)
    df.index = df.order_no
    
    logger.info('计算一级代理商')
    df['agentA'] = df.apply(lambda row:getAgentByLevel(row['info'], 1), axis=1)
    logger.info('计算二级代理商')
    df['agentB'] = df.apply(lambda row:getAgentByLevel(row['info'], 2), axis=1)
    logger.info('计算三级代理商')
    df['agentC'] = df.apply(lambda row:getAgentByLevel(row['info'], 3), axis=1)
    logger.info('计算四级代理商')
    df['agentD'] = df.apply(lambda row:getAgentByLevel(row['info'], 4), axis=1)
    logger.info('计算监管代理商')
    df['agentS'] = df.apply(lambda row:getSVAgentName(row['agentA']), axis=1)
    logger.info('计算监管代理商 完成')
    return df

def getSVAgentName(agentID):
    # 获取agentID对应的监管代理商
    if not agentID:
        return '-'
    if agentID == '-':
        return '-'
    
    # logger.debug('AgentID:%s' % agentID)
    agentName = '-'
    try:
        svAgent = SV_AGENT[SV_AGENT.id == agentID]
        if svAgent.empty:
            return '-'
        agentName = svAgent.sv_name.values[0]
    except Exception as e:
        # logger.error('异常:无法获取监管商名称:AgentID:%s' % agentID)
        return agentName
    return agentName
 
 
 
def getMicroCertAuth():
    # 获取小微实名登记表
    sql = '''
    select plog.paymenttransseq as m_order_no,mca.status m_status,mca.ext m_ext
  from hpay.payment_transaction_log plog, hpay.micro_card_auth mca
 where plog.PAYMENTTOOLACCOUNTNO = mca.card_no
   and mca.cert_type = '0'
   and plog.paymenttransdate between
       to_char(%s, 'yyyymmdd') || '000000' and
       to_char(%s, 'yyyymmdd') || '235959'
   --and plog.BACKENDTRANSSEQ is not null
   --and plog.status = '00'  
    ''' % (CONF_ORDER_TIME, CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    df_mcl = pd.read_sql_query(sql, engine)
    df_mcl.index = df_mcl.m_order_no
    return df_mcl

def getMcaStatusName(status):
    # microCertAuth    
    if status =='-':
        return '-'
    #logger.info(status)
    if CONF_TRANS_CONFIRM_STATUS.has_key(status):
        return CONF_TRANS_CONFIRM_STATUS[status]
    else:
        logger.error('异常:小微实名认证记录不完整,不存在定义状态含义:%s ,请修改 CONF_TRANS_CONFIRM_STATUS' % status)
        return '状态信息不存在'

 
def getPayChannelName(channel_code):
    # 获取交易通道名称
    if not channel_code:
        return '-'
    if not CONF_TRAN_PAYMENT_CHANNEL.has_key(channel_code):
        logger.error('异常,渠道代码不存在,渠道代码:%s' % channel_code)
        return '异常渠道'
    return CONF_TRAN_PAYMENT_CHANNEL[channel_code]

def getTransferLog():
    # 获取TransLog
    sql = '''
    select plog.paymenttransseq t_order_no, SWIPER_TYPE t_swiper_type,plog.status t_status
    from hpay.transfer_log trslog, hpay.payment_transaction_log plog
    where trslog.payment_trans_seq = plog.paymenttransseq
    and plog.paymenttransdate between
               to_char(%s, 'yyyymmdd') || '000000' and
               to_char(%s, 'yyyymmdd') || '235959'
    ''' % (CONF_ORDER_TIME, CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql, engine)

def getBankCardType(card_type, status):
    # 获取银行卡类型
    card_type_name = '-'
    if status != '00':
        return '未知'
    try:
        card_type_name = CONF_BANK_CARD_TYPE[card_type]
        return card_type_name
    except Exception as e:
        logger.error('异常 获取银行卡类型失败,异常信息: %s' % str(e.message))
        logger.error('参数:card_type:%s,status:%s' % (card_type, status))
    finally:
        return card_type_name

def getChargeConfigRule():
    # 获取费率规则
    sql = '''
    select plog.paymenttransseq ,plog.paymentamount,plog.status,
            plog.transchargeid, pcr.rulevalue,pcr.lowerlimitamount,
           pcr.upperlimitamount,pcr.increase_or_reducea_mount,  pcr.ruletype
          from hpay.payment_transaction_log     plog,
               hpay.payment_merch_charge_config pmcc,
               hpay.payment_charge_config_rule  pccr,
               hpay.payment_charge_rule         pcr
         where plog.transchargeid = pmcc.chargeid
           and pmcc.chargeid = pccr.configid
           and pcr.ruleid = pccr.ruleid
           and plog.paymenttransdate between
               to_char(%s, 'yyyymmdd') || '000000' and
               to_char(%s, 'yyyymmdd') || '235959'

    ''' % (CONF_ORDER_TIME, CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql,engine )



def getChargeRate(payseq, amount):
    # 根据订单号获取费率
    # logger.error(payseq)
    '''
    if amount == 0:
        # logger.info('手续费为0,不计算费率')
        return '交易手续费金额为0'
    '''
    pay = PAYMENT_CHARGE_CONFIG_RULE[(PAYMENT_CHARGE_CONFIG_RULE.paymenttransseq == payseq) & (PAYMENT_CHARGE_CONFIG_RULE.paymentamount >= PAYMENT_CHARGE_CONFIG_RULE.lowerlimitamount) 
                                     & ((PAYMENT_CHARGE_CONFIG_RULE.paymentamount <= PAYMENT_CHARGE_CONFIG_RULE.upperlimitamount) | (pd.isnull(PAYMENT_CHARGE_CONFIG_RULE.upperlimitamount))) ]
    # 规则无法获取
    if  pay.empty:
        logger.error('订单 %s 无对应费率规则' % payseq)
        return '-'
    
    if len(pay) == 0:
        logger.error('订单 %s 无对应费率规则' % payseq)
        return '-'
    '''
    # 非成功交易,不计算费率
    if pay.status.values[0] != '00':
        return '非成功交易'
    '''
    '''
    if ( len(pay.ruletype.values) != 1 ):
        print payseq
        print len(pay.ruletype.values) 
        print pay
        '''
    charge_type = pay.ruletype.values[0]
    
    # rate = None
    if charge_type == '01':
        #rate = "%2.2f%%" % (pay.rulevalue.values[0] * 100) 
        rate = "%2.4f" % (pay.rulevalue.values[0]) 
    else:
        #rate = "%s元" % (pay.rulevalue.values[0] / 100)
        rate = "%s" % (pay.rulevalue.values[0] / 100)
        rate = rate.decode('utf-8').encode('gbk')
    return rate
    
def getSubAddress(address, address_type, order_no, order_status):
    # print address
    try: 
        # if order_status != '00':
            # return '+'
        if not address or address == 'null':
            return '-'
        
        if address_type == 'province':
            return address.split('|')[0]
        if address_type == 'city':
            return address.split('|')[1]
        if address_type == 'county':
            return address.split('|')[2]
    except Exception as e:
        logger.error('异常 地址信息失败,异常信息: %s' % str(e.message)) 
        logger.error('输入参数 address:%s,address_type:%s' % (address, address_type))
        logger.error('订单号:%s' % order_no)
        return '-'
    return '-'
        


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

def getTransType(tran_type):
    # 获取交易类型
    if CONF_TRANS_TYPE.has_key(tran_type):
        return CONF_TRANS_TYPE[tran_type]  
    return tran_type
     
def getTransStatus(trans_status):
    # 获取交易状态
    if CONF_TRANS_STATUS.has_key(trans_status):
        return CONF_TRANS_STATUS[trans_status]
    return trans_status
    
def formatDateTimeString(date_time):
    try:
        dt = "%s-%s-%s %s:%s:%s" % (date_time[:4], date_time[4:6], date_time[6:8], date_time[8:10], date_time[10:12], date_time[12:14])
        return dt
    except:
        return date_time


def getOrders():
    '''获取订单'''
    sql = """
    select paymenttransseq as ORDER_NO,
        PAYMENTTRANSDATE as TRANS_TIME,
        '' TRANS_TIME_YEAR, 
        '' TRANS_TIME_MONTH,
        '' TRANS_TIME_DAY,
        '' TRANS_TIME_HOUR,
        BACKendTRANSSEQ BANK_TRANS_SEQ,
        BACKendTRANSDATE TRANS_NOTICE_DATE,
        type TRANS_TYPE,
        status TRANS_STATUS,
        PAYMENTAMOUNT PAY_AMOUNT,
        TRANSCHARGE TRANS_CHARGE,
        0 TRANS_CHARGE_RATE,
        replace(address,',','.') TRANS_ADDRESS,
        '' TRANS_ADDRESS_PROVINCE,
        '' TRANS_ADDRESS_CITY,
        '' TRANS_ADDRESS_COUNTY,
        PAYMENTTOOLACCOUNTNO BANK_ACCOUNT_NO,
        backendid BACKend_ID,
        '' SWIPER_TYPE,
        BACKendRESPCODE CHANNEL_RESP_CODE ,
        replace(CONTENT,',','|') CHANNEL_RESP_CONTENT,
        CHANNEL PRODUCT_NAME,
        '' PRODUCT_TYPE,
        '' PRODUCT_OEM,
        backendid PRODUCT_TRANS_TYPE,
        '-' TRAN_CHARGE_TEMPLATE,
        '-' IS_MUL_RATE,
        '-' IS_SKIP,
        '-' NO_SKIP_RATE,
         '普通商户' MERCHANT_RATE_TYPE,
         PAYMENT_CHANNEL PAY_CHANNEL,
        0 BT_RATE,
        0 BT_AMOUNT,
        '' TRANS_CONFIRM_STATUS,
        '' TRANS_CONFIRM_RESP,
        CARD_TYPE FROM_CARD_TYPE,
        '' FROM_CARD_BANK,
        '' AGENT_SUPERVISOR,
        '' AGENT_1,
        '' AGENT_2,
        '' AGENT_3,
        '' AGENT_4,
        '' SECURITY_CODE,
        '' PRODUCE_DATE,
        '' PRODUCE_DATE_YEAR,
        '' PRODUCE_DATE_MONTH,
        '' SALE_REGION,
        '' ASSET_STATUS,
        csn ASSET_CSN,
        userid,
        '' USER_REG_DATE,
        '' USER_REG_YEAR,
        '' USER_REG_MONTH,
        '' USER_REAL_NAME,
        --'' USER_MOBILE,
        '' UESR_CERT_TYPE,
        '' USER_CERT_NUMBER,
        '' USER_REGION,
        merchantcode MERCH_CODE,
        '' MERCH_NAME,
        '' MERCH_FULL_NAME,
        '' MERCH_REG_DATE,
        '' MERCH_REG_DATE_YEAR,
        '' MERCH_REG_DATE_MONTH,
        '' MERCH_STATUS,
        '' MERCH_ACTIVE_DATE,
        '' MERCH_ACTIVE_DATE_YEAR,
        '' MERCH_ACTIVE_DATE_MONTH,
        merchantbackendcode BIG_MERCH_NUMBER,
        '' BIG_MERCH_NAME,
        '' BIG_MERCH_NICK_NAME,
        '' UNIPAY_LOCATION,
        '' IS_SETTLE,
        bizctrl_audit_status IS_AUDIT_PHOTO,
        paymenttransdate AUDIT_PHOTO_TIME,
        BIZCTRL_AUDIT_TIME AUDIT_PHOTO_COMPLETE_TIME,
        '' IS_AUDIT_PHOTO_FREEZE,
        '' RISK_SUBMIT_TIME,
        '' RISK_AUDIT_TIME,
        '' IS_RISK_FREEZE,
        '' RISK_TX_TIME,
        '' TO_BANK,
        IMEI,
        MAC,
        IP,
        CANCEL_STATUS,
        ACCOUNTTRANSTIME
from hpay.payment_transaction_log l
  where l.paymenttransdate  between to_char(%s,'yyyymmdd')||'000000' 
  and to_char(%s,'yyyymmdd')||'235959' 
  and l.type in ('00','TSWITHDRAW')
  --and l.BACKENDTRANSSEQ is not null 
  --and l.status='00'
  --and rownum<=10
""" % (CONF_ORDER_TIME, CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql, engine) 

def saveToExcel(df):
    df_cols = []
    import re
    now = datetime.datetime.now()
    #now = datetime.now()
    m=re.match('.*(\d+)',CONF_ORDER_TIME)
    days = 1
    if m:
        days = int(m.groups()[0])
    oneday = datetime.timedelta(days=days)
    target_date = now - oneday
    target_date = target_date.strftime("%Y%m%d")
    if not os.path.exists('%s/report' % CUR_PATH):
        os.mkdir('%s/report' % CUR_PATH)
    fileName = '%s/report/Orders-%s.csv' % (CUR_PATH,target_date)
    report_col_dict = {} 
    for k, v in CONF_REPORT_COLS_DICT.items():
        report_col_dict[k.lower()] = v    
    for k in df.columns:
        if report_col_dict.has_key(k):
            df_cols.append(report_col_dict[k])
        else:
            #df_cols.append('unTitled')
            df_cols.append('unTitled-%s' % k)
            logger.error('异常,标题字段不存在,字段名称:%s' % k)
    df.columns = df_cols
    #excel = df
    #excel.columns = df_cols
    # print fileName
    if os.path.exists(fileName):
        os.remove(fileName)
        #excel.to_excel(fileName)
    df.to_csv(fileName)


def getRiskInfo():
    # 一般情况交易成功的数据才会进到rsk_trans_log此表RISK_TX_TIME,IS_RISK_FREEZE 
    sql = '''select payment_trans_seq as order_no,
       freeze_status as is_risk_freeze,
       to_char(tx_success_time, 'yyyymmddhh24miss') as risk_tx_time      
  from rcontrol.rsk_trans_log
 where tx_success_time is not null
   and tx_success_time >=
       to_timestamp(to_char(%s, 'yyyymmdd') || '000000',
                    'syyyy-mm-dd hh24:mi:ss.ff')
   and tx_success_time <
       to_timestamp(to_char(%s+2, 'yyyymmdd') || '000000',
                    'syyyy-mm-dd hh24:mi:ss.ff')
   and payment_trans_seq is not null
  '''   % (CONF_ORDER_TIME,CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql, engine)
    # 此处代码可优化成变量，人为判断替换进
    pd_result['is_risk_freeze'] = pd_result['is_risk_freeze'].replace('FREEZED', '冻结')
    pd_result['is_risk_freeze'] = pd_result['is_risk_freeze'].replace('UN_FREEZE', '正常')
    pd_result['is_risk_freeze'] = pd_result['is_risk_freeze'].replace([None], ['正常'], regex=True)
    pd_result['order_no'] = pd_result.apply(lambda row: int(row['order_no']), axis=1) 
    return pd_result

def getBankPrefixInfo():
    # 获取银行卡信息
    sql = '''select card_no_prefix c_prefix_num,name_cn c_bankname from hpay.t_card_info'''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    df = pd.read_sql_query(sql, engine)
    df.index = df.c_prefix_num   
    dt = DataFrame.to_dict(df)
    #return df['name_cn']
    return dt
    
def getOrdersInId(opt_type):
    # inputaccountno为收款银行卡银行操作，paymenttoolaccountno为付款银行卡银行,此处可优化
    sql = '''
    select paymenttransseq as order_no,hpay.aes128_decrypt(%s) as %s
    from hpay.payment_transaction_log  
    where paymenttransdate  between to_char(%s,'yyyymmdd')||'000000' and to_char(%s,'yyyymmdd')||'235959'
    and BACKENDTRANSSEQ is not null and %s is not null ''' % (opt_type, opt_type, CONF_ORDER_TIME, CONF_ORDER_TIME, opt_type)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql, engine)

def getBankInfo():
    sql = '''
    select paymenttransseq as b_order_no,inputaccountno b_input_account,paymenttoolaccountno b_paied_account
    from hpay.payment_transaction_log  
    where paymenttransdate  between to_char(%s,'yyyymmdd')||'000000' and to_char(%s,'yyyymmdd')||'235959'
    and BACKENDTRANSSEQ is not null and inputaccountno is not null 
    ''' % (CONF_ORDER_TIME,CONF_ORDER_TIME) 
     
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)
    
def getBankName(card_no):
    # 根据银行卡获取银行名称
    card_no = aes.decrypt(str(card_no))
    if not card_no:
        return '-'
    for i in range(2, 11):
        card_prefix = card_no[:i]
        if BANKDICT.has_key(card_prefix):
            return BANKDICT[card_prefix]
    return '-'

def optToBank():
    sql = '''select name_cn,card_no_prefix from hpay.t_card_info'''
    df = pd.read_sql_query(sql,DB_CONN_STRING)
    df.index = df.card_no_prefix   
    df = DataFrame.to_dict(df)
    return df['name_cn'] 
    
#大商户信息
COMMIONNDICT = {}  
def getCommionInfo(name_type):
    #加载获取所有大商户信息BIG_MERCH_NICK_NAME
    sql = '''
    select merchantbackendcode as big_merch_number ,merchantbackendname as big_merch_name,
    params as big_merch_nick_name from hpay.payment_merchantcode_config
     '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql,engine)
    pd_result.index = pd_result.big_merch_number
    pd_result['big_merch_nick_name'] = pd_result['big_merch_nick_name'].replace('^merAbbr=','',regex=True)
    pd_result = DataFrame.to_dict(pd_result)
    if '0' == name_type:
        return pd_result['big_merch_name']
    if '1' == name_type:
        return pd_result['big_merch_nick_name']

def getCommionName(big_merch_number):
    #name_type = 0 name, name_type == 1 nick_name
    if big_merch_number == '':
        return '-'
    else:
        if COMMIONNDICT.has_key(big_merch_number):
            return COMMIONNDICT[big_merch_number]
        return '-'
#照片审核时间                        
def getAuditPhoto(status):
    # BIZCTRL_AUDIT_STATUS
    if not status or status == '-':
        return '-'
    else:
        if BIZCTRL_AUDIT_STATUS.has_key(status):
            return BIZCTRL_AUDIT_STATUS[status]
        else:
            logger.error('审计状态%s不在配置dict里,请修改CONF_TRANS_CONFIRM_STATUS' % status)
            return '状态信息不存在'

           
def getAuditCompTime(is_audit_photo,order_no):
    if is_audit_photo == '待审核':
        return '-'
    else:
        if not order_no:
            return '-'
            logger.error('单号不存在')
        else:
            return orders.ix[int(order_no),'audit_photo_complete_time']

def getAuditStatus(status):
    # BIZCTRL_AUDIT_STATUS
    if not status:
        return '-'
    elif str(status).lower()== 'nan':
        return '-'
    else:
        if AUDIT_STATUS_DICT.has_key(status):
            return AUDIT_STATUS_DICT[status]
        else:
            logger.error('%s 状态不存在，请修改AUDIT_STATUS_DICT' % status )            
            return '状态信息不存在'       
#信审信息         
def getRskTime():    
    sql = ''' select merchant_code, create_time, audit_time, audit_status
        from (select merchant_code,
               create_time,
               audit_time,
               audit_status,
               Row_Number() OVER(partition by merchant_code ORDER BY audit_time desc nulls last ) as cou
          from rcontrol.rsk_aptitude_apply
         where merchant_code in
               (select distinct plog.merchantcode
                  from hpay.payment_transaction_log plog
                 where plog.paymenttransdate between
                       to_char(%s, 'yyyymmdd') || '000000' and
                       to_char(%s, 'yyyymmdd') || '235959'))
                       where cou = 1
    ''' % (CONF_ORDER_TIME,CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql,engine)
    return pd_result

#获取小商户状态
def getMerchStatus(status):
    # CONFIG_MERCH_STATUS
    if not status:
        return '-'
    else:
        if CONFIG_MERCH_STATUS.has_key(status):
            return CONFIG_MERCH_STATUS[status]
        else:
            logger.error('小商户状态')
            return '状态信息不存在'

#获取小商户名称及其全称,
def getMerchInfo():
    sql = ''' select merchantcode as merch_code,
       replace(replace(replace(fullname,CHR(10),''),chr(13),''),',','|') as fullname,
       merchantname,
       status
  from hpay.merch_merchant mm
 where merchantcode in (select distinct merchantcode
          from hpay.payment_transaction_log plog
         where plog.paymenttransdate between
               to_char(%s, 'yyyymmdd') || '000000' and
               to_char(%s, 'yyyymmdd') || '235959') 
        ''' % (CONF_ORDER_TIME,CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql,engine)
    return pd_result

#获取小商户激活日及其入网日期
def getMerchDate(opttype):
    #激活日期'00',入网日为’01‘
    if '01' == opttype:
        sql = ''' select tmc.merchantcode as merch_code,
       tmc.csn as asset_csn,
       tmc.CREATE_DATE as reg_date
        from hpay.terminal_info ti,hpay.t_merchant_csn tmc 
        where ti.csn = tmc.csn(+) 
        and tmc.merchantcode in  (select distinct merchantcode
          from hpay.payment_transaction_log plog2
         where plog2.paymenttransdate between
               to_char(%s, 'yyyymmdd') || '000000' and
               to_char(%s, 'yyyymmdd') || '235959') 
               ''' % (CONF_ORDER_TIME,CONF_ORDER_TIME)
        engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
        pd_result = pd.read_sql_query(sql,engine)
        return pd_result
    if '00' == opttype:
        try:
            
            sql = ''' 
            select plog1.merchantcode as merch_code, min(plog1.paymenttransdate) as active_date
            from hpay.payment_transaction_log plog1 
            where exists ( select 1 
            from hpay.payment_transaction_log plog2 
            where plog2.paymenttransdate between 
            to_char(%s, 'yyyymmdd') || '000000' and 
            to_char(%s, 'yyyymmdd') || '235959'
            and plog2.merchantcode = plog1.merchantcode) 
            group by plog1.merchantcode 
            ''' % (CONF_ORDER_TIME,CONF_ORDER_TIME)
            engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
            pd_result = pd.read_sql_query(sql,engine)
            return pd_result
        except:
            logger.info('sql执行有问题，请检查函数getMerchDate')

#交易结算状态信息IS_SETTLE           
def getTranferSettleStatus(status):
    # SETTLE_STATUS_DICT
    if not status:
        return '-'
    elif str(status).lower()== 'nan':
        # logger.error('订单信息不在交易状态表里')
        return '异常:%s订单信息不在交易状态表里' % status
    else:
        if SETTLE_STATUS_DICT.has_key(status):
            return SETTLE_STATUS_DICT[status]
        else:
            #logger.error('SETTLE_STATUS_DICT信息不存在 %s！！！！' % status)
            return '异常:%s 状态信息不存在' % status
            
def getSettleStatus():
    sql = '''
        select payment_trans_seq as order_no, SETTLE_STATUS
  from smhpay.account_voucher_log alog
 where payment_trans_seq in
       (select plog.paymenttransseq
          from hpay.payment_transaction_log plog
         where plog.paymenttransdate between
               to_char(%s, 'yyyymmdd') || '000000' and
               to_char(%s, 'yyyymmdd') || '235959')
        ''' % (CONF_ORDER_TIME,CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql,engine)
    return pd_result

def geBankLocationCode(mcode):
    '''切割大商户号'''
    if not mcode:
        return '-'
    elif str(mcode) == 'nan':
        return '-'        
    else:
        if LOCATIONDICT.has_key(mcode[3:7]):
            return LOCATIONDICT[mcode[3:7]]
        return ''
        
LOCATIONDICT = {}
LOCATIONDICT2={'7910':'陕西省'}
def getBankLocationDict():
    #获取商户号落地地区数据字典
    sql = '''
    select district_code,district_name from hpay.bank_district
     '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql,engine)
    pd_result.index = pd_result.district_code
    pd_result = DataFrame.to_dict(pd_result)
    return pd_result['district_name']

    
def getSkip():
    #跳码可能是y，n，或者为空，或者这单号压根没有跳码记录在route_log
    sql = '''
    select paymenttransseq as order_no, 
    is_skip as isskip from hpay.route_log rl,hpay.payment_transaction_log plog 
    where plog.paymenttransseq = rl.trans_seq(+) 
    and plog.paymenttransdate  between to_char(%s,'yyyymmdd')||'000000' 
    and to_char(%s,'yyyymmdd')||'235959' 
    ''' % (CONF_ORDER_TIME,CONF_ORDER_TIME)
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    pd_result = pd.read_sql_query(sql,engine)
    pd_result['isskip'] = pd_result['isskip'].replace('Y', '是')
    pd_result['isskip'] = pd_result['isskip'].replace('N', '否')
    pd_result['isskip'] = pd_result['isskip'].replace([None], ['-'], regex=True)
    return pd_result
    
#获取用户撤销状态
def getCancelStatus(cancel_status):
    # CONFIG_CANCEL_STATUS
    if not cancel_status:
        return '未撤销'
    else:
        if CONFIG_CANCEL_STATUS.has_key(cancel_status):
            return CONFIG_CANCEL_STATUS[cancel_status]
        else:
            logger.error('用户撤销状态不在config.py文件中CONFIG_CANCEL_STATUS')
            return '撤销状态不存在'
    
#mail function
def mail(logfile):
    mail_to=['dba@handpay.com.cn']
    port="25"
    m=Mail(port=port,mailTo=mail_to)
    try:
        subject='数据仓库orders表加工'
        content=open(logfile,'r').read()
        m.sendMsg(subject,content)
    except:
        return
        
def logFormat(info,flag):
    if not os.path.exists('%s/log' % CUR_PATH):
        os.mkdir('%s/log' % CUR_PATH)
    logfile= CUR_PATH+'/log/'+'orders_'+(dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'.log'
    if flag == 'w':
        f=open(logfile,'w')
        f.write('['+time.strftime("%Y:%m:%d %H:%M:%S",time.localtime())+']==>>'+info+'\n')
        f.close()
    elif flag == 'a':
        f=open(logfile,'a')
        f.write('['+time.strftime("%Y:%m:%d %H:%M:%S",time.localtime())+']==>>'+info+'\n')
        f.close()
    else:
        rown=os.popen('cat '+CUR_PATH+'/report/Orders-'+(dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'.csv|tail -n +2|wc -l').read()
        f=open(logfile,'a')
        f.write('['+time.strftime("%Y:%m:%d %H:%M:%S",time.localtime())+']==>>'+(dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'当天,orders表到csv里数据量为:'+rown+'\n')
        f.close()        

def sed(type,filename="",s_str="",d_str=""):  
  '''
  a behind the "s_str" line append "d_str". if "s_str" is null, then append in the end of the file;
  i in front of "s_str" line append "d_str". if "s_str" is null,then append in the header of the;
  d  delete the "s_str" line 
  s replace string of "s_str" to "d_str"
  '''

  fp = open(filename)
  cont = fp.read()
  fp.close()
  content = cont.split("\n")
  content2 = cont.split("\n")
  cnt = 0
  idx = 0
  if type == 'a':
      if not s_str:
          content.append(d_str)
      else:
          for i in content2:
              if i.find(s_str) != -1:
                  x = idx + 1 + cnt
                  content.insert(x,d_str)
                  cnt += 1
              idx += 1
  elif type == 'i': 
      if not s_str: 
          content.insert(0,d_str)
      else:  
          for i in content2: 
              if i.find(s_str) != -1:  
                  x = idx + cnt  
                  content.insert(x,d_str)  
                  cnt += 1 
              idx += 1 
  elif type == 'd':  
      for i in content2:  
          if i.find(s_str) != -1:  
              idx = content.remove(i)  
  elif type == 's':
      cont=str.replace(cont,s_str,d_str)
      content=cont.split("\n")
  fp = open(filename, "w")  
  fp.write("\n".join(content))  
  fp.close()

def getSqlldir():            
    ctlfile=CUR_PATH+'/sqlldir/orders.ctl'
    csvfile1=CUR_PATH+'/report/Orders-'+(dt.now()+timedelta(days=-2)).strftime("%Y%m%d")+'.csv'
    csvfile2=CUR_PATH+'/report/Orders-'+(dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'.csv'
    sqllogfile=CUR_PATH+'/sqlldir/log/Orders_sqlldir_'+(dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'.log'
    badfile=CUR_PATH+'/sqlldir/log/Orders_sqlldir_'+(dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'.bad'
    #modify the sqlldr controlfile    
    sed('s',ctlfile,csvfile1,csvfile2)
    #load the csv to oracle database.
    os.system('/u01/app/oracle/product/11.2.0/dbhome_1/bin/sqlldr system/oracle control='+ctlfile+' log='+sqllogfile+' bad='+badfile+' skip=1')

def getLogInfoDetail():
  if not os.path.exists('%s/log' % CUR_PATH):
        os.mkdir('%s/log' % CUR_PATH)
  logfile= CUR_PATH+'/log/'+'orders_'+(dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'.log'
  sqllogfile=CUR_PATH+'/sqlldir/log/Orders_sqlldir_'+(dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'.log'
  fp=open(sqllogfile)
  cont = fp.read()
  fp.close()
  results=re.findall('[\x80-\xff+]+文件:+.*|[\x80-\xff+]+记录总数:+.*',cont)
  count =0
  for result in results:
	if count == 0:
		f=open(logfile,'a')
		f.write('['+time.strftime("%Y:%m:%d %H:%M:%S",time.localtime())+']==>>'+(dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'加载CSV数据到数据库里日志信息记录如下: \n')
        	f.close()
		count +=1
	else:
		f=open(logfile,'a')
		f.write('    '+result+'\n')
		f.close()

def ResultLog():
  if not os.path.exists('%s/log' % CUR_PATH):
  	os.mkdir('%s/log' % CUR_PATH)
  logfile= CUR_PATH+'/log/'+'orders_'+str(int(time.strftime("%Y%m%d",time.localtime()))-1)+'.log'
  fp=open(logfile)
  cont1 = fp.read()
  fp.close()
  r2=re.findall(' ?([0-9]+)',cont1)
  f=open(logfile,'a')
  f.write('[The dbhouse sync result]==>>')
  if r2[7] == r2[15] == r2[23] == r2[-3]:
        f.write('The dhouse db sync successful!\n')
  else:
        f.write('The dhouse db sync failed and Please connect the DBA.\n')
  f.close()

if __name__ == '__main__':
    logger.info('开始')
    
    aes = AESCipher()
    
    # 加载数据 
    logger.info('订单数据加载  开始')
    logger.info('订单数据加载  开始')
    orders = getOrders()
    orders.index = orders.order_no

    logger.info('订单数据加载  完成')
    if orders.empty:
        logger.error('订单数据不存在')
        sys.exit(1)
    logFormat((dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'当天,orders表开始时数据量为:'+str(orders.shape[0]),'w')
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('照片审核 开始')
    #IS_AUDIT_PHOTO AUDIT_PHOTO_COMPLETE_TIME
    orders['is_audit_photo'] = orders.apply(lambda row:getAuditPhoto(row['is_audit_photo']), axis=1)
    orders['audit_photo_complete_time'] = orders.apply(lambda row:getAuditCompTime(row['is_audit_photo'],row['order_no']), axis=1)
    logger.info('照片审核 完成')
    
    logger.info('大商户信息 开始')
    COMMIONNDICT = getCommionInfo('0')
    orders['big_merch_name'] = orders.apply(lambda row: getCommionName(row['big_merch_number']), axis=1)
    COMMIONNDICT = getCommionInfo('1')
    orders['big_merch_nick_name'] = orders.apply(lambda row: getCommionName(row['big_merch_number']), axis=1)
    logger.info('大商户信息 完成')
    
    #is_skip
    logger.info('跳码信息加载 开始')
    isskip = getSkip()
    isskip.index = isskip.order_no
    logger.info('跳码信息加载 结束')
    logger.info('跳码数据加工 开始')
    orders = pd.merge(orders,isskip,on='order_no',how='left')
    orders['is_skip'] = orders['isskip']
    del orders['isskip']
    del isskip
    gc.collect()
    logger.info('跳码数据加工 结束')    
    
    logger.info('统计行数为：%s ' % orders.shape[0])
    #小商户号，名称，店铺名称
    logger.info('小商户信息状态加载 开始')
    merchinfo = getMerchInfo()
    merchinfo.index = merchinfo.merch_code
    logger.info('小商户信息状态加载 完成')
    
    logger.info('小商户信息处理 开始')
    orders = pd.merge(orders,merchinfo,on='merch_code',how='left')
    orders['merch_name'] = orders['fullname']
    orders['merch_full_name'] = orders['merchantname']
    orders['merch_status'] = orders['status'].fillna('-')
    orders['merch_status'] = orders.apply(lambda row:getAuditPhoto(row['merch_status']), axis=1)
    del orders['fullname']
    del orders['merchantname']
    del orders['status']   
    #del merchinfo
    logger.info('小商户信息处理 完成') 
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    #获取小商户号激活日期
    logger.info('小商户激活日数据加载 开始')
    merchdate = getMerchDate('00')
    logger.info('小商户激活日数据加载 完成')
    merchdate.index = merchdate.merch_code
    orders = pd.merge(orders,merchdate,on='merch_code',how='left')
    orders['merch_active_date'] = orders['active_date']
    del orders['active_date']
    orders['merch_active_date_year'] = orders.apply(lambda row:getSubDate(row['merch_active_date'],'year'), axis=1)
    orders['merch_active_date_month'] = orders.apply(lambda row:getSubDate(row['merch_active_date'],'month'), axis=1)   
    del merchdate
    gc.collect()
    logger.info('小商户激活日期处理 完成')   
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    #获取小商户入网日 
    logger.info('小商户入网日 加载 开始')
    merchdate = getMerchDate('01')
    logger.info('小商户入网日加载 完成')
    orders = pd.merge(orders,merchdate,on=['merch_code','asset_csn'],how='left')
    orders['merch_reg_date'] = orders['reg_date']
    del orders['reg_date']
    orders['merch_reg_date_year'] = orders.apply(lambda row:getSubDate(row['merch_reg_date'],'year'), axis=1)
    orders['merch_reg_date_month'] = orders.apply(lambda row:getSubDate(row['merch_reg_date'],'month'), axis=1)
    del merchdate
    logger.info('小商户入网日处理 结束')
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    #UNIPAY_LOCATION
    logger.info('落地地区信息加载 开始')
    LOCATIONDICT1 = getBankLocationDict()
    LOCATIONDICT =dict(LOCATIONDICT1,**LOCATIONDICT2)
    orders['unipay_location'] = orders.apply(lambda row:geBankLocationCode(row['big_merch_number']), axis=1)   
    logger.info('落地地区信息加载 结束')
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    #用户撤销状态
    logger.info('用户撤销状态  开始')
    orders['cancel_status'] = orders.apply(lambda row:getCancelStatus(row['cancel_status']), axis=1)
    logger.info('用户撤销状态 完成')
    
    #交易结算状态信息IS_SETTLE
    logger.info('交易结算状态加载  开始')
    settlestatus = getSettleStatus()
    logger.info('交易结算状态加载 完成')
    
    orders = pd.merge(orders,settlestatus,on='order_no',how='left')
    orders['is_settle'] = orders['settle_status']
    del orders['settle_status']
    del settlestatus
    orders['is_settle'] = orders.apply(lambda row:getTranferSettleStatus(row['is_settle']), axis=1)
    gc.collect()  
    logger.info('交易结算状态数据处理 完成')
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('加载产品数据  开始')
    if not os.path.exists('product_channel_detail.csv'):
        logger.error('product_channel_detail.csv 文件不存在,无法加载产品数据,请检查')
        sys.exit(1)
    PRODUCT = pd.read_csv('product_channel_detail.csv')
    PRODUCT.columns = ('p_product_class', 'p_product_code', 'p_product_name', 'p_product_brand')
    PRODUCT.index = PRODUCT.p_product_code
    logger.info('加载产品数据  结束')
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('费率规则加载  开始')
    PAYMENT_CHARGE_CONFIG_RULE = getChargeConfigRule()
    PAYMENT_CHARGE_CONFIG_RULE.index = PAYMENT_CHARGE_CONFIG_RULE.paymenttransseq    
    logger.info('费率规则加载  完成')
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('转账数据加载  开始')
    TRANFER_LOG = getTransferLog()
    # select plog.paymenttransseq t_order_no, SWIPER_TYPE t_swiper_type,plog.status t_status
    logger.info('转账数据加载  完成')
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('转账数据转义  开始')
    TRANFER_LOG['t_swiper_type'] = TRANFER_LOG.apply(lambda row: getBankCardType(row['t_swiper_type'], row['t_status']), axis=1)
    del TRANFER_LOG['t_status']  
    gc.collect()
    logger.info('转账数据转义  完成')    
    logger.info('统计行数为：%s ' % orders.shape[0])
 
    # 数据转义
    logger.info('订单数据转义')
    logger.info(' 交易时间-年  开始')
    orders['trans_time_year'] = orders.apply(lambda row: getSubDate(row['trans_time'], 'year'), axis=1)
    logger.info(' 交易时间-年  完成')
    logger.info(' 交易时间-月  开始')
    orders['trans_time_month'] = orders.apply(lambda row: getSubDate(row['trans_time'], 'month'), axis=1)
    logger.info(' 交易时间-月  完成')
    logger.info(' 交易时间-天  开始')
    orders['trans_time_day'] = orders.apply(lambda row: getSubDate(row['trans_time'], 'day'), axis=1)
    logger.info(' 交易时间-天  完成')
    logger.info(' 交易时间-小时  开始')
    orders['trans_time_hour'] = orders.apply(lambda row: getSubDate(row['trans_time'], 'hour'), axis=1)
    logger.info(' 交易时间-小时   完成')
    logger.info('统计行数为：%s ' % orders.shape[0])
    logger.info(' 交易通知日期  开始')
    orders['trans_notice_date'] = orders.apply(lambda row:formatDateTimeString(row['trans_notice_date']), axis=1)
    logger.info(' 交易通知日期  完成')
    logger.info('统计行数为：%s ' % orders.shape[0])
    logger.info('交易类型  开始')
    orders['trans_type'] = orders.apply(lambda row:getTransType(row['trans_type']), axis=1)
    logger.info('交易类型  完成')
    logger.info('交易状态  开始')
    orders['trans_status'] = orders.apply(lambda row:getTransStatus(row['trans_status']), axis=1)
    logger.info('交易状态  完成 ')
    logger.info('交易金额  开始')
    orders['pay_amount'] = orders.apply(lambda row:round(row['pay_amount'] / 100.0, 2), axis=1)
    logger.info('交易金额  完成')
    logger.info('交易手续费金额 开始')
    orders['trans_charge'] = orders.apply(lambda row:round(row['trans_charge'] / 100.0, 2), axis=1)
    logger.info('交易手续费金额 完成')
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('交易手续费率 开始')
    orders['trans_charge_rate'] = orders.apply(lambda row:getChargeRate(row['order_no'], row['trans_charge']), axis=1)
    logger.info('交易手续费率 完成')
    del PAYMENT_CHARGE_CONFIG_RULE
    gc.collect()
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('交易发生地-省   开始')
    orders['trans_address_province'] = orders.apply(lambda row: getSubAddress(row['trans_address'], 'province', row['order_no'], row['trans_status']), axis=1)
    logger.info('交易发生地-省   完成')
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('交易发生地-市 开始')
    orders['trans_address_city'] = orders.apply(lambda row: getSubAddress(row['trans_address'], 'city', row['order_no'], row['trans_status']), axis=1)
    logger.info('交易发生地-市 完成')
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('交易发生地-区县 开始')
    orders['trans_address_county'] = orders.apply(lambda row: getSubAddress(row['trans_address'], 'county', row['order_no'], row['trans_status']), axis=1)
    logger.info('交易发生地-区县 完成')
    logger.info('统计行数为：%s ' % orders.shape[0]) 

    logger.info('银行卡类型转义 开始')      
    orders = pd.merge(orders, TRANFER_LOG, left_on='order_no',right_on='t_order_no',how='left') 
    orders['swiper_type'] = orders['t_swiper_type']
       
    del TRANFER_LOG  
    del  orders['t_swiper_type']
    del  orders['t_order_no']
    gc.collect()
    logger.info('银行卡类型转义 完成')
    logger.info('统计行数为：%s ' % orders.shape[0])
     
    logger.info('交易通道转义 开始')
    orders['pay_channel'] = orders.apply(lambda row: getPayChannelName(row['pay_channel']), axis=1)
    logger.info('交易通道转义 完成')
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('BT金额   开始')
    orders['bt_amount'] = orders.apply(lambda row: round(row['bt_amount'] / 100.0, 2), axis=1)
    logger.info('BT金额   完成')
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('加载小微实名记录 开始')
    microCertAuth = getMicroCertAuth()
    #  plog.paymenttransseq as m_order_no,mca.status m_status,mca.ext m_ext
    microCertAuth['m_status'] = microCertAuth['m_status'].fillna('-')
    microCertAuth['m_status'] = microCertAuth.apply(lambda row:getMcaStatusName(row['m_status']), axis=1)
    logger.info('加载小微实名记录 完成')    
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('交易验证状态  开始') 
    orders = pd.merge(orders, microCertAuth, left_on='order_no',right_on='m_order_no',how='left')    
    orders['trans_confirm_status'] = orders['m_status']
    del orders['m_status']
    del orders['m_order_no']
    logger.info('交易验证状态  完成')  
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('交易验证状态返回码描述  开始')  
    # orders['trans_confirm_resp'] = orders.apply(lambda row:  getMcaExtName(row['order_no']), axis=1)
    orders['trans_confirm_resp'] = orders['m_ext'] 
    del orders['m_ext']    
    logger.info('交易验证状态返回码描述  完成')     
    logger.info('清理小微实名记录内存 ')    
    del MCA_DATAFRAME
    gc.collect()

    logger.info('产品转义 开始')
    #PRODUCT.columns = ('p_product_class', 'p_product_code', 'p_product_name', 'p_product_brand')
    orders['product_name'] = orders['product_name'].fillna('-')
    orders = pd.merge(orders,PRODUCT,left_on='product_name',right_on='p_product_code',how='left')
    del PRODUCT
    # PRODUCT_NAME 所属产品
    orders['product_name'] = orders['p_product_name']#.fillna('channel:%s 无对应产品名' % (orders['product_name']) )
    orders['product_name'] = orders['product_name'].fillna('-')
    del orders['p_product_name']
    # PRODUCT_TYPE 产品类型
    orders['product_type'] = orders['p_product_class'].fillna('channel无对应产品类型')
    del orders['p_product_class']
    # PRODUCT_OEM 所属产品下的OEM产品
    orders['product_oem'] = orders['p_product_brand'].fillna('channel无对应产品oem')
    del orders['p_product_code']
    del orders['p_product_brand']
    gc.collect()
    # PRODUCT_TRANS_TYPE 产品交易类型  getProductTransType
    orders.index = orders.product_trans_type
    orders['product_trans_type'] = orders.apply(lambda row:getProductTransType(row['product_trans_type']),axis=1)
    logger.info('产品转义 结束')
    logger.info('产品数据 清理') 
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('交易银行卡种  开始')
    logger.info('加载银行银行信息')
    BANKDICT = getBankPrefixInfo()
    logger.info('银行卡信息转义')
    orders['bank_account_no'] = orders.apply(lambda row:aes.decrypt(row['bank_account_no']),axis=1)
    logger.info('获取银行卡信息')
    orders['to_bank'] = orders.apply(lambda row:getBankName(row['bank_account_no']),axis=1)
    
    logger.info('获取交易银行卡种信息')
    orders['from_card_type'] = orders.apply(lambda row: getBankCardTypeName(row['from_card_type']), axis=1)
    
    logger.info('收付款银行卡银行名称及信息加载 开始')
    BANKDICT = optToBank()
    orderBankAccount = getBankInfo()
    logger.info('收付款银行卡银行名称及信息加载 结束')
    logger.info('收付款银行卡银行  开始')
    orders = pd.merge(orders,orderBankAccount,left_on='order_no',right_on='b_order_no',how='left')
    orders['to_bank'] = orders.apply(lambda row:getBankName(row['b_input_account']),axis=1)  
    orders['from_card_bank'] = orders.apply(lambda row:getBankName(row['b_paied_account']),axis=1)
    del orders['b_input_account']
    del orders['b_paied_account']
    del orders['b_order_no']
    del orderBankAccount
    del BANKDICT
    logger.info('收付款银行卡银行  完成')
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('监管代理商  开始')
    logger.info('加载监管代理商  开始')
    SV_AGENT = getSVAgent()
    logger.info('加载监管代理商  完成')  
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('加载订单代理商数据  开始')
    orderAgentInfo = loadOrderAgentInfo()
    logger.info('加载订单代理商数据  完成')
    logger.info('加载代理商基础信息数据 ')
    AGENTINFO = loadAgentInfo()
    logger.info('加载代理商基础信息数据  完成')
    logger.info('转义代理商名称 开始')
    logger.info('转义一级代理商')
    orderAgentInfo['agentA'] = orderAgentInfo.apply(lambda row:getAgentNameByID(row['agentA'], row['order_no']), axis=1)
    logger.info('转义二级代理商')
    orderAgentInfo['agentB'] = orderAgentInfo.apply(lambda row:getAgentNameByID(row['agentB'], row['order_no']), axis=1)
    logger.info('转义三级代理商')
    orderAgentInfo['agentC'] = orderAgentInfo.apply(lambda row:getAgentNameByID(row['agentC'], row['order_no']), axis=1)
    logger.info('转义四级代理商')
    orderAgentInfo['agentD'] = orderAgentInfo.apply(lambda row:getAgentNameByID(row['agentD'], row['order_no']), axis=1)
    # agentInfo = pd.concat([DataFrame(columns=['agentJ','agentA',]),agentInfo])
    logger.info('转义代理商名称  完成')
    logger.info('清除代理商基础信息数据 ') 
    orders = pd.merge(orders, orderAgentInfo, on='order_no',how='left') 
    logger.info('监管代理商  开始')    
    orders['agent_supervisor'] = orders['agentS']
    logger.info('监管代理商  完成')
    logger.info('一级代理商  开始')   
    orders['agent_1'] = orders['agentA']
    logger.info('一级代理商  完成')
    logger.info('二级代理商  开始') 
    orders['agent_2'] = orders['agentB']
    logger.info('二级代理商  完成')
    logger.info('三级代理商  开始') 
    orders['agent_3'] = orders['agentC']
    logger.info('三级代理商  完成')
    logger.info('四级代理商  开始') 
    orders['agent_4'] = orders['agentD']
    logger.info('四级代理商  完成')
    logger.info('代理商数据清理  开始') 
    del orders['agentS'] 
    del orders['agentA']
    del orders['agentB']
    del orders['agentC']
    del orders['agentD']
    gc.collect()
    logger.info('代理商数据清理  完成')
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('交易风控信息 开始')
    #is_risk_freeze,risk_tx_time字段信息
    riskinfo = getRiskInfo()
    riskinfo.index = riskinfo.order_no
    orders = pd.merge(orders,riskinfo,on='order_no',how='left')
    orders['is_risk_freeze'] = orders['is_risk_freeze_y']
    orders['risk_tx_time'] = orders['risk_tx_time_y']
    del orders['is_risk_freeze_y']
    del orders['risk_tx_time_y']
    del riskinfo
    gc.collect()
    logger.info('交易风控信息 完成')    
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('银联防伪码 开始')
    orderSecurityCode = getOrderUnionPaySecurityCode()
    orderSecurityCode.index = orderSecurityCode.csn    
    orders = pd.merge(orders, orderSecurityCode, left_on='asset_csn', right_on='csn',how='left')
    orders['security_code'] = orders['t_scode']
    del orders['t_scode']
    del orders['csn']
    del orderSecurityCode
    gc.collect()
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('设备生产日期 开始')    
    orders['produce_date'] = orders['t_ptime']    
    del orders['t_ptime']
    logger.info('设备生产日期 结束')
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('设备生产日期-年 开始')
    orders['produce_date_year'] = orders.apply(lambda row:getSubDate(row['produce_date'], 'year'), axis=1)
    logger.info('设备生产日期-年 结束')
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('设备生产日期-月 开始')
    orders['produce_date_month'] = orders.apply(lambda row:getSubDate(row['produce_date'], 'month'), axis=1)
    logger.info('设备生产日期-月 结束')
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('销售区域 开始')
    orders['sale_region'] = orders['t_addr']
    orders['sale_region'] = orders.sale_region.fillna('全国')
    del orders['t_addr']
    logger.info('销售区域 结束')    
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('设备状态 开始')
    orders['asset_status'] = orders['t_status']    
    del  orders['t_status']
    orders['asset_status'] = orders.apply(lambda row:getAssetTypeName(row['asset_status']), axis=1) 
    logger.info('设备状态 结束')    
    logger.info('统计行数为：%s ' % orders.shape[0])

    logger.info('用户信息处理 开始')
    logger.info('加载用户信息')
    userinfo = getUserInfo()
    userinfo = userinfo.drop_duplicates(['u_merch_code'])
    orders = pd.merge(orders,userinfo,left_on="merch_code",right_on="u_merch_code",how='left')
    orders['user_reg_date'] = orders['u_create_time']
    orders['user_reg_date'] = orders['user_reg_date'].fillna('-')
    
    del orders['u_create_time']
    del userinfo
    gc.collect()
    
    orders['user_reg_year'] = orders.apply(lambda row:getSubDate(row['trans_time'], 'year'),axis=1)
    orders['user_reg_year'] = orders['user_reg_year'].fillna('-')
    
    orders['user_reg_month'] = orders.apply(lambda row:getSubDate(row['user_reg_date'], 'month'),axis=1)
    orders['user_reg_month'] = orders['user_reg_month'].fillna('-')
    
    orders['user_real_name'] = orders['u_realname']
    orders['user_real_name'] = orders['user_real_name'].fillna('-')
    del orders['u_realname']
    
    #orders['user_mobile'] = orders.apply(lambda row:aes.decrypt(row['u_mobile']),axis=1)
    #orders['user_mobile'] = orders['user_mobile'].fillna('-')
    #del orders['u_mobile']
    
    orders['uesr_cert_type'] = orders['u_cert_type']
    orders['uesr_cert_type'] = orders['uesr_cert_type'].fillna('-')
    del orders['u_cert_type']
    
    orders['user_cert_number'] = orders['u_cert_num']
    orders['user_cert_number'] = orders['user_cert_number'].fillna('-')
    del orders['u_cert_num']
    orders['user_region'] = orders['user_region'].fillna('-')
    orders['user_region'] = orders.apply(lambda row:getUserRegion(row['user_cert_number']),axis=1)
    del orders['u_userid']
    del orders['userid']
    del orders['u_merch_code']
    gc.collect()
    
    logger.info('用户信息处理 结束')   
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    logger.info('信审数据 开始')
    rsktime = getRskTime()  
    orders = pd.merge(orders,rsktime,left_on='merch_code',right_on='merchant_code',how='left')
    orders['is_audit_photo_freeze'] = orders['audit_status']
    orders['risk_submit_time'] = orders['create_time']
    orders['risk_audit_time'] = orders['audit_time']
    del orders['create_time']
    del orders['audit_time']
    del orders['audit_status']    
    orders['is_audit_photo_freeze'] = orders.apply(lambda row:getAuditStatus(row['is_audit_photo_freeze']), axis=1)
    del rsktime    
    logger.info('信审数据 完成')  
    logger.info('统计行数为：%s ' % orders.shape[0])
    
    del orders['info']
    del orders['risk_tx_time_x']
    del orders['is_risk_freeze_x']
    del orders['merchant_code']
    del orders['u_mobile']

    #gc.collect()
    logger.info('统计行数为：%s ' % orders.shape[0])
    logFormat((dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'当天,orders表分析后数据量为:'+str(orders.shape[0]),'a')
    saveToExcel(orders)
    logFormat('c','o')
    getSqlldir()
    getLogInfoDetail()
    ResultLog()
    logfile= CUR_PATH+'/log/'+'orders_'+(dt.now()+timedelta(days=-1)).strftime("%Y%m%d")+'.log'
    mail(logfile)
    
    logger.info('... 开始')
    logger.info('... 结束')    
        
    logger.info('完成')

