# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 14:36:05 2016
以terminal_info表为基础来做扩展
@author: mzhang
"""

import os 
import pandas as pd
import numpy as np  
import sys
import gc
from mail import Mail
import time

#from matplotlib.pyplot import axis 
import datetime
from anaRateConf import CONF_ORDER_TIME,DB_CONN_STRING
from anaRateConf import CONF_RATE_ALIAES_NAME,CONF_LOAN_RATE_INFO,CONF_BORROW_RATE_INFO
from utility import logger
from pandas import Series, DataFrame
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


reload(sys)
sys.setdefaultencoding('utf-8') 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
CUR_PATH = os.path.dirname(os.path.realpath(__file__))

'''生成execl'''
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
    #target_date = now - oneday
    target_date = now - oneday + oneday
    target_date = target_date.strftime("%Y%m%d")
    if not os.path.exists('%s/report' % CUR_PATH):
        os.mkdir('%s/report' % CUR_PATH)
    fileName = '%s/report/Orders-%s.xlsx' % (CUR_PATH,target_date)
    report_col_dict = {} 
    for k, v in CONF_RATE_ALIAES_NAME.items():
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
    # print fileNamew
    if os.path.exists(fileName):
        os.remove(fileName)
    excel.to_excel(fileName)
    #df.to_csv(fileName)

#获取信息
def getRateCostInfo():
    sql = '''SELECT   BIG_MERCH_NUMBER,
                      FROM_CARD_TYPE,
                       PAY_AMOUNT,
                       TRANS_CHARGE,
                       O.MERCH_NAME,
                       R_BORROW,
                       R_LOAN,
                       R_YINLIAN,
                       CHANNEL_NAME,
                       TYPE,
                       AREA_32,
                       BEIZHU,
                       '' COST,
                       '' GRAIN
                  FROM BDATA.ORDERS O, BDATA.RATE L
                 WHERE O.BIG_MERCH_NUMBER = L.MERCH_NO(+)
                   AND O.TRANS_STATUS = '交易成功'
                   AND O.ACCOUNTTRANSTIME = '20170830'
                   AND O.BACKEND_ID = 'posPay'
                   AND O.TRANS_TIME >= '2017083000000' and o.Trans_Time < '20170901000000'
            '''
    engine = create_engine(DB_CONN_STRING,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)

'''利用FROM_CARD_TYPE字段来分片
贷记卡
准贷记卡
借记卡
'''

'''
借记卡				
0.3一笔	      每笔成本都是0.3元一笔		  0.3
0.413%-17	   成本=金额*0.413%，若大于17则取17  if(成本>17){17}else{amount*0.413%}	
0.309%-13.28	成本=金额*0.309%，若大于13.28则取13.28
0.31%-14	      成本=金额*0.31%，若大于14则取14	
0.18%保底0.3	   成本=金额*0.18%，若小于0.3则取0.3	
0.393%-16.85	成本=金额*0.393%，若大于16.85则取16.85
0.312%-13.28	成本=金额*0.312%，若大于13.28则取13.28
0.309%-13.68	成本=金额*0.309%，若大于13.68则取13.68
0.16%	         成本=金额*0.16%			
0.35%13元封顶  	发卡=金额*0.35%，若大于13则取13
'''

def OptBorrowCost(r_borrow,pay_amount):
    if CONF_BORROW_RATE_INFO.get(r_borrow) == '1':
        return 0.3
    if CONF_BORROW_RATE_INFO.get(r_borrow) == '2':
        ct = (0.413 * float(pay_amount) ) / 100
        if ct > 17:
            return 17
        else:
            return ct
    if CONF_BORROW_RATE_INFO.get(r_borrow) == '3':
        ct = (0.309 * float(pay_amount) ) / 100
        if ct > 13.28:
            return 13.28
        else:
            return ct
    if CONF_BORROW_RATE_INFO.get(r_borrow) == '4':
        ct = (0.31 * float(pay_amount) ) / 100
        if ct > 14:
            return 14
        else:
            return ct
    if CONF_BORROW_RATE_INFO.get(r_borrow) == '5':
        ct = (0.18 * float(pay_amount) ) / 100
        if ct < 0.3:
            return 0.3
        else:
            return ct
    if CONF_BORROW_RATE_INFO.get(r_borrow) == '6':
        ct = (0.393 * float(pay_amount) ) / 100
        if ct > 16.85:
            return 16.85
        else:
            return ct
    if CONF_BORROW_RATE_INFO.get(r_borrow) == '7':
        ct = (0.312 * float(pay_amount) ) / 100
        if ct > 13.28:
            return 13.28
        else:
            return ct
    if CONF_BORROW_RATE_INFO.get(r_borrow) == '8':
        ct = (0.309 * float(pay_amount) ) / 100
        if ct > 13.68:
            return 13.68
        else:
            return ct
    if CONF_BORROW_RATE_INFO.get(r_borrow) == '9':
        return (0.16 * float(pay_amount) ) / 100
    if CONF_BORROW_RATE_INFO.get(r_borrow) == '10':
        ct = (0.35 * float(pay_amount) ) / 100
        if ct > 13:
            return 13
        else:
            return ct
    else:
        #error:说明借费率不在CONF_BORROW_RATE_INFO文件中或者费率导入时的格式不符合
        return 'berror'
    
'''					
贷或者准贷卡				
0.45%	         发卡=金额*0.45%			
0.410%	      成本=金额*0.41%			
0.407%	      成本=金额*0.407%			
0.2%保底0.3元	   成本=金额*0.2%，若小于0.3则取0.3	
0.3元一笔	      每笔成本都是0.3元一笔		
0.16%	         成本=金额*0.16%			
0.513%	      成本=金额*0.513%			
'''
def OptLoanCost(r_loan,pay_amount):
    if CONF_LOAN_RATE_INFO.get(r_loan) == '1':
        return (0.45 * float(pay_amount) ) / 100
    if CONF_LOAN_RATE_INFO.get(r_loan) == '2':
        ct = (0.410 * float(pay_amount) ) / 100
        return ct
    if CONF_LOAN_RATE_INFO.get(r_loan) == '3':
        ct = (0.407 * float(pay_amount) ) / 100
        return ct
    if CONF_LOAN_RATE_INFO.get(r_loan) == '4':
        ct = (0.2 * float(pay_amount) ) / 100
        if ct < 0.3:
            return 0.3
        else:
            return ct
    if CONF_LOAN_RATE_INFO.get(r_loan) == '5':
            return 0.3
    if CONF_LOAN_RATE_INFO.get(r_loan) == '6':
        ct = (0.16 * float(pay_amount) ) / 100
        return ct
    if CONF_LOAN_RATE_INFO.get(r_loan) == '7':
        ct = (0.513 * float(pay_amount) ) / 100
        return ct
    else:
        #errorl:说明贷费率不在CONF_BORROW_RATE_INFO文件中或者费率导入时的格式不符合
        return 'lerror'
'''				
银联				
0.0325%3.25元封顶	成本=金额*0.0325%，若大于3.25则取3.25	
'''
def OptYinLianCost():
    pass
   
    
     
if __name__ == '__main__':
    logger.info('开始')
    # 加载数据 
    logger.info('费率数据加载  开始')
    rate = getRateCostInfo()
    rate1 = rate[rate.from_card_type=='借记卡']
    rate1['cost'] = rate1.apply(lambda row: OptBorrowCost(row['r_borrow'],row['pay_amount']), axis=1)
    rate2 = rate[(rate.from_card_type=='准贷记卡') | (rate.from_card_type=='贷记卡')]
    rate2['cost'] = rate2.apply(lambda row: OptLoanCost(row['r_loan'],row['pay_amount']), axis=1)
    df = [rate1,rate2]
    rate3 = pd.concat(df)
    logger.info('费率数据加载  完成')
    gc.collect()
    #计算利润
    
    logger.info('生成excel文件，开始')
    saveToExcel(rate3)
    
    logger.info('生成execl文件,结束')
    
