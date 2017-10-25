# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 14:36:05 2016
业务容量监控信息
@author: mzhang
"""

import os 
import pandas as pd
#import numpy as np  
import sys
import gc
from mail import Mail
import time
import zipfile
import socket
import shutil

import datetime
from utility import logger
from monitorConf import DB_CONN_STRING_HPRISK,DB_CONN_STRING_RCONTROL,CONF_ORDER_TIME,CONF_RATE_ALIAES_NAME
#from pandas import Series, DataFrame
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

reload(sys)
sys.setdefaultencoding('utf-8') 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
CUR_PATH = os.path.dirname(os.path.realpath(__file__))

'''生成execl'''
def delDirectory():
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

   if os.path.exists('%s/result' %CUR_PATH):
        shutil.rmtree('%s/result' % CUR_PATH)
        logger.info('删除报表数据目录,完成')

def saveToExcel(df,owner):
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
    
    if not os.path.exists('%s/result' % CUR_PATH):
        os.mkdir('%s/result' % CUR_PATH)
    fileName = '%s/result/%s-%s.xlsx' % (CUR_PATH,owner,target_date)
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
    print fileName
    if os.path.exists(fileName):
        os.remove(fileName)
    excel.to_excel(fileName)
    #df.to_csv(fileName)
    
def createZip(filename,srcDirectory):
    ''' 创建压缩文件 '''
    zip=zipfile.ZipFile(filename,'w', zipfile.ZIP_DEFLATED) 
    os.chdir(srcDirectory)
    for r, d, fs in os.walk(srcDirectory):
     for f in fs:
    #print f
      zip.write(os.path.basename(f))
    zip.close()

def getAttachFileList(srcDirectory):
    '''获取附件列表'''
    os.chdir(srcDirectory)
    fl=[]
    for r, d, fs in os.walk(srcDirectory):
     for f in fs:
     #print f
      fl.append(os.path.abspath(f))
    return fl

'''邮件发送'''
def sendMailInfo():
    import re
    now = datetime.datetime.now()
    m=re.match('.*(\d+)',CONF_ORDER_TIME)
    days = 1
    if m:
        days = int(m.groups()[0])
    oneday = datetime.timedelta(days=days)
    target_date = now - oneday + oneday
    target_date = target_date.strftime("%Y%m%d")
        
    zipDirectory = os.path.join(os.path.abspath(os.path.dirname(__file__)),'zipDir')
    if not os.path.exists(zipDirectory):
      os.mkdir(zipDirectory) 
      logger.info('临时目录不存在创建临时目录:'+zipDirectory)
    zipfileName=os.path.join(zipDirectory,'report_'+target_date+'.zip')
    
    if os.path.exists(zipfileName):
      os.remove(zipfileName) 
      logger.info('存在相同压缩文件名，删除历史数据:'+zipfileName)
    logger.info('创建压缩文件:'+zipfileName)
    
    if not os.path.exists('%s/result' % CUR_PATH):
       os.mkdir('%s/result' % CUR_PATH)
    fileDirectory = '%s/result' % CUR_PATH
    os.chdir(fileDirectory)
    os.system(("zip %s *")%(zipfileName))
    createZip(zipfileName,fileDirectory)
    files=getAttachFileList(fileDirectory)
    print files
    logger.info('压缩结束')
    
    mail_to=['rskrpt@handpay.com.cn']
    port="25"
    m=Mail(port=port,mailTo=mail_to)
    try:
        logger.info('send mail starting....')
        subject='风控数据量监控'
        content='When the REPORT has problems, please contact DBA!'
        m.sendMsg(subject,content,[zipfileName])
        logger.info('send mail end.')
    except:
        return

def getHprisk():
    sql = '''SELECT 'VELOCITY_CSN_NO' AS CNAME, COUNT(1) AS CSUM FROM HPRISK.VELOCITY_CSN_NO
                UNION ALL
                SELECT 'VELOCITY_CUSTOM', COUNT(1) FROM HPRISK.VELOCITY_CUSTOM
                UNION ALL
                SELECT 'VELOCITY_ID_NUMBER', COUNT(1) FROM HPRISK.VELOCITY_ID_NUMBER
                UNION ALL
                SELECT 'VELOCITY_IMEI', COUNT(1) FROM HPRISK.VELOCITY_IMEI
                UNION ALL
                SELECT 'VELOCITY_MERCHANT_NO', COUNT(1) FROM HPRISK.VELOCITY_MERCHANT_NO
                UNION ALL
                SELECT 'VELOCITY_SOURCE_PT_NO', COUNT(1) FROM HPRISK.VELOCITY_SOURCE_PT_NO
                UNION ALL
                SELECT 'VELOCITY_TARGET_PT_NO', COUNT(1) FROM HPRISK.VELOCITY_TARGET_PT_NO
                UNION ALL
                SELECT 'VELOCITY_TUD_ID', COUNT(1) FROM HPRISK.VELOCITY_TUD_ID
                UNION ALL
                SELECT 'VELOCITY_USER_NO', COUNT(1) FROM HPRISK.VELOCITY_USER_NO
            ''' 
    engine = create_engine(DB_CONN_STRING_HPRISK,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)

def getHpnewrisk():
    sql = '''SELECT 'VELOCITY_CSN_NO' as CNAME, COUNT(1) AS CSUM FROM NEWRISK.VELOCITY_CSN_NO
                UNION ALL
                SELECT 'VELOCITY_CUSTOM', COUNT(1) FROM NEWRISK.VELOCITY_CUSTOM
                UNION ALL
                SELECT 'VELOCITY_ID_NUMBER', COUNT(1) FROM NEWRISK.VELOCITY_ID_NUMBER
                UNION ALL
                SELECT 'VELOCITY_IMEI', COUNT(1) FROM NEWRISK.VELOCITY_IMEI
                UNION ALL
                SELECT 'VELOCITY_MERCHANT_NO', COUNT(1) FROM NEWRISK.VELOCITY_MERCHANT_NO
                UNION ALL
                SELECT 'VELOCITY_SOURCE_PT_NO', COUNT(1) FROM NEWRISK.VELOCITY_SOURCE_PT_NO
                UNION ALL
                SELECT 'VELOCITY_TARGET_PT_NO', COUNT(1) FROM NEWRISK.VELOCITY_TARGET_PT_NO
                UNION ALL
                SELECT 'VELOCITY_TUD_ID', COUNT(1) FROM NEWRISK.VELOCITY_TUD_ID
                UNION ALL
                SELECT 'VELOCITY_USER_NO', COUNT(1) FROM NEWRISK.VELOCITY_USER_NO
            ''' 
    engine = create_engine(DB_CONN_STRING_HPRISK,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)

def getHprcontrol():
    sql = '''SELECT 'RSK_TRANS_LOG' AS CNAME, COUNT(1) AS CSUM FROM RCONTROL.RSK_TRANS_LOG
                UNION ALL
                SELECT 'RSK_TRANS_LOG_HISTORY', COUNT(1) FROM RCONTROL.RSK_TRANS_LOG_HISTORY
                UNION ALL
                SELECT 'RSK_TRANS_LOG_BAK_2015', COUNT(1) FROM RCONTROL.RSK_TRANS_LOG_BAK_2015
                UNION ALL
                SELECT 'RSK_WARN', COUNT(1) FROM RCONTROL.RSK_WARN
                UNION ALL
                SELECT 'RSK_WARN_HISTORY', COUNT(1) FROM RCONTROL.RSK_WARN_HISTORY
                UNION ALL
                SELECT 'RSK_WARN_BAK_2015', COUNT(1) FROM RCONTROL.RSK_WARN_BAK_2015
            ''' 
    engine = create_engine(DB_CONN_STRING_RCONTROL,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)


if __name__ == '__main__':
    logger.info('开始')
    #加载有卡数据

    delDirectory()
    hprisk = getHprisk()
    saveToExcel(hprisk,'HPRISK')

    newrisk = getHpnewrisk()
    saveToExcel(newrisk,'NEWRISK')
    rcontrol = getHprcontrol()     
    saveToExcel(rcontrol,'RCONTROL')
    logger.info('生成execl文件,结束')
    
    logger.info('打包并发送数据,开始')
    sendMailInfo()
    logger.info('打包并发送数据,结束')
    gc.collect()
      
    
    
