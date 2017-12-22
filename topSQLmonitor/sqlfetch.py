# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 14:36:05 2016
业务容量监控信息
@author: mzhang
"""

import os 
import pandas as pd
import numpy as np  
import sys
import gc
from mail import Mail
import time
import zipfile
import socket
import shutil

import datetime
from utility import logger
from topsqlmonitorConf1 import DB_CONN_STRING_1,CONF_ORDER_TIME,CONF_RATE_ALIAES_NAME
#from pandas import Series, DataFrame
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

reload(sys)
sys.setdefaultencoding('utf-8') 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
CUR_PATH = os.path.dirname(os.path.realpath(__file__))

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
    target_date = now - oneday
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
    
    mail_to=['trsenzhang@oracle.com.cn']
    port="25"
    m=Mail(port=port,mailTo=mail_to)
    try:
        logger.info('send mail starting....')
        subject='核心库SQL监控'
        content='When the REPORT has problems, please contact DBA!'
        m.sendMsg(subject,content,[zipfileName])
        logger.info('send mail end.')
    except:
        return    

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
            df_cols.append('%s' % k)
            logger.error('异常,标题字段不存在,字段名称:%s' % k)
    df.columns = df_cols
    excel = df
    excel.columns = df_cols
    print fileName
    if os.path.exists(fileName):
        os.remove(fileName)
    excel.to_excel(fileName)
    #df.to_csv(fileName)

def getDBInfo():
    sql = '''select dbid from v$database'''
    engine = create_engine(DB_CONN_STRING_1,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)
    
def getMinSnapId(dbid,inst_num,stime):
#stime的格式为yyyymmddhh24
    sql = '''select min(snap_id) as snapid 
	from dba_hist_active_sess_history  
	where dbid=%s and instance_number= %s and to_char(sample_time,'yyyymmddhh24')= '%s' ''' % (dbid,inst_num,stime)
    engine = create_engine(DB_CONN_STRING_1,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)

def getTopSqlElapTime(dbid,beg_snap,end_snap,inst_num):
    sql = '''select parsing_schema_name,executions,Elapsed_Time_Per_Exec,sql_id,sql_text
              from (select
                           sqt.exec As Executions,
                           decode(sqt.exec,
                                  0,
                                  to_number(null),
                                  (sqt.elap / sqt.exec / 1000000)) AS Elapsed_Time_Per_Exec,
                           sqt.sql_id AS SQL_ID,
                            nvl(st.sql_text, to_clob(' ** SQL Text Not Available ** ')) AS SQL_Text,
                            parsing_schema_name
                      from (select sql_id,
                                   max(module) module,
                                   sum(elapsed_time_delta) elap,
                                   sum(cpu_time_delta) cput,
                                   sum(executions_delta) exec,
                                   parsing_schema_name
                              from dba_hist_sqlstat
                             where dbid =  %s
                               and instance_number = %s
                               and  %s < snap_id
                               and snap_id <=  %s
                               and parsing_schema_name not in ('SYSTEM','SYS','DBSNMP','MZHANG','YANGLI')
                               and parsing_schema_name is not null
                             group by sql_id,parsing_schema_name) sqt,
                           dba_hist_sqltext st
                     where st.sql_id(+) = sqt.sql_id
                       and st.dbid(+) =  %s
                     order by nvl(sqt.elap, -1) desc, sqt.sql_id)
                     order by Elapsed_Time_Per_Exec  desc
            '''  % (dbid,inst_num,beg_snap,end_snap,dbid)
    engine = create_engine(DB_CONN_STRING_1,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)


if __name__ == '__main__':
    logger.info('开始')
    #加载有卡数据

    delDirectory()
    
    dbid = getDBInfo().values
    if not dbid[0][0] or dbid[0][0] == 'null':
        logger.info('获取的dbid失败:%s' % dbid[0][0])
    else:
        logger.info('成功获取dbid:%s' % dbid[0][0])
    '''
        抓取50.51上所有的sql信息
    '''
    now = datetime.datetime.now()
    days = 1
    oneday = datetime.timedelta(days=days)
    stime = now - oneday
    stime = stime.strftime("%Y%m%d")
    #第一个时间点早上8点，第二时间点晚上00
    stime2 = '%s%s' % (stime,'23')
    stime1 = '%s%s' % (stime,'08')
   	
    for inst_num in 1,2:
	beg_snap = getMinSnapId(dbid[0][0],inst_num,stime1).values
	logger.info('获取08点的snapid为:%s' % beg_snap[0][0])
	end_snap = getMinSnapId(dbid[0][0],inst_num,stime2).values
	logger.info('获取23点的snapid为:%s' % end_snap[0][0])
	topsqlelaptime = getTopSqlElapTime(dbid[0][0],beg_snap[0][0],end_snap[0][0],inst_num)
	logger.info('生成execl文件，开始')
	if inst_num == 1:
		saveToExcel(topsqlelaptime,'sql_hpaydb1_everyday_08-23_power')
	else:
		saveToExcel(topsqlelaptime,'sql_hpaydb2_everyday_08-23_power')
	logger.info('生成execl文件,结束')

    stime4 = '%s%s' % (stime,'08')
    stime3 = '%s%s' % (stime,'00')
    for inst_num in 1,2:
        beg_snap = getMinSnapId(dbid[0][0],inst_num,stime3).values
        logger.info('获取00点的snapid为:%s' % beg_snap[0][0])
        end_snap = getMinSnapId(dbid[0][0],inst_num,stime4).values
        logger.info('获取08点的snapid为:%s' % end_snap[0][0])
        topsqlelaptime = getTopSqlElapTime(dbid[0][0],beg_snap[0][0],end_snap[0][0],inst_num)
        logger.info('生成execl文件，开始')
	if inst_num == 1:
        	saveToExcel(topsqlelaptime,'sql_hpaydb1_everyday_00-08_lower')
	else:
		saveToExcel(topsqlelaptime,'sql_hpaydb2_everyday_00-08_lower')
        logger.info('生成execl文件,结束')    

    
    logger.info('打包并发送数据,开始')
    sendMailInfo()
    logger.info('打包并发送数据,结束')
    #gc.collect()
      
    
    
