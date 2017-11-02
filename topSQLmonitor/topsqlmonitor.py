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
from topsqlmonitorConf import DB_CONN_STRING_1,CONF_ORDER_TIME,CONF_RATE_ALIAES_NAME
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
    
    mail_to=['min.zhang@handpay.com.cn']
    port="25"
    m=Mail(port=port,mailTo=mail_to)
    try:
        logger.info('send mail starting....')
        subject='GEEDUN库TOP_10_SQL监控'
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

def getDBInfo(flag,dbid,inst_num,snap_id):
    if flag == '1':
        sql = '''select dbid from v$database
        '''
    if flag == '2':
        sql = '''
        select instance_number from v$instance
        '''
    if flag == '3':
        sql = '''
        select max(snap_id) as end_snap
        from dba_hist_active_sess_history s
        where dbid='%s' and instance_number='%s'
        ''' % (dbid,inst_num)
    if flag == '4':
        sql = '''select max(snap_id) as beg_snap
          from dba_hist_active_sess_history
         where substr(to_char(cast(sample_time as date), 'yyyymmddhh24miss'), 1, 10) =
               (select substr(to_char(cast(sample_time as date) - 6,
                                      'yyyymmddhh24miss'),
                              1,
                              10)
                  from dba_hist_active_sess_history s
                 where snap_id = '%s'
                   and dbid = '%s'
                   and instance_number = '%s'
                   and rownum = 1)
           and dbid = '%s'
           and instance_number = '%s'
        ''' % (snap_id,dbid,inst_num,dbid,inst_num)
    engine = create_engine(DB_CONN_STRING_1,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)
    
def getTopSqlElapTime(dbid,beg_snap,end_snap,inst_num,owner):
    sql = '''select elapsed_time,cpu_time,executions,Elapsed_Time_Per_Exec,sql_id,SQL_Module,sql_text
              from (select nvl((sqt.elap / 1000000), to_number(null)) AS Elapsed_Time,
                           nvl((sqt.cput / 1000000), to_number(null)) AS CPU_Time,
                           sqt.exec As Executions,
                           decode(sqt.exec,
                                  0,
                                  to_number(null),
                                  (sqt.elap / sqt.exec / 1000000)) AS Elapsed_Time_Per_Exec,
                           (100 *
                           (sqt.elap / (SELECT sum(e.VALUE) - sum(b.value)
                                           FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
                                          WHERE B.SNAP_ID =  %s
                                            AND E.SNAP_ID =  %s
                                            AND B.DBID =  %s
                                            AND E.DBID =  %s
                                            AND B.INSTANCE_NUMBER =  %s
                                            AND E.INSTANCE_NUMBER =  %s
                                            and e.STAT_NAME = 'DB time'
                                            and b.stat_name = 'DB time'))) AS Elapsed_Time_Per_Total,
                           sqt.sql_id AS SQL_ID,
                           to_clob(decode(sqt.module,
                                          null,
                                          null,
                                          'Module: ' || sqt.module)) AS SQL_Module,
                            nvl(st.sql_text, to_clob(' ** SQL Text Not Available ** ')) AS SQL_Text
                      from (select sql_id,
                                   max(module) module,
                                   sum(elapsed_time_delta) elap,
                                   sum(cpu_time_delta) cput,
                                   sum(executions_delta) exec
                              from dba_hist_sqlstat
                             where dbid =  %s
                               and instance_number = %s
                               and  %s < snap_id
                               and snap_id <=  %s
                               and parsing_schema_name= '%s'
                             group by sql_id) sqt,
                           dba_hist_sqltext st
                     where st.sql_id(+) = sqt.sql_id
                       and st.dbid(+) =  %s
                     order by nvl(sqt.elap, -1) desc, sqt.sql_id)
             where rownum < 65
               and (rownum <=10 or Elapsed_Time_Per_Total > 1)
            '''  % (beg_snap,end_snap,dbid,dbid,inst_num,inst_num,dbid,inst_num,beg_snap,end_snap,owner,dbid)
    engine = create_engine(DB_CONN_STRING_1,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)


def getTopSqlCpuTime(dbid,beg_snap,end_snap,inst_num,owner):
    sql = '''select cpu_time,elapsed_time,cpu_time_per_exec,Executions,sql_id,SQL_Module,sql_text
       from (select nvl((sqt.cput / 1000000), to_number(null)) AS CPU_TIME,
                    nvl((sqt.elap / 1000000), to_number(null)) AS Elapsed_Time,
                    sqt.exec AS Executions,
                    decode(sqt.exec,
                           0,
                           to_number(null),
                           (sqt.cput / sqt.exec / 1000000)) AS CPU_Time_Per_Exec,
                    (100 * (sqt.elap /
                    (SELECT sum(e.VALUE) - sum(b.value)
                               FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
                              WHERE B.SNAP_ID = %s
                                AND E.SNAP_ID = %s
                                AND B.DBID = %s
                                AND E.DBID = %s
                                AND B.INSTANCE_NUMBER = %s
                                AND E.INSTANCE_NUMBER = %s
                                and e.STAT_NAME = 'DB time'
                                and b.stat_name = 'DB time'))) AS Elapsed_Time_Per_Total,
                    sqt.sql_id AS SQL_ID ,
                    to_clob(decode(sqt.module,
                                   null,
                                   null,
                                   'Module: ' || sqt.module)) AS SQL_Module ,
                    nvl(st.sql_text, to_clob('** SQL Text Not Available **')) AS SQL_TEXT
               from (select sql_id,
                            max(module) module,
                            sum(cpu_time_delta) cput,
                            sum(elapsed_time_delta) elap,
                            sum(executions_delta) exec
                       from dba_hist_sqlstat
                      where dbid = %s
                        and instance_number = %s
                        and %s < snap_id
                        and snap_id <= %s
                        and parsing_schema_name= '%s'
                      group by sql_id) sqt,
                    dba_hist_sqltext st
              where st.sql_id(+) = sqt.sql_id
                and st.dbid(+) = %s
              order by nvl(sqt.cput, -1) desc, sqt.sql_id)
      where rownum < 65
        and (rownum <= 10 or Elapsed_Time_Per_Total > 1)
            ''' % (beg_snap,end_snap,dbid,dbid,inst_num,inst_num,dbid,inst_num,beg_snap,end_snap,owner,dbid)
    engine = create_engine(DB_CONN_STRING_1,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)

def getTopSqlPhyReadTime(dbid,beg_snap,end_snap,inst_num,owner):
    sql = '''select Physical_Reads,Executions,Reads_per_Exec,physical_read_CPU_Time,physical_read_Elapsed_Time,SQL_ID,SQL_Module,SQL_TEXT
     from (select sqt.dskr AS Physical_Reads,
                  sqt.exec AS Executions,
                  decode(sqt.exec, 0, to_number(null), (sqt.dskr / sqt.exec)) AS Reads_per_Exec,
                  (100 * sqt.dskr) /
                  (SELECT sum(e.VALUE) - sum(b.value)
                     FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
                    WHERE B.SNAP_ID = %s
                      AND E.SNAP_ID = %s
                      AND B.DBID = %s
                      AND E.DBID = %s
                      AND B.INSTANCE_NUMBER = %s
                      AND E.INSTANCE_NUMBER = %s
                      and e.STAT_NAME = 'physical reads'
                      and b.stat_name = 'physical reads') AS physical_read_Per_Total,
                  nvl((sqt.cput / 1000000), to_number(null)) AS physical_read_CPU_Time,
                  nvl((sqt.elap / 1000000), to_number(null)) AS physical_read_Elapsed_Time,
                  sqt.sql_id AS SQL_ID,
                  decode(sqt.module, null, null, 'Module: ' || sqt.module) AS SQL_Module,
                  nvl(st.sql_text, to_clob('** SQL Text Not Available **')) AS SQL_TEXT
             from (select sql_id,
                          max(module) module,
                          sum(disk_reads_delta) dskr,
                          sum(executions_delta) exec,
                          sum(cpu_time_delta) cput,
                          sum(elapsed_time_delta) elap
                     from dba_hist_sqlstat
                    where dbid = %s
                      and instance_number = %s
                      and %s < snap_id
                      and snap_id <= %s
                      and parsing_schema_name= '%s' 
                    group by sql_id) sqt,
                  dba_hist_sqltext st
            where st.sql_id(+) = sqt.sql_id
              and st.dbid(+) = %s
              and (SELECT sum(e.VALUE) - sum(b.value)
                     FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
                    WHERE B.SNAP_ID = %s
                      AND E.SNAP_ID = %s
                      AND B.DBID = %s
                      AND E.DBID = %s
                      AND B.INSTANCE_NUMBER = %s
                      AND E.INSTANCE_NUMBER = %s
                      and e.STAT_NAME = 'physical reads'
                      and b.stat_name = 'physical reads') > 0
            order by nvl(sqt.dskr, -1) desc, sqt.sql_id)
    where rownum < 65and(rownum <= 10
                      or physical_read_Per_Total > 1)

            ''' % (beg_snap,end_snap,dbid,dbid,inst_num,inst_num,dbid,inst_num,beg_snap,end_snap,owner,dbid,beg_snap,end_snap,dbid,dbid,inst_num,inst_num)
    engine = create_engine(DB_CONN_STRING_1,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)

def getTopSqlLogicalReadTime(dbid,beg_snap,end_snap,inst_num,owner):
    sql = '''select buffer_gets,Executions,Reads_per_Exec,logical_read_CPU_Time,logical_read_Elapsed_Time,sql_id,SQL_Module,SQL_TEXT
  from (select sqt.bget as buffer_gets,
               sqt.exec as Executions,
               decode(sqt.exec, 0, to_number(null), (sqt.bget / sqt.exec)) as Reads_per_Exec,
               (100 * sqt.bget) /
               (SELECT sum(e.VALUE) - sum(b.value)
                  FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
                 WHERE B.SNAP_ID = %s
                   AND E.SNAP_ID = %s
                   AND B.DBID = %s
                   AND E.DBID = %s
                   AND B.INSTANCE_NUMBER = %s
                   AND E.INSTANCE_NUMBER = %s
                   and e.STAT_NAME = 'session logical reads'
                   and b.stat_name = 'session logical reads') as logical_read_Per_Total,
               nvl((sqt.cput / 1000000), to_number(null)) as logical_read_CPU_Time,
               nvl((sqt.elap / 1000000), to_number(null)) as logical_read_Elapsed_Time,
               sqt.sql_id as sql_id,
               to_clob(decode(sqt.module,
                              null,
                              null,
                              'Module: ' || sqt.module)) AS SQL_Module,
               nvl(st.sql_text, to_clob('** SQL Text Not Available **')) AS SQL_TEXT
          from (select sql_id,
                       max(module) module,
                       sum(buffer_gets_delta) bget,
                       sum(executions_delta) exec,
                       sum(cpu_time_delta) cput,
                       sum(elapsed_time_delta) elap
                  from dba_hist_sqlstat
                 where dbid = %s
                   and instance_number = %s
                   and %s < snap_id
                   and snap_id <= %s
                   and parsing_schema_name= '%s'
                 group by sql_id) sqt,
               dba_hist_sqltext st
         where st.sql_id(+) = sqt.sql_id
           and st.dbid(+) = %s
         order by nvl(sqt.bget, -1) desc, sqt.sql_id)
 where rownum < 65
   and (rownum <= 10 or logical_read_Per_Total > 1)
            ''' % (beg_snap,end_snap,dbid,dbid,inst_num,inst_num,dbid,inst_num,beg_snap,end_snap,owner,dbid)
    engine = create_engine(DB_CONN_STRING_1,poolclass=NullPool)
    return pd.read_sql_query(sql,engine)


if __name__ == '__main__':
    logger.info('开始')
    #加载有卡数据

    delDirectory()
    
    dbid = getDBInfo('1',-1,-1,-1).values
    inst_num = getDBInfo('2',-1,-1,-1).values
    if not dbid[0][0] or dbid[0][0] == 'null' or not inst_num[0][0] or inst_num[0][0] == 'null' or inst_num[0][0] == 0:
        logger.info('获取的dbid失败:%s' % dbid[0][0])
        logger.info('获取的inst_num失败:%s' % inst_num[0][0])
    else:
        logger.info('成功获取dbid:%s' % dbid[0][0])
        logger.info('成功获取inst_num:%s' % inst_num[0][0])
        end_snap = getDBInfo('3',dbid[0][0],inst_num[0][0],-1).values
        beg_snap = getDBInfo('4',dbid[0][0],inst_num[0][0],end_snap[0][0]).values
        logger.info('成功获取beg_snap:%s' % beg_snap[0][0])
        logger.info('成功获取end_snap:%s' % end_snap[0][0])
    '''
        geedun用户下的TOP sql抓取工作
    '''
    topsqlelaptime = getTopSqlElapTime(dbid[0][0],beg_snap[0][0],end_snap[0][0],inst_num[0][0],'BDATA')
    topsqlcputime = getTopSqlCpuTime(dbid[0][0],beg_snap[0][0],end_snap[0][0],inst_num[0][0],'BDATA')
    topsqlphyreadtime = getTopSqlPhyReadTime(dbid[0][0],beg_snap[0][0],end_snap[0][0],inst_num[0][0],'BDATA')
    topSqllogicalreadtime = getTopSqlLogicalReadTime(dbid[0][0],beg_snap[0][0],end_snap[0][0],inst_num[0][0],'BDATA')
    logger.info('生成execl文件，开始')
    saveToExcel(topsqlelaptime,'TopSqlElapTime')
    saveToExcel(topsqlcputime,'TopSqlCpuTime')
    saveToExcel(topsqlphyreadtime,'TopSqlPhyReadTime')
    saveToExcel(topSqllogicalreadtime,'TopSqlLogicalReadTime')
    logger.info('生成execl文件,结束')
    
    logger.info('打包并发送数据,开始')
    sendMailInfo()
    logger.info('打包并发送数据,结束')
    #gc.collect()
      
    
    
