# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 14:36:05 2016
监控存储容量信息
@author: mzhang
"""

import pandas as pd
import smtplib  
from email.mime.text import MIMEText  
from email.mime.multipart import MIMEMultipart
import paramiko
from backupStoMonConf import CONF_DATA_DICT_INFO,CONF_TERM_DICT_INFO
import datetime

def send_mail(to_list,sub,context):  #to_list：收件人；sub：主题；content：邮件内容
    mail_host='mail.handpay.com.cn'  #设置服务器
    port = '25'
    #mail_postfix="handpay.com.cn"  #发件箱的后缀
    me="itsupport@handpay.com.cn"
    msg = MIMEMultipart() #给定msg类型
    msg['Subject'] = sub #邮件主题
    msg['From'] = me
    msg['To'] = ";".join(mailto_list) 
    msg.attach(context)
    try:  
        s = smtplib.SMTP()  
        s.connect(mail_host,port)  #连接smtp服务器
        s.sendmail(me, mailto_list, msg.as_string())  #发送邮件
        s.close() 
        return True  
    except Exception:    
        return False 
    
def connInfo(ip,port,user,pwd,cmd,flag):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip,port,user,pwd)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    if flag == '1':
        dict_data ={}
        for i in stdout.readlines():     
            list_data=i.split()
            #print b
            for j in range(0,len(list_data),2):
                dict_data[list_data[j+1]] = list_data[j]
            dict_data = dict(dict_data)
        return dict_data
    if flag == '2':
        for i in stdout.readlines():
            b=i.split()
        return b
    ssh.close()
    
#获取ssh得到的数据
def dict_get(dict_t,objkey,num):
    if dict_t.has_key(objkey.index[num]):
        return dict_t[objkey.index[num]]
    else:
        #说明目录存在或者统计值没有
        return -2
    #说明传入该函数的值有问题
    return -1

def dict_get2(dict_t,objkey):
    if dict_t.has_key(objkey):
        return dict_t[objkey]
    else:
        #说明目录存在
        return -3
    #说明传入该函数的值有问题
    return -4


def getMailContent(obj,env):
    d=''
    html=''
    for i in range(len(obj)):
        d = d+"""
        <tr>
          <td>""" + str(obj.index[i]) + """</td>
          <td>""" + str(obj.iloc[i][0]) + """</td>
          <td width="60" align="center">""" + str(obj.iloc[i][1]) + """</td>
          <td width="75">""" + str(obj.iloc[i][2]) + """</td>
          <td width="80">""" + str(obj.iloc[i][3]) + """</td>
        </tr>"""
    html= """\
            <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            <body>
            <div id="container">
            <p></p>
            <div id="content">
            <style>
            td{ border-top:0;border-right: 1px black solid ;border-bottom:1px black solid ;border-left:0;}
            table{ border-top:1px black solid;border-right:0 ;border-bottom:0 ;border-left:1px black solid; }
            </style>
             <table width="50%" border="1" bordercolor="black" cellspacing="0" cellpadding="0">
            <tr>
              <td width="50" bgcolor="90EE90"><strong>"""+str(env)+"""路径</strong></td>
              <td width="10" bgcolor="90EE90"><strong>分类</strong></td>
              <td width="200" bgcolor="90EE90"><strong>备份内容</strong></td>
              <td width="100" bgcolor="90EE90"><strong>统计日期</strong></td>
              <td width="120" bgcolor="90EE90"><strong>当前大小(G)</strong></td>
            </tr>"""+d+"""
            </table>
            </div>
            </div>
            </div>
            </body>
            </html>
      """
    return html
   
def getProStorageInfo(flag):
    '''
    检索dba存储目录：du -sh /data/* |grep -v sys|grep -v net
    检索sys存储目录：du -sh /data/sys/*
    检索net存储目录：du -sh /data/* |grep net
    定义0位execl里的占位数字，通过修改0来实现操作
    '''    
    if flag == 'db' :
        db_data=connInfo("11.11.11.11",22,"root","handpay76!","du -sh /data/* |grep -v sys|grep -v net",'1')
        db_pro=pd.DataFrame({'1备份分类':(0,0,0,0,0),'2备份内容':(0,0,0,0,0),'3统计日期':(0,0,0,0,0),'4备份大小':(0,0,0,0,0)})
        db_pro.index={'/data/app',#生产db备份
                        '/data/hpaydb',
                        '/data/ket_report',
                        '/data/zfquery',
                        '/data/riskdb'} 
        info_pro = db_pro
        info = db_data
    if  flag == 'sys' :
        sys_data=connInfo("10.148.180.115",22,"root","handpay76!","du -sh /data/sys/*",'1')
        sys_pro=pd.DataFrame({'1备份分类':(0,0,0,0,0,0,0,0,0,0),'2备份内容':(0,0,0,0,0,0,0,0,0,0),'3统计日期':(0,0,0,0,0,0,0,0,0,0),'4备份大小':(0,0,0,0,0,0,0,0,0,0)})
        sys_pro.index={'/data/sys/dns',
                       '/data/sys/hornetq',
                        '/data/sys/mfs',
                        '/data/sys/mfs2',
                        '/data/sys/nginx',
                        '/data/sys/opms',
                        '/data/sys/ovirtm',
                        '/data/sys/puppet',
                        '/data/sys/redis',
                        '/data/sys/scrpits'}
        info_pro = sys_pro
        info = sys_data
    if flag == 'net' : 
        net_data=connInfo("11.11.11.11",22,"root","handpay76!","du -sh /data/* |grep net",'1') 
        net_pro=pd.DataFrame({'1备份分类':(0,),'2备份内容':(0,),'3统计日期':(0,),'4备份大小':(0,)})
        net_pro.index={'/data/net'}
        info_pro = net_pro
        info = net_data
        
    '''
    从存储服务器获取各个目录的数据信息
    '''
    for m in range(len(info_pro)):
        info_pro.loc[info_pro.index[m],'4备份大小'] = dict_get(info,info_pro,m)
        info_pro.loc[info_pro.index[m],'2备份内容'] = dict_get(CONF_DATA_DICT_INFO,info_pro,m)
        info_pro.loc[info_pro.index[m],'3统计日期'] = datetime.datetime.now().strftime("%Y%m%d")
        info_pro.loc[info_pro.index[m],'1备份分类'] = dict_get2(CONF_TERM_DICT_INFO,flag)
    return  info_pro


def getOffStorageInfo(flag):
    '''
    检索dba存储目录：du -sh /data/dbbackup/* |grep -v rman_fullbak.sh|grep -v scripts
    检索sys存储目录：du -sh /data/sys/hp*/*
    检索net存储目录：du -sh /data/* |grep net
    '''    
    if flag == 'db' :
        db_data=connInfo("11.11.11.11",22,"root","handpay76!","du -sh /data/dbbackup/* |grep -v rman_fullbak.sh|grep -v scripts",'1')
        db_pro=pd.DataFrame({'1备份分类':(0,0,0,0,0),'2备份内容':(0,0,0,0,0),'3统计日期':(0,0,0,0,0),'4备份大小':(0,0,0,0,0)})
        db_pro.index={'/data/dbbackup/bigdata',
                        '/data/dbbackup/hprp2',
                        '/data/dbbackup/HPSCMDB',
                        '/data/dbbackup/rnhpay',
                       '/data/dbbackup/OTP'
                        } 
        info_pro = db_pro
        info = db_data
    if  flag == 'sys' :
        sys_data=connInfo("11.11.11.11",22,"root","handpay76!","du -sh /data/sys/hp*/*",'1')
        sys_pro=pd.DataFrame({'1备份分类':(0,0,0,0,0,0,0,0,0,0),'2备份内容':(0,0,0,0,0,0,0,0,0,0),'3统计日期':(0,0,0,0,0,0,0,0,0,0),'4备份大小':(0,0,0,0,0,0,0,0,0,0)})
        sys_pro.index={'/data/sys/hpay/hpaypic',
                        '/data/sys/hpc/dns',
                        '/data/sys/hpc/hornetq',
                        '/data/sys/hpc/mfs',
                        '/data/sys/hpc/mfs2',
                        '/data/sys/hpc/nginx',
                        '/data/sys/hpc/opms',
                        '/data/sys/hpc/ovirtm',
                        '/data/sys/hpc/puppet',
                        '/data/sys/hpc/zabbix'}
        info_pro = sys_pro
        info = sys_data
    if flag == 'net' : 
        net_data=connInfo("11.11.11.11",22,"root","handpay76!","du -sh /data/* |grep net",'1') 
        net_pro=pd.DataFrame({'1备份分类':(0,),'2备份内容':(0,),'3统计日期':(0,),'4备份大小':(0,)})
        net_pro.index={'/data/net'}
        info_pro = net_pro
        info = net_data
        
    '''
    从存储服务器获取各个目录的数据信息
    '''
    for m in range(len(info_pro)):
        info_pro.loc[info_pro.index[m],'4备份大小'] = dict_get(info,info_pro,m)
        info_pro.loc[info_pro.index[m],'2备份内容'] = dict_get(CONF_DATA_DICT_INFO,info_pro,m)
        info_pro.loc[info_pro.index[m],'3统计日期'] = datetime.datetime.now().strftime("%Y%m%d")
        info_pro.loc[info_pro.index[m],'1备份分类'] = dict_get2(CONF_TERM_DICT_INFO,flag)
    return  info_pro

def getTest(env,size):
     html_head= """\
            <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />            
            <body>
            <div id="container">
            <p><strong>"""+str(env)+"""备份服务器存储容量信息</strong></p>
            </div>
            </div>
            </body>
            </html>
        """
     return html_head

def getStorageDir():
    size = connInfo("11.11.11.11",22,"root","handpay76!","df -h |grep /u02",'2')
    return size 

def getHtmlHead(env,size):
   #使用率
   userate = size[4]
   #磁盘目录总大小
   dirtotalsize = size[1]
   #目录已用大小
   dirusesize = size[2]
   #目录未使用大小
   diravailsize = size[3]
   #目录对应的存储路径
   #dirdevpath = size[0]
   #目录名
   dirname = size[5]
   if (int(str(userate).strip("%")) > 80):
       td = """<td width="80"><font color="red">"""+str(userate)+"""</font></td>"""
   else:
       td = """<td width="80">"""+str(userate)+"""</td>"""
   d = """
        <tr>
          <td width="50">"""+str(dirname)+"""</td>
          <td width="10">"""+str(dirtotalsize)+"""</td>
          <td width="60">"""+str(dirusesize)+ """</td>
          <td width="75">""" +str(diravailsize)+"""</td>
          """+td+"""
        </tr>"""
   html_head= """\
            <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            <body>
            <div id="container">
            <p><strong>"""+str(env)+"""备份服务器存储容量信息</strong></p>
            <div id="content">
            <style>
            td{ border-top:0;border-right: 1px black solid ;border-bottom:1px black solid ;border-left:0;}
            table{ border-top:1px black solid;border-right:0 ;border-bottom:0 ;border-left:1px black solid; }
            </style>
             <table width="50%" border="1" bordercolor="black" cellspacing="0" cellpadding="0">
            <tr>
              <td width="50" bgcolor="90EE90"><strong>"""+str(env)+"""目录</strong></td>
              <td width="10" bgcolor="90EE90"><strong>总容量</strong></td>
              <td width="200" bgcolor="90EE90"><strong>已用容量</strong></td>
              <td width="100" bgcolor="90EE90"><strong>可使用容量</strong></td>
              <td width="120" bgcolor="90EE90"><strong>空间使用率</strong></td>
            </tr>"""+d+"""
            </table>
            </div>
            </div>
            </div>
            </body>
            </html>
        """
   return html_head
 
if __name__ == '__main__':
    
    '''
        获取execl里的数据
        获取execl框架，并填入数据
    '''
    
    a = getStorageDir()
    html_db = getMailContent(getProStorageInfo('db'),'dba')
    html_sys = getMailContent(getProStorageInfo('sys'),'sys')
    html_net = getMailContent(getProStorageInfo('net'),'net')
    
    html_db_off = getMailContent(getOffStorageInfo('db'),'dba')
    html_sys_off = getMailContent(getOffStorageInfo('sys'),'sys')
    html_net_off = getMailContent(getOffStorageInfo('net'),'net')
    
    html = getHtmlHead('生产环境',a) + html_db + html_sys + html_net + getHtmlHead('灾备环境',a) + html_db_off + html_sys_off + html_net_off

    
    '''
        定义邮件发送信息
    '''

    sub="生产存储服务器容量监控"""
    mailto_list=['min.zhang@handpay.com.cn']
    context = MIMEText(html,_subtype='html',_charset='utf-8')  #解决乱码
    if send_mail(mailto_list,sub,context):  
        print ("发送成功")  
    else:  
        print( "发送失败")
        
        