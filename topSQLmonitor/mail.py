#/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import email
import datetime
import smtplib
import urllib2
import sys
import socket
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

class Mail(object):
 """邮件"""
 host = 'mail.oracle.com'
 port = ''
 user = ''
 psd = ''
 fromAdd = ''
 toAdd = []
 subject = ''
 plainText = ''
 htmlText = '''<html>
 <head><title>''' + subject + '''</title></HEAD>
 <body><span style='cursor=hand'><B>HTML文本</B></span></html>
  '''
 def __init__(self, host='mail.oracle.com.cn', port='25', user='', psd='', mailFrom='dbmonitor@oracle.com.cn',mailTo=['min.zhang@oracle.com.cn'],
  #ipadd=socket.gethostbyname(host)
  self.port = port
  #print ipadd
  #if ipadd=='10.48.196.170':
  # #print 'matched'
  # self.port="587" 
  self.host = host
  self.user = user
  self.psd = psd
  self.fromAdd = mailFrom
  self.toAdds = mailTo
  self.port = port
  #sendEmail(authInfo, fromAdd, toAdd, subject, plainText, htmlText)
  
 #def sendMsg(self, subject, msg, attachment_file=None, _login='true'):
 def sendMsg(self, subject, msg, attachment_file=None, _login='false'):
  server = self.host 
  #user = self.user
  #psd = self.psd
  mailto = ';'.join(self.toAdds)
  #print mailto
  #print self.fromAdd
  msgRoot = MIMEMultipart('related')
  msgRoot['Subject'] = subject
  msgRoot['From'] = 'GEEDUN TOP SQL监控<' + self.fromAdd + '>'
  msgRoot['To'] = mailto
  msgRoot.preamble = 'This is a multi-part message in MIME format.'
  # Encapsulate the plain and HTML versions of the message body in an
  # 'alternative' part, so message agents can decide which they want to display.
  msgAlternative = MIMEMultipart('alternative')
  msgRoot.attach(msgAlternative)
  #设定纯文本信�?  msgText = MIMEText(msg, 'plain', 'gb2312')
  msgAlternative.attach(msgText)
  #设定HTML信息
  #msgText = MIMEText(htmlText, 'html', 'gb2312')
  #msgAlternative.attach(msgText)
  contype = 'application/octet-stream'
  maintype, subtype = contype.split('/', 1)
  #print attachment_file
  if attachment_file:
   
   #data = open(attachment_file, 'rb')
   #file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
   #file_msg.set_payload(data.read())
   #data.close()
   #email.Encoders.encode_base64(file_msg)

   ## 设置附件�?   #print file
   for fn in attachment_file:
    data = open(fn,'rb')
    file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg.set_payload(data.read())
    data.close()
    bn=os.path.basename(fn)
    file_msg.add_header('Content-Disposition','attachment',filename=bn)
    msgAlternative.attach(file_msg)
    email.Encoders.encode_base64(file_msg)
   
   #basename = os.path.basename(attachment_file)
   #file_msg.add_header('Content-Disposition','attachment', filename=basename)
   #msgAlternative.attach(file_msg)
  #发送邮�?  try:
   smtp = smtplib.SMTP()
   #设定调试级别，依情况而定
   #smtp.set_debuglevel(1)
   #print self.port
   smtp.connect(server, self.port)
   #smtp.ehlo()
   #print "_login:"+_login
   if _login == 'true':
    smtp.ehlo()
    smtp.starttls()
    smtp.ehlo()
   #smtp.ehlo()
   #smtp.starttls()
   #smtp.ehlo()
   # smtp.login(config['username'], config['password'])
   #print user
   #print psd
   #smtp.login(user, psd)
   #print self.fromAdd
   #print self.toAdds
   #print msgRoot.as_string()
   #smtp.sendmail(from_addr, to_addrs, msg, mail_options, rcpt_options)
   smtp.sendmail(self.fromAdd, self.toAdds, msgRoot.as_string())
   smtp.quit()
   smtp.close()
  except Exception,e :
   #print '发送邮件异�?'.decode('utf-8').encode('gb2312')+str(e)
   return
  return
  
 def sendEmail(self, authInfo, fromAdd, toAdd, subject, plainText, htmlText):
  strFrom = fromAdd
  strTo = ', '.join(toAdd)
  server = authInfo.get('server')
  user = authInfo.get('user')
  passwd = authInfo.get('password')
  if not (server and user and passwd) :
    #print 'incomplete login info, exit now'
    return

  # 设定root信息
  msgRoot = MIMEMultipart('related')
  msgRoot['Subject'] = subject
  msgRoot['From'] = strFrom
  msgRoot['To'] = strTo
  #msgRoot.preamble = 'This is a multi-part message in MIME format.'

  # Encapsulate the plain and HTML versions of the message body in an
  # 'alternative' part, so message agents can decide which they want to display.
  msgAlternative = MIMEMultipart('alternative')
  msgRoot.attach(msgAlternative)
  #设定纯文本信�?  msgText = MIMEText(plainText, 'plain', 'gb2312')
  msgAlternative.attach(msgText)
  #发送邮�?  smtp = smtplib.SMTP()
  #设定调试级别，依情况而定
  #smtp.set_debuglevel(1)
  smtp.connect(server)
  #smtp.login(user, passwd)
  smtp.sendmail(strFrom, strTo, msgRoot.as_string())
  smtp.quit()
  return 
