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
 host = 'mail.xxx.com'
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
 def __init__(self, host='xx.xx.com.cn', port='25', user='xx', psd='xxx', mailFrom='xxx@xxx.com.cn', mailTo=['xx@xx.com.cn']):
  ipadd=socket.gethostbyname(host)
  self.port = port
  self.host = host
  self.user = user
  self.psd = psd
  self.fromAdd = mailFrom
  self.toAdds = mailTo
  self.port = port

 def sendMsg(self, subject, msg, attachment_file=None, _login='false'):
  server = self.host 
  user = self.user
  psd = self.psd
  mailto = ';'.join(self.toAdds)

  msgRoot = MIMEMultipart('related')
  msgRoot['Subject'] = subject
  msgRoot['From'] = '系统监测<' + self.fromAdd + '>'
  msgRoot['To'] = mailto
  msgRoot.preamble = 'This is a multi-part message in MIME format.'
 
  msgAlternative = MIMEMultipart('alternative')
  msgRoot.attach(msgAlternative)
  #设定纯文本信息
  msgText = MIMEText(msg, 'plain', 'gb2312')
  msgAlternative.attach(msgText)
  
  contype = 'application/octet-stream'
  maintype, subtype = contype.split('/', 1)
  print attachment_file
  if attachment_file:
   
 
   for fn in attachment_file:
    data = open(fn,'rb')
    file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg.set_payload(data.read())
    data.close()
    bn=os.path.basename(fn)
    file_msg.add_header('Content-Disposition','attachment',filename=bn)
    msgAlternative.attach(file_msg)
    email.Encoders.encode_base64(file_msg)
   

  #发送邮件
  try:
   smtp = smtplib.SMTP()
   #设定调试级别，依情况而定

   smtp.connect(server, self.port)

   if _login == 'true':
    smtp.starttls()
  
   smtp.login(user, psd)

   smtp.sendmail(self.fromAdd, self.toAdds, msgRoot.as_string())
   smtp.quit()
   smtp.close()
  except Exception,e :
   return
  return
  
 def sendEmail(self, authInfo, fromAdd, toAdd, subject, plainText, htmlText):
  strFrom = fromAdd
  strTo = ', '.join(toAdd)
  server = authInfo.get('server')
  user = authInfo.get('user')
  passwd = authInfo.get('password')
  if not (server and user and passwd) :
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
  #设定纯文本信息
  msgText = MIMEText(plainText, 'plain', 'gb2312')
  msgAlternative.attach(msgText)
  #发送邮件
  smtp = smtplib.SMTP()
  #设定调试级别，依情况而定
  #smtp.set_debuglevel(1)
  smtp.connect(server)
  #smtp.login(user, passwd)
  smtp.sendmail(strFrom, strTo, msgRoot.as_string())
  smtp.quit()
  return 
