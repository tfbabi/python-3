# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 14:36:05 2016
以terminal_info表为基础来做扩展
@author: mzhang
"""

from sqlalchemy import create_engine
#日志对象
from utility import logger

#邮件信息
mail_to = ['test@oracle.com.cn']
port = "25"

#数据库连接
DB_CONN_STRING_HPRISK = 'oracle://test:test@10.10.10.10:1521/test'
DB_CONN_STRING_RCONTROL = 'oracle://test1:test1@10.10.10.10:1521/test1'

import datetime
days = 1
CONF_ORDER_TIME = "sysdate -%s" % days



#列名别名配置信息
CONF_RATE_ALIAES_NAME = {
                         'CNAME':'表名',
                       'CSUM':'数量'
                           }
