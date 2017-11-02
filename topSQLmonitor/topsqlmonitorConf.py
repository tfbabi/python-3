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
mail_to = ['min.zhang@handpay.com.cn']
port = "25"

#数据库连接
DB_CONN_STRING_1 = 'oracle://test1:test1@10.10.10.10:1521/test1'

import datetime
days = 1
CONF_ORDER_TIME = "sysdate -%s" % days



#列名别名配置信息
CONF_RATE_ALIAES_NAME = {
                        'elapsed_time':'运行时间',
                        'cpu_time':'cpu时间',
                        'executions':'执行次数',
                        'elapsed_time_per_exec':'平均执行时间',
                        'elapsed_time_per_total':'该sql运行时间占总时间的比例',
                        'sql_id':'sql标识',
                        'sql_module':'连接驱动信息',
                        'sql_text':'sql内容',
                        'cpu_time_per_exec':'平均cpu时间',
                        'physical_reads':'物理读',
                        'physical_read_per_total':'该sql物理读所占总物理消耗比例',
                        'physical_read_cpu_time':'该sql物理读所占cpu时间',
                        'physical_read_elapsed_time':'该sql物理读所占运行时间',
                        'buffer_gets':'逻辑读',
                        'reads_per_exec':'每次逻辑读数',
                        'logical_read_per_total':'该sql逻辑读所占总物理消耗比例',
                        'logical_read_cpu_time':'该sql逻辑读所占cpu时间',
                        'logical_read_elapsed_time':'该sql逻辑读所占运行时间',
                           }
