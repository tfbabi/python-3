# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 14:36:04 2016
监控存储容量信息
@author: mzhang
"""




#列名别名配置信息
CONF_DATA_DICT_INFO = {'/data/hprp2':'支付灾备库备份',
                        '/data/HPSCMDB':'基础库，软件发布平台备份',
                        '/data/rnhpay':'软件发布平台备份',
                        '/data/OTP':'IT OTP数据库备份',
                        '/data/sys/hpay/hpaypic':'hpa系统归档图片',
                        '/data/sys/hpc/dns':'hpc系统dns配置文件',
                        '/data/sys/hpc/hornetq':'hpc系统hornetq队列文件',
                        '/data/sys/hpc/mfs':'hpc系统mfs存储文件',
                        '/data/sys/hpc/mfs2':'hpc系统mfs2存储文件',
                        '/data/sys/hpc/nginx':'hpc系统nginx配置文件',
                        '/data/sys/hpc/opms':'hpc系统opms数据库',
                        '/data/sys/hpc/ovirtm':'hpc系统ovirtm数据库及秘钥',
                        '/data/sys/hpc/puppet':'hpc系统puppet配置文件',
                        '/data/sys/hpc/zabbix':'hpc系统zabbix数据库',
                        '/data/hpaydb':'支付核心库备份',
                        '/data/riskdb':'同盾数据库备份',
                        '/data/ket_report':'报表脚本及信息备份',
                        '/data/zfquery':'业务支撑数据库备份',
                        '/data/sys/dns':'hpa系统dns配置文件',
                        '/data/sys/hornetq':'hpa系统hornetq队列文件',
                        '/data/sys/mfs':'hpa系统mfs存储文件',
                        '/data/sys/mfs2':'hpa系统mfs2存储文件',
                        '/data/sys/nginx':'hpa系统nginx配置文件',
                        '/data/sys/opms':'hpa系统opms数据库',
                        '/data/sys/ovirtm':'hpa系统ovirtm数据库及秘钥',
                        '/data/sys/puppet':'hpa系统puppet配置文件',
                        '/data/sys/redis':'hpa系统redis持久化数据',
                        '/data/sys/scrpits':'hpa系统定时命令脚本',
                        '/data/net':'网络'}


#列名别名配置信息
CONF_TERM_DICT_INFO = {'db':'数据库组','net':'网络组','sys':'系统组'}
