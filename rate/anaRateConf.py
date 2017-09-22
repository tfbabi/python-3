﻿# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 14:36:05 2016
以terminal_info表为基础来做扩展
@author: mzhang
"""

from sqlalchemy import create_engine
#日志对象
from utility import logger

#数据库连接
DB_CONN_STRING = 'oracle://trsenzhang:trsenzhang@10.10.10.10:1521/db'

import datetime
days = 1
days1 = 2
CONF_ORDER_TIME = "sysdate -%s" % days
CONF_ORDER_TIME1 = "sysdate"
CONF_ORDER_TIME2 = "sysdate -%s" % days1


'''					
贷				
0.45%	         发卡=金额*0.45%			
0.410%	      成本=金额*0.41%			
0.407%	      成本=金额*0.407%			
0.2%保底0.3元	   成本=金额*0.2%，若小于0.3则取0.3	
0.3元一笔	      每笔成本都是0.3元一笔		
0.16%	         成本=金额*0.16%			
0.513%	      成本=金额*0.513%	

0.52%	成本=金额*0.52%
0.03%	成本=金额*0.03%
0.51%	成本=金额*0.51%

		
'''
CONF_LOAN_RATE_INFO={
        '0.45%':'1',
        '0.410%':'2',
        '0.407%':'3',
        '0.2%保底0.3元':'4',
        '0.3元一笔':'5',
        '0.16%':'6',
        '0.513%':'7',
        '0.450%':'8',
        '0.52%':'9',
        '0.03%':'10',
        '0.51%':'11'
        }

'''
借 				
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

0.32%-13.5	成本=金额*0.32%，若大于13.5则取13.5
0.4%-17	成本=金额*0.4%，若大于17则取17
0.03%	成本=金额*0.03%

'''
CONF_BORROW_RATE_INFO={
        '0.3一笔':'1',
        '0.413%-17':'2',
        '0.309%-13.28':'3',
        '0.31%-14':'4',
        '0.18%保底0.3':'5',
        '0.393%-16.85':'6',
        '0.312%-13.28':'7',
        '0.309%-13.68':'8',
        '0.16%':'9',
        '0.35%13元封顶':'10',
        '0.32%-13.5':'11',
        '0.4%-17':'12',
        '0.03%':'13'
        }

#列名别名配置信息
CONF_RATE_ALIAES_NAME = {
                         'big_merch_number':'商户号',
                       'from_card_type':'支付卡类型',
                       'pay_amount':'支付金额',
                       'trans_charge':'手续费',
                       'merch_name':'商户名',
                       'r_borrow':'借记卡',
                       'r_loan':'贷记卡',
                       'r_yinlian':'银联',
                       'channel_name':'通道名',
                       'type':'类型',
                       'area_32':'区域32',
                       'beizhu':'备注',
                       'COST':'成本',
                       'GRAIN':'利润',
                       'ORDER_NO':'流水号',
                       'TRANS_CHARGE_RATE':'交易费率',
                       'TRANS_ADDRESS_PROVINCE':'省',
                       'TRANS_ADDRESS_CITY':'市',
                       'TRANS_ADDRESS_COUNTY':'地区',
                       'PRODUCT_NAME':'产品名',
                       'BACKEND_ID':'BACKEND_ID',
                       'PRODUCT_OEM':'产品OEM',
                       'TRANS_TIME':'交易时间',
                       'ACCOUNTTRANSTIME':'日切时间'
                           }