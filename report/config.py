# coding:utf-8
from sqlalchemy import create_engine
# 日志对象
from utility import logger

# 数据库连接
DB_CONN_STRING = 'oracle://xxx:xxxx@xx.xx.xx.xx:1521/xxx'


# 订单时间
import datetime
#today = datetime.datetime.today().date()
#target_date = datetime.date(year=2016,month=11,day=17)
#days = (today -target_date).days
days = 1
#CONF_ORDER_TIME = "to_date('2016-10-01','yyyy-mm-dd')+1 - 1"
CONF_ORDER_TIME = "sysdate -%s" % days


#交易结算状态信息IS_SETTLE
SETTLE_STATUS_DICT = {  '00':'已记账',
                        '01':'未结算',
                        '02':'内部核实完成',
                        '03':'已结算'
                        }  

#照片审核
BIZCTRL_AUDIT_STATUS ={'NA':'无需审核', 'WW':'待审核','00':'审核通过','09':'审核拒绝'}

#信审状态
AUDIT_STATUS_DICT = {'0':'审核中','1':'审核通过','2':'审核不通过'}
                        
# 产品交易类型字典 PRODUCT_TRANS_TYPE
CONF_PRODUCT_TRANS_TYPE = {
                           'wechatPay':'微信',
                           'aliPay':'支付宝',
                           'posPay':'线下收单',
                           'hpayUnion125MerchPay':'刷卡收款',
                           'hpayUnion78MerchPay':'刷卡收款',
                           'hpayUnion78MerchPayNotTop':'刷卡收款',
			   'unionIcCardPay':'刷卡收款',
                           'hpayUnionMerchPay':'刷卡收款',
                           'union125IcCardPay':'刷卡收款',
                           'union78IcPay':'刷卡收款',
                           'union78IcPayNotTop':'刷卡收款',
                           'unionMerchPay':'刷卡收款',
                           'SuperTransfer':'超级转账'
                           }

# 设备状态信息
CONF_ASSET_STATUS = {
                     "00":"可用","01":"不可用","02":"挂失","03":"注销","04":"作废"
                     }

# 交易类型字典
CONF_TRANS_TYPE = {
             '00':'支付交易', '01':'冲正交易', '02':'退款交易',
             '03':'撤消交易', '04':'提现交易', '05':'手工调账',
             '06':'圈存交易', 'merdk':'印天代扣', 'RC':'充值',
             'TF':'转账', 'T0WITHDRAW':'T+0提现交易', 'TSWITHDRAW':'T+0秒提现交易',
             'T1PWITHDRAW':'T+1个人提现交易', 'E23':'退单', 'E24':'再请款', 'E25':'退单'
             }

# 交易状态字典
CONF_TRANS_STATUS = {
                'WW':'初始/等待支付状态', '00':'交易成功', '01':'交易失败', '02':'冲正成功',
                '03':'冲正失败', '04':'处理中', '05':'未知', '06':'圈存交易',
                '10':'组合支付失败', '90':'交易异常', 'EE':'支付过期'
                }

# 银行卡类型字典
CONF_BANK_CARD_TYPE = {
                      'I':'IC卡',
                      'S':'磁条卡'
                      }
# 所属产品字典
CONF_CHANNEL_NAME = {
                     'ABCZZT':'ABCZZT','CMBCZZT':'CMBCZZT','COLOURFUL.C':'COLOURFUL.C',
                     'FASTBILL':'FASTBILL','GDRCU.KFB.C':'GDRCU.KFB.C','HPZZT':'HPZZT',
                     'HXZZT':'HXZZT','LCT.C':'LCT.C','M.SUPERPOS':'M.SUPERPOS',
                     'MSF':'MSF','NBCB.C.P':'NBCB.C.P','PAYEASE.C':'PAYEASE.C','pgw':'pgw',
                     'PPK.C':'PPK.C','RYB':'RYB','SMARTPOS':'SMARTPOS','STARPAY.C':'STARPAY.C',
                     'SUPERATM':'SUPERATM','SVS':'SVS','SX.CUP.MNA.C':'SX.CUP.MNA.C',
                     'TBHP':'TBHP','TBHPCT':'TBHPCT'  
                     }

# 交易通道字典，渠道代码
CONF_TRAN_PAYMENT_CHANNEL = {
                             '01':'瀚银通道','02':'瀚银通道','03':'瀚银通道','04':'瀚银通道','05':'瀚银通道',
                             '06':'恒丰通道','07':'杉德通道','08':'杉德通道',
                             '11':'微信通道',
                             '12':'支付宝通道','13':'民生备付金出款','14':'天津银联代付出款','15':'SD非接接入','16':'ZX支付(聚合码上付)','17':'宁波银联全渠道B2B接入','18':'DY接入','10':'HX转账','20':'ZX-WFT反扫通道','21':'宁波民生代付通道'}

# 交易验证状态字典
CONF_TRANS_CONFIRM_STATUS ={
                       'WW':'初始未认证','00':'认证成功','01':'认证失败'
                       }

#用户交易是否撤销
CONFIG_CANCEL_STATUS ={'00':'撤销成功','01':'撤销失败','02':'撤销中'
                    }
                    
#小商户状态配置信息
CONFIG_MERCH_STATUS = {
                  '00':'正常',
                  '01':'注册',
                  '02':'停用',
                  '03':'注销'
                      }

# 银行卡类型
CONF_BANK_CARD_CLASS = {
                       '1':'借记卡','2':'贷记卡','3':'准贷记卡','4':'借贷合一','00':'其它'
                       }

# 订单明细字典
CONF_REPORT_COLS_DICT = {'ORDER_NO':'订单号',  # PAYMENTTRANSSEQ
                    'TRANS_TIME':'交易时间',  # PAYMENTTRANSDATE
                    'TRANS_TIME_YEAR':'交易时间-年',
                    'TRANS_TIME_MONTH':'交易时间-月',
                    'TRANS_TIME_DAY':'交易时间-日',
                    'TRANS_TIME_HOUR':'交易时间-24小时',
                    'BANK_TRANS_SEQ':'银行流水号',  # BACKENDTRANSSEQ
                    'TRANS_NOTICE_DATE':'交易通知日期',  # BACKENDTRANSDATE
                    'TRANS_TYPE':'交易类型',  # type
                                            # 交易类型 00:支付交易 01:冲正交易 02:退款交易 03:撤消交易 04:提现交易 05:手工调账 06:圈存交易
                                            # '':'交易类型描述', # 交易类型 00:支付交易 01:冲正交易 02:退款交易 03:撤消交易 04:提现交易 05:手工调账 06:圈存交易
                    'TRANS_STATUS':'交易状态',  # status
                                           # 交易状态。  WW:初始/等待支付状态00:交易成功01:交易失败02:冲正成功03:冲正失败04:处理中；“05”：未知；“06”：圈存交易；“10”：组合支付失败；“90”：交易异常；“EE”：支付过期；
                    # '':'交易状态描述', 
                    'PAY_AMOUNT':'交易金额',  # PAYMENTAMOUNT
                    'TRANS_CHARGE':'交易手续费金额',  # TRANSCHARGE
                    'TRANS_CHARGE_RATE':'交易手续费率',  # [需要计算]
                                                    # 单笔交易的手续费费率。（当前交易时的实时收费费，如果商户费率为0.78%，35封顶，请明确手续费是0.78%，还是35封顶的手续费）
                    'TRANS_ADDRESS':'交易发生地',  # address
                    'TRANS_ADDRESS_PROVINCE':'交易发生地-省',
                    'TRANS_ADDRESS_CITY':'交易发生地-市',
                    'TRANS_ADDRESS_COUNTY':'交易发生地-区县',
                    'BANK_ACCOUNT_NO':'交易银行卡号',  # PAYMENTTOOLACCOUNTNO
                    'BACKEND_ID':'支付方式代码',  # backendid 
                                            # 刷卡；alipay支付宝；wechatPay微信；NFC
                    'SWIPER_TYPE':'交易银行卡类型 ',  # transfer_log,
                                                # SWIPER_TYPE
                                                # I:IC卡，S:磁条卡
                    'CHANNEL_RESP_CODE':'通道返回码',  # BACKENDRESPCODE
                    'CHANNEL_RESP_CONTENT':'通道返回码描述',  # CONTENT
                    'PRODUCT_NAME':'所属产品',  # CHANNEL
                                            # 微POS；微POS华夏版（原账账通）；秒秒通；智能POS;卡乐付；卡付宝；星乐付；超级ATM；超级POS;瀚银钱包；手付通；码上付；
                    'PRODUCT_TYPE':'产品类型',  # [无字段]
                                            # 收单；个人移动vipos；个人移动；商户移动；金融服务
                    'PRODUCT_OEM':'所属产品下的OEM产品',  # [无解释]
                    'PRODUCT_TRANS_TYPE':'产品交易类型',  # backendid [无解释]
                    'TRAN_CHARGE_TEMPLATE':'选择的交易费率模板',  # [需计算]
                                                            # 建议参照pkg_agent_util.fnc_get_trans_charge_rule({交易金额},{BT手续费ID}) 写一个类似的函数
                    'IS_MUL_RATE':'是否为多费率交易',  # route_log, is_public
                    'IS_SKIP':'是否跳码',  # route_log,IS_SKIP
                    'NO_SKIP_RATE':'不跳码费率 ',  # route_security_code,type
                                                # 交易费率为1.25%的不跳吗费率为1.25；0.78%-0.78%；封顶商户对应0.78%，26封顶商户；一般费率商户对应0.38%费率
                                                # 01: 0.78,26元封顶; 02：0.78%; 03：1.25% 如设备在此张表中没有，则为 0.38%
                    'MERCHANT_RATE_TYPE':'商户类型',  # [需计算]
                                            # t_agent_chrg_merch_open_info,CHRG_OPEN_FLAG
                                            # 01：多费率商户，其他状态代表普通商户，如表中不存在商户记录，表示普通商户
                    'PAY_CHANNEL':'交易通道',  # PAYMENT_CHANNEL
                                            # 01、02、03、04、05：瀚银通道
                                            # 06：恒丰通道
                                            # 07、08：杉德通道
                                            # 11：微信通道
                                            # 12：支付宝通道
                                            # 13：民生银行
                                            # 14：天津银联代付通道
                    'BT_RATE':'BT费率',  # [需计算]
                                        # 建议参照pkg_agent_util.fnc_get_trans_charge_rule({交易金额},{BT手续费ID}) 写一个类似的函数
                                        # 参数：
                                        # 交易金额：payment_transaction_log表中的 ATTACH_CHARGE
                                        # 手续费ID：payment_transaction_log表中的 ATTACH_CHARGE_ID
                                        # 返回值：0.005%
                    'BT_AMOUNT':'BT金额',  # ATTACH_CHARGE ,单位:分
                    'TRANS_CONFIRM_STATUS':'交易验证状态',  # [需计算]
                                                        # payment_transaction_log plog
                                                        # micro_card_auth mca
                                                        # 条件:
                                                        # plog.PAYMENTTOOLACCOUNTNO = mca.card_no
                                                        # and plog.CERT_TYPE = '0'
                                                        #
                                                        # STATUS
                                                        # WW-初始未认证、00-认证成功、01-认证失败
                    'TRANS_CONFIRM_RESP':'交易验证状态返回码描述',  # [需计算]
                                                             # micro_card_auth
                                                             # EXT
                    #'TRANS_AMOUNT_STAGE':'单笔交易金额段',  # [无字段]
                    'FROM_CARD_TYPE':'交易银行卡种',  # CARD_TYPE
                                            # 1借记卡；2贷记卡；3准贷记卡；4借贷合一
                    'FROM_CARD_BANK':'付款银行卡银行',  # [需计算]
                                            # 参考SQL：
                                            # SELECT tci.name_cn
                                            # FROM hpay.t_card_info tci
                                            # WHERE rownum = 1
                                            # AND instr(hpay.aes128_decrypt(plog.paymenttoolaccountno), tci.card_no_prefix) = 1
                    'AGENT_SUPERVISOR':'监管代理商',  # [无字段]
                    'AGENT_1':'一级代理商',  # [需计算]
                                # terminal_info
                                # 建议使用hpay.pkg_agent_util.fnc_sub_str_by_sign(agent_info,1)，
                                # 使用取出来的id查询t_agent_info表中的corp_full_name
                    'AGENT_2':'二级代理商',
                    'AGENT_3':'三级代理商',
                    'AGENT_4':'四级代理商',
                    'SECURITY_CODE':'银联防伪码',  # terminal_info, security_code
                    'PRODUCE_DATE':'生产日期',  # [需计算]
                                # terminal_info,PERSONLIZED_TIME
                                # 
                    'PRODUCE_DATE_YEAR':'生产日期-年',  # 同PRODUCE_DATE
                    'PRODUCE_DATE_MONTH':'生产日期-月',
                    'SALE_REGION':'销售区域',  # [需计算]
                                # terminal_info,address
                                # 空值代表“全国”
                    'ASSET_STATUS':'设备状态',
                    'ASSET_CSN':'设备号CSN',  # CSN
                    'USER_REG_DATE':'用户注册日期',  # user_info,createtime
                    'USER_REG_YEAR':'用户注册日期-年',  # user_info,createtime
                    'USER_REG_MONTH':'用户注册日期-月',  # user_info,createtime
                    'USER_REAL_NAME':'用户姓名',  # user_info,realname
                    'USER_MOBILE':'用户手机',  # user_info,mobile
                    'UESR_CERT_TYPE':'证件类型',  # user_info,CERTIFICATETYPE
                    'USER_CERT_NUMBER':'证件号',  # user_info,CERTIFICATENUMBER
                    'USER_REGION':'身份证地区',  # [需计算]
                                # 新建一张身份证地区码维度表，然后使用身份证号截取6位进行判断
                                # http://www.cnblogs.com/wormday/articles/278709.html
                    'MERCH_CODE':'小商户号',  # merch_merchant,merchantcode
                    'MERCH_NAME':'小商户名称',  # merch_merchant,fullname
                    'MERCH_FULL_NAME':'小商户店铺名称',  # merch_merchant,MERCHANTNAME
                    'MERCH_REG_DATE':'小商户入网日期',  # [需计算],tmc.CREATE_DATE
                                    # terminal_info ti
                                    # t_merchant_csn tmc
                                    # ti.csn = tmc.csn(+)
                    'MERCH_REG_DATE_YEAR':'小商户入网日期-年',
                    'MERCH_REG_DATE_MONTH':'小商户入网日期-月',
                    'MERCH_STATUS':'小商户状态',  # merch_merchant,STATUS
                                            # 00-正常
                                            # 01-注册
                                            # 02-停用
                                            # 03-注销
                    'MERCH_ACTIVE_DATE':'小商户激活日期',  # [需计算]
                                                    # payment_transaction_log
                                                    # select min(paymenttransdate) from payment_transaction_log plog where plog.merchantcode = '{商户号}'
                    'MERCH_ACTIVE_DATE_YEAR':'小商户激活日期-年',
                    'MERCH_ACTIVE_DATE_MONTH':'小商户激活日期-月',
                    'BIG_MERCH_NUMBER':'银联商户号',  # [需计算]
                                # payment_transaction_log
                                # payment_commision_config
                                # MERCHANTBACKENDCODE
                    'BIG_MERCH_NAME':'银联商户号名称',  # payment_commision_config,COMMISONNAME
                    'BIG_MERCH_NICK_NAME':'银联商户号简称',  # payment_commision_config,PARAMS
                    'UNIPAY_LOCATION':'落地地区',  # payment_merchantcode_config,BANK_LOCATION  1
                    'IS_SETTLE':'交易是否结算',  # smhpay.account_voucher_log,SETTLE_STATUS 1
                                    # 01、02：未结算
                                    # 03：已结算
                    'IS_AUDIT_PHOTO':'交易结算是否照片冻结',  # [疑问] 照片审核?
                                                    # payment_transaction_log,BIZCTRL_AUDIT_STATUS
                                                    # NA无需审核；WW待审核；00审核通过；09审核拒绝
                    'AUDIT_PHOTO_TIME':'照片审核提交时间',  # [疑问] 交易时间等于审核时间? paymenttransdate
                    'AUDIT_PHOTO_COMPLETE_TIME':'照片审核结束时间',  # 需计算 
                                    # payment_transaction_log.BIZCTRL_AUDIT_TIME
                                    # BIZCTRL_AUDIT_STATUS != WW，取该值
                    'IS_AUDIT_PHOTO_FREEZE':'交易结算是否信审冻结',  # [无描述]
                    'RISK_SUBMIT_TIME':'信审审核提交时间',  # rcontrol.rsk_aptitude_apply,create_time   1
                                                    # merchant_code
                    'RISK_AUDIT_TIME':'信审审核结束时间',  # rcontrol.rsk_aptitude_apply.audit_time   1
                                                    # merchant_code
                    'IS_RISK_FREEZE':'交易结算是否风控冻结',  # rcontrol.RSK_TRANS_LOG .FREEZE_STATUS   1
                                                     # FREEZED：冻结，其他状态为正常
                    'RISK_TX_TIME':'提交风控时间',  # rcontrol.RSK_TRANS_LOG.TX_SUCCESS_TIME  
                    'TO_BANK':'收款银行卡银行',  # [需计算]
                                            # t_card_info
                                            # payment_transaction_log
                                            # 参考SQL：
                                            # SELECT tci.name_cn
                                            # FROM hpay.t_card_info tci
                                            # WHERE rownum = 1
                                            # AND instr(hpay.aes128_decrypt(plog.INPUTACCOUNTNO), tci.card_no_prefix) = 1
                    'IMEI':'IMEI',  # payment_transaction_log.imei
                    'MAC':'MAC',  # payment_transaction_log.mac
                    'IP':'IP地址',
                    'CANCEL_STATUS':'撤销状态',
		    'ACCOUNTTRANSTIME':'日切时间'}


