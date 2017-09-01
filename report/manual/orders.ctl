load   data   
CHARACTERSET AL32UTF8
infile  '/report/manual/Orders_sqlldir_20170611.bad'   
append into table bdata.orders 
fields terminated by ',' 
TRAILING NULLCOLS
(
id,
order_no,
trans_time,
trans_time_year,
trans_time_month,
trans_time_day,
trans_time_hour,
bank_trans_seq,
trans_notice_date,
trans_type,
trans_status,
pay_amount,
trans_charge,
trans_charge_rate,
trans_address,
trans_address_province,
trans_address_city,
trans_address_county,
bank_account_no,
backend_id,
swiper_type,
channel_resp_code,
channel_resp_content,
product_name,
product_type,
product_oem,
product_trans_type,
tran_charge_template,
is_mul_rate,
is_skip,
no_skip_rate,
merchant_rate_type,
pay_channel,
bt_rate,
bt_amount,
trans_confirm_status,
trans_confirm_resp,
from_card_type,
from_card_bank,
agent_supervisor,
agent_1,
agent_2,
agent_3,
agent_4,
security_code,
produce_date,
produce_date_year,
produce_date_month,
sale_region,
asset_status,
asset_csn,
user_reg_date,
user_reg_year,
user_reg_month,
user_real_name,
uesr_cert_type,
user_cert_number,
user_region,
merch_code,
merch_name,
merch_full_name,
merch_reg_date,
merch_reg_date_year,
merch_reg_date_month,
merch_status,
merch_active_date,
merch_active_date_year,
merch_active_date_month,
big_merch_number,
big_merch_name,
big_merch_nick_name,
unipay_location,
is_settle,
is_audit_photo,
audit_photo_time,
audit_photo_complete_time,
is_audit_photo_freeze,
risk_submit_time,
risk_audit_time,
to_bank,
imei,
mac,
ip,
cancel_status,
accounttranstime,
is_risk_freeze,
risk_tx_time
)
