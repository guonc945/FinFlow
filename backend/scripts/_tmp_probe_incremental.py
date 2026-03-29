import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from database import SessionLocal
from models import Bill, ReceiptBill
from utils.marki_client import marki_client, get_api_url_by_id, get_api_url

community_id = 10956

db=SessionLocal()
try:
    max_deal_log = db.query(Bill.deal_log_id).filter(Bill.community_id==community_id).order_by(Bill.deal_log_id.desc()).first()
    max_deal_log = int(max_deal_log[0] or 0) if max_deal_log else 0
    max_deal_time = db.query(ReceiptBill.deal_time).filter(ReceiptBill.community_id==community_id).order_by(ReceiptBill.deal_time.desc()).first()
    max_deal_time = int(max_deal_time[0] or 0) if max_deal_time else 0
finally:
    db.close()

print('max_deal_log', max_deal_log)
print('max_receipt_deal_time', max_deal_time)

# bills full-ish
url_bills = get_api_url('getBillList', preloaded_vars={'communityID':str(community_id),'endMonth':'2026-12','page':'1'})
payload_full = {"badBillCheck":0,"chargeItemVersion":2,"communityID":community_id,"dealLogId":0,"endMonth":"2026-12","index":"","pageSize":1000,"payStatus":3,"page":1}
resp = marki_client.request('POST', url_bills, json_data=payload_full)
full_list = ((resp or {}).get('data') or {}).get('list') or []
print('bills_full_page1', len(full_list))

payload_inc = dict(payload_full)
payload_inc['dealLogId'] = max_deal_log
payload_inc['page'] = 1
resp2 = marki_client.request('POST', url_bills, json_data=payload_inc)
inc_list = ((resp2 or {}).get('data') or {}).get('list') or []
print('bills_inc_page1', len(inc_list))
print('bills_inc_sample_ids', [x.get('id') for x in inc_list[:5]])

# receipt incremental probe (api id 29)
url_receipt = get_api_url_by_id(29, preloaded_vars={})
payload_r_full = {"maxDealTime": 4102444800, "minDealTime": 1262275200, "communityId": community_id, "pageSize": 1000}
r1 = marki_client.request('POST', url_receipt, json_data=payload_r_full)
r1_list = ((r1 or {}).get('data') or {}).get('list') or []
print('receipt_full_page1', len(r1_list))

payload_r_inc = dict(payload_r_full)
payload_r_inc['minDealTime'] = max_deal_time
r2 = marki_client.request('POST', url_receipt, json_data=payload_r_inc)
r2_list = ((r2 or {}).get('data') or {}).get('list') or []
print('receipt_inc_page1', len(r2_list))
print('receipt_inc_sample_ids', [x.get('id') for x in r2_list[:5]])