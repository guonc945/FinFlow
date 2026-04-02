import time
import database, models
import main
from api import voucher_preview_handlers as h

orig_bill = main.preview_voucher_for_bill
orig_receipt_via = main._preview_voucher_for_bill_via_receipt_templates
bill_stats = []
via_stats = []

def wrap_bill(*args, **kwargs):
    t0=time.time(); r=orig_bill(*args, **kwargs); dt=(time.time()-t0)*1000
    bid=kwargs.get('bill_id') if kwargs else (args[0] if args else None)
    bill_stats.append((bid, dt, (r.get('matched') if isinstance(r,dict) else None), (r.get('matched_root_source') if isinstance(r,dict) else None)))
    if len(bill_stats)<=5 or dt>1000:
        print('preview_bill', bid, 'ms', round(dt,1), 'matched', (r.get('matched') if isinstance(r,dict) else None), 'root', (r.get('matched_root_source') if isinstance(r,dict) else None), flush=True)
    return r

def wrap_via(*args, **kwargs):
    t0=time.time(); r=orig_receipt_via(*args, **kwargs); dt=(time.time()-t0)*1000
    bill=kwargs.get('bill') if kwargs else None
    bid=getattr(bill,'id',None)
    via_stats.append((bid, dt, (r.get('matched') if isinstance(r,dict) else None)))
    return r

main.preview_voucher_for_bill = wrap_bill
main._preview_voucher_for_bill_via_receipt_templates = wrap_via

db=database.SessionLocal()
try:
    u=db.query(models.User).filter(models.User.status==1).order_by(models.User.id.asc()).first()
    t0=time.time()
    r=h.preview_voucher_for_receipt(
        receipt_bill_id=16408521,
        community_id=10956,
        x_account_book_id=None,
        x_account_book_name=None,
        x_account_book_number='001',
        allow_bill_fallback=True,
        current_user=u,
        db=db,
        allowed_community_ids=[10956],
    )
    total=(time.time()-t0)*1000
    print('TOTAL ms', round(total,1), 'matched', r.get('matched'), 'matched_bills', r.get('matched_bills'))
    if bill_stats:
        vals=[x[1] for x in bill_stats]
        print('bill_calls', len(vals), 'avg', round(sum(vals)/len(vals),1), 'max', round(max(vals),1), 'min', round(min(vals),1))
        top=sorted(bill_stats,key=lambda x:x[1], reverse=True)[:10]
        print('bill_top10', [(t[0], round(t[1],1), t[2], t[3]) for t in top])
    if via_stats:
        vals=[x[1] for x in via_stats]
        print('via_calls', len(vals), 'avg', round(sum(vals)/len(vals),1), 'max', round(max(vals),1), 'min', round(min(vals),1))
finally:
    db.close()
