import time
import database, models
import main
from api import voucher_preview_handlers as h

orig = main._preview_voucher_for_bill_via_receipt_templates
stats = []

def wrapped(*args, **kwargs):
    t0 = time.time()
    r = orig(*args, **kwargs)
    dt = (time.time() - t0) * 1000
    bill = kwargs.get('bill') if kwargs else None
    bid = getattr(bill, 'id', None)
    matched = r.get('matched') if isinstance(r, dict) else None
    stats.append((bid, dt, matched))
    if len(stats) <= 5 or dt > 1000:
        print('bill', bid, 'ms', round(dt, 1), 'matched', matched, flush=True)
    return r

main._preview_voucher_for_bill_via_receipt_templates = wrapped

db = database.SessionLocal()
try:
    u = db.query(models.User).filter(models.User.status == 1).order_by(models.User.id.asc()).first()
    t0 = time.time()
    r = h.preview_voucher_for_receipt(
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
    total = (time.time() - t0) * 1000
    print('TOTAL ms', round(total, 1), 'matched', r.get('matched'), 'matched_bills', r.get('matched_bills'))
    if stats:
        vals = [x[1] for x in stats]
        print('calls', len(vals), 'avg', round(sum(vals) / len(vals), 1), 'max', round(max(vals), 1), 'min', round(min(vals), 1))
        top = sorted(stats, key=lambda x: x[1], reverse=True)[:10]
        print('top10', [(t[0], round(t[1], 1), t[2]) for t in top])
finally:
    db.close()
