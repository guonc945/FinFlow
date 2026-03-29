import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from fetch_bills import sync_bills_for_community
from fetch_receipt_bills import sync_receipt_bills_for_community

community_id = 10956
print(f"Benchmark target community: {community_id}")

start = time.perf_counter()
try:
    bills_rows = sync_bills_for_community(community_id)
    bills_ok = True
except Exception as exc:
    bills_rows = -1
    bills_ok = False
    print(f"bills_error={exc}")
end = time.perf_counter()
print(f"bills_ok={bills_ok} bills_rows={bills_rows} bills_seconds={end-start:.3f}")

start = time.perf_counter()
try:
    receipt_rows = sync_receipt_bills_for_community(community_id)
    receipt_ok = True
except Exception as exc:
    receipt_rows = -1
    receipt_ok = False
    print(f"receipt_error={exc}")
end = time.perf_counter()
print(f"receipt_ok={receipt_ok} receipt_rows={receipt_rows} receipt_seconds={end-start:.3f}")