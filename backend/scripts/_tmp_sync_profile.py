import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import fetch_bills

community_id = 10956

orig_request = fetch_bills.marki_client.request
orig_insert = fetch_bills.insert_bills_data

stats = {
    "request_seconds": 0.0,
    "insert_seconds": 0.0,
    "request_calls": 0,
    "insert_calls": 0,
}

def timed_request(*args, **kwargs):
    t0 = time.perf_counter()
    try:
        return orig_request(*args, **kwargs)
    finally:
        stats["request_seconds"] += time.perf_counter() - t0
        stats["request_calls"] += 1

def timed_insert(*args, **kwargs):
    t0 = time.perf_counter()
    try:
        return orig_insert(*args, **kwargs)
    finally:
        stats["insert_seconds"] += time.perf_counter() - t0
        stats["insert_calls"] += 1

fetch_bills.marki_client.request = timed_request
fetch_bills.insert_bills_data = timed_insert

t0 = time.perf_counter()
rows = fetch_bills.sync_bills_for_community(community_id)
total = time.perf_counter() - t0

print(f"rows={rows}")
print(f"total_seconds={total:.3f}")
print(f"request_calls={stats['request_calls']} request_seconds={stats['request_seconds']:.3f}")
print(f"insert_calls={stats['insert_calls']} insert_seconds={stats['insert_seconds']:.3f}")
print(f"other_seconds={total - stats['request_seconds'] - stats['insert_seconds']:.3f}")