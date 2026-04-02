# FinFlow Performance Playbook

## Scope
This playbook is for diagnosing slow request chains in FinFlow (frontend + backend + database), especially API-driven table/detail pages.

## 1) Confirm The Symptom
- Capture one concrete case: API path, business id, community id, expected vs actual latency.
- Use fixed timestamps and sample ids in notes.
- Separate "UI feels slow" from "API is slow" with browser Network timing.

## 2) Split End-To-End Time
- In browser DevTools:
- Check TTFB vs content download vs render time.
- Verify whether duplicate requests are triggered.
- In backend:
- Add phase timing around endpoint logic:
- auth/dependency
- primary row query
- relation queries
- response assembly/serialization

Example logging fields:
- request key: receipt_bill_id, community_id, deal_type
- counts: section count, related row count
- timings: t_primary_ms, t_relations_ms, t_serialize_ms, t_total_ms

## 3) Validate Real Indexes (Not Just ORM Definitions)
- ORM model indexes may exist in code but be missing in historical databases.
- Always inspect current DB metadata.
- Verify hot-path compound indexes first (join/filter order aligned with WHERE clause).

For receipt->bills drilldown hot path, validate:
- bills(community_id, deal_log_id)
- receipt_bill_users(receipt_bill_id, community_id)

## 4) Isolate SQL Cost
Run small, direct SQL checks with exact filter conditions:
- total table row count
- filtered count by business keys
- top-N row fetch with sort used by API

If SQL takes milliseconds but endpoint takes seconds, the bottleneck is likely:
- excessive relation loading
- large object serialization
- repeated enrichment logic
- frontend rendering/duplicate fetches

## 5) Minimize Payload For Detail Grids
For drilldown tables, only select fields that are actually displayed.
Avoid returning full rows with large JSON/text columns when the UI needs 6-10 fields.

Pattern:
- Keep existing full loader for template/matching features.
- Add a light loader dedicated to drilldown UI.

## 6) Load Relations By Deal Type (Or Business Type)
Do not load unrelated relations.
Example pattern:
- deal_type in {3,4}: load bills only
- deal_type == 5: load deposit collect only
- deal_type == 6: load refund/transfer paths only

This prevents hidden N+1 style overhead on non-relevant branches.

## 7) Make Perf Logs Switchable
Keep detailed timing logs behind an env flag.
Default off in normal operation.

Recommended env switch:
- ENABLE_RECEIPT_DRILLDOWN_PERF_LOG=1

## 8) Frontend Checks
- Ensure one user action maps to one detail API request.
- Confirm table preference/config requests are not mistaken as primary bottleneck.
- Watch for repeated component mounts causing extra fetches.

## 9) Regression Guard
When a slow path is fixed:
- Add concise code comments where optimization is non-obvious.
- Keep a reproducible sample id for quick re-check.
- Re-run with perf logs enabled and record before/after totals.

## Quick Run Checklist
1. Reproduce with exact id and timestamp.
2. Capture browser Network waterfall.
3. Enable endpoint phase timing logs.
4. Verify real DB indexes for hot predicates.
5. Time direct SQL for the same predicates.
6. Trim response payload to visible columns.
7. Remove unrelated relation loads.
8. Re-test and compare t_total_ms.
