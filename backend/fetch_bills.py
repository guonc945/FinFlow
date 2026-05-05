# -*- coding: utf-8 -*-
import json
import os
import time
from collections import defaultdict
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import func

from database import SessionLocal
from models import Bill, BillUser, ExternalApi, ExternalService, GlobalVariable, ProjectList
from sync_tracker import tracker
from utils.api_config import require_api_id
from utils.db_compat import fetch_all_project_ids, upsert_model_rows
from utils.marki_client import get_api_url_by_id, marki_client
from utils.sqlserver_partitions import ensure_default_financial_partitions
from utils.variable_parser import resolve_dict_variables

load_dotenv()
MARKI_BILL_API_ID = require_api_id("MARKI_BILL_API_ID")


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _parse_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        value = int(str(raw).strip())
        return value if value > 0 else int(default)
    except Exception:
        return int(default)


def _load_last_full_sync_time(db, community_id: int):
    key = f"BILL_LAST_FULL_SYNC_{int(community_id)}"
    row = db.query(GlobalVariable).filter(GlobalVariable.key == key).first()
    if not row or not row.value:
        return None
    try:
        return datetime.fromisoformat(str(row.value).strip())
    except Exception:
        return None


def _load_last_empty_incremental_fallback_time(db, community_id: int):
    key = f"BILL_LAST_EMPTY_INCREMENTAL_FALLBACK_{int(community_id)}"
    row = db.query(GlobalVariable).filter(GlobalVariable.key == key).first()
    if not row or not row.value:
        return None
    try:
        return datetime.fromisoformat(str(row.value).strip())
    except Exception:
        return None


def _save_last_full_sync_time(db, community_id: int):
    key = f"BILL_LAST_FULL_SYNC_{int(community_id)}"
    now_value = datetime.utcnow().isoformat()
    row = db.query(GlobalVariable).filter(GlobalVariable.key == key).first()
    if row:
        row.value = now_value
        row.description = "Last successful full sync time for bills"
        row.category = "sync_status"
        row.is_secret = False
    else:
        db.add(
            GlobalVariable(
                key=key,
                value=now_value,
                description="Last successful full sync time for bills",
                category="sync_status",
                is_secret=False,
            )
        )


def _save_last_empty_incremental_fallback_time(db, community_id: int):
    key = f"BILL_LAST_EMPTY_INCREMENTAL_FALLBACK_{int(community_id)}"
    now_value = datetime.utcnow().isoformat()
    row = db.query(GlobalVariable).filter(GlobalVariable.key == key).first()
    if row:
        row.value = now_value
        row.description = "Last empty incremental fallback time for bills"
        row.category = "sync_status"
        row.is_secret = False
    else:
        db.add(
            GlobalVariable(
                key=key,
                value=now_value,
                description="Last empty incremental fallback time for bills",
                category="sync_status",
                is_secret=False,
            )
        )


def _should_use_full_sync(db, community_id: int):
    if _parse_bool_env("MARKI_BILL_FORCE_FULL_SYNC", False):
        return True, "force flag"

    interval_hours = _parse_positive_int_env("MARKI_BILL_FULL_SYNC_INTERVAL_HOURS", 24 * 7)
    if interval_hours <= 0:
        return False, "interval disabled"

    last_full = _load_last_full_sync_time(db, community_id)
    if last_full is None:
        return True, "missing full-sync marker"

    elapsed_seconds = (datetime.utcnow() - last_full).total_seconds()
    if elapsed_seconds >= interval_hours * 3600:
        return True, f"interval reached ({interval_hours}h)"
    return False, "within interval"


def _can_retry_empty_incremental_with_full_sync(db, community_id: int):
    cooldown_hours = _parse_positive_int_env(
        "MARKI_BILL_EMPTY_INCREMENTAL_FULL_SYNC_COOLDOWN_HOURS",
        24,
    )
    if cooldown_hours <= 0:
        return True, "cooldown disabled"

    last_retry = _load_last_empty_incremental_fallback_time(db, community_id)
    if last_retry is None:
        return True, "missing fallback marker"

    elapsed_seconds = (datetime.utcnow() - last_retry).total_seconds()
    if elapsed_seconds >= cooldown_hours * 3600:
        return True, f"cooldown elapsed ({cooldown_hours}h)"
    return False, f"cooldown active ({cooldown_hours}h)"


def validate_timestamp(timestamp):
    try:
        if timestamp is None:
            return None
        if isinstance(timestamp, str):
            ts = timestamp.strip()
            if not ts or not ts.isdigit():
                return None
            timestamp = int(ts)
        timestamp = int(timestamp)
        if timestamp > 0:
            return timestamp
    except Exception:
        return None
    return None


def normalize_datetime(value):
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)) and value > 0:
            return datetime.fromtimestamp(int(value))
        if isinstance(value, str):
            v = value.strip()
            if not v:
                return None
            if v.isdigit():
                return datetime.fromtimestamp(int(v))
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except Exception:
                return None
    except Exception:
        return None
    return None


def format_amount(val):
    if val is None:
        return None
    try:
        from decimal import Decimal, ROUND_HALF_UP

        dec_val = Decimal(str(val))
        result = dec_val / Decimal("100")
        return float(result.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    except Exception:
        return val


def _to_json_str(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)



def _iter_chunks(items, size: int):
    if size <= 0:
        raise ValueError("size must be positive")
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _extract_bills_data(result):
    if isinstance(result, dict) and isinstance(result.get("data"), dict) and isinstance(result["data"].get("list"), list):
        return result["data"]["list"]
    if isinstance(result, dict) and isinstance(result.get("list"), list):
        return result["list"]
    if isinstance(result, list):
        return result
    return []


def _reconcile_missing_bills_for_community(
    community_id: int,
    seen_bill_ids,
    reconciled_at: datetime,
    task_id: str = None,
):
    grace_hours = _parse_positive_int_env("MARKI_BILL_MISSING_GRACE_HOURS", 24 * 7)
    active_rows = []
    db = SessionLocal()
    try:
        active_rows = (
            db.query(Bill.id, Bill.last_seen_at)
            .filter(
                Bill.community_id == int(community_id),
                (Bill.source_deleted == False) | (Bill.source_deleted.is_(None)),
            )
            .all()
        )
        if not active_rows:
            return {"missing": 0, "soft_deleted": 0, "deferred": 0}

        cutoff = reconciled_at.timestamp() - grace_hours * 3600
        to_soft_delete = []
        deferred_count = 0
        for row in active_rows:
            bill_id = int(row.id)
            if bill_id in seen_bill_ids:
                continue
            last_seen_at = row.last_seen_at
            last_seen_ts = last_seen_at.timestamp() if last_seen_at else 0
            if last_seen_ts <= cutoff:
                to_soft_delete.append(bill_id)
            else:
                deferred_count += 1

        for bill_id_chunk in _iter_chunks(sorted(to_soft_delete), 900):
            (
                db.query(Bill)
                .filter(
                    Bill.community_id == int(community_id),
                    Bill.id.in_(bill_id_chunk),
                )
                .update(
                    {
                        Bill.source_deleted: True,
                        Bill.source_deleted_at: reconciled_at,
                    },
                    synchronize_session=False,
                )
            )

        db.commit()
        msg = (
            f"Community {community_id}: full bill reconciliation checked {len(active_rows)} active rows, "
            f"missing {len(to_soft_delete) + deferred_count}, soft-deleted {len(to_soft_delete)}, "
            f"deferred {deferred_count}."
        )
        print(msg)
        if task_id:
            tracker.add_log(task_id, msg, "info")
        return {
            "missing": len(to_soft_delete) + deferred_count,
            "soft_deleted": len(to_soft_delete),
            "deferred": deferred_count,
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def insert_bills_data(data_list, community_id_context=None):
    db = SessionLocal()
    inserted_count = 0
    skipped_count = 0
    unchanged_count = 0
    touched_bill_ids_by_community = defaultdict(set)
    bill_user_rows = []
    staged_entries = []
    seen_at = datetime.utcnow()

    try:
        for item in data_list:
            bill_id = item.get("id")
            community_id = community_id_context or item.get("communityId") or item.get("communityID")
            if bill_id is None or community_id is None:
                skipped_count += 1
                continue

            try:
                bill_id = int(bill_id)
                community_id = int(community_id)
            except Exception:
                skipped_count += 1
                continue

            pay_time = validate_timestamp(item.get("payTime"))
            receive_date = None
            if pay_time:
                try:
                    receive_date = datetime.fromtimestamp(pay_time).date()
                except Exception:
                    receive_date = None

            values = {
                "charge_item_id": item.get("chargeItemID"),
                "ci_snapshot_id": item.get("ciSnapshotId"),
                "charge_item_name": item.get("chargeItemName"),
                "charge_item_type": item.get("chargeItemType"),
                "category_name": item.get("categoryName"),
                "asset_id": item.get("assetID"),
                "asset_name": item.get("assetName"),
                "asset_type": item.get("assetType"),
                "asset_type_str": item.get("assetTypeStr"),
                "house_id": item.get("houseId"),
                "full_house_name": item.get("FullHouseName"),
                "bind_house_id": (item.get("bindHouseInfo") or {}).get("id"),
                "bind_house_name": (item.get("bindHouseInfo") or {}).get("name"),
                "park_id": item.get("parkId"),
                "park_name": item.get("parkName"),
                "bill_month": None,
                "in_month": item.get("inMonth"),
                "start_time": validate_timestamp(item.get("startTime")),
                "end_time": validate_timestamp(item.get("endTime")),
                "amount": format_amount(item.get("amount")),
                "bill_amount": format_amount(item.get("billAmount")),
                "discount_amount": format_amount(item.get("discountAmount")),
                "late_money_amount": format_amount(item.get("lateMoneyAmount")),
                "deposit_amount": format_amount(item.get("depositAmount")),
                "second_pay_amount": format_amount(item.get("secondPayAmount")),
                "pay_status": item.get("payStatus"),
                "pay_status_str": item.get("payStatusStr"),
                "pay_type": item.get("payType"),
                "pay_type_str": item.get("payTypeStr"),
                "pay_time": pay_time,
                "receive_date": receive_date,
                "second_pay_channel": item.get("secondPayChannel"),
                "bill_type": item.get("billType"),
                "bill_type_str": item.get("billTypeStr"),
                "deal_log_id": item.get("dealLogId"),
                "receipt_id": item.get("receiptId"),
                "sub_mch_id": item.get("subMchId"),
                "sub_mch_name": item.get("subMchName"),
                "bad_bill_state": item.get("badBillState"),
                "is_bad_bill": item.get("isBadBill"),
                "has_split": item.get("hasSplit"),
                "split_desc": item.get("splitDesc"),
                "visible_type": (item.get("visibleInfo") or {}).get("visibleType"),
                "visible_desc_str": (item.get("visibleInfo") or {}).get("visibleDescStr"),
                "can_revoke": item.get("canRevoke"),
                "version": item.get("version"),
                "meter_type": item.get("meterType"),
                "snapshot_size": item.get("snapshotSize"),
                "now_size": item.get("nowSize"),
                "remark": item.get("remark"),
                "bind_toll": _to_json_str(item.get("bindToll", [])),
                "user_list": _to_json_str(item.get("userList", [])),
                "create_time": validate_timestamp(item.get("createTime")),
                "last_op_time": normalize_datetime(item.get("lastOpTime")),
                "last_seen_at": seen_at,
                "source_deleted": False,
                "source_deleted_at": None,
            }

            normalized_row = {
                "id": bill_id,
                "community_id": community_id,
                **values,
            }
            staged_entries.append((item, normalized_row))

        existing_rows = {}
        community_to_bill_ids = defaultdict(list)
        for _, row in staged_entries:
            community_to_bill_ids[int(row["community_id"])].append(int(row["id"]))

        for community_id, bill_ids in community_to_bill_ids.items():
            unique_bill_ids = sorted(set(bill_ids))
            for bill_id_chunk in _iter_chunks(unique_bill_ids, 900):
                rows = (
                    db.query(Bill.id, Bill.community_id, Bill.version, Bill.last_op_time, Bill.user_list)
                    .filter(
                        Bill.community_id == community_id,
                        Bill.id.in_(bill_id_chunk),
                    )
                    .all()
                )
                for row in rows:
                    existing_rows[(int(row.id), int(row.community_id))] = row

        changed_rows = []
        for item, normalized_row in staged_entries:
            key = (int(normalized_row["id"]), int(normalized_row["community_id"]))
            existing = existing_rows.get(key)
            incoming_user_list = normalized_row.get("user_list")
            same_snapshot = (
                existing is not None
                and existing.version == normalized_row.get("version")
                and existing.last_op_time == normalized_row.get("last_op_time")
                and (existing.user_list or None) == (incoming_user_list or None)
            )
            if same_snapshot:
                unchanged_count += 1
                continue

            changed_rows.append(normalized_row)
            touched_bill_ids_by_community[key[1]].add(key[0])

            user_list_raw = item.get("userList", [])
            if isinstance(user_list_raw, list):
                for user in user_list_raw:
                    if not isinstance(user, dict):
                        continue
                    bill_user_rows.append(
                        {
                            "bill_id": key[0],
                            "community_id": key[1],
                            "user_id": user.get("id"),
                            "user_name": user.get("name", ""),
                            "is_system": user.get("isSystem", 0),
                        }
                    )

        if changed_rows:
            upsert_model_rows(
                db,
                Bill,
                changed_rows,
                key_fields=("id", "community_id"),
            )
            inserted_count = len(changed_rows)

        # SQL Server has a 2100 parameter cap; keep IN-chunks conservative.
        for community_id, bill_ids in touched_bill_ids_by_community.items():
            bill_id_list = sorted(bill_ids)
            for bill_id_chunk in _iter_chunks(bill_id_list, 900):
                db.query(BillUser).filter(
                    BillUser.community_id == community_id,
                    BillUser.bill_id.in_(bill_id_chunk),
                ).delete(synchronize_session=False)

        if bill_user_rows:
            db.bulk_insert_mappings(BillUser, bill_user_rows)

        db.commit()
        print(f"Sync: Upserted {inserted_count}, Unchanged {unchanged_count}, Skipped {skipped_count}")
        return {"inserted": inserted_count, "unchanged": unchanged_count, "skipped": skipped_count}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def sync_bills_for_community(community_id: int, task_id: str = None):
    page = 1
    total_inserted = 0
    total_skipped = 0

    current_year = datetime.now().year

    db = SessionLocal()
    try:
        service = db.query(ExternalService).filter_by(service_name="marki").first()
        api = db.query(ExternalApi).filter(ExternalApi.id == MARKI_BILL_API_ID).first()

        preloaded_vars = {
            "communityID": str(community_id),
            "community_id": str(community_id),
            "year": str(current_year),
            "CURRENT_YEAR": str(current_year),
            "endMonth": f"{current_year}-12",
        }

        try:
            url = get_api_url_by_id(MARKI_BILL_API_ID, preloaded_vars=preloaded_vars)
        except Exception as exc:
            print(f"Failed to get bill list URL: {exc}")
            return 0

        if not api or not api.request_body:
            request_data = {
                "badBillCheck": 0,
                "chargeItemVersion": 2,
                "communityID": community_id,
                "dealLogId": 0,
                "endMonth": f"{current_year}-12",
                "index": "",
                "pageSize": "1000",
                "payStatus": 3,
            }
        else:
            raw_body = json.loads(api.request_body)
            request_data = resolve_dict_variables(raw_body, db, preloaded_vars=preloaded_vars)
            if (
                "communityID" in request_data
                and isinstance(request_data["communityID"], str)
                and request_data["communityID"].isdigit()
            ):
                request_data["communityID"] = int(request_data["communityID"])

        configured_end_month = (os.getenv("MARKI_BILL_END_MONTH") or "").strip()
        if configured_end_month:
            request_data["endMonth"] = configured_end_month
        else:
            request_data.pop("endMonth", None)

        # Incremental mode with periodic full backfill:
        # default full-sync interval is 7 days to self-heal historical gaps.
        use_full_sync, full_sync_reason = _should_use_full_sync(db, community_id)
        overlap = _parse_positive_int_env("MARKI_BILL_INCREMENTAL_OVERLAP", 200)
        max_deal_log_id = (
            db.query(func.max(Bill.deal_log_id))
            .filter(Bill.community_id == int(community_id))
            .scalar()
        )
        max_deal_log_id = int(max_deal_log_id or 0)
        if use_full_sync or max_deal_log_id <= 0:
            request_data["dealLogId"] = 0
        else:
            request_data["dealLogId"] = max(0, max_deal_log_id - overlap)

        can_retry_empty_incremental_with_full_sync, empty_incremental_retry_reason = (
            _can_retry_empty_incremental_with_full_sync(db, community_id)
        )

        request_data["page"] = 1
    finally:
        db.close()

    print(f"Starting sync for community {community_id}...")
    sync_mode_msg = (
        f"Community {community_id}: using FULL sync (dealLogId=0, reason={full_sync_reason})."
        if use_full_sync or max_deal_log_id <= 0
        else f"Community {community_id}: using INCREMENTAL sync (dealLogId={request_data.get('dealLogId')}, overlap={overlap})."
    )
    print(sync_mode_msg)
    if task_id:
        tracker.add_log(task_id, sync_mode_msg, "info")
    if configured_end_month:
        end_month_msg = f"Community {community_id}: request endMonth={configured_end_month}"
    else:
        end_month_msg = f"Community {community_id}: request endMonth not set (no upper-month cap)"
    print(end_month_msg)
    if task_id:
        tracker.add_log(task_id, end_month_msg, "info")

    request_failed = False
    attempted_incremental_fallback = False
    initial_incremental_deal_log_id = int(request_data.get("dealLogId") or 0)
    using_incremental_mode = not (use_full_sync or max_deal_log_id <= 0) and initial_incremental_deal_log_id > 0
    should_reconcile_missing = bool(use_full_sync or max_deal_log_id <= 0)
    full_sync_seen_bill_ids = set()
    full_sync_reconciled_at = datetime.utcnow()
    while True:
        request_data["page"] = page
        try:
            result = marki_client.request("POST", url, json_data=request_data)
        except Exception as exc:
            msg = f"Request failed for community {community_id} on page {page}: {exc}"
            print(msg)
            if task_id:
                tracker.add_log(task_id, msg, "error")
            request_failed = True
            break

        bills_data = _extract_bills_data(result)

        if not bills_data:
            if using_incremental_mode and page == 1 and not attempted_incremental_fallback:
                attempted_incremental_fallback = True
                if can_retry_empty_incremental_with_full_sync:
                    fallback_msg = (
                        f"Community {community_id}: incremental bill sync returned empty on page 1 "
                        f"(dealLogId={initial_incremental_deal_log_id}); retrying once with FULL sync "
                        f"({empty_incremental_retry_reason})."
                    )
                    print(fallback_msg)
                    if task_id:
                        tracker.add_log(task_id, fallback_msg, "warning")
                    marker_db = SessionLocal()
                    try:
                        _save_last_empty_incremental_fallback_time(marker_db, int(community_id))
                        marker_db.commit()
                    except Exception as exc:
                        marker_db.rollback()
                        marker_msg = (
                            f"Community {community_id}: failed to persist empty-incremental fallback marker: {exc}"
                        )
                        print(marker_msg)
                        if task_id:
                            tracker.add_log(task_id, marker_msg, "warning")
                    finally:
                        marker_db.close()
                    request_data["dealLogId"] = 0
                    page = 1
                    continue
                fallback_skip_msg = (
                    f"Community {community_id}: incremental bill sync returned empty on page 1 "
                    f"(dealLogId={initial_incremental_deal_log_id}); skip FULL retry because "
                    f"{empty_incremental_retry_reason}."
                )
                print(fallback_skip_msg)
                if task_id:
                    tracker.add_log(task_id, fallback_skip_msg, "warning")
            print(f"No more bills for community {community_id} on page {page}.")
            break

        counts = insert_bills_data(bills_data, community_id_context=community_id)
        total_inserted += int(counts.get("inserted", 0) or 0)
        total_skipped += int(counts.get("skipped", 0) or 0)
        if should_reconcile_missing:
            for item in bills_data:
                bill_id = item.get("id")
                if bill_id is None:
                    continue
                try:
                    full_sync_seen_bill_ids.add(int(bill_id))
                except Exception:
                    continue

        unchanged = int(counts.get("unchanged", 0) or 0)
        msg = (
            f"Community {community_id} - Page {page}: "
            f"fetched {len(bills_data)} upserted {counts.get('inserted', 0)} "
            f"unchanged {unchanged} skipped {counts.get('skipped', 0)}."
        )
        print(msg)
        if task_id:
            tracker.add_log(task_id, msg, "info")

        page += 1
        time.sleep(0.01)

    if should_reconcile_missing and not request_failed:
        if full_sync_seen_bill_ids:
            _reconcile_missing_bills_for_community(
                int(community_id),
                full_sync_seen_bill_ids,
                full_sync_reconciled_at,
                task_id=task_id,
            )
        else:
            msg = (
                f"Community {community_id}: full bill reconciliation skipped because sync returned 0 visible rows."
            )
            print(msg)
            if task_id:
                tracker.add_log(task_id, msg, "warning")

    if (use_full_sync or max_deal_log_id <= 0) and not request_failed:
        marker_db = SessionLocal()
        try:
            _save_last_full_sync_time(marker_db, int(community_id))
            marker_db.commit()
        except Exception as exc:
            marker_db.rollback()
            marker_msg = f"Community {community_id}: failed to persist full-sync marker: {exc}"
            print(marker_msg)
            if task_id:
                tracker.add_log(task_id, marker_msg, "warning")
        finally:
            marker_db.close()

    print(f"Completed sync for community {community_id}: inserted {total_inserted}, skipped {total_skipped}.")
    return total_inserted


def sync_bills(community_ids: list = None, task_id: str = None):
    if not community_ids:
        db = SessionLocal()
        try:
            community_ids = list(fetch_all_project_ids(db, ProjectList))
        except Exception as exc:
            print(f"Failed to retrieve community_ids from DB: {exc}")
            community_ids = []
        finally:
            db.close()

        if not community_ids:
            fallback_var = os.getenv("MARKI_SYSTEM_ID", "")
            if fallback_var and fallback_var.isdigit():
                community_ids = [int(fallback_var)]
            else:
                print("No community IDs provided and none found in DB.")
                return 0

    if task_id:
        tracker.update_status(task_id, "running")
        tracker.add_log(task_id, f"Start syncing bills for {len(community_ids)} communities", "info")

    try:
        ensure_default_financial_partitions(community_ids)
    except Exception as exc:
        msg = f"Auto partition expansion skipped due to error: {exc}"
        print(msg)
        if task_id:
            tracker.add_log(task_id, msg, "warning")

    total_records = 0
    for index, community_id in enumerate(community_ids):
        try:
            if task_id:
                tracker.update_progress(task_id, index, f"Community ID: {community_id}")

            records = sync_bills_for_community(int(community_id), task_id)
            total_records += records
        except Exception as exc:
            msg = f"Error syncing community {community_id}: {exc}"
            print(msg)
            if task_id:
                tracker.add_log(task_id, msg, "error")
            continue

    if task_id:
        tracker.update_progress(task_id, len(community_ids), "Sync completed")
        tracker.update_status(task_id, "completed")
        tracker.add_log(task_id, f"Bill sync completed. Processed {total_records} rows", "info")

    print(f"Bill sync completed. Total records processed: {total_records}")
    return total_records


if __name__ == "__main__":
    sync_bills()

