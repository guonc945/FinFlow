import json
import os
import time
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import func

from database import SessionLocal
from models import ExternalApi, ProjectList, ReceiptBill, ReceiptBillUser
from receipt_bill_deposit_links import rebuild_receipt_bill_deposit_refund_links
from sync_tracker import tracker
from utils.db_compat import fetch_all_project_ids, upsert_model_rows
from utils.marki_client import get_api_url_by_id, marki_client
from utils.sqlserver_partitions import ensure_default_financial_partitions

load_dotenv()


RECEIPT_BILL_API_ID = int(os.getenv("MARKI_RECEIPT_BILL_API_ID", "29"))


def validate_timestamp(timestamp):
    if timestamp and str(timestamp).isdigit() and int(timestamp) > 0:
        return int(timestamp)
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
def insert_receipt_bills_data(data_list, community_id_context: int):
    db = SessionLocal()
    inserted_count = 0
    skipped_count = 0
    unchanged_count = 0
    touched_receipt_bill_ids = set()
    receipt_user_rows = []
    staged_entries = []

    try:
        community_id = int(community_id_context)

        for item in data_list:
            receipt_bill_id = item.get("id")
            if receipt_bill_id is None:
                skipped_count += 1
                continue

            deal_time = validate_timestamp(item.get("dealTime"))
            deal_date = None
            if deal_time:
                try:
                    deal_date = datetime.fromtimestamp(int(deal_time)).date()
                except Exception:
                    deal_date = None

            values = {
                "deal_type": item.get("dealType"),
                "asset_type": item.get("assetType"),
                "asset_name": item.get("assetName"),
                "asset_id": item.get("assetId"),
                "income_amount": format_amount(item.get("incomeAmount")),
                "amount": format_amount(item.get("amount")),
                "discount_amount": format_amount(item.get("discountAmount")),
                "late_money_amount": format_amount(item.get("lateMoneyAmount")),
                "bill_amount": format_amount(item.get("billAmount")),
                "deposit_amount": format_amount(item.get("depositAmount")),
                "pay_channel": item.get("payChannel"),
                "pay_channel_list": _to_json_str(item.get("payChannelList")),
                "pay_channel_str": item.get("payChannelStr"),
                "deal_time": deal_time,
                "deal_date": deal_date,
                "remark": item.get("remark"),
                "fk_id": item.get("fkId"),
                "receipt_id": item.get("receiptId"),
                "receipt_record_id": item.get("receiptRecordId"),
                "receipt_version": item.get("receiptVersion"),
                "invoice_number": item.get("invoiceNumber"),
                "invoice_urls": _to_json_str(item.get("invoiceUrls")),
                "invoice_status": item.get("invoiceStatus"),
                "open_invoice": item.get("openInvoice"),
                "payee": item.get("payee"),
                "bind_users_raw": _to_json_str(item.get("bindUsers")),
            }

            receipt_bill_id = int(receipt_bill_id)
            normalized_row = {
                "id": receipt_bill_id,
                "community_id": community_id,
                **values,
            }
            staged_entries.append((item, normalized_row))

        existing_rows = {}
        receipt_bill_ids = sorted({int(row["id"]) for _, row in staged_entries})
        for receipt_bill_id_chunk in _iter_chunks(receipt_bill_ids, 900):
            rows = (
                db.query(
                    ReceiptBill.id,
                    ReceiptBill.community_id,
                    ReceiptBill.receipt_version,
                    ReceiptBill.deal_time,
                    ReceiptBill.bind_users_raw,
                )
                .filter(
                    ReceiptBill.community_id == community_id,
                    ReceiptBill.id.in_(receipt_bill_id_chunk),
                )
                .all()
            )
            for row in rows:
                existing_rows[(int(row.id), int(row.community_id))] = row

        changed_rows = []
        for item, normalized_row in staged_entries:
            key = (int(normalized_row["id"]), int(normalized_row["community_id"]))
            existing = existing_rows.get(key)
            same_snapshot = (
                existing is not None
                and existing.receipt_version == normalized_row.get("receipt_version")
                and existing.deal_time == normalized_row.get("deal_time")
                and (existing.bind_users_raw or None) == (normalized_row.get("bind_users_raw") or None)
            )
            if same_snapshot:
                unchanged_count += 1
                continue

            changed_rows.append(normalized_row)
            touched_receipt_bill_ids.add(key[0])

            bind_users = item.get("bindUsers") or []
            if isinstance(bind_users, list):
                for user in bind_users:
                    if not isinstance(user, dict):
                        continue
                    receipt_user_rows.append(
                        {
                            "receipt_bill_id": key[0],
                            "community_id": key[1],
                            "user_id": user.get("id"),
                            "user_name": user.get("name") or "",
                            "phone": user.get("phone") or "",
                        }
                    )

        if changed_rows:
            upsert_model_rows(
                db,
                ReceiptBill,
                changed_rows,
                key_fields=("id", "community_id"),
            )
            inserted_count = len(changed_rows)

        receipt_bill_ids = sorted(touched_receipt_bill_ids)
        for receipt_bill_id_chunk in _iter_chunks(receipt_bill_ids, 900):
            db.query(ReceiptBillUser).filter(
                ReceiptBillUser.community_id == community_id,
                ReceiptBillUser.receipt_bill_id.in_(receipt_bill_id_chunk),
            ).delete(synchronize_session=False)

        if receipt_user_rows:
            db.bulk_insert_mappings(ReceiptBillUser, receipt_user_rows)

        db.commit()
        return {"inserted": inserted_count, "unchanged": unchanged_count, "skipped": skipped_count}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def _load_api_config():
    db = SessionLocal()
    try:
        return db.query(ExternalApi).filter(ExternalApi.id == RECEIPT_BILL_API_ID).first()
    finally:
        db.close()


def _parse_list_response(result):
    data_list = []
    has_more = False
    next_id = None

    if not isinstance(result, dict):
        return data_list, has_more, next_id

    data_obj = result.get("data")
    if isinstance(data_obj, dict):
        if isinstance(data_obj.get("list"), list):
            data_list = data_obj.get("list") or []
        has_more = bool(data_obj.get("hasMore") or data_obj.get("has_more") or False)
        next_id = data_obj.get("nextId") if "nextId" in data_obj else data_obj.get("index")
    elif isinstance(data_obj, list):
        data_list = data_obj

    if not data_list and isinstance(result.get("list"), list):
        data_list = result.get("list") or []

    if not has_more and "hasMore" in result:
        has_more = bool(result.get("hasMore"))
    if next_id is None and "nextId" in result:
        next_id = result.get("nextId")

    return data_list, has_more, next_id


def _coerce_common_ints(payload: dict):
    if not isinstance(payload, dict):
        return payload

    for key in list(payload.keys()):
        value = payload.get(key)
        if isinstance(value, str) and value.isdigit() and key.lower() in {
            "communityid",
            "community_id",
            "page",
            "pageno",
            "page_no",
            "pagesize",
            "page_size",
            "nextid",
            "index",
        }:
            payload[key] = int(value)
    return payload


def _ensure_community_id(payload: dict, community_id: int):
    if not isinstance(payload, dict):
        return payload

    if "communityId" in payload and not payload.get("communityId"):
        payload["communityId"] = int(community_id)
    if "communityID" in payload and not payload.get("communityID"):
        payload["communityID"] = int(community_id)
    if "community_id" in payload and not payload.get("community_id"):
        payload["community_id"] = int(community_id)

    if "communityId" not in payload and "communityID" not in payload and "community_id" not in payload:
        payload["communityId"] = int(community_id)

    return payload


def sync_receipt_bills_for_community(community_id: int, task_id: str = None):
    from utils.variable_parser import build_variable_map, resolve_dict_variables

    api_config = _load_api_config()
    if not api_config:
        msg = f"Receipt bill API config not found: external_apis.id={RECEIPT_BILL_API_ID}"
        if task_id:
            tracker.add_log(task_id, msg, "error")
        raise RuntimeError(msg)

    db = SessionLocal()
    try:
        base_vars = build_variable_map(db)
        max_deal_time_local = (
            db.query(func.max(ReceiptBill.deal_time))
            .filter(ReceiptBill.community_id == int(community_id))
            .scalar()
        )
        max_deal_time_local = int(max_deal_time_local or 0)
        # Automatic overlap window (no manual tuning required).
        overlap_seconds = 600

        base_vars.update(
            {
                "communityID": str(community_id),
                "communityId": str(community_id),
                "community_id": str(community_id),
                "page": "1",
                "pageNo": "1",
                "pageSize": "500",
                "nextId": "0",
                "index": "",
            }
        )

        url = get_api_url_by_id(RECEIPT_BILL_API_ID, preloaded_vars=base_vars)

        method = (api_config.method or "POST").upper()
        body_template = {}
        if api_config.request_body:
            try:
                body_template = json.loads(api_config.request_body)
            except Exception:
                body_template = {}

        page = 1
        next_id = None
        total_processed = 0

        while True:
            current_vars = dict(base_vars)
            current_vars.update({"page": str(page), "pageNo": str(page)})
            if next_id is not None:
                current_vars["nextId"] = str(next_id)
                current_vars["index"] = str(next_id)

            request_data = resolve_dict_variables(body_template, db, preloaded_vars=current_vars)
            request_data = _coerce_common_ints(request_data)
            request_data = _ensure_community_id(request_data, community_id)

            if max_deal_time_local > 0:
                request_data["minDealTime"] = max(0, max_deal_time_local - overlap_seconds)

            if next_id is not None:
                request_data.setdefault("nextId", int(next_id))

            if not request_data:
                request_data = {"communityID": int(community_id), "page": page, "pageSize": 500}
                if next_id is not None:
                    request_data["nextId"] = int(next_id)

            params = request_data if method == "GET" else None
            json_body = None if method == "GET" else request_data

            try:
                result = marki_client.request(method, url, params=params, json_data=json_body)
            except Exception as exc:
                msg = f"Receipt bill request failed: community={community_id} page={page} err={exc}"
                if task_id:
                    tracker.add_log(task_id, msg, "error")
                break

            data_list, has_more, resp_next_id = _parse_list_response(result)
            if not data_list:
                break

            counts = insert_receipt_bills_data(data_list, community_id_context=community_id)
            total_processed += counts["inserted"]

            msg = (
                f"community {community_id} page {page}: processed {len(data_list)} rows "
                f"(upserted {counts.get('inserted', 0)} unchanged {counts.get('unchanged', 0)} "
                f"skipped {counts.get('skipped', 0)})"
            )
            if task_id:
                tracker.add_log(task_id, msg, "info")

            if resp_next_id is not None:
                try:
                    resp_next_id_int = int(resp_next_id)
                except Exception:
                    resp_next_id_int = None

                if not has_more or resp_next_id_int in (-1, 0, None):
                    break

                next_id = resp_next_id_int
                page += 1
                time.sleep(0.01)
                continue

            page += 1
            time.sleep(0.01)
    finally:
        db.close()

    return total_processed


def sync_receipt_bills(community_ids: list = None, task_id: str = None):
    if not community_ids:
        db = SessionLocal()
        try:
            community_ids = [str(cid) for cid in fetch_all_project_ids(db, ProjectList)]
        except Exception:
            community_ids = []
        finally:
            db.close()

    if not community_ids:
        return 0

    if task_id:
        tracker.update_status(task_id, "running")
        tracker.add_log(task_id, f"Start syncing receipt bills for {len(community_ids)} communities", "info")

    try:
        ensure_default_financial_partitions(community_ids)
    except Exception as exc:
        if task_id:
            tracker.add_log(task_id, f"Auto partition expansion skipped: {exc}", "warning")

    total_all = 0
    for index, cid in enumerate(community_ids):
        try:
            if task_id:
                tracker.update_progress(task_id, index, f"Community ID: {cid}")

            total_all += sync_receipt_bills_for_community(int(cid), task_id)
            link_counts = rebuild_receipt_bill_deposit_refund_links([int(cid)])
            if task_id:
                tracker.add_log(
                    task_id,
                    (
                        f"Community {cid}: rebuilt links "
                        f"{link_counts['total_links']} total, "
                        f"{link_counts['transfer_to_prepayment_links']} transfer-to-prepayment, "
                        f"{link_counts['actual_refund_links']} actual refunds"
                    ),
                    "info",
                )
        except Exception as exc:
            if task_id:
                tracker.add_log(task_id, f"Sync failed: community={cid} err={exc}", "error")
            continue

    if task_id:
        tracker.update_progress(task_id, len(community_ids), "sync completed")
        tracker.update_status(task_id, "completed")
        tracker.add_log(task_id, f"Receipt bill sync completed. Processed {total_all} rows", "info")

    return total_all


if __name__ == "__main__":
    sync_receipt_bills(["10956"])

