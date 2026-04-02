import json
import os
import time
from datetime import datetime

from dotenv import load_dotenv

from database import SessionLocal
from models import DepositRecord, ExternalApi, House, ProjectList, ReceiptBill
from receipt_bill_deposit_links import rebuild_receipt_bill_deposit_refund_links
from sync_tracker import tracker
from utils.db_compat import upsert_model_row
from utils.marki_client import get_api_url_by_id, marki_client

load_dotenv()


DEPOSIT_RECORD_API_ID = int(os.getenv("MARKI_DEPOSIT_RECORD_API_ID", "30"))
DEFAULT_PAGE_SIZE = int(os.getenv("MARKI_DEPOSIT_RECORD_PAGE_SIZE", "1000"))


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


def _load_api_config():
    db = SessionLocal()
    try:
        return db.query(ExternalApi).filter(ExternalApi.id == DEPOSIT_RECORD_API_ID).first()
    finally:
        db.close()


def _parse_json_object(value):
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _load_house_context_map(house_ids):
    normalized_house_ids = sorted({str(hid).strip() for hid in house_ids if str(hid).strip()})
    if not normalized_house_ids:
        return {}

    db = SessionLocal()
    try:
        house_rows = (
            db.query(House.house_id, House.community_id, House.community_name, House.house_name)
            .filter(House.house_id.in_(normalized_house_ids))
            .all()
        )

        community_ids = []
        for row in house_rows:
            try:
                community_ids.append(int(row.community_id))
            except (TypeError, ValueError):
                continue

        project_name_map = {}
        if community_ids:
            for row in db.query(ProjectList.proj_id, ProjectList.proj_name).filter(ProjectList.proj_id.in_(community_ids)).all():
                project_name_map[int(row.proj_id)] = row.proj_name

        context_map = {}
        for row in house_rows:
            try:
                community_id = int(row.community_id) if row.community_id is not None else None
            except (TypeError, ValueError):
                community_id = None

            context_map[str(row.house_id)] = {
                "community_id": community_id,
                "community_name": project_name_map.get(community_id) or row.community_name,
                "house_name": row.house_name,
            }

        return context_map
    finally:
        db.close()


def insert_deposit_records(data_list, community_ids_filter=None):
    db = SessionLocal()
    inserted_count = 0
    skipped_count = 0
    allowed_community_ids = {int(cid) for cid in (community_ids_filter or [])}

    house_context_map = _load_house_context_map(
        [item.get("houseId") for item in data_list if isinstance(item, dict)]
    )

    try:
        for item in data_list:
            record_id = item.get("id")
            if record_id is None:
                skipped_count += 1
                continue

            house_id = item.get("houseId")
            house_key = str(house_id).strip() if house_id is not None else ""
            house_context = house_context_map.get(house_key, {})

            community_id = house_context.get("community_id")
            community_name = house_context.get("community_name")
            if allowed_community_ids and community_id not in allowed_community_ids:
                skipped_count += 1
                continue

            operate_time = validate_timestamp(item.get("operateTime"))
            operate_date = None
            if operate_time:
                try:
                    operate_date = datetime.fromtimestamp(int(operate_time)).date()
                except Exception:
                    operate_date = None

            pay_time = validate_timestamp(item.get("payTime"))
            pay_date = None
            if pay_time:
                try:
                    pay_date = datetime.fromtimestamp(int(pay_time)).date()
                except Exception:
                    pay_date = None

            values = {
                "community_id": community_id,
                "community_name": community_name,
                "house_id": house_id,
                "house_name": item.get("houseName") or house_context.get("house_name"),
                "amount": format_amount(item.get("amount")),
                "operate_type": item.get("operateType"),
                "operator": item.get("operator"),
                "operator_name": item.get("operatorName"),
                "operate_time": operate_time,
                "operate_date": operate_date,
                "cash_pledge_name": item.get("cashPledgeName"),
                "remark": item.get("remark"),
                "pay_time": pay_time,
                "pay_date": pay_date,
                "has_refund_receipt": bool(item.get("hasRefundReceipt")),
                "refund_receipt_id": item.get("refundReceiptId"),
                "pay_channel_str": item.get("payChannelStr"),
                "raw_data": _to_json_str(item),
            }

            upsert_model_row(db, DepositRecord, {"id": int(record_id)}, values)
            inserted_count += 1

        db.commit()
        return {"inserted": inserted_count, "skipped": skipped_count}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _parse_list_response(result):
    data_list = []
    total = 0
    has_more = False

    if not isinstance(result, dict):
        return data_list, total, has_more

    data_obj = result.get("data")
    if isinstance(data_obj, dict):
        if isinstance(data_obj.get("list"), list):
            data_list = data_obj.get("list") or []
        total = int(data_obj.get("total") or 0)
        has_more = bool(data_obj.get("hasMore") or data_obj.get("has_more") or False)
    elif isinstance(data_obj, list):
        data_list = data_obj

    if not data_list and isinstance(result.get("list"), list):
        data_list = result.get("list") or []

    if not total and result.get("total") is not None:
        try:
            total = int(result.get("total"))
        except (TypeError, ValueError):
            total = 0

    if not has_more and "hasMore" in result:
        has_more = bool(result.get("hasMore"))

    return data_list, total, has_more


def _coerce_common_ints(payload):
    if not isinstance(payload, dict):
        return payload

    for key, value in list(payload.items()):
        if isinstance(value, str) and value.isdigit() and key.lower() in {
            "page",
            "pageno",
            "page_no",
            "pagesize",
            "page_size",
        }:
            payload[key] = int(value)

    return payload


def _resolve_request_headers(headers_template, db_session, preloaded_vars):
    from utils.variable_parser import resolve_dict_variables

    headers = resolve_dict_variables(headers_template, db_session, preloaded_vars=preloaded_vars)
    if not isinstance(headers, dict):
        return {}
    return {str(key): str(value) for key, value in headers.items() if value is not None}


def _sync_deposit_payment_ids(community_id: int):
    db = SessionLocal()
    try:
        db.query(DepositRecord).filter(
            DepositRecord.community_id == community_id,
            (DepositRecord.operate_type != 1) | (DepositRecord.payment_id.isnot(None)),
        ).update({DepositRecord.payment_id: None}, synchronize_session=False)

        receipt_rows = (
            db.query(ReceiptBill.id, ReceiptBill.deal_time, ReceiptBill.asset_id)
            .filter(
                ReceiptBill.community_id == community_id,
                ReceiptBill.deal_time.isnot(None),
                ReceiptBill.deal_type == 5,
                ReceiptBill.asset_id.isnot(None),
            )
            .order_by(ReceiptBill.id.desc())
            .all()
        )

        payment_id_map = {}
        for row in receipt_rows:
            key = (int(row.deal_time), int(row.asset_id))
            if key not in payment_id_map:
                payment_id_map[key] = int(row.id)

        deposits = (
            db.query(DepositRecord)
            .filter(
                DepositRecord.community_id == community_id,
                DepositRecord.operate_type == 1,
                DepositRecord.pay_time.isnot(None),
                DepositRecord.house_id.isnot(None),
            )
            .all()
        )

        matched_total = 0
        for deposit in deposits:
            key = (int(deposit.pay_time), int(deposit.house_id))
            payment_id = payment_id_map.get(key)
            deposit.payment_id = payment_id
            if payment_id is not None:
                matched_total += 1

        collected_total = len(deposits)
        db.commit()
        return {"collected_total": collected_total, "matched_total": matched_total}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def sync_deposit_records(community_ids=None, task_id=None):
    from utils.variable_parser import build_variable_map, resolve_dict_variables

    api_config = _load_api_config()
    if not api_config:
        msg = f"Deposit API config not found: external_apis.id={DEPOSIT_RECORD_API_ID}"
        if task_id:
            tracker.add_log(task_id, msg, "error")
            tracker.update_status(task_id, "failed")
        raise RuntimeError(msg)

    vars_db = SessionLocal()
    try:
        base_vars = build_variable_map(vars_db)
        url = get_api_url_by_id(DEPOSIT_RECORD_API_ID, preloaded_vars=base_vars)
        method = (api_config.method or "POST").upper()

        body_template = {}
        if api_config.request_body:
            try:
                body_template = json.loads(api_config.request_body)
            except Exception:
                body_template = {}
        headers_template = _parse_json_object(api_config.request_headers)

        total_processed = 0
        community_ids = [int(cid) for cid in (community_ids or [])]

        if task_id:
            tracker.update_status(task_id, "running")
            tracker.add_log(task_id, f"Starting deposit record sync with api_id={DEPOSIT_RECORD_API_ID}", "info")

        try:
            for index, community_id in enumerate(community_ids, start=1):
                if task_id:
                    tracker.update_progress(task_id, index - 1, str(community_id))
                    tracker.add_log(task_id, f"Syncing community {community_id}", "info")

                page = 1
                expected_total = None

                while True:
                    current_vars = dict(base_vars)
                    current_vars.update(
                        {
                            "page": str(page),
                            "pageNo": str(page),
                            "pageSize": str(DEFAULT_PAGE_SIZE),
                            "communityId": str(community_id),
                            "communityID": str(community_id),
                            "community_id": str(community_id),
                            "MARKI_COMMUNITY_IDS": str(community_id),
                        }
                    )

                    request_data = resolve_dict_variables(body_template, vars_db, preloaded_vars=current_vars)
                    request_data = _coerce_common_ints(request_data)

                    if not request_data:
                        request_data = {"page": page, "pageSize": DEFAULT_PAGE_SIZE}
                    else:
                        request_data.setdefault("page", page)
                        request_data.setdefault("pageSize", DEFAULT_PAGE_SIZE)

                    request_headers = _resolve_request_headers(headers_template, vars_db, current_vars)

                    params = request_data if method == "GET" else None
                    json_body = None if method == "GET" else request_data

                    try:
                        result = marki_client.request(
                            method,
                            url,
                            params=params,
                            json_data=json_body,
                            extra_headers=request_headers,
                        )
                    except Exception as exc:
                        if task_id:
                            tracker.add_log(task_id, f"Community {community_id} page {page} failed: {exc}", "error")
                        raise

                    data_list, total, has_more = _parse_list_response(result)
                    if expected_total is None and total:
                        expected_total = total

                    if not data_list:
                        break

                    counts = insert_deposit_records(data_list, community_ids_filter=[community_id])
                    total_processed += counts["inserted"]

                    if task_id:
                        tracker.add_log(
                            task_id,
                            f"Community {community_id} page {page}: fetched {len(data_list)}, inserted {counts['inserted']}, skipped {counts['skipped']}",
                            "info",
                        )

                    page_size = int(request_data.get("pageSize") or DEFAULT_PAGE_SIZE)
                    if not has_more:
                        if expected_total is not None and page * page_size < expected_total and len(data_list) == page_size:
                            page += 1
                            time.sleep(0.6)
                            continue
                        break

                    page += 1
                    time.sleep(0.6)

                payment_sync_counts = _sync_deposit_payment_ids(community_id)
                if task_id:
                    tracker.add_log(
                        task_id,
                        (
                            f"Community {community_id}: linked payment IDs for "
                            f"{payment_sync_counts['matched_total']}/{payment_sync_counts['collected_total']} collected deposits"
                        ),
                        "info",
                    )

                link_counts = rebuild_receipt_bill_deposit_refund_links([community_id])
                if task_id:
                    tracker.add_log(
                        task_id,
                        (
                            f"Community {community_id}: rebuilt links "
                            f"{link_counts['total_links']} total, "
                            f"{link_counts['transfer_to_prepayment_links']} transfer-to-prepayment, "
                            f"{link_counts['actual_refund_links']} actual refunds"
                        ),
                        "info",
                    )

            if task_id:
                tracker.update_progress(task_id, len(community_ids), "deposit records")
                tracker.update_status(task_id, "completed")
                tracker.add_log(task_id, f"Completed deposit record sync, processed {total_processed} rows", "info")

            return total_processed
        except Exception as exc:
            if task_id:
                tracker.add_log(task_id, f"Deposit sync failed: {exc}", "error")
                tracker.update_status(task_id, "failed")
            raise
    finally:
        vars_db.close()


if __name__ == "__main__":
    sync_deposit_records()
