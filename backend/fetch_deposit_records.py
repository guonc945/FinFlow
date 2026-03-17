import json
import os
import time
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

from sync_tracker import tracker
from utils.marki_client import get_api_url_by_id, marki_client

load_dotenv()


DEPOSIT_RECORD_API_ID = int(os.getenv("MARKI_DEPOSIT_RECORD_API_ID", "30"))
DEFAULT_PAGE_SIZE = int(os.getenv("MARKI_DEPOSIT_RECORD_PAGE_SIZE", "1000"))


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "finflow"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def validate_timestamp(timestamp):
    if timestamp and int(timestamp) > 0:
        return int(timestamp)
    return None


def format_amount(val):
    """Convert cents to Yuan and keep two decimals."""
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
    from database import SessionLocal
    from models import ExternalApi

    db = SessionLocal()
    try:
        return db.query(ExternalApi).filter(ExternalApi.id == DEPOSIT_RECORD_API_ID).first()
    finally:
        db.close()


def _load_house_context_map(house_ids):
    from database import SessionLocal
    from models import House, ProjectList

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
    conn = get_db_connection()
    cursor = conn.cursor()

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

            house_name = item.get("houseName") or house_context.get("house_name")
            amount = format_amount(item.get("amount"))
            operate_type = item.get("operateType")
            operator = item.get("operator")
            operator_name = item.get("operatorName")
            operate_time = validate_timestamp(item.get("operateTime"))
            operate_date = None
            if operate_time:
                try:
                    operate_date = datetime.fromtimestamp(int(operate_time)).date()
                except Exception:
                    operate_date = None

            cash_pledge_name = item.get("cashPledgeName")
            remark = item.get("remark")

            pay_time = validate_timestamp(item.get("payTime"))
            pay_date = None
            if pay_time:
                try:
                    pay_date = datetime.fromtimestamp(int(pay_time)).date()
                except Exception:
                    pay_date = None

            has_refund_receipt = bool(item.get("hasRefundReceipt"))
            refund_receipt_id = item.get("refundReceiptId")
            pay_channel_str = item.get("payChannelStr")
            raw_data = _to_json_str(item)

            cursor.execute(
                """
                INSERT INTO deposit_records (
                    id, community_id, community_name,
                    house_id, house_name,
                    amount, operate_type, operator, operator_name, operate_time, operate_date,
                    cash_pledge_name, remark,
                    pay_time, pay_date, has_refund_receipt, refund_receipt_id, pay_channel_str,
                    raw_data
                ) VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s
                )
                ON CONFLICT (id) DO UPDATE SET
                    community_id = EXCLUDED.community_id,
                    community_name = EXCLUDED.community_name,
                    house_id = EXCLUDED.house_id,
                    house_name = EXCLUDED.house_name,
                    amount = EXCLUDED.amount,
                    operate_type = EXCLUDED.operate_type,
                    operator = EXCLUDED.operator,
                    operator_name = EXCLUDED.operator_name,
                    operate_time = EXCLUDED.operate_time,
                    operate_date = EXCLUDED.operate_date,
                    cash_pledge_name = EXCLUDED.cash_pledge_name,
                    remark = EXCLUDED.remark,
                    pay_time = EXCLUDED.pay_time,
                    pay_date = EXCLUDED.pay_date,
                    has_refund_receipt = EXCLUDED.has_refund_receipt,
                    refund_receipt_id = EXCLUDED.refund_receipt_id,
                    pay_channel_str = EXCLUDED.pay_channel_str,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = NOW()
                """,
                (
                    record_id,
                    community_id,
                    community_name,
                    house_id,
                    house_name,
                    amount,
                    operate_type,
                    operator,
                    operator_name,
                    operate_time,
                    operate_date,
                    cash_pledge_name,
                    remark,
                    pay_time,
                    pay_date,
                    has_refund_receipt,
                    refund_receipt_id,
                    pay_channel_str,
                    raw_data,
                ),
            )

            inserted_count += 1

        conn.commit()
        return {"inserted": inserted_count, "skipped": skipped_count}
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


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


def sync_deposit_records(community_ids=None, task_id=None):
    from database import SessionLocal
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

        page = 1
        total_processed = 0
        expected_total = None

        if task_id:
            tracker.update_status(task_id, "running")
            tracker.add_log(
                task_id,
                f"Starting deposit record sync with api_id={DEPOSIT_RECORD_API_ID}",
                "info",
            )

        while True:
            current_vars = dict(base_vars)
            current_vars.update(
                {
                    "page": str(page),
                    "pageNo": str(page),
                    "pageSize": str(DEFAULT_PAGE_SIZE),
                }
            )

            request_data = resolve_dict_variables(body_template, vars_db, preloaded_vars=current_vars)
            request_data = _coerce_common_ints(request_data)

            if not request_data:
                request_data = {"page": page, "pageSize": DEFAULT_PAGE_SIZE}
            else:
                request_data.setdefault("page", page)
                request_data.setdefault("pageSize", DEFAULT_PAGE_SIZE)

            params = request_data if method == "GET" else None
            json_body = None if method == "GET" else request_data

            try:
                result = marki_client.request(method, url, params=params, json_data=json_body)
            except Exception as exc:
                if task_id:
                    tracker.add_log(task_id, f"Request failed on page {page}: {exc}", "error")
                    tracker.update_status(task_id, "failed")
                raise

            data_list, total, has_more = _parse_list_response(result)
            if expected_total is None and total:
                expected_total = total

            if not data_list:
                break

            counts = insert_deposit_records(data_list, community_ids_filter=community_ids)
            total_processed += counts["inserted"]

            if task_id:
                tracker.add_log(
                    task_id,
                    f"Page {page}: fetched {len(data_list)}, inserted {counts['inserted']}, skipped {counts['skipped']}",
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

        if task_id:
            tracker.update_progress(task_id, len(community_ids or []), "deposit records")
            tracker.update_status(task_id, "completed")
            tracker.add_log(task_id, f"Completed deposit record sync, processed {total_processed} rows", "info")

        return total_processed
    finally:
        vars_db.close()


if __name__ == "__main__":
    sync_deposit_records()
