import json
import os
import time
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

from sync_tracker import tracker
from utils.marki_client import get_api_url_by_id, marki_client

load_dotenv()


PREPAYMENT_RECORD_API_ID = int(os.getenv("MARKI_PREPAYMENT_RECORD_API_ID", "31"))
DEFAULT_PAGE_SIZE = int(os.getenv("MARKI_PREPAYMENT_RECORD_PAGE_SIZE", "1000"))


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


def parse_iso_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None

    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt


def parse_iso_timestamp(value):
    dt = parse_iso_datetime(value)
    if not dt:
        return None
    try:
        return int(dt.timestamp())
    except Exception:
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
    from database import SessionLocal
    from models import ExternalApi

    db = SessionLocal()
    try:
        return db.query(ExternalApi).filter(ExternalApi.id == PREPAYMENT_RECORD_API_ID).first()
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


def _load_context_maps(community_ids, house_ids):
    from database import SessionLocal
    from models import House, ProjectList

    normalized_community_ids = sorted({int(cid) for cid in community_ids if cid is not None})
    normalized_house_ids = sorted({str(hid).strip() for hid in house_ids if str(hid).strip()})

    db = SessionLocal()
    try:
        project_name_map = {}
        if normalized_community_ids:
            project_rows = (
                db.query(ProjectList.proj_id, ProjectList.proj_name)
                .filter(ProjectList.proj_id.in_(normalized_community_ids))
                .all()
            )
            project_name_map = {int(row.proj_id): row.proj_name for row in project_rows}

        house_name_map = {}
        if normalized_house_ids:
            house_rows = (
                db.query(House.house_id, House.house_name)
                .filter(House.house_id.in_(normalized_house_ids))
                .all()
            )
            house_name_map = {str(row.house_id): row.house_name for row in house_rows}

        return project_name_map, house_name_map
    finally:
        db.close()


def insert_prepayment_records(data_list, community_ids_filter=None):
    conn = get_db_connection()
    cursor = conn.cursor()

    inserted_count = 0
    skipped_count = 0
    allowed_community_ids = {int(cid) for cid in (community_ids_filter or [])}

    community_ids = []
    house_ids = []
    for item in data_list:
        if not isinstance(item, dict):
            continue
        change_log = item.get("changeLog") or {}
        if not isinstance(change_log, dict):
            continue
        community_ids.append(change_log.get("communityId"))
        house_ids.append(change_log.get("houseId"))

    project_name_map, house_name_map = _load_context_maps(community_ids, house_ids)

    try:
        for item in data_list:
            if not isinstance(item, dict):
                skipped_count += 1
                continue

            change_log = item.get("changeLog") or {}
            refund_receipt = item.get("refundReceipt") or {}
            if not isinstance(change_log, dict):
                skipped_count += 1
                continue

            record_id = change_log.get("id")
            if record_id is None:
                skipped_count += 1
                continue

            community_id = change_log.get("communityId")
            try:
                community_id = int(community_id) if community_id is not None else None
            except (TypeError, ValueError):
                community_id = None

            if allowed_community_ids and community_id not in allowed_community_ids:
                skipped_count += 1
                continue

            house_id = change_log.get("houseId")
            house_key = str(house_id).strip() if house_id is not None else ""

            operate_time = parse_iso_timestamp(change_log.get("createTime"))
            operate_date = None
            if operate_time:
                try:
                    operate_date = datetime.fromtimestamp(int(operate_time)).date()
                except Exception:
                    operate_date = None

            source_updated_time = parse_iso_datetime(change_log.get("updateTime"))
            pay_time = validate_timestamp(change_log.get("payTime"))
            pay_date = None
            if pay_time:
                try:
                    pay_date = datetime.fromtimestamp(int(pay_time)).date()
                except Exception:
                    pay_date = None

            cursor.execute(
                """
                INSERT INTO prepayment_records (
                    id, community_id, community_name,
                    account_id, building_id, unit_id, house_id, house_name,
                    amount, balance_after_change,
                    operate_type, operate_type_label,
                    pay_channel_id, pay_channel_str,
                    operator, operator_name,
                    operate_time, operate_date, source_updated_time,
                    remark, deposit_order_id,
                    pay_time, pay_date,
                    category_id, category_name, status,
                    has_refund_receipt, refund_receipt_id,
                    raw_data
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s
                )
                ON CONFLICT (id) DO UPDATE SET
                    community_id = EXCLUDED.community_id,
                    community_name = EXCLUDED.community_name,
                    account_id = EXCLUDED.account_id,
                    building_id = EXCLUDED.building_id,
                    unit_id = EXCLUDED.unit_id,
                    house_id = EXCLUDED.house_id,
                    house_name = EXCLUDED.house_name,
                    amount = EXCLUDED.amount,
                    balance_after_change = EXCLUDED.balance_after_change,
                    operate_type = EXCLUDED.operate_type,
                    operate_type_label = EXCLUDED.operate_type_label,
                    pay_channel_id = EXCLUDED.pay_channel_id,
                    pay_channel_str = EXCLUDED.pay_channel_str,
                    operator = EXCLUDED.operator,
                    operator_name = EXCLUDED.operator_name,
                    operate_time = EXCLUDED.operate_time,
                    operate_date = EXCLUDED.operate_date,
                    source_updated_time = EXCLUDED.source_updated_time,
                    remark = EXCLUDED.remark,
                    deposit_order_id = EXCLUDED.deposit_order_id,
                    pay_time = EXCLUDED.pay_time,
                    pay_date = EXCLUDED.pay_date,
                    category_id = EXCLUDED.category_id,
                    category_name = EXCLUDED.category_name,
                    status = EXCLUDED.status,
                    has_refund_receipt = EXCLUDED.has_refund_receipt,
                    refund_receipt_id = EXCLUDED.refund_receipt_id,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = NOW()
                """,
                (
                    record_id,
                    community_id,
                    project_name_map.get(community_id),
                    change_log.get("accountId"),
                    change_log.get("buildingId"),
                    change_log.get("unitId"),
                    house_id,
                    item.get("houseName") or house_name_map.get(house_key),
                    format_amount(change_log.get("money")),
                    format_amount(change_log.get("balanceAfterChange")),
                    change_log.get("type"),
                    item.get("opTypeStr"),
                    change_log.get("payChannelId"),
                    change_log.get("payChannelStr"),
                    change_log.get("createUid"),
                    item.get("operatorName"),
                    operate_time,
                    operate_date,
                    source_updated_time,
                    change_log.get("remark"),
                    change_log.get("depositOrderId"),
                    pay_time,
                    pay_date,
                    change_log.get("categoryId"),
                    change_log.get("categoryName"),
                    change_log.get("status"),
                    bool(refund_receipt.get("hasRefundReceipt")),
                    refund_receipt.get("refundReceiptId"),
                    _to_json_str(item),
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


def _resolve_request_headers(headers_template, db_session, preloaded_vars):
    from utils.variable_parser import resolve_dict_variables

    headers = resolve_dict_variables(headers_template, db_session, preloaded_vars=preloaded_vars)
    if not isinstance(headers, dict):
        return {}
    return {str(key): str(value) for key, value in headers.items() if value is not None}


def _sync_prepayment_payment_ids(community_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            UPDATE prepayment_records
            SET payment_id = NULL,
                updated_at = NOW()
            WHERE community_id = %s
              AND (operate_type <> 1 OR payment_id IS NOT NULL)
            """,
            (community_id,),
        )

        cursor.execute(
            """
            WITH matched_receipts AS (
                SELECT DISTINCT ON (community_id, deal_time, asset_id)
                    id,
                    community_id,
                    deal_time,
                    asset_id
                FROM receipt_bills
                WHERE community_id = %s
                  AND deal_time IS NOT NULL
                  AND deal_type = 1
                  AND asset_id IS NOT NULL
                ORDER BY community_id, deal_time, asset_id, id DESC
            )
            UPDATE prepayment_records AS p
            SET payment_id = matched_receipts.id,
                updated_at = NOW()
            FROM matched_receipts
            WHERE p.community_id = matched_receipts.community_id
              AND p.pay_time = matched_receipts.deal_time
              AND p.house_id = matched_receipts.asset_id
              AND p.operate_type = 1
              AND p.pay_time IS NOT NULL
              AND p.house_id IS NOT NULL
              AND p.community_id = %s
            """,
            (community_id, community_id),
        )

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM prepayment_records
            WHERE community_id = %s
              AND operate_type = 1
            """,
            (community_id,),
        )
        collected_total = int(cursor.fetchone()[0] or 0)

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM prepayment_records
            WHERE community_id = %s
              AND operate_type = 1
              AND payment_id IS NOT NULL
            """,
            (community_id,),
        )
        matched_total = int(cursor.fetchone()[0] or 0)

        conn.commit()
        return {
            "collected_total": collected_total,
            "matched_total": matched_total,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def sync_prepayment_records(community_ids=None, task_id=None):
    from database import SessionLocal
    from utils.variable_parser import build_variable_map, resolve_dict_variables

    api_config = _load_api_config()
    if not api_config:
        msg = f"Prepayment API config not found: external_apis.id={PREPAYMENT_RECORD_API_ID}"
        if task_id:
            tracker.add_log(task_id, msg, "error")
            tracker.update_status(task_id, "failed")
        raise RuntimeError(msg)

    vars_db = SessionLocal()
    try:
        base_vars = build_variable_map(vars_db)
        url = get_api_url_by_id(PREPAYMENT_RECORD_API_ID, preloaded_vars=base_vars)
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
            tracker.add_log(
                task_id,
                f"Starting prepayment record sync with api_id={PREPAYMENT_RECORD_API_ID}",
                "info",
            )

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
                        tracker.update_status(task_id, "failed")
                    raise

                data_list, total, has_more = _parse_list_response(result)
                if expected_total is None and total:
                    expected_total = total

                if not data_list:
                    break

                counts = insert_prepayment_records(data_list, community_ids_filter=[community_id])
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

            payment_sync_counts = _sync_prepayment_payment_ids(community_id)
            if task_id:
                tracker.add_log(
                    task_id,
                    (
                        f"Community {community_id}: linked payment IDs for "
                        f"{payment_sync_counts['matched_total']}/{payment_sync_counts['collected_total']} "
                        "prepayment recharge records"
                    ),
                    "info",
                )

        if task_id:
            tracker.update_progress(task_id, len(community_ids), "prepayment records")
            tracker.update_status(task_id, "completed")
            tracker.add_log(task_id, f"Completed prepayment record sync, processed {total_processed} rows", "info")

        return total_processed
    finally:
        vars_db.close()


if __name__ == "__main__":
    sync_prepayment_records()
