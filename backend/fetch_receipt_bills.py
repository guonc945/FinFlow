import json
import os
import time
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

from sync_tracker import tracker
from utils.marki_client import marki_client, get_api_url_by_id

load_dotenv()


RECEIPT_BILL_API_ID = int(os.getenv("MARKI_RECEIPT_BILL_API_ID", "29"))


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
    """Convert cents to Yuan (Decimal -> float), keeping 2 decimals."""
    if val is None:
        return None
    try:
        from decimal import Decimal, ROUND_HALF_UP

        dec_val = Decimal(str(val))
        result = dec_val / Decimal("100")
        return float(result.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    except Exception:
        return val


def _to_json_str(v):
    if v is None:
        return None
    if isinstance(v, str):
        return v
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


def insert_receipt_bills_data(data_list, community_id_context: int):
    conn = get_db_connection()
    cursor = conn.cursor()

    inserted_count = 0
    skipped_count = 0

    try:
        for item in data_list:
            receipt_bill_id = item.get("id")
            community_id = int(community_id_context)
            if receipt_bill_id is None:
                skipped_count += 1
                continue

            deal_type = item.get("dealType")
            asset_type = item.get("assetType")
            asset_name = item.get("assetName")
            asset_id = item.get("assetId")

            income_amount = format_amount(item.get("incomeAmount"))
            amount = format_amount(item.get("amount"))
            discount_amount = format_amount(item.get("discountAmount"))
            late_money_amount = format_amount(item.get("lateMoneyAmount"))
            bill_amount = format_amount(item.get("billAmount"))
            deposit_amount = format_amount(item.get("depositAmount"))

            pay_channel = item.get("payChannel")
            pay_channel_list = _to_json_str(item.get("payChannelList"))
            pay_channel_str = item.get("payChannelStr")

            deal_time = validate_timestamp(item.get("dealTime"))
            deal_date = None
            if deal_time:
                try:
                    deal_date = datetime.fromtimestamp(int(deal_time)).date()
                except Exception:
                    deal_date = None

            remark = item.get("remark")
            fk_id = item.get("fkId")

            receipt_id = item.get("receiptId")
            receipt_record_id = item.get("receiptRecordId")
            receipt_version = item.get("receiptVersion")

            invoice_number = item.get("invoiceNumber")
            invoice_urls = _to_json_str(item.get("invoiceUrls"))
            invoice_status = item.get("invoiceStatus")
            open_invoice = item.get("openInvoice")

            payee = item.get("payee")

            bind_users_raw = _to_json_str(item.get("bindUsers"))

            cursor.execute(
                """
                INSERT INTO receipt_bills (
                    id, community_id,
                    deal_type, asset_type, asset_name, asset_id,
                    income_amount, amount, discount_amount, late_money_amount, bill_amount, deposit_amount,
                    pay_channel, pay_channel_list, pay_channel_str,
                    deal_time, deal_date,
                    remark, fk_id,
                    receipt_id, receipt_record_id, receipt_version,
                    invoice_number, invoice_urls, invoice_status, open_invoice,
                    payee, bind_users_raw
                ) VALUES (
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (id, community_id) DO UPDATE SET
                    deal_type = EXCLUDED.deal_type,
                    asset_type = EXCLUDED.asset_type,
                    asset_name = EXCLUDED.asset_name,
                    asset_id = EXCLUDED.asset_id,
                    income_amount = EXCLUDED.income_amount,
                    amount = EXCLUDED.amount,
                    discount_amount = EXCLUDED.discount_amount,
                    late_money_amount = EXCLUDED.late_money_amount,
                    bill_amount = EXCLUDED.bill_amount,
                    deposit_amount = EXCLUDED.deposit_amount,
                    pay_channel = EXCLUDED.pay_channel,
                    pay_channel_list = EXCLUDED.pay_channel_list,
                    pay_channel_str = EXCLUDED.pay_channel_str,
                    deal_time = EXCLUDED.deal_time,
                    deal_date = EXCLUDED.deal_date,
                    remark = EXCLUDED.remark,
                    fk_id = EXCLUDED.fk_id,
                    receipt_id = EXCLUDED.receipt_id,
                    receipt_record_id = EXCLUDED.receipt_record_id,
                    receipt_version = EXCLUDED.receipt_version,
                    invoice_number = EXCLUDED.invoice_number,
                    invoice_urls = EXCLUDED.invoice_urls,
                    invoice_status = EXCLUDED.invoice_status,
                    open_invoice = EXCLUDED.open_invoice,
                    payee = EXCLUDED.payee,
                    bind_users_raw = EXCLUDED.bind_users_raw,
                    updated_at = NOW()
                """,
                (
                    receipt_bill_id,
                    community_id,
                    deal_type,
                    asset_type,
                    asset_name,
                    asset_id,
                    income_amount,
                    amount,
                    discount_amount,
                    late_money_amount,
                    bill_amount,
                    deposit_amount,
                    pay_channel,
                    pay_channel_list,
                    pay_channel_str,
                    deal_time,
                    deal_date,
                    remark,
                    fk_id,
                    receipt_id,
                    receipt_record_id,
                    receipt_version,
                    invoice_number,
                    invoice_urls,
                    invoice_status,
                    open_invoice,
                    payee,
                    bind_users_raw,
                ),
            )

            cursor.execute(
                "DELETE FROM receipt_bill_users WHERE receipt_bill_id = %s AND community_id = %s",
                (receipt_bill_id, community_id),
            )

            bind_users = item.get("bindUsers") or []
            if isinstance(bind_users, list):
                for u in bind_users:
                    if not isinstance(u, dict):
                        continue
                    cursor.execute(
                        """
                        INSERT INTO receipt_bill_users (
                            receipt_bill_id, community_id, user_id, user_name, phone
                        ) VALUES (
                            %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (receipt_bill_id, community_id, user_id) DO UPDATE SET
                            user_name = EXCLUDED.user_name,
                            phone = EXCLUDED.phone
                        """,
                        (
                            receipt_bill_id,
                            community_id,
                            u.get("id"),
                            u.get("name") or "",
                            u.get("phone") or "",
                        ),
                    )

            inserted_count += 1

        conn.commit()
        return {"inserted": inserted_count, "skipped": skipped_count}
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def _load_api_config():
    from database import SessionLocal
    from models import ExternalApi

    db = SessionLocal()
    try:
        api = db.query(ExternalApi).filter(ExternalApi.id == RECEIPT_BILL_API_ID).first()
        return api
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

    for k in list(payload.keys()):
        v = payload.get(k)
        if isinstance(v, str) and v.isdigit():
            if k.lower() in {
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
                payload[k] = int(v)
    return payload


def _ensure_community_id(payload: dict, community_id: int):
    """Marki endpoints are inconsistent: communityId vs communityID."""
    if not isinstance(payload, dict):
        return payload

    if "communityId" in payload and not payload.get("communityId"):
        payload["communityId"] = int(community_id)
    if "communityID" in payload and not payload.get("communityID"):
        payload["communityID"] = int(community_id)
    if "community_id" in payload and not payload.get("community_id"):
        payload["community_id"] = int(community_id)

    # If none of the keys exist in template, still inject the most common one.
    if "communityId" not in payload and "communityID" not in payload and "community_id" not in payload:
        payload["communityId"] = int(community_id)

    return payload


def sync_receipt_bills_for_community(community_id: int, task_id: str = None):
    from database import SessionLocal
    from utils.variable_parser import resolve_dict_variables, build_variable_map

    api_config = _load_api_config()
    if not api_config:
        msg = f"未找到收款明细接口配置: external_apis.id={RECEIPT_BILL_API_ID}"
        if task_id:
            tracker.add_log(task_id, msg, "error")
        raise RuntimeError(msg)

    db = SessionLocal()
    try:
        base_vars = build_variable_map(db)
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

        # URL may also contain variables
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
            current_vars.update(
                {
                    "page": str(page),
                    "pageNo": str(page),
                }
            )
            if next_id is not None:
                current_vars["nextId"] = str(next_id)
                current_vars["index"] = str(next_id)

            request_data = resolve_dict_variables(body_template, db, preloaded_vars=current_vars)
            request_data = _coerce_common_ints(request_data)
            request_data = _ensure_community_id(request_data, community_id)

            # If API supports cursor pagination, send nextId when we have it,
            # even if the interface center request_body didn't include it.
            if next_id is not None:
                request_data.setdefault("nextId", int(next_id))

            # If no configured body, fall back to the common Marki params.
            if not request_data:
                request_data = {"communityID": int(community_id), "page": page, "pageSize": 500}
                if next_id is not None:
                    request_data["nextId"] = int(next_id)

            params = request_data if method == "GET" else None
            json_body = None if method == "GET" else request_data

            try:
                result = marki_client.request(method, url, params=params, json_data=json_body)
            except Exception as e:
                msg = f"收款明细请求失败: community={community_id} page={page} err={e}"
                if task_id:
                    tracker.add_log(task_id, msg, "error")
                break

            data_list, has_more, resp_next_id = _parse_list_response(result)
            if not data_list:
                break

            counts = insert_receipt_bills_data(data_list, community_id_context=community_id)
            total_processed += counts["inserted"]

            msg = f"community {community_id} page {page}: processed {len(data_list)} (inserted {counts['inserted']})"
            if task_id:
                tracker.add_log(task_id, msg, "info")

            # Cursor pagination: if response includes nextId (docs/1.json does),
            # we keep fetching by nextId regardless of request_body template.
            if resp_next_id is not None:
                try:
                    resp_next_id_int = int(resp_next_id)
                except Exception:
                    resp_next_id_int = None

                # Marki uses nextId=-1 when no more.
                if not has_more or resp_next_id_int in (-1, 0, None):
                    break

                next_id = resp_next_id_int
                page += 1
                time.sleep(0.8)
                continue

            # Page-based pagination
            page += 1
            time.sleep(0.8)

    finally:
        db.close()

    return total_processed


def sync_receipt_bills(community_ids: list = None, task_id: str = None):
    if not community_ids:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT proj_id FROM projects_lists")
            community_ids = [str(row[0]) for row in cur.fetchall()]
        except Exception:
            community_ids = []
        finally:
            try:
                cur.close()
            except Exception:
                pass
            conn.close()

    if not community_ids:
        return 0

    if task_id:
        tracker.update_status(task_id, "running")
        tracker.add_log(task_id, f"开始同步收款账单: {len(community_ids)} 个园区", "info")

    total_all = 0
    for i, cid in enumerate(community_ids):
        try:
            if task_id:
                tracker.update_progress(task_id, i, f"园区 ID: {cid}")
            total_all += sync_receipt_bills_for_community(int(cid), task_id)
        except Exception as e:
            if task_id:
                tracker.add_log(task_id, f"同步失败: community={cid} err={e}", "error")
            continue

    if task_id:
        tracker.update_progress(task_id, len(community_ids), "同步完成")
        tracker.update_status(task_id, "completed")
        tracker.add_log(task_id, f"同步完成，共处理 {total_all} 条收款明细", "info")

    return total_all


if __name__ == "__main__":
    # For manual testing
    sync_receipt_bills(["10956"])
