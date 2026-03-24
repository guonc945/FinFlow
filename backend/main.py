import json
import hashlib
import re
import uuid
import csv
import io
from datetime import datetime, timedelta
from urllib.parse import quote
from utils.crypto import encrypt_value
from fastapi import FastAPI, Depends, HTTPException, Query, Request, status, BackgroundTasks, Header
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session, joinedload, selectinload
from sqlalchemy import and_, desc, func, extract, or_, inspect, text, cast, String
from sqlalchemy.exc import IntegrityError
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple
import models, schemas, database
from fetch_bills import sync_bills
from fetch_deposit_records import sync_deposit_records
from fetch_prepayment_records import sync_prepayment_records
from fetch_receipt_bills import sync_receipt_bills
from fetch_charge_items import sync_charge_items
from fetch_houses import sync_houses
from fetch_residents import sync_residents
from fetch_parks import sync_parks
from utils.variable_parser import (
    build_variable_map,
    get_builtin_variable_keys,
    resolve_dict_variables,
    resolve_variables,
)
from utils.expression_functions import (
    extract_expression_function_names,
    get_public_expression_function_names,
    get_public_expression_functions,
)
from sync_tracker import tracker
import logging
import os
from dotenv import load_dotenv

load_dotenv()
from scripts.fetch_projects import main as fetch_projects_main
from services.kingdee_auth import KingdeeAuthService
from services.reporting_database import (
    ReportingDatabaseError,
    ReportingDatabaseService,
    UnsafeQueryError,
)
from services.sync_schedule_service import (
    DEFAULT_TIMEZONE,
    SyncScheduleService,
    compute_next_run_at,
    normalize_weekdays,
    parse_json_list,
    serialize_json_list,
    utcnow_naive,
)
from voucher_source_registry import (
    VoucherRelationMeta,
    VoucherSourceMeta,
    VoucherSourceModuleMeta,
    build_relation_payload,
    build_source_modules_payload,
)
from voucher_field_mapping import (
    build_source_field_options as mapping_build_source_field_options,
    build_source_fields as mapping_build_source_fields,
    enrich_source_data as mapping_enrich_source_data,
    prefix_source_fields as mapping_prefix_source_fields,
)

# Configure logger for project sync
logger = logging.getLogger('project_sync')
if not logger.handlers:
    log_path = os.path.join(os.path.dirname(__file__), 'scripts', 'fetch_projects.log')
    handler = logging.FileHandler(log_path)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Create tables
models.Base.metadata.create_all(bind=database.engine)

sync_schedule_service = SyncScheduleService(database.SessionLocal, database.engine)


def _ensure_voucher_columns():
    inspector = inspect(database.engine)
    tables = inspector.get_table_names()
    if "voucher_template" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("voucher_template")}
    with database.engine.begin() as conn:
        if "priority" not in existing_cols:
            conn.execute(text("ALTER TABLE voucher_template ADD COLUMN priority INTEGER DEFAULT 100"))
        if "bizdate_expr" not in existing_cols:
            conn.execute(text("ALTER TABLE voucher_template ADD COLUMN bizdate_expr VARCHAR(100) DEFAULT '{CURRENT_DATE}'"))
        if "bookeddate_expr" not in existing_cols:
            conn.execute(text("ALTER TABLE voucher_template ADD COLUMN bookeddate_expr VARCHAR(100) DEFAULT '{CURRENT_DATE}'"))
        if "source_module" not in existing_cols:
            conn.execute(text("ALTER TABLE voucher_template ADD COLUMN source_module VARCHAR(50)"))
        if "category_id" not in existing_cols:
            conn.execute(text("ALTER TABLE voucher_template ADD COLUMN category_id INTEGER"))


_ensure_voucher_columns()


def _ensure_house_columns():
    inspector = inspect(database.engine)
    tables = inspector.get_table_names()
    if "houses" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("houses")}

    # 注意：这里只做「增量补列」，不做删改列类型，避免破坏存量数据
    with database.engine.begin() as conn:
        if "kingdee_house_id" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN kingdee_house_id VARCHAR(50)"))

        if "building_id" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN building_id BIGINT"))
        if "unit_id" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN unit_id BIGINT"))
        if "unit_name" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN unit_name VARCHAR(255)"))
        if "layer" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN layer INTEGER"))
        if "building_size" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN building_size DECIMAL(10,2)"))
        if "usable_size" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN usable_size DECIMAL(10,2)"))

        if "user_num" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN user_num INTEGER"))
        if "charge_num" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN charge_num INTEGER"))
        if "park_num" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN park_num INTEGER"))
        if "car_num" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN car_num INTEGER"))

        if "combina_name" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN combina_name VARCHAR(255)"))
        if "create_uid" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN create_uid BIGINT"))
        if "disable" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN disable BOOLEAN DEFAULT FALSE"))

        if "expand" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN expand TEXT"))
        if "expand_info" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN expand_info TEXT"))
        if "tag_list" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN tag_list TEXT"))
        if "attachment_list" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN attachment_list TEXT"))
        if "house_type_name" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN house_type_name VARCHAR(100)"))
        if "house_status_name" not in existing_cols:
            conn.execute(text("ALTER TABLE houses ADD COLUMN house_status_name VARCHAR(100)"))


_ensure_house_columns()


def _ensure_park_columns():
    inspector = inspect(database.engine)
    tables = inspector.get_table_names()
    if "parks" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("parks")}
    with database.engine.begin() as conn:
        if "house_id" not in existing_cols:
            conn.execute(text("ALTER TABLE parks ADD COLUMN house_id VARCHAR(50)"))
        if "house_fk" not in existing_cols:
            conn.execute(text("ALTER TABLE parks ADD COLUMN house_fk INTEGER"))


_ensure_park_columns()


def _ensure_bill_columns():
    inspector = inspect(database.engine)
    tables = inspector.get_table_names()
    if "bills" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("bills")}
    added_receive_date = False

    with database.engine.begin() as conn:
        if "receive_date" not in existing_cols:
            conn.execute(text("ALTER TABLE bills ADD COLUMN receive_date DATE"))
            added_receive_date = True

        # Backfill receive_date from pay_time (unix timestamp seconds)
        if added_receive_date or "receive_date" in existing_cols:
            dialect = database.engine.dialect.name
            if dialect == "postgresql":
                conn.execute(text("""
                    UPDATE bills
                    SET receive_date = to_timestamp(pay_time)::date
                    WHERE receive_date IS NULL
                      AND pay_time IS NOT NULL
                      AND pay_time > 0
                """))
            else:
                rows = conn.execute(text("""
                    SELECT id, community_id, pay_time
                    FROM bills
                    WHERE receive_date IS NULL
                      AND pay_time IS NOT NULL
                      AND pay_time > 0
                """)).fetchall()
                for row in rows:
                    try:
                        dt = datetime.fromtimestamp(int(row.pay_time)).date()
                    except Exception:
                        continue
                    conn.execute(
                        text("UPDATE bills SET receive_date = :d WHERE id = :id AND community_id = :cid"),
                        {"d": dt, "id": row.id, "cid": row.community_id},
                    )


_ensure_bill_columns()


def _ensure_deposit_record_columns():
    inspector = inspect(database.engine)
    tables = inspector.get_table_names()
    if "deposit_records" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("deposit_records")}
    with database.engine.begin() as conn:
        if "payment_id" not in existing_cols:
            conn.execute(text("ALTER TABLE deposit_records ADD COLUMN payment_id BIGINT"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_deposit_records_payment_id ON deposit_records (payment_id)"))


_ensure_deposit_record_columns()


def _ensure_prepayment_record_columns():
    inspector = inspect(database.engine)
    tables = inspector.get_table_names()
    if "prepayment_records" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("prepayment_records")}
    with database.engine.begin() as conn:
        if "payment_id" not in existing_cols:
            conn.execute(text("ALTER TABLE prepayment_records ADD COLUMN payment_id BIGINT"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_prepayment_records_payment_id ON prepayment_records (payment_id)"))


_ensure_prepayment_record_columns()


def _ensure_user_email_index():
    inspector = inspect(database.engine)
    tables = inspector.get_table_names()
    if "users" not in tables:
        return

    indexes = {idx["name"]: idx for idx in inspector.get_indexes("users")}
    email_index = indexes.get("ix_users_email")

    with database.engine.begin() as conn:
        if email_index and email_index.get("unique"):
            conn.execute(text("DROP INDEX IF EXISTS ix_users_email"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_users_email ON users (email)"))


_ensure_user_email_index()

BILL_VOUCHER_PUSH_STATUS_LABELS = {
    "not_pushed": "未推送",
    "pushing": "推送中",
    "success": "已推送",
    "failed": "推送失败",
}


def _decode_header_value(value: Optional[str]) -> str:
    if not value:
        return ""
    from urllib.parse import unquote
    return unquote(value).strip()


def _normalize_bill_refs(refs: Optional[List[Any]]) -> List[Dict[str, int]]:
    normalized: List[Dict[str, int]] = []
    seen = set()

    for ref in refs or []:
        if isinstance(ref, dict):
            bill_id = ref.get("bill_id")
            community_id = ref.get("community_id")
        else:
            bill_id = getattr(ref, "bill_id", None)
            community_id = getattr(ref, "community_id", None)

        if bill_id is None or community_id is None:
            continue

        key = (int(bill_id), int(community_id))
        if key in seen:
            continue
        seen.add(key)
        normalized.append({
            "bill_id": int(bill_id),
            "community_id": int(community_id),
        })

    return normalized


def _build_bill_push_status_entry(
    bill_id: int,
    community_id: int,
    push_status: str = "not_pushed",
    push_batch_no: Optional[str] = None,
    voucher_number: Optional[str] = None,
    voucher_id: Optional[str] = None,
    pushed_at: Optional[datetime] = None,
    message: Optional[str] = None,
    account_book_number: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_status = push_status if push_status in BILL_VOUCHER_PUSH_STATUS_LABELS else "not_pushed"
    return {
        "bill_id": int(bill_id),
        "community_id": int(community_id),
        "push_status": normalized_status,
        "push_status_label": BILL_VOUCHER_PUSH_STATUS_LABELS.get(normalized_status, "未推送"),
        "push_batch_no": push_batch_no,
        "voucher_number": voucher_number,
        "voucher_id": voucher_id,
        "pushed_at": pushed_at,
        "message": message,
        "account_book_number": account_book_number,
    }


def _get_bill_push_status_map(
    db: Session,
    refs: Optional[List[Any]],
    account_book_number: Optional[str] = None,
) -> Dict[tuple, Dict[str, Any]]:
    normalized_refs = _normalize_bill_refs(refs)
    status_map = {
        (ref["bill_id"], ref["community_id"]): _build_bill_push_status_entry(
            bill_id=ref["bill_id"],
            community_id=ref["community_id"],
            account_book_number=account_book_number or None,
        )
        for ref in normalized_refs
    }

    if not normalized_refs:
        return status_map

    conditions = [
        and_(
            models.BillVoucherPushRecord.bill_id == ref["bill_id"],
            models.BillVoucherPushRecord.community_id == ref["community_id"],
        )
        for ref in normalized_refs
    ]

    latest_query = db.query(
        models.BillVoucherPushRecord.bill_id.label("bill_id"),
        models.BillVoucherPushRecord.community_id.label("community_id"),
        models.BillVoucherPushRecord.push_status.label("push_status"),
        models.BillVoucherPushRecord.push_batch_no.label("push_batch_no"),
        models.BillVoucherPushRecord.voucher_number.label("voucher_number"),
        models.BillVoucherPushRecord.voucher_id.label("voucher_id"),
        models.BillVoucherPushRecord.pushed_at.label("pushed_at"),
        models.BillVoucherPushRecord.message.label("message"),
        models.BillVoucherPushRecord.account_book_number.label("account_book_number"),
        models.BillVoucherPushRecord.created_at.label("created_at"),
        models.BillVoucherPushRecord.id.label("id"),
    ).filter(or_(*conditions))

    normalized_book_number = (account_book_number or "").strip()
    if normalized_book_number:
        latest_query = latest_query.filter(
            models.BillVoucherPushRecord.account_book_number == normalized_book_number
        )

    latest_rows = latest_query.order_by(
        models.BillVoucherPushRecord.bill_id.asc(),
        models.BillVoucherPushRecord.community_id.asc(),
        models.BillVoucherPushRecord.created_at.desc(),
        models.BillVoucherPushRecord.id.desc(),
    ).distinct(
        models.BillVoucherPushRecord.bill_id,
        models.BillVoucherPushRecord.community_id,
    ).all()

    for row in latest_rows:
        key = (int(row.bill_id), int(row.community_id))
        status_map[key] = _build_bill_push_status_entry(
            bill_id=row.bill_id,
            community_id=row.community_id,
            push_status=row.push_status or "not_pushed",
            push_batch_no=row.push_batch_no,
            voucher_number=row.voucher_number,
            voucher_id=row.voucher_id,
            pushed_at=row.pushed_at,
            message=row.message,
            account_book_number=row.account_book_number,
        )

    return status_map


def _summarize_bill_push_statuses(statuses: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {
        "total": len(statuses),
        "not_pushed": 0,
        "pushing": 0,
        "success": 0,
        "failed": 0,
    }

    for item in statuses:
        status_key = item.get("push_status") or "not_pushed"
        if status_key not in summary:
            status_key = "not_pushed"
        summary[status_key] += 1

    return summary


def _get_related_bill_refs_for_receipts(
    db: Session,
    receipts: Optional[List[Any]],
) -> Dict[tuple, List[Dict[str, int]]]:
    normalized_receipts: List[Dict[str, int]] = []
    seen = set()

    for ref in receipts or []:
        if isinstance(ref, dict):
            receipt_bill_id = ref.get("receipt_bill_id", ref.get("id"))
            community_id = ref.get("community_id")
        else:
            receipt_bill_id = getattr(ref, "receipt_bill_id", getattr(ref, "id", None))
            community_id = getattr(ref, "community_id", None)

        if receipt_bill_id is None or community_id is None:
            continue

        key = (int(receipt_bill_id), int(community_id))
        if key in seen:
            continue

        seen.add(key)
        normalized_receipts.append({
            "receipt_bill_id": int(receipt_bill_id),
            "community_id": int(community_id),
        })

    result_map: Dict[tuple, List[Dict[str, int]]] = {
        (ref["receipt_bill_id"], ref["community_id"]): []
        for ref in normalized_receipts
    }
    if not normalized_receipts:
        return result_map

    conditions = [
        and_(
            models.Bill.deal_log_id == ref["receipt_bill_id"],
            models.Bill.community_id == ref["community_id"],
        )
        for ref in normalized_receipts
    ]
    rows = db.query(
        models.Bill.id.label("bill_id"),
        models.Bill.community_id.label("community_id"),
        models.Bill.deal_log_id.label("receipt_bill_id"),
    ).filter(or_(*conditions)).all()

    for row in rows:
        if row.receipt_bill_id is None:
            continue
        key = (int(row.receipt_bill_id), int(row.community_id))
        result_map.setdefault(key, []).append({
            "bill_id": int(row.bill_id),
            "community_id": int(row.community_id),
        })

    return result_map


def _normalize_receipt_refs(receipts: Optional[List[Any]]) -> List[Dict[str, int]]:
    normalized_receipts: List[Dict[str, int]] = []
    seen = set()

    for ref in receipts or []:
        if isinstance(ref, dict):
            receipt_bill_id = ref.get("receipt_bill_id", ref.get("id"))
            community_id = ref.get("community_id")
        else:
            receipt_bill_id = getattr(ref, "receipt_bill_id", getattr(ref, "id", None))
            community_id = getattr(ref, "community_id", None)

        if receipt_bill_id is None or community_id is None:
            continue

        key = (int(receipt_bill_id), int(community_id))
        if key in seen:
            continue

        seen.add(key)
        normalized_receipts.append({
            "receipt_bill_id": int(receipt_bill_id),
            "community_id": int(community_id),
        })

    return normalized_receipts


def _jsonify_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _prefix_source_fields(data: Dict[str, Any], source_type: str, module_prefix: str = "marki") -> Dict[str, Any]:
    return mapping_prefix_source_fields(data, source_type, module_prefix=module_prefix)


def _serialize_receipt_bill_model(
    receipt_bill: models.ReceiptBill,
    db: Session,
) -> Dict[str, Any]:
    project_name = (
        db.query(models.ProjectList.proj_name)
        .filter(models.ProjectList.proj_id == int(receipt_bill.community_id))
        .scalar()
    )
    users = list(getattr(receipt_bill, "users", None) or [])
    payer_name = ", ".join(
        [str(getattr(user, "user_name", "")).strip() for user in users if str(getattr(user, "user_name", "")).strip()]
    )

    data = {
        "id": receipt_bill.id,
        "community_id": receipt_bill.community_id,
        "community_name": project_name or "",
        "payer_name": payer_name,
        "receipt_id": receipt_bill.receipt_id,
        "deal_type": receipt_bill.deal_type,
        "deal_type_label": RECEIPT_BILL_DEAL_TYPE_LABELS.get(receipt_bill.deal_type, "其他"),
        "asset_type": receipt_bill.asset_type,
        "asset_name": receipt_bill.asset_name,
        "asset_id": receipt_bill.asset_id,
        "income_amount": receipt_bill.income_amount,
        "amount": receipt_bill.amount,
        "discount_amount": receipt_bill.discount_amount,
        "late_money_amount": receipt_bill.late_money_amount,
        "bill_amount": receipt_bill.bill_amount,
        "deposit_amount": receipt_bill.deposit_amount,
        "pay_channel": receipt_bill.pay_channel,
        "pay_channel_list": receipt_bill.pay_channel_list,
        "pay_channel_str": receipt_bill.pay_channel_str,
        "deal_time": receipt_bill.deal_time,
        "deal_date": receipt_bill.deal_date,
        "remark": receipt_bill.remark,
        "fk_id": receipt_bill.fk_id,
        "receipt_record_id": receipt_bill.receipt_record_id,
        "receipt_version": receipt_bill.receipt_version,
        "invoice_number": receipt_bill.invoice_number,
        "invoice_urls": receipt_bill.invoice_urls,
        "invoice_status": receipt_bill.invoice_status,
        "open_invoice": receipt_bill.open_invoice,
        "payee": receipt_bill.payee,
        "bind_users_raw": receipt_bill.bind_users_raw,
        "created_at": receipt_bill.created_at,
        "updated_at": receipt_bill.updated_at,
    }
    return {key: _jsonify_scalar(value) for key, value in data.items()}


def _enrich_receipt_bill_data(
    receipt_data: Dict[str, Any],
    receipt_bill: Optional[models.ReceiptBill] = None,
    db: Optional[Session] = None,
) -> Dict[str, Any]:
    return mapping_enrich_source_data("receipt_bills", receipt_data, db=db, record=receipt_bill)


def _serialize_deposit_record_model(record: models.DepositRecord) -> Dict[str, Any]:
    data = {
        "id": record.id,
        "community_id": record.community_id,
        "community_name": record.community_name,
        "house_id": record.house_id,
        "house_name": record.house_name,
        "amount": record.amount,
        "operate_type": record.operate_type,
        "operate_type_label": DEPOSIT_OPERATE_TYPE_LABELS.get(record.operate_type, "其他"),
        "operator": record.operator,
        "operator_name": record.operator_name,
        "operate_time": record.operate_time,
        "operate_date": record.operate_date,
        "cash_pledge_name": record.cash_pledge_name,
        "remark": record.remark,
        "pay_time": record.pay_time,
        "pay_date": record.pay_date,
        "payment_id": record.payment_id,
        "has_refund_receipt": record.has_refund_receipt,
        "refund_receipt_id": record.refund_receipt_id,
        "pay_channel_str": record.pay_channel_str,
        "raw_data": record.raw_data,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }
    return {key: _jsonify_scalar(value) for key, value in data.items()}


def _enrich_deposit_record_data(record_data: Dict[str, Any], db: Optional[Session] = None) -> Dict[str, Any]:
    return mapping_enrich_source_data("deposit_records", record_data, db=db)


def _serialize_prepayment_record_model(record: models.PrepaymentRecord) -> Dict[str, Any]:
    data = {
        "id": record.id,
        "community_id": record.community_id,
        "community_name": record.community_name,
        "account_id": record.account_id,
        "building_id": record.building_id,
        "unit_id": record.unit_id,
        "house_id": record.house_id,
        "house_name": record.house_name,
        "amount": record.amount,
        "balance_after_change": record.balance_after_change,
        "operate_type": record.operate_type,
        "operate_type_label": record.operate_type_label or PREPAYMENT_OPERATE_TYPE_LABELS.get(record.operate_type, "其他"),
        "pay_channel_id": record.pay_channel_id,
        "pay_channel_str": record.pay_channel_str,
        "operator": record.operator,
        "operator_name": record.operator_name,
        "operate_time": record.operate_time,
        "operate_date": record.operate_date,
        "source_updated_time": record.source_updated_time,
        "remark": record.remark,
        "deposit_order_id": record.deposit_order_id,
        "pay_time": record.pay_time,
        "pay_date": record.pay_date,
        "category_id": record.category_id,
        "category_name": record.category_name,
        "status": record.status,
        "payment_id": record.payment_id,
        "has_refund_receipt": record.has_refund_receipt,
        "refund_receipt_id": record.refund_receipt_id,
        "raw_data": record.raw_data,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }
    return {key: _jsonify_scalar(value) for key, value in data.items()}


def _enrich_prepayment_record_data(record_data: Dict[str, Any], db: Optional[Session] = None) -> Dict[str, Any]:
    return mapping_enrich_source_data("prepayment_records", record_data, db=db)


RECEIPT_DEPOSIT_REFUND_LINK_TYPE_LABELS = {
    "actual_refund": "实际退款",
    "transfer_to_prepayment": "转预存",
    "mixed": "混合",
    "unmatched": "未匹配",
}


def _load_receipt_deposit_refund_links(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> List[models.ReceiptBillDepositRefundLink]:
    return (
        db.query(models.ReceiptBillDepositRefundLink)
        .filter(
            models.ReceiptBillDepositRefundLink.receipt_bill_id == int(receipt_bill.id),
            models.ReceiptBillDepositRefundLink.community_id == int(receipt_bill.community_id),
        )
        .order_by(models.ReceiptBillDepositRefundLink.id.asc())
        .all()
    )


def _serialize_receipt_deposit_refund_link_model(
    link: models.ReceiptBillDepositRefundLink,
) -> Dict[str, Any]:
    data = {
        "id": link.id,
        "receipt_bill_id": link.receipt_bill_id,
        "community_id": link.community_id,
        "deposit_record_id": link.deposit_record_id,
        "prepayment_record_id": link.prepayment_record_id,
        "link_type": link.link_type,
        "link_type_label": RECEIPT_DEPOSIT_REFUND_LINK_TYPE_LABELS.get(link.link_type, "其他"),
        "match_rule": link.match_rule,
        "match_confidence": link.match_confidence,
        "created_at": link.created_at,
        "updated_at": link.updated_at,
    }
    return {key: _jsonify_scalar(value) for key, value in data.items()}


def _build_receipt_deposit_refund_link_summary(
    links: List[models.ReceiptBillDepositRefundLink],
) -> Dict[str, Any]:
    if not links:
        return {
            "matched": False,
            "link_count": 0,
            "link_type": "unmatched",
            "link_type_label": RECEIPT_DEPOSIT_REFUND_LINK_TYPE_LABELS["unmatched"],
            "match_rule": None,
            "match_confidence": None,
        }

    unique_types = {str(link.link_type or "").strip() for link in links if link.link_type}
    if len(unique_types) == 1:
        link_type = next(iter(unique_types))
    else:
        link_type = "mixed"

    confidences = [float(link.match_confidence) for link in links if link.match_confidence is not None]
    summary_rule = links[0].match_rule if len({link.match_rule for link in links}) == 1 else "multiple_rules"

    return {
        "matched": True,
        "link_count": len(links),
        "link_type": link_type,
        "link_type_label": RECEIPT_DEPOSIT_REFUND_LINK_TYPE_LABELS.get(link_type, "其他"),
        "match_rule": summary_rule,
        "match_confidence": round(max(confidences), 4) if confidences else None,
    }


def _load_deposit_records_by_link_ids(
    db: Session,
    links: List[models.ReceiptBillDepositRefundLink],
) -> List[models.DepositRecord]:
    deposit_ids = [int(link.deposit_record_id) for link in links if link.deposit_record_id is not None]
    if not deposit_ids:
        return []

    rows = db.query(models.DepositRecord).filter(models.DepositRecord.id.in_(deposit_ids)).all()
    by_id = {int(row.id): row for row in rows}
    return [by_id[deposit_id] for deposit_id in deposit_ids if deposit_id in by_id]


def _load_prepayment_records_by_link_ids(
    db: Session,
    links: List[models.ReceiptBillDepositRefundLink],
) -> List[models.PrepaymentRecord]:
    prepayment_ids = [int(link.prepayment_record_id) for link in links if link.prepayment_record_id is not None]
    if not prepayment_ids:
        return []

    rows = db.query(models.PrepaymentRecord).filter(models.PrepaymentRecord.id.in_(prepayment_ids)).all()
    by_id = {int(row.id): row for row in rows}
    return [by_id[prepayment_id] for prepayment_id in prepayment_ids if prepayment_id in by_id]


def _load_direct_receipt_deposit_refund_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> List[models.DepositRecord]:
    if receipt_bill.asset_id is None or receipt_bill.deal_time is None:
        return []

    return (
        db.query(models.DepositRecord)
        .filter(
            models.DepositRecord.community_id == int(receipt_bill.community_id),
            models.DepositRecord.house_id == int(receipt_bill.asset_id),
            models.DepositRecord.pay_time == int(receipt_bill.deal_time),
            models.DepositRecord.operate_type == 2,
        )
        .order_by(models.DepositRecord.id.asc())
        .all()
    )


def _load_direct_receipt_transfer_prepayment_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> List[models.PrepaymentRecord]:
    if receipt_bill.asset_id is None or receipt_bill.deal_time is None:
        return []

    return (
        db.query(models.PrepaymentRecord)
        .filter(
            models.PrepaymentRecord.community_id == int(receipt_bill.community_id),
            models.PrepaymentRecord.house_id == int(receipt_bill.asset_id),
            models.PrepaymentRecord.pay_time == int(receipt_bill.deal_time),
            models.PrepaymentRecord.operate_type == 1,
            or_(
                models.PrepaymentRecord.pay_channel_str.ilike("%押金转预存%"),
                models.PrepaymentRecord.remark.ilike("%押金转入预存款%"),
                models.PrepaymentRecord.remark.ilike("%押金转预存%"),
            ),
        )
        .order_by(models.PrepaymentRecord.id.asc())
        .all()
    )


def _load_receipt_to_bills_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> List[Dict[str, Any]]:
    bills = (
        db.query(models.Bill)
        .filter(
            models.Bill.deal_log_id == int(receipt_bill.id),
            models.Bill.community_id == int(receipt_bill.community_id),
        )
        .order_by(models.Bill.id.asc())
        .all()
    )
    return [
        mapping_enrich_source_data(
            "bills",
            {col.name: _jsonify_scalar(getattr(bill, col.name, None)) for col in models.Bill.__table__.columns},
            db,
        )
        for bill in bills
    ]


def _count_receipt_to_bills_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> int:
    return int(
        db.query(models.Bill)
        .filter(
            models.Bill.deal_log_id == int(receipt_bill.id),
            models.Bill.community_id == int(receipt_bill.community_id),
        )
        .count()
        or 0
    )


def _load_receipt_to_deposit_collect_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> List[Dict[str, Any]]:
    records = (
        db.query(models.DepositRecord)
        .filter(
            models.DepositRecord.community_id == int(receipt_bill.community_id),
            models.DepositRecord.payment_id == int(receipt_bill.id),
            models.DepositRecord.operate_type == 1,
        )
        .order_by(models.DepositRecord.id.asc())
        .all()
    )
    return [_enrich_deposit_record_data(_serialize_deposit_record_model(record), db=db) for record in records]


def _count_receipt_to_deposit_collect_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> int:
    return int(
        db.query(models.DepositRecord)
        .filter(
            models.DepositRecord.community_id == int(receipt_bill.community_id),
            models.DepositRecord.payment_id == int(receipt_bill.id),
            models.DepositRecord.operate_type == 1,
        )
        .count()
        or 0
    )


def _load_receipt_to_deposit_refund_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> List[Dict[str, Any]]:
    links = _load_receipt_deposit_refund_links(db, receipt_bill)
    records = _load_deposit_records_by_link_ids(db, links)
    if not records:
        records = _load_direct_receipt_deposit_refund_relation(db, receipt_bill)
    if not records:
        records = (
            db.query(models.DepositRecord)
            .filter(
                models.DepositRecord.community_id == int(receipt_bill.community_id),
                models.DepositRecord.refund_receipt_id == int(receipt_bill.id),
                models.DepositRecord.operate_type == 2,
            )
            .order_by(models.DepositRecord.id.asc())
            .all()
        )
    return [_enrich_deposit_record_data(_serialize_deposit_record_model(record), db=db) for record in records]


def _count_receipt_to_deposit_refund_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> int:
    links = _load_receipt_deposit_refund_links(db, receipt_bill)
    if links:
        return len([link for link in links if link.deposit_record_id is not None])

    direct_count = int(
        db.query(models.DepositRecord)
        .filter(
            models.DepositRecord.community_id == int(receipt_bill.community_id),
            models.DepositRecord.house_id == int(receipt_bill.asset_id),
            models.DepositRecord.pay_time == int(receipt_bill.deal_time),
            models.DepositRecord.operate_type == 2,
        )
        .count()
        or 0
    ) if receipt_bill.asset_id is not None and receipt_bill.deal_time is not None else 0
    if direct_count > 0:
        return direct_count

    return int(
        db.query(models.DepositRecord)
        .filter(
            models.DepositRecord.community_id == int(receipt_bill.community_id),
            models.DepositRecord.refund_receipt_id == int(receipt_bill.id),
            models.DepositRecord.operate_type == 2,
        )
        .count()
        or 0
    )


def _load_receipt_to_prepayment_recharge_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> List[Dict[str, Any]]:
    records: List[models.PrepaymentRecord] = []
    if int(receipt_bill.deal_type or 0) == 6:
        links = _load_receipt_deposit_refund_links(db, receipt_bill)
        records = _load_prepayment_records_by_link_ids(db, links)
        if not records:
            records = _load_direct_receipt_transfer_prepayment_relation(db, receipt_bill)
    else:
        records = (
            db.query(models.PrepaymentRecord)
            .filter(
                models.PrepaymentRecord.community_id == int(receipt_bill.community_id),
                models.PrepaymentRecord.payment_id == int(receipt_bill.id),
                models.PrepaymentRecord.operate_type == 1,
            )
            .order_by(models.PrepaymentRecord.id.asc())
            .all()
        )
    return [_enrich_prepayment_record_data(_serialize_prepayment_record_model(record), db=db) for record in records]


def _count_receipt_to_prepayment_recharge_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> int:
    return int(
        db.query(models.PrepaymentRecord)
        .filter(
            models.PrepaymentRecord.community_id == int(receipt_bill.community_id),
            models.PrepaymentRecord.payment_id == int(receipt_bill.id),
            models.PrepaymentRecord.operate_type == 1,
        )
        .count()
        or 0
    )


def _count_direct_receipt_transfer_prepayment_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> int:
    if receipt_bill.asset_id is None or receipt_bill.deal_time is None:
        return 0

    return int(
        db.query(models.PrepaymentRecord)
        .filter(
            models.PrepaymentRecord.community_id == int(receipt_bill.community_id),
            models.PrepaymentRecord.house_id == int(receipt_bill.asset_id),
            models.PrepaymentRecord.pay_time == int(receipt_bill.deal_time),
            models.PrepaymentRecord.operate_type == 1,
            or_(
                models.PrepaymentRecord.pay_channel_str.ilike("%押金转预存%"),
                models.PrepaymentRecord.remark.ilike("%押金转入预存款%"),
                models.PrepaymentRecord.remark.ilike("%押金转预存%"),
            ),
        )
        .count()
        or 0
    )


def _load_receipt_to_prepayment_refund_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> List[Dict[str, Any]]:
    records: List[models.PrepaymentRecord] = []
    if receipt_bill.asset_id is not None and receipt_bill.deal_time is not None:
        records = (
            db.query(models.PrepaymentRecord)
            .filter(
                models.PrepaymentRecord.community_id == int(receipt_bill.community_id),
                models.PrepaymentRecord.house_id == int(receipt_bill.asset_id),
                models.PrepaymentRecord.pay_time == int(receipt_bill.deal_time),
                models.PrepaymentRecord.operate_type == 2,
            )
            .order_by(models.PrepaymentRecord.id.asc())
            .all()
        )
    if not records:
        records = (
            db.query(models.PrepaymentRecord)
            .filter(
                models.PrepaymentRecord.community_id == int(receipt_bill.community_id),
                models.PrepaymentRecord.refund_receipt_id == int(receipt_bill.id),
                models.PrepaymentRecord.operate_type == 2,
            )
            .order_by(models.PrepaymentRecord.id.asc())
            .all()
        )
    return [_enrich_prepayment_record_data(_serialize_prepayment_record_model(record), db=db) for record in records]


def _count_receipt_to_prepayment_refund_relation(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> int:
    direct_count = int(
        db.query(models.PrepaymentRecord)
        .filter(
            models.PrepaymentRecord.community_id == int(receipt_bill.community_id),
            models.PrepaymentRecord.house_id == int(receipt_bill.asset_id),
            models.PrepaymentRecord.pay_time == int(receipt_bill.deal_time),
            models.PrepaymentRecord.operate_type == 2,
        )
        .count()
        or 0
    ) if receipt_bill.asset_id is not None and receipt_bill.deal_time is not None else 0
    if direct_count > 0:
        return direct_count

    return int(
        db.query(models.PrepaymentRecord)
        .filter(
            models.PrepaymentRecord.community_id == int(receipt_bill.community_id),
            models.PrepaymentRecord.refund_receipt_id == int(receipt_bill.id),
            models.PrepaymentRecord.operate_type == 2,
        )
        .count()
        or 0
    )


def _build_receipt_drilldown_meta(
    db: Session,
    receipt_bill: models.ReceiptBill,
) -> Dict[str, Any]:
    deal_type = int(receipt_bill.deal_type or 0)
    sections: List[Dict[str, Any]] = []

    if deal_type in {3, 4}:
        bill_count = _count_receipt_to_bills_relation(db, receipt_bill)
        sections.append({
            "relation_key": "receipt_to_bills",
            "source_type": "bills",
            "label": "运营账单",
            "count": bill_count,
        })
    elif deal_type == 5:
        deposit_collect_count = _count_receipt_to_deposit_collect_relation(db, receipt_bill)
        sections.append({
            "relation_key": "receipt_to_deposit_collect",
            "source_type": "deposit_records",
            "label": "押金收取",
            "count": deposit_collect_count,
        })
    elif deal_type == 6:
        deposit_refund_count = _count_receipt_to_deposit_refund_relation(db, receipt_bill)
        prepayment_transfer_count = _count_direct_receipt_transfer_prepayment_relation(db, receipt_bill)
        sections.append({
            "relation_key": "receipt_to_deposit_refund",
            "source_type": "deposit_records",
            "label": "押金退款",
            "count": deposit_refund_count,
        })
        if prepayment_transfer_count > 0:
            sections.append({
                "relation_key": "receipt_to_prepayment_transfer",
                "source_type": "prepayment_records",
                "label": "转入预存",
                "count": prepayment_transfer_count,
            })
    elif deal_type == 1:
        prepayment_recharge_count = _count_receipt_to_prepayment_recharge_relation(db, receipt_bill)
        sections.append({
            "relation_key": "receipt_to_prepayment_recharge",
            "source_type": "prepayment_records",
            "label": "预存款充值",
            "count": prepayment_recharge_count,
        })
    elif deal_type == 2:
        prepayment_refund_count = _count_receipt_to_prepayment_refund_relation(db, receipt_bill)
        sections.append({
            "relation_key": "receipt_to_prepayment_refund",
            "source_type": "prepayment_records",
            "label": "预存款退款",
            "count": prepayment_refund_count,
        })

    sections = [section for section in sections if int(section.get("count") or 0) > 0]
    total_count = sum(int(section["count"]) for section in sections)
    unique_sources = {section["source_type"] for section in sections}
    primary_source = next(iter(unique_sources)) if len(unique_sources) == 1 and unique_sources else ("mixed" if unique_sources else None)
    summary = " / ".join([f"{section['label']} {section['count']} 条" for section in sections]) if sections else "暂无关联数据"

    return {
        "drilldown_enabled": bool(sections),
        "drilldown_source": primary_source,
        "drilldown_count": total_count,
        "drilldown_summary": summary,
        "drilldown_sections": sections,
        "supports_bill_push_ops": any(section["source_type"] == "bills" for section in sections),
    }


def _build_receipt_drilldown_sections(
    receipt_bill: models.ReceiptBill,
    related_bills: List[Dict[str, Any]],
    related_deposit_collect: List[Dict[str, Any]],
    related_deposit_refund: List[Dict[str, Any]],
    related_prepayment_recharge: List[Dict[str, Any]],
    related_prepayment_refund: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    deal_type = int(receipt_bill.deal_type or 0)
    sections: List[Dict[str, Any]] = []

    def _append_section(relation_key: str, source_type: str, label: str, items: List[Dict[str, Any]]):
        if not items:
            return
        sections.append({
            "relation_key": relation_key,
            "source_type": source_type,
            "label": label,
            "count": len(items),
            "items": items,
        })

    if deal_type in {3, 4}:
        _append_section("receipt_to_bills", "bills", "运营账单", related_bills)
    elif deal_type == 5:
        _append_section("receipt_to_deposit_collect", "deposit_records", "押金收取", related_deposit_collect)
    elif deal_type == 6:
        _append_section("receipt_to_deposit_refund", "deposit_records", "押金退款", related_deposit_refund)
        _append_section("receipt_to_prepayment_transfer", "prepayment_records", "转入预存", related_prepayment_recharge)
    elif deal_type == 1:
        _append_section("receipt_to_prepayment_recharge", "prepayment_records", "预存款充值", related_prepayment_recharge)
    elif deal_type == 2:
        _append_section("receipt_to_prepayment_refund", "prepayment_records", "预存款退款", related_prepayment_refund)

    return sections


def _aggregate_receipt_bill_push_status(statuses: List[Dict[str, Any]]) -> Dict[str, Any]:
    related_bill_count = len(statuses)
    summary = _summarize_bill_push_statuses(statuses)

    if related_bill_count == 0:
        push_status = "not_pushed"
        push_status_label = "未推送"
    elif summary["pushing"] > 0:
        push_status = "pushing"
        push_status_label = "推送中"
    elif summary["success"] == related_bill_count:
        push_status = "success"
        push_status_label = "已推送"
    elif summary["failed"] == related_bill_count:
        push_status = "failed"
        push_status_label = "推送失败"
    elif summary["success"] > 0:
        push_status = "partial"
        push_status_label = "部分已推送"
    elif summary["failed"] > 0:
        push_status = "partial"
        push_status_label = "部分失败"
    else:
        push_status = "not_pushed"
        push_status_label = "未推送"

    successful_statuses = [
        item for item in statuses
        if (item.get("push_status") or "").strip() == "success"
    ]

    def _unique_nonempty_values(items: List[Dict[str, Any]], key: str) -> List[str]:
        values = {
            str(item.get(key) or "").strip()
            for item in items
            if str(item.get(key) or "").strip()
        }
        return sorted(values)

    voucher_numbers = _unique_nonempty_values(successful_statuses, "voucher_number")
    voucher_ids = _unique_nonempty_values(successful_statuses, "voucher_id")
    push_batch_nos = _unique_nonempty_values(successful_statuses, "push_batch_no")
    account_book_numbers = _unique_nonempty_values(successful_statuses, "account_book_number")

    pushed_times = [
        item.get("pushed_at")
        for item in successful_statuses
        if isinstance(item.get("pushed_at"), datetime)
    ]
    pushed_at = max(pushed_times) if pushed_times else None

    latest_message = next(
        (
            str(item.get("message") or "").strip()
            for item in sorted(
                statuses,
                key=lambda current: current.get("pushed_at") or datetime.min,
                reverse=True,
            )
            if str(item.get("message") or "").strip()
        ),
        None,
    )

    voucher_number: Optional[str]
    if len(voucher_numbers) == 1:
        voucher_number = voucher_numbers[0]
    elif len(voucher_numbers) > 1:
        voucher_number = f"多张凭证({len(voucher_numbers)})"
    else:
        voucher_number = None

    return {
        "related_bill_count": related_bill_count,
        "related_bill_push_summary": summary,
        "push_status": push_status,
        "push_status_label": push_status_label,
        "push_batch_no": push_batch_nos[0] if len(push_batch_nos) == 1 else None,
        "voucher_number": voucher_number,
        "voucher_id": voucher_ids[0] if len(voucher_ids) == 1 else None,
        "pushed_at": pushed_at,
        "message": latest_message,
        "account_book_number": account_book_numbers[0] if len(account_book_numbers) == 1 else None,
    }


def _find_bill_push_conflicts(statuses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        item for item in statuses
        if (item.get("push_status") or "").strip() in {"success", "pushing"}
    ]


def _extract_kingdee_voucher_result(resp_data: Any) -> Dict[str, Optional[str]]:
    result_item: Optional[Dict[str, Any]] = None

    if isinstance(resp_data, dict):
        data_obj = resp_data.get("data")
        if isinstance(data_obj, dict):
            result = data_obj.get("result")
            if isinstance(result, list):
                result_item = next((item for item in result if isinstance(item, dict)), None)
            elif isinstance(result, dict):
                result_item = result

            if result_item is None:
                rows = data_obj.get("rows")
                if isinstance(rows, list):
                    result_item = next((item for item in rows if isinstance(item, dict)), None)

        if result_item is None:
            result = resp_data.get("result")
            if isinstance(result, list):
                result_item = next((item for item in result if isinstance(item, dict)), None)
            elif isinstance(result, dict):
                result_item = result

    voucher_id = None
    voucher_number = None
    bill_status = None

    if result_item:
        voucher_id = str(
            result_item.get("id")
            or result_item.get("voucher_id")
            or result_item.get("voucherId")
            or result_item.get("innerId")
            or result_item.get("FID")
            or ""
        ).strip() or None
        voucher_number = str(
            result_item.get("number")
            or result_item.get("billno")
            or result_item.get("voucherNumber")
            or result_item.get("voucher_no")
            or ""
        ).strip() or None
        bill_status = result_item.get("billStatus")

    return {
        "voucher_id": voucher_id,
        "voucher_number": voucher_number,
        "bill_status": bill_status,
    }


def _extract_kingdee_push_message(resp_data: Any, fallback_message: str) -> str:
    if not isinstance(resp_data, dict):
        return fallback_message

    error_messages: List[str] = []
    data_obj = resp_data.get("data")
    result = data_obj.get("result") if isinstance(data_obj, dict) else None

    if isinstance(result, list):
        for item in result:
            if not isinstance(item, dict):
                continue
            errors = item.get("errors") or []
            if not isinstance(errors, list):
                continue
            for error_item in errors:
                if not isinstance(error_item, dict):
                    continue
                row_messages = error_item.get("rowMsg") or []
                if not isinstance(row_messages, list):
                    continue
                for row_message in row_messages:
                    text_value = str(row_message).strip()
                    if text_value:
                        error_messages.append(text_value)

    if error_messages:
        return "; ".join(error_messages[:3])

    message = str(resp_data.get("message") or "").strip()
    return message or fallback_message


def _finalize_bill_push_records(
    db: Session,
    push_batch_no: str,
    push_status: str,
    message: str,
    response_payload: Optional[str] = None,
    voucher_number: Optional[str] = None,
    voucher_id: Optional[str] = None,
) -> None:
    records = db.query(models.BillVoucherPushRecord).filter(
        models.BillVoucherPushRecord.push_batch_no == push_batch_no
    ).all()

    pushed_at = datetime.now() if push_status == "success" else None
    for record in records:
        record.push_status = push_status
        record.message = message
        record.response_payload = response_payload
        record.voucher_number = voucher_number
        record.voucher_id = voucher_id
        record.pushed_at = pushed_at

    db.commit()


app = FastAPI(title="FinFlow Middleware")


# (moved) Route is registered after dependencies below.
def _reset_bill_voucher_binding_impl(
    payload: schemas.BillVoucherResetRequest,
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
    current_user: models.User,
    db: Session,
    allowed_community_ids: List[int],
):
    """
    Reset local voucher push status for bills.

    Use case: voucher pushed successfully but deleted in Kingdee, so user wants to
   解除关联并允许二次推送。
    """
    refs = _normalize_bill_refs(payload.bills)
    if not refs:
        raise HTTPException(status_code=400, detail="bills is required")

    if not allowed_community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    allowed_set = set(allowed_community_ids)
    unauthorized = [ref for ref in refs if int(ref["community_id"]) not in allowed_set]
    if unauthorized:
        preview = ", ".join([f"{ref['community_id']}:{ref['bill_id']}" for ref in unauthorized[:10]])
        raise HTTPException(status_code=403, detail=f"Unauthorized bill communities: {preview}")

    account_book_id = _decode_header_value(x_account_book_id) or None
    account_book_name = _decode_header_value(x_account_book_name) or None
    account_book_number = _decode_header_value(x_account_book_number) or None
    push_batch_no = f"VR{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:8].upper()}"

    # Lock bills to avoid racing with /vouchers/push
    conditions = [
        and_(
            models.Bill.id == ref["bill_id"],
            models.Bill.community_id == ref["community_id"],
        )
        for ref in refs
    ]
    locked_bills = db.query(models.Bill).filter(or_(*conditions)).with_for_update().all()
    locked_keys = {(int(b.id), int(b.community_id)) for b in locked_bills}
    missing_refs = [
        ref for ref in refs
        if (int(ref["bill_id"]), int(ref["community_id"])) not in locked_keys
    ]
    if missing_refs:
        preview = ", ".join([f"{ref['community_id']}:{ref['bill_id']}" for ref in missing_refs[:10]])
        raise HTTPException(status_code=404, detail=f"Bills not found: {preview}")

    latest_status_map = _get_bill_push_status_map(
        db,
        refs,
        account_book_number=account_book_number,
    )
    reason = (payload.reason or "").strip()
    for ref in refs:
        latest = latest_status_map.get((ref["bill_id"], ref["community_id"])) or {}
        prev_voucher = latest.get("voucher_number") or latest.get("voucher_id") or ""
        msg_parts = ["Reset voucher binding"]
        if prev_voucher:
            msg_parts.append(f"previous={prev_voucher}")
        if reason:
            msg_parts.append(f"reason={reason}")
        db.add(models.BillVoucherPushRecord(
            bill_id=ref["bill_id"],
            community_id=ref["community_id"],
            push_batch_no=push_batch_no,
            push_status="not_pushed",
            account_book_id=account_book_id,
            account_book_name=account_book_name,
            account_book_number=account_book_number,
            pushed_by=current_user.id,
            message="; ".join(msg_parts),
            voucher_number=None,
            voucher_id=None,
            pushed_at=None,
        ))
    db.commit()

    return {
        "success": True,
        "push_batch_no": push_batch_no,
        "reset_bills": refs,
    }

origins_raw = os.getenv("ALLOWED_ORIGINS", "")
origins = [o.strip() for o in origins_raw.split(",") if o.strip()]
allow_all_origins = "*" in origins
if not origins:
    origins = [
        "http://localhost:5273",
        "http://127.0.0.1:5273",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8100",
        "http://127.0.0.1:8100",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if allow_all_origins else origins,
    # Support dev access via LAN IP without having to enumerate it in ALLOWED_ORIGINS.
    allow_origin_regex=None
    if allow_all_origins
    else r"^http://(localhost|127\.0\.0\.1|192\.168\.\d+\.\d+|10\.\d+\.\d+\.\d+)(:\d+)?$",
    # This project uses Authorization header (Bearer token) instead of cookies.
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency
def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_current_user(request: Request, db: Session = Depends(get_db)):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = auth_header.split(" ")[1]
    import utils.auth as auth_utils
    payload = auth_utils.verify_access_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
        
    user_id = payload.get("sub")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.status != 1:
        raise HTTPException(status_code=403, detail="User account is disabled")
        
    return user


def require_admin(current_user: models.User = Depends(get_current_user)):
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


MENU_PERMISSION_ROLE_DEFINITIONS = [
    {
        "role": "admin",
        "label": "管理员",
        "description": "拥有全部菜单和接口访问权限，当前角色固定显示全部能力。",
        "editable": False,
    },
    {
        "role": "user",
        "label": "普通用户",
        "description": "可按角色配置菜单可见范围，并进一步控制后台管理接口访问权限。",
        "editable": True,
    },
]

MENU_PERMISSION_DEFINITIONS = [
    {"key": "/", "label": "首页仪表盘", "section": "工作台", "group": "首页", "required": True, "admin_only": False, "default_enabled": True},
    {"key": "/receipt-bills", "label": "收款单据", "section": "马克业务", "group": "业务单据", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/deposit-records", "label": "押金管理", "section": "马克业务", "group": "业务单据", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/prepayment-records", "label": "预存款管理", "section": "马克业务", "group": "业务单据", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/bills", "label": "运营账单", "section": "马克业务", "group": "业务单据", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/projects", "label": "园区管理", "section": "马克业务", "group": "基础资料", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/charge-items", "label": "收费项目", "section": "马克业务", "group": "基础资料", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/houses", "label": "房屋管理", "section": "马克业务", "group": "基础资料", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/residents", "label": "住户管理", "section": "马克业务", "group": "基础资料", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/parks", "label": "车位管理", "section": "马克业务", "group": "基础资料", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/account-books", "label": "账簿管理", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/accounting-subjects", "label": "会计科目", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/auxiliary-data-categories", "label": "辅助资料分类", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/auxiliary-data", "label": "辅助资料", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/customers", "label": "客户管理", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/suppliers", "label": "供应商管理", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/kd-houses", "label": "金蝶房号", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/bank-accounts", "label": "银行账户", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/vouchers/templates", "label": "凭证模板", "section": "金蝶财务", "group": "凭证管理", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/vouchers/categories", "label": "模板分类", "section": "金蝶财务", "group": "凭证管理", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/oa-center", "label": "泛微协同", "section": "泛微协同", "group": "协同入口", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/integrations/reporting", "label": "报表设计", "section": "集成中心", "group": "集成能力", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/integrations/sync-schedules", "label": "同步计划", "section": "集成中心", "group": "集成能力", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/integrations/credentials", "label": "凭证配置", "section": "集成中心", "group": "集成能力", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/integrations/apis", "label": "接口管理", "section": "集成中心", "group": "集成能力", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/report-center", "label": "报表中心", "section": "报表中心", "group": "数据展示", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/organizations", "label": "组织管理", "section": "系统管理", "group": "权限与系统", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/users", "label": "用户管理", "section": "系统管理", "group": "权限与系统", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/menu-permissions", "label": "菜单权限", "section": "系统管理", "group": "权限与系统", "required": False, "admin_only": True, "default_enabled": False},
    {"key": "/settings", "label": "系统设置", "section": "系统管理", "group": "权限与系统", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/account", "label": "个人设置", "section": "系统管理", "group": "权限与系统", "required": True, "admin_only": False, "default_enabled": True},
]

API_PERMISSION_DEFINITIONS = [
    {"key": "project.manage", "label": "园区管理接口", "section": "马克业务", "group": "基础资料", "description": "允许访问园区管理页涉及的更新与同步接口。", "admin_only": False, "default_enabled": False},
    {"key": "charge_item.manage", "label": "收费项目接口", "section": "马克业务", "group": "基础资料", "description": "允许维护收费项目映射与同步任务。", "admin_only": False, "default_enabled": False},
    {"key": "credential.manage", "label": "凭证配置接口", "section": "集成中心", "group": "集成能力", "description": "允许维护外部系统凭证、令牌和连接测试。", "admin_only": False, "default_enabled": False},
    {"key": "api_registry.manage", "label": "接口管理接口", "section": "集成中心", "group": "集成能力", "description": "允许维护外部服务接口定义和调试配置。", "admin_only": False, "default_enabled": False},
    {"key": "sync_schedule.manage", "label": "同步计划接口", "section": "集成中心", "group": "集成能力", "description": "允许维护多进程同步计划、手动执行和查看执行记录。", "admin_only": False, "default_enabled": False},
    {"key": "reporting.manage", "label": "报表设计接口", "section": "集成中心", "group": "集成能力", "description": "允许维护报表连接、数据集和报表定义。", "admin_only": False, "default_enabled": False},
    {"key": "voucher_template.manage", "label": "凭证模板接口", "section": "金蝶财务", "group": "凭证管理", "description": "允许维护凭证模板和模板分类。", "admin_only": False, "default_enabled": False},
    {"key": "organization.manage", "label": "组织管理接口", "section": "系统管理", "group": "权限与系统", "description": "允许维护组织架构信息。", "admin_only": False, "default_enabled": False},
    {"key": "user.manage", "label": "用户管理接口", "section": "系统管理", "group": "权限与系统", "description": "允许查看、创建、更新和删除用户。", "admin_only": False, "default_enabled": False},
    {"key": "setting.manage", "label": "系统设置接口", "section": "系统管理", "group": "权限与系统", "description": "允许维护全局变量与系统设置。", "admin_only": False, "default_enabled": False},
    {"key": "menu_permission.manage", "label": "菜单权限接口", "section": "系统管理", "group": "权限与系统", "description": "允许维护角色的菜单与接口权限。", "admin_only": True, "default_enabled": False},
]

MENU_PERMISSION_ROLE_MAP = {item["role"]: item for item in MENU_PERMISSION_ROLE_DEFINITIONS}
MENU_PERMISSION_DEFINITION_MAP = {item["key"]: item for item in MENU_PERMISSION_DEFINITIONS}
API_PERMISSION_DEFINITION_MAP = {item["key"]: item for item in API_PERMISSION_DEFINITIONS}


def _ordered_permission_keys(definitions: List[Dict[str, Any]], keys: Set[str], key_field: str) -> List[str]:
    ordered: List[str] = []
    for item in definitions:
        key = item[key_field]
        if key in keys:
            ordered.append(key)
    return ordered


def _get_allowed_permission_keys(definitions: List[Dict[str, Any]], role: str, key_field: str) -> Set[str]:
    normalized_role = str(role or "user").strip() or "user"
    return {
        item[key_field]
        for item in definitions
        if normalized_role == "admin" or not item.get("admin_only")
    }


def _get_required_menu_keys(role: str) -> Set[str]:
    normalized_role = str(role or "user").strip() or "user"
    return {
        item["key"]
        for item in MENU_PERMISSION_DEFINITIONS
        if item["required"] and (normalized_role == "admin" or not item["admin_only"])
    }


def _get_default_menu_keys(role: str) -> List[str]:
    normalized_role = str(role or "user").strip() or "user"
    if normalized_role == "admin":
        return [item["key"] for item in MENU_PERMISSION_DEFINITIONS]

    default_keys = {
        item["key"]
        for item in MENU_PERMISSION_DEFINITIONS
        if item.get("default_enabled") and not item.get("admin_only")
    }
    return _ordered_permission_keys(MENU_PERMISSION_DEFINITIONS, default_keys, "key")


def _get_default_api_keys(role: str) -> List[str]:
    normalized_role = str(role or "user").strip() or "user"
    if normalized_role == "admin":
        return [item["key"] for item in API_PERMISSION_DEFINITIONS]

    default_keys = {
        item["key"]
        for item in API_PERMISSION_DEFINITIONS
        if item.get("default_enabled") and not item.get("admin_only")
    }
    return _ordered_permission_keys(API_PERMISSION_DEFINITIONS, default_keys, "key")


def _get_role_menu_keys(db: Session, role: str) -> List[str]:
    normalized_role = str(role or "user").strip() or "user"
    allowed_definition_keys = _get_allowed_permission_keys(MENU_PERMISSION_DEFINITIONS, normalized_role, "key")
    required_keys = _get_required_menu_keys(normalized_role)

    if normalized_role == "admin":
        return _ordered_permission_keys(MENU_PERMISSION_DEFINITIONS, allowed_definition_keys, "key")

    rows = (
        db.query(models.RoleMenuPermission.menu_key)
        .filter(models.RoleMenuPermission.role == normalized_role)
        .all()
    )
    if rows:
        persisted_keys = {row[0] for row in rows if row[0] in allowed_definition_keys}
        return _ordered_permission_keys(MENU_PERMISSION_DEFINITIONS, persisted_keys | required_keys, "key")

    return _ordered_permission_keys(
        MENU_PERMISSION_DEFINITIONS,
        set(_get_default_menu_keys(normalized_role)) | required_keys,
        "key",
    )


def _get_role_api_keys(db: Session, role: str) -> List[str]:
    normalized_role = str(role or "user").strip() or "user"
    allowed_definition_keys = _get_allowed_permission_keys(API_PERMISSION_DEFINITIONS, normalized_role, "key")

    if normalized_role == "admin":
        return _ordered_permission_keys(API_PERMISSION_DEFINITIONS, allowed_definition_keys, "key")

    rows = (
        db.query(models.RoleApiPermission.api_key)
        .filter(models.RoleApiPermission.role == normalized_role)
        .all()
    )
    if rows:
        persisted_keys = {row[0] for row in rows if row[0] in allowed_definition_keys}
        return _ordered_permission_keys(API_PERMISSION_DEFINITIONS, persisted_keys, "key")

    return _ordered_permission_keys(
        API_PERMISSION_DEFINITIONS,
        set(_get_default_api_keys(normalized_role)),
        "key",
    )


def _has_api_permission(db: Session, user: models.User, permission_key: str) -> bool:
    if not user:
        return False
    if user.role == "admin":
        return True
    return permission_key in set(_get_role_api_keys(db, user.role))


def _require_api_permission(db: Session, user: models.User, permission_key: str) -> None:
    if not _has_api_permission(db, user, permission_key):
        raise HTTPException(status_code=403, detail="Permission denied")


def _require_any_api_permission(db: Session, user: models.User, permission_keys: List[str]) -> None:
    if any(_has_api_permission(db, user, permission_key) for permission_key in permission_keys):
        return
    raise HTTPException(status_code=403, detail="Permission denied")


def _build_menu_permission_role_state(db: Session, role: str) -> Dict[str, Any]:
    role_meta = MENU_PERMISSION_ROLE_MAP.get(role, {
        "role": role,
        "label": role,
        "description": "",
        "editable": role != "admin",
    })
    normalized_role = role_meta["role"]
    return {
        "role": normalized_role,
        "label": role_meta["label"],
        "description": role_meta.get("description"),
        "editable": bool(role_meta.get("editable", True)),
        "menu_keys": _get_role_menu_keys(db, normalized_role),
        "api_keys": _get_role_api_keys(db, normalized_role),
    }


def _normalize_column_preference_items(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    normalized: List[str] = []
    seen: Set[str] = set()
    for value in values:
        if value is None:
            continue
        key = str(value).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return normalized


def _deserialize_column_preference(raw_value: Optional[str]) -> List[str]:
    if not raw_value:
        return []

    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError):
        return []

    return _normalize_column_preference_items(parsed)


def _serialize_column_preference(values: Any) -> str:
    return json.dumps(_normalize_column_preference_items(values), ensure_ascii=False)


def get_user_context(
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    current_user: models.User = Depends(get_current_user)
) -> Dict[str, str]:
    """Helper to extract user context from request for variable resolution."""
    from urllib.parse import unquote
    org_name = current_user.organization.name if current_user.organization else "未分配"
    return {
        "current_user_id": str(current_user.id),
        "current_username": current_user.username,
        "current_user_realname": current_user.real_name or current_user.username,
        "current_org_id": str(current_user.org_id) if current_user.org_id else "",
        "current_org_name": org_name,
        "current_account_book_number": unquote(x_account_book_number) if x_account_book_number else "",
        "current_account_book_name": unquote(x_account_book_name) if x_account_book_name else "",
    }


def get_allowed_community_ids(
    request: Request,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
) -> List[int]:
    """Get the community IDs the current user can access."""
    # 澧炲姞绠＄悊鍛樿鑹插垽鏂細绠＄悊鍛樺彲浠ユ搷浣滄墍鏈夊洯鍖?
    if current_user and current_user.role == 'admin':
        all_ids = db.query(models.ProjectList.proj_id).all()
        return [r[0] for r in all_ids]

    account_book_number = request.headers.get('X-Account-Book-Number')
    if not account_book_number:
        # 濡傛灉娌℃湁璐︾翱鍙凤紝杩斿洖绌哄垪琛ㄥ疄鐜板畬鍏ㄩ殧绂伙紝闄ら潪鏄壒娈婄鐞嗗憳鏉冮檺锛堟殏涓嶅鐞嗭級
        return []
        
    from sqlalchemy import cast, String
    from urllib.parse import unquote
    book_num = unquote(account_book_number)
    
    # 鏌ユ壘鍏宠仈鍒版璐︾翱鐨勬墍鏈夊洯鍖篒D
    allowed_ids = db.query(models.ProjectList.proj_id).join(
        models.KingdeeAccountBook, 
        cast(models.ProjectList.kingdee_account_book_id, String) == cast(models.KingdeeAccountBook.id, String)
    ).filter(
        models.KingdeeAccountBook.number == book_num
    ).all()
    
    return [r[0] for r in allowed_ids]

@app.get("/")
def read_root():
    return {"message": "FinFlow Middleware is running"}

# 1. OA Data Ingestion
@app.post("/api/oa/callback", response_model=schemas.CashJournalResponse)
def receive_oa_callback(data: schemas.OACallback, db: Session = Depends(get_db)):
    # Check if flow_id already exists
    existing = db.query(models.ApprovalFormSnapshot).filter(models.ApprovalFormSnapshot.flow_id == data.flow_id).first()
    if existing:
        return existing.journal

    # Save Snapshot
    snapshot = models.ApprovalFormSnapshot(
        flow_id=data.flow_id,
        business_type=data.business_type,
        applicant_id=data.applicant_id,
        applicant_name=data.applicant_name,
        department_code=data.department_code,
        total_amount=data.total_amount,
        approved_at=data.approved_at,
        form_data_raw=json.dumps(data.form_data)
    )
    db.add(snapshot)
    
    # Create Journal Entry (Pending)
    journal = models.CashJournal(
        flow_id=data.flow_id,
        amount=data.total_amount,
        direction='O', # Default to Outflow for expenses, logic can be complex
        status='pending'
    )
    db.add(journal)
    db.commit()
    db.refresh(journal)
    return journal

# 2. List Journals
@app.get("/api/journals", response_model=List[schemas.CashJournalResponse])
def list_journals(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    journals = db.query(models.CashJournal).offset(skip).limit(limit).all()
    return journals

# 3. Preview Voucher
@app.post("/api/journals/{flow_id}/preview", response_model=schemas.VoucherPreview)
def preview_voucher(flow_id: str, db: Session = Depends(get_db)):
    journal = db.query(models.CashJournal).filter(models.CashJournal.flow_id == flow_id).first()
    if not journal:
        raise HTTPException(status_code=404, detail="Journal not found")
    
    snapshot = journal.snapshot
    # --- MOCK LOGIC FOR PREVIEW ---
    # Real logic would load VoucherTemplate based on snapshot.business_type
    # and parse expressions.
    
    entries = [
        schemas.VoucherEntry(
            line_no=1, 
            dr_cr="D", 
            account_code="6602.01", 
            amount=journal.amount, 
            summary=f"Reimbursement for {snapshot.applicant_name}",
            aux_items={"employee": snapshot.applicant_id}
        ),
        schemas.VoucherEntry(
            line_no=2, 
            dr_cr="C", 
            account_code="1002.01", 
            amount=journal.amount, 
            summary="Payment",
            aux_items={}
        )
    ]
    
    return schemas.VoucherPreview(
        entries=entries,
        total_debit=journal.amount,
        total_credit=journal.amount,
        is_balanced=True
    )

# 4. Push to Kingdee
@app.post("/api/journals/{flow_id}/push", response_model=schemas.PushResult)
def push_to_kingdee(flow_id: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    journal = db.query(models.CashJournal).filter(models.CashJournal.flow_id == flow_id).first()
    if not journal:
        raise HTTPException(status_code=404, detail="Journal not found")

    if journal.status == 'pushed':
         return schemas.PushResult(success=True, voucher_id=journal.voucher_id, message="Already pushed")

    # Mock Push
    # In reality, this would make an HTTP request to Kingdee
    # background_tasks.add_task(actual_push_function, flow_id)
    
    import uuid
    mock_voucher_id = f"V_{uuid.uuid4().hex[:8].upper()}"
    
    journal.status = 'pushed'
    journal.voucher_id = mock_voucher_id
    journal.pushed_at = datetime.now()
    db.commit()

    return schemas.PushResult(success=True, voucher_id=mock_voucher_id, message="Push successful")

# Dashboard Stats - Removed Globally



# Charge Items
@app.get("/api/charge-items", response_model=List[schemas.ChargeItemResponse])
def get_charge_items(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    _require_api_permission(db, current_user, "charge_item.manage")
    query = db.query(models.ChargeItem)
    if allowed_community_ids:
        # communityid 鍦ㄦā鍨嬩腑鏄?String
        c_ids = [str(cid) for cid in allowed_community_ids]
        query = query.filter(models.ChargeItem.communityid.in_(c_ids))
    else:
        return []
    
    items = query.offset(skip).limit(limit).all()
    return items

@app.put("/api/charge-items/{item_id}")
def update_charge_item(
    item_id: int, 
    data: schemas.ChargeItemUpdate, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    _require_api_permission(db, current_user, "charge_item.manage")
        
    item = db.query(models.ChargeItem).filter(models.ChargeItem.item_id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Charge item not found")
    
    update_data = data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(item, key, value)
    
    db.commit()
    return {"message": "Updated successfully"}

@app.post("/api/charge-items/sync")
def sync_charge_items_endpoint(
    background_tasks: BackgroundTasks, 
    request: schemas.BillSyncRequest = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    """Sync charge items for the specified communities."""
    _require_api_permission(db, current_user, "charge_item.manage")
    if request and request.community_ids:
        # 鏍￠獙璇锋眰鐨勫洯鍖烘槸鍚﹀湪璐︾翱鑼冨洿鍐?
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        # 濡傛灉鏈寚瀹氾紝鍒欏悓姝ヨ璐︾翱涓嬫墍鏈夊洯鍖?
        community_ids = allowed_community_ids
        
    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")
        
    str_ids = [str(cid) for cid in community_ids]
    background_tasks.add_task(sync_charge_items, str_ids)
    return {"message": "Charge items sync started", "community_ids": str_ids}

# Projects
@app.get("/api/projects")
def get_projects(
    request: Request,
    skip: int = 0, 
    limit: int = 100, 
    current_account_book_only: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    _require_api_permission(db, current_user, "project.manage")
    query = db.query(models.ProjectList)
    if allowed_community_ids:
        query = query.filter(models.ProjectList.proj_id.in_(allowed_community_ids))
    else:
        return {"items": [], "total": 0}

    if current_account_book_only:
        from sqlalchemy import cast, String

        account_book_number = _decode_header_value(request.headers.get("X-Account-Book-Number")) if request else None
        if not account_book_number:
            return {"items": [], "total": 0}

        query = query.join(
            models.KingdeeAccountBook,
            cast(models.ProjectList.kingdee_account_book_id, String) == cast(models.KingdeeAccountBook.id, String)
        ).filter(
            models.KingdeeAccountBook.number == account_book_number
        )
        
    total = query.count()
    projects = query.options(
        joinedload(models.ProjectList.kingdee_project),
        joinedload(models.ProjectList.default_receive_bank),
        joinedload(models.ProjectList.default_pay_bank),
        joinedload(models.ProjectList.kingdee_account_book)
    ).offset(skip).limit(limit).all()
    
    items = [{
        "proj_id": project.proj_id,
        "proj_name": project.proj_name,
        "kingdee_project_id": project.kingdee_project_id,
        "kingdee_project": {
            "id": project.kingdee_project.id,
            "number": project.kingdee_project.number,
            "name": project.kingdee_project.name,
            "group_name": project.kingdee_project.group_name
        } if project.kingdee_project else None,
        "default_receive_bank_id": project.default_receive_bank_id,
        "default_receive_bank": {
            "id": project.default_receive_bank.id,
            "name": project.default_receive_bank.name,
            "bankaccountnumber": project.default_receive_bank.bankaccountnumber,
            "bank_name": project.default_receive_bank.bank_name
        } if project.default_receive_bank else None,
        "default_pay_bank_id": project.default_pay_bank_id,
        "default_pay_bank": {
            "id": project.default_pay_bank.id,
            "name": project.default_pay_bank.name,
            "bankaccountnumber": project.default_pay_bank.bankaccountnumber,
            "bank_name": project.default_pay_bank.bank_name
        } if project.default_pay_bank else None,
        "kingdee_account_book_id": project.kingdee_account_book_id,
        "kingdee_account_book": {
            "id": project.kingdee_account_book.id,
            "number": project.kingdee_account_book.number,
            "name": project.kingdee_account_book.name
        } if project.kingdee_account_book else None,
        "created_at": project.created_at
    } for project in projects]
    
    return {"items": items, "total": total}

@app.put("/api/projects/{proj_id}")
def update_project(
    proj_id: int, 
    data: schemas.ProjectUpdate, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    _require_api_permission(db, current_user, "project.manage")
        
    project = db.query(models.ProjectList).filter(models.ProjectList.proj_id == proj_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    if data.kingdee_project_id is not None:
        project.kingdee_project_id = data.kingdee_project_id
    if data.default_receive_bank_id is not None:
        # 鍏佽浼犵┖瀛楃涓叉潵娓呴櫎缁戝畾
        project.default_receive_bank_id = data.default_receive_bank_id or None
    if data.default_pay_bank_id is not None:
        project.default_pay_bank_id = data.default_pay_bank_id or None
    if hasattr(data, 'kingdee_account_book_id') and data.kingdee_account_book_id is not None:
        project.kingdee_account_book_id = data.kingdee_account_book_id or None
        
    db.commit()
    return {"message": "Success"}

# Houses
@app.get("/api/houses", response_model=List[schemas.HouseResponse])
def get_houses(
    community_id: Optional[str] = None, 
    search: Optional[str] = None, 
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    query = db.query(models.House).options(
        joinedload(models.House.kingdee_house),
        selectinload(models.House.user_list),
        selectinload(models.House.parks),
    )
    
    # 寮哄埗璐︾翱闅旂
    if allowed_community_ids:
        c_ids = [str(cid) for cid in allowed_community_ids]
        query = query.filter(models.House.community_id.in_(c_ids))
    else:
        return []

    if community_id:
        query = query.filter(models.House.community_id == community_id)
    if search:
        search_filter = or_(
            models.House.house_name.ilike(f"%{search}%"),
            models.House.house_id.ilike(f"%{search}%"),
            models.House.building_name.ilike(f"%{search}%")
        )
        query = query.filter(search_filter)
    houses = query.order_by(models.House.created_at.desc()).offset(skip).limit(limit).all()
    return houses

@app.put("/api/houses/{house_id}", response_model=schemas.HouseResponse)
def update_house(house_id: int, data: schemas.HouseUpdate, db: Session = Depends(get_db)):
    house = db.query(models.House).filter(models.House.id == house_id).first()
    if not house:
        raise HTTPException(status_code=404, detail="House not found")
    
    if data.kingdee_house_id is not None:
        house.kingdee_house_id = data.kingdee_house_id
        
    db.commit()
    db.refresh(house)
    return house

@app.post("/api/houses/sync")
def sync_houses_endpoint(
    background_tasks: BackgroundTasks,
    request: schemas.HouseSyncRequest,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    """Sync house data for the specified communities."""
    if request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids
        
    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")
        
    str_ids = [str(cid) for cid in community_ids]
    
    # 鍒涘缓璺熻釜浠诲姟
    task_id = tracker.create_task(str_ids)
    
    background_tasks.add_task(sync_houses, str_ids, task_id)
    return {
        "message": "House sync started", 
        "task_id": task_id,
        "community_ids": str_ids
    }

# Residents
@app.get("/api/residents")
def get_residents(
    community_id: Optional[str] = None, 
    search: Optional[str] = None, 
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    query = db.query(models.Resident)
    
    # 寮哄埗璐︾翱闅旂
    if allowed_community_ids:
        c_ids = [str(cid) for cid in allowed_community_ids]
        query = query.filter(models.Resident.community_id.in_(c_ids))
    else:
        return {"items": [], "total": 0}

    if community_id:
        query = query.filter(models.Resident.community_id == community_id)
    if search:
        search_filter = or_(
            models.Resident.name.ilike(f"%{search}%"),
            models.Resident.resident_id.ilike(f"%{search}%"),
            models.Resident.phone.ilike(f"%{search}%")
        )
        query = query.filter(search_filter)
    
    total = query.count()
    residents = query.options(joinedload(models.Resident.kingdee_customer)).order_by(models.Resident.created_at.desc()).offset(skip).limit(limit).all()
    
    return {
        "items": residents,
        "total": total
    }

@app.put("/api/residents/{resident_id}", response_model=schemas.ResidentResponse)
def update_resident(resident_id: int, data: schemas.ResidentUpdate, db: Session = Depends(get_db)):
    resident = db.query(models.Resident).filter(models.Resident.id == resident_id).first()
    if not resident:
        raise HTTPException(status_code=404, detail="Resident not found")
    
    if data.kingdee_customer_id is not None:
        resident.kingdee_customer_id = data.kingdee_customer_id
        
    db.commit()
    db.refresh(resident)
    return resident

@app.post("/api/residents/sync")
def sync_residents_endpoint(
    background_tasks: BackgroundTasks,
    request: schemas.ResidentSyncRequest,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    """Sync resident data for the specified communities."""
    if request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids
        
    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")
        
    str_ids = [str(cid) for cid in community_ids]
    
    # 鍒涘缓璺熻釜浠诲姟
    task_id = tracker.create_task(str_ids)
    
    background_tasks.add_task(sync_residents, str_ids, task_id)
    return {
        "message": "Resident sync started", 
        "task_id": task_id,
        "community_ids": str_ids
    }

# Parks
@app.get("/api/parks", response_model=List[schemas.ParkResponse])
def get_parks(
    community_id: Optional[str] = None, 
    search: Optional[str] = None, 
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    query = db.query(models.Park)
    
    # 寮哄埗璐︾翱闅旂
    if allowed_community_ids:
        c_ids = [str(cid) for cid in allowed_community_ids]
        query = query.filter(models.Park.community_id.in_(c_ids))
    else:
        return []

    if community_id:
        query = query.filter(models.Park.community_id == community_id)
    if search:
        search_filter = or_(
            models.Park.name.ilike(f"%{search}%"),
            models.Park.park_id.ilike(f"%{search}%"),
            models.Park.user_name.ilike(f"%{search}%")
        )
        query = query.filter(search_filter)
    parks = query.order_by(models.Park.created_at.desc()).offset(skip).limit(limit).all()
    return parks

@app.put("/api/parks/{park_id}")
def update_park(park_id: int, data: schemas.ParkUpdate, db: Session = Depends(get_db)):
    park = db.query(models.Park).filter(models.Park.id == park_id).first()
    if not park:
        raise HTTPException(status_code=404, detail="Park not found")
    
    update_data = data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(park, key, value)
    
    db.commit()
    db.refresh(park)
    return {"message": "Updated successfully"}

@app.post("/api/parks/sync")
def sync_parks_endpoint(
    background_tasks: BackgroundTasks,
    request: schemas.ParkSyncRequest,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    """Sync park data for the specified communities."""
    if request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids
        
    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")
        
    str_ids = [str(cid) for cid in community_ids]
    
    # 鍒涘缓璺熻釜浠诲姟
    task_id = tracker.create_task(str_ids)
    
    background_tasks.add_task(sync_parks, str_ids, task_id)
    return {
        "message": "Park sync started", 
        "task_id": task_id,
        "community_ids": str_ids
    }


@app.post("/api/bills/voucher/reset")
def reset_bill_voucher_binding(
    payload: schemas.BillVoucherResetRequest,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    return _reset_bill_voucher_binding_impl(
        payload,
        x_account_book_id,
        x_account_book_name,
        x_account_book_number,
        current_user,
        db,
        allowed_community_ids,
    )


@app.post("/api/vouchers/query")
def query_voucher_by_id(
    payload: schemas.VoucherQueryRequest,
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db),
):
    """Query Kingdee voucher by internal id using ExternalApi named 凭证查询."""
    import requests
    import json as json_mod
    from services.external_auth import ExternalAuthService

    api_record = db.query(models.ExternalApi).filter(
        models.ExternalApi.name == "凭证查询",
        models.ExternalApi.is_active == True
    ).first()
    if not api_record:
        api_record = db.query(models.ExternalApi).filter(
            models.ExternalApi.url_path.ilike("%voucherQuery%"),
            models.ExternalApi.is_active == True
        ).first()
    if not api_record:
        raise HTTPException(status_code=404, detail="ExternalApi not found: 凭证查询")

    service = db.query(models.ExternalService).filter(models.ExternalService.id == api_record.service_id).first()
    if not service:
        raise HTTPException(status_code=404, detail="External service not found for 凭证查询")

    auth = ExternalAuthService(db=db, service_record=service, user_context=user_ctx)
    try:
        auth.get_token()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Auth failed: {exc}")

    full_url = api_record.url_path or ""
    if not full_url.startswith("http"):
        full_url = (service.base_url or "") + full_url
    url = resolve_variables(full_url or "", db, user_context=user_ctx)

    user_headers = api_record.request_headers or {}
    if isinstance(user_headers, str):
        try:
            user_headers = json_mod.loads(user_headers)
        except Exception:
            user_headers = {}
    user_headers = resolve_dict_variables(user_headers, db, user_context=user_ctx)

    headers = auth.get_auth_headers()
    for k, v in user_headers.items():
        if isinstance(v, str) and "{access_token}" in v and service.access_token:
            v = v.replace("{access_token}", service.access_token)
        headers[k] = str(v)

    body_template = api_record.request_body
    body: Dict[str, Any] = {}
    if body_template:
        if isinstance(body_template, str):
            try:
                body = json_mod.loads(body_template)
            except Exception:
                body = {}
        elif isinstance(body_template, dict):
            body = dict(body_template)
    body = resolve_dict_variables(body, db, user_context=user_ctx)

    if not body:
        body = {"data": {}}
    if "data" not in body or not isinstance(body["data"], dict):
        body["data"] = {}

    body["data"]["id"] = payload.voucher_id
    body["pageNo"] = payload.page_no or 1
    body["pageSize"] = payload.page_size or 10

    resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
    try:
        resp_data = resp.json()
    except Exception:
        resp_data = {"raw": resp.text}

    exists = False
    if isinstance(resp_data, dict):
        data_obj = resp_data.get("data")
        if isinstance(data_obj, dict):
            rows = data_obj.get("rows")
            if isinstance(rows, list) and len(rows) > 0:
                exists = True

    return {
        "success": bool(resp.ok),
        "status_code": resp.status_code,
        "voucher_id": payload.voucher_id,
        "exists": exists,
        "response": resp_data,
    }

@app.get("/api/bills")
def get_bills(
    search: Optional[str] = None, 
    community_ids: Optional[str] = None,
    status: Optional[str] = None,
    charge_items: Optional[str] = None,
    customer_name: Optional[str] = None,
    bill_id: Optional[str] = None,
    receipt_id: Optional[str] = None,
    house_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    in_month_start: Optional[str] = None,
    in_month_end: Optional[str] = None,
    pay_date_start: Optional[str] = None,
    pay_date_end: Optional[str] = None,
    pay_time_start: Optional[str] = None,
    pay_time_end: Optional[str] = None,
    deal_log_id: Optional[int] = None,
    skip: int = 0, 
    limit: int = 25, 
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    from sqlalchemy import func as sa_func
    from datetime import datetime
    
    # 瀛愭煡璇細鑱氬悎姣忎釜璐﹀崟鐨勫鎴峰悕绉?
    customer_subq = (
        db.query(
            models.BillUser.bill_id,
            models.BillUser.community_id,
            sa_func.string_agg(models.BillUser.user_name, ', ').label('customer_name')
        )
        .group_by(models.BillUser.bill_id, models.BillUser.community_id)
        .subquery()
    )
    
    query = db.query(
        models.Bill,
        models.ProjectList.proj_name,
        customer_subq.c.customer_name
    ).outerjoin(
        models.ProjectList, models.Bill.community_id == models.ProjectList.proj_id
    ).outerjoin(
        customer_subq,
        (models.Bill.id == customer_subq.c.bill_id) & 
        (models.Bill.community_id == customer_subq.c.community_id)
    )

    # 寮哄埗璐︾翱闅旂
    if allowed_community_ids:
        query = query.filter(models.Bill.community_id.in_(allowed_community_ids))
    else:
        return {"total": 0, "total_amount": 0.00, "items": []}

    # 缁村害绛涢€?
    if community_ids:
        # 鏀寔閫楀彿鍒嗛殧鐨勫涓洯鍖篒D
        try:
            ids = [int(cid.strip()) for cid in community_ids.split(",") if cid.strip()]
            if ids:
                query = query.filter(models.Bill.community_id.in_(ids))
        except ValueError:
            pass
            
    if customer_name:
        query = query.filter(customer_subq.c.customer_name.ilike(f"%{customer_name}%"))
    
    if status and status != '全部状态':
        query = query.filter(models.Bill.pay_status_str == status)
        
    if charge_items:
        c_items = [ci.strip() for ci in charge_items.split(",") if ci.strip()]
        if c_items:
            condition_list = []
            just_names = []
            for item in c_items:
                if '|' in item:
                    try:
                        pid, name = item.split('|', 1)
                        condition_list.append((models.Bill.community_id == int(pid)) & (models.Bill.charge_item_name == name))
                    except ValueError:
                        just_names.append(item)
                else:
                    just_names.append(item)
            
            # Combine all conditions
            all_conditions = list(condition_list)
            if just_names:
                all_conditions.append(models.Bill.charge_item_name.in_(just_names))
                
            if all_conditions:
                query = query.filter(or_(*all_conditions))
        
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            query = query.filter(models.Bill.created_at >= start_dt)
        except ValueError:
            pass
            
    if end_date:
        try:
            end_dt = datetime.strptime(f"{end_date} 23:59:59", '%Y-%m-%d %H:%M:%S')
            query = query.filter(models.Bill.created_at <= end_dt)
        except ValueError:
            pass

    if in_month_start:
        query = query.filter(models.Bill.in_month >= in_month_start)
    if in_month_end:
        query = query.filter(models.Bill.in_month <= in_month_end)

    if pay_date_start:
        try:
            date_start = datetime.strptime(pay_date_start, '%Y-%m-%d').date()
            query = query.filter(models.Bill.receive_date >= date_start)
        except ValueError:
            pass

    if pay_date_end:
        try:
            date_end = datetime.strptime(pay_date_end, '%Y-%m-%d').date()
            query = query.filter(models.Bill.receive_date <= date_end)
        except ValueError:
            pass

    if pay_time_start:
        try:
            pt_start = int(datetime.strptime(f"{pay_time_start} 00:00:00", '%Y-%m-%d %H:%M:%S').timestamp())
            query = query.filter(models.Bill.pay_time >= pt_start)
        except ValueError:
            pass

    if pay_time_end:
        try:
            pt_end = int(datetime.strptime(f"{pay_time_end} 23:59:59", '%Y-%m-%d %H:%M:%S').timestamp())
            query = query.filter(models.Bill.pay_time <= pt_end)
        except ValueError:
            pass

    if deal_log_id is not None:
        try:
            query = query.filter(models.Bill.deal_log_id == int(deal_log_id))
        except Exception:
            pass

    if bill_id:
        try:
            query = query.filter(models.Bill.id == int(bill_id))
        except Exception:
            pass

    if receipt_id:
        query = query.filter(models.Bill.receipt_id.ilike(f"%{receipt_id}%"))

    if house_name:
        like = f"%{house_name}%"
        query = query.filter(or_(
            models.Bill.full_house_name.ilike(like),
            models.Bill.bind_house_name.ilike(like),
            models.Bill.asset_name.ilike(like)
        ))

    if search:
        keyword = search.strip()
        if keyword:
            like = f"%{keyword}%"
            search_conditions = [
                models.Bill.receipt_id.ilike(like),
                models.Bill.full_house_name.ilike(like),
                models.Bill.bind_house_name.ilike(like),
                models.Bill.asset_name.ilike(like),
                customer_subq.c.customer_name.ilike(like),
            ]

            if keyword.isdigit():
                numeric_value = int(keyword)
                search_conditions.extend([
                    models.Bill.id == numeric_value,
                    models.Bill.deal_log_id == numeric_value,
                ])
            else:
                from sqlalchemy import cast, String as SAString

                search_conditions.extend([
                    cast(models.Bill.id, SAString).ilike(like),
                    cast(models.Bill.deal_log_id, SAString).ilike(like),
                ])

            query = query.filter(or_(*search_conditions))
    
    total = query.count()
    total_amount = query.with_entities(sa_func.sum(models.Bill.amount)).scalar()
    
    results = query.order_by(models.Bill.created_at.desc()).offset(skip).limit(limit).all()
    status_map = _get_bill_push_status_map(
        db,
        [{"bill_id": bill.id, "community_id": bill.community_id} for bill, _, _ in results],
        account_book_number=_decode_header_value(x_account_book_number) or None,
    )
    
    return {
        "total": total,
        "total_amount": float(total_amount) if total_amount else 0.00,
        "items": [{
            "id": bill.id,
            "community_id": bill.community_id,
            "community_name": proj_name or f"椤圭洰{bill.community_id}",
            "charge_item_name": bill.charge_item_name,
            "asset_name": bill.asset_name,
            "full_house_name": bill.full_house_name,
            "in_month": bill.in_month,
            "amount": float(bill.amount) if bill.amount else 0,
            "pay_status_str": bill.pay_status_str,
            "pay_time": bill.pay_time,
            "receive_date": bill.receive_date,
            "deal_log_id": bill.deal_log_id,
            "created_at": bill.created_at,
            "customer_name": customer_name or "",
            **status_map.get(
                (int(bill.id), int(bill.community_id)),
                _build_bill_push_status_entry(int(bill.id), int(bill.community_id)),
            ),
        } for bill, proj_name, customer_name in results]
    }


@app.get("/api/bills/export")
def export_bills(
    search: Optional[str] = None,
    community_ids: Optional[str] = None,
    status: Optional[str] = None,
    charge_items: Optional[str] = None,
    customer_name: Optional[str] = None,
    bill_id: Optional[str] = None,
    receipt_id: Optional[str] = None,
    house_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    in_month_start: Optional[str] = None,
    in_month_end: Optional[str] = None,
    pay_date_start: Optional[str] = None,
    pay_date_end: Optional[str] = None,
    pay_time_start: Optional[str] = None,
    pay_time_end: Optional[str] = None,
    deal_log_id: Optional[int] = None,
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    customer_subq = (
        db.query(
            models.BillUser.bill_id,
            models.BillUser.community_id,
            func.string_agg(models.BillUser.user_name, ', ').label('customer_name')
        )
        .group_by(models.BillUser.bill_id, models.BillUser.community_id)
        .subquery()
    )

    query = db.query(
        models.Bill,
        models.ProjectList.proj_name,
        customer_subq.c.customer_name
    ).outerjoin(
        models.ProjectList, models.Bill.community_id == models.ProjectList.proj_id
    ).outerjoin(
        customer_subq,
        (models.Bill.id == customer_subq.c.bill_id) &
        (models.Bill.community_id == customer_subq.c.community_id)
    )

    if allowed_community_ids:
        query = query.filter(models.Bill.community_id.in_(allowed_community_ids))
    else:
        query = query.filter(text("1=0"))

    if community_ids:
        try:
            ids = [int(cid.strip()) for cid in community_ids.split(",") if cid.strip()]
            if ids:
                query = query.filter(models.Bill.community_id.in_(ids))
        except ValueError:
            pass

    if customer_name:
        query = query.filter(customer_subq.c.customer_name.ilike(f"%{customer_name}%"))

    if status and status != "全部状态":
        query = query.filter(models.Bill.pay_status_str == status)

    if charge_items:
        c_items = [ci.strip() for ci in charge_items.split(",") if ci.strip()]
        if c_items:
            condition_list = []
            just_names = []
            for item in c_items:
                if '|' in item:
                    try:
                        pid, name = item.split('|', 1)
                        condition_list.append((models.Bill.community_id == int(pid)) & (models.Bill.charge_item_name == name))
                    except ValueError:
                        just_names.append(item)
                else:
                    just_names.append(item)

            all_conditions = list(condition_list)
            if just_names:
                all_conditions.append(models.Bill.charge_item_name.in_(just_names))

            if all_conditions:
                query = query.filter(or_(*all_conditions))

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            query = query.filter(models.Bill.created_at >= start_dt)
        except ValueError:
            pass

    if end_date:
        try:
            end_dt = datetime.strptime(f"{end_date} 23:59:59", '%Y-%m-%d %H:%M:%S')
            query = query.filter(models.Bill.created_at <= end_dt)
        except ValueError:
            pass

    if in_month_start:
        query = query.filter(models.Bill.in_month >= in_month_start)
    if in_month_end:
        query = query.filter(models.Bill.in_month <= in_month_end)

    if pay_date_start:
        try:
            date_start = datetime.strptime(pay_date_start, '%Y-%m-%d').date()
            query = query.filter(models.Bill.receive_date >= date_start)
        except ValueError:
            pass

    if pay_date_end:
        try:
            date_end = datetime.strptime(pay_date_end, '%Y-%m-%d').date()
            query = query.filter(models.Bill.receive_date <= date_end)
        except ValueError:
            pass

    if pay_time_start:
        try:
            pt_start = int(datetime.strptime(f"{pay_time_start} 00:00:00", '%Y-%m-%d %H:%M:%S').timestamp())
            query = query.filter(models.Bill.pay_time >= pt_start)
        except ValueError:
            pass

    if pay_time_end:
        try:
            pt_end = int(datetime.strptime(f"{pay_time_end} 23:59:59", '%Y-%m-%d %H:%M:%S').timestamp())
            query = query.filter(models.Bill.pay_time <= pt_end)
        except ValueError:
            pass

    if deal_log_id is not None:
        try:
            query = query.filter(models.Bill.deal_log_id == int(deal_log_id))
        except Exception:
            pass

    if bill_id:
        try:
            query = query.filter(models.Bill.id == int(bill_id))
        except Exception:
            pass

    if receipt_id:
        query = query.filter(models.Bill.receipt_id.ilike(f"%{receipt_id}%"))

    if house_name:
        like = f"%{house_name}%"
        query = query.filter(or_(
            models.Bill.full_house_name.ilike(like),
            models.Bill.bind_house_name.ilike(like),
            models.Bill.asset_name.ilike(like)
        ))

    if search:
        keyword = search.strip()
        if keyword:
            like = f"%{keyword}%"
            search_conditions = [
                models.Bill.receipt_id.ilike(like),
                models.Bill.full_house_name.ilike(like),
                models.Bill.bind_house_name.ilike(like),
                models.Bill.asset_name.ilike(like),
                customer_subq.c.customer_name.ilike(like),
            ]

            if keyword.isdigit():
                numeric_value = int(keyword)
                search_conditions.extend([
                    models.Bill.id == numeric_value,
                    models.Bill.deal_log_id == numeric_value,
                ])
            else:
                search_conditions.extend([
                    cast(models.Bill.id, String).ilike(like),
                    cast(models.Bill.deal_log_id, String).ilike(like),
                ])

            query = query.filter(or_(*search_conditions))

    results = query.order_by(models.Bill.created_at.desc()).all()
    status_map = _get_bill_push_status_map(
        db,
        [{"bill_id": bill.id, "community_id": bill.community_id} for bill, _, _ in results],
        account_book_number=_decode_header_value(x_account_book_number) or None,
    )

    def _format_dt(value):
        if not value:
            return ""
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    def _format_timestamp(value):
        if value in (None, ""):
            return ""
        try:
            return datetime.fromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(value)

    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output)
    writer.writerow([
        "账单ID",
        "缴费ID",
        "收据ID",
        "园区",
        "房产名称",
        "房号",
        "客户名称",
        "收费项目",
        "所属月份",
        "收款金额",
        "收费状态",
        "支付日期",
        "支付时间",
        "创建时间",
        "推送状态",
        "凭证号",
    ])

    for bill, proj_name, customer_name_value in results:
        push_status = status_map.get(
            (int(bill.id), int(bill.community_id)),
            _build_bill_push_status_entry(int(bill.id), int(bill.community_id)),
        )
        writer.writerow([
            bill.id,
            bill.deal_log_id or "",
            bill.receipt_id or "",
            proj_name or bill.community_id,
            bill.asset_name or "",
            bill.full_house_name or "",
            customer_name_value or "",
            bill.charge_item_name or "",
            bill.in_month or "",
            float(bill.amount) if bill.amount else 0,
            bill.pay_status_str or "",
            bill.receive_date or "",
            _format_timestamp(bill.pay_time),
            _format_dt(bill.created_at),
            push_status.get("push_status_label", ""),
            push_status.get("voucher_number", "") or "",
        ])

    filename = f"bills_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    response = StreamingResponse(iter([output.getvalue()]), media_type="text/csv; charset=utf-8")
    response.headers["Content-Disposition"] = (
        f"attachment; filename={filename}; filename*=UTF-8''{quote(filename)}"
    )
    return response

@app.post("/api/bills/sync")
def sync_bills_endpoint(
    background_tasks: BackgroundTasks, 
    request: schemas.BillSyncRequest = None,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    """Sync bill data for the specified communities."""
    if request and request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids
        
    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")
        
    str_ids = [str(cid) for cid in community_ids]
    
    # Create a tracking task
    task_id = tracker.create_task(str_ids)
    
    background_tasks.add_task(sync_bills, str_ids, task_id)
    
    return {
        "message": "Bill synchronization started",
        "task_id": task_id,
        "community_ids": str_ids
    }

@app.get("/api/bills/sync/status/{task_id}")
def get_sync_status(task_id: str):
    """Get the current status of a sync task.
    """
    status = tracker.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")
    return status

@app.get("/api/bills/charge-items")
def get_bill_charge_items(
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    """Get all distinct charge items from existing projects and charge items mapping."""
    if not allowed_community_ids:
        return []
        
    from sqlalchemy import cast, String
    query = db.query(
        models.ChargeItem.item_name,
        models.ProjectList.proj_name,
        models.ProjectList.proj_id
    ).join(
        models.ProjectList, models.ChargeItem.communityid == cast(models.ProjectList.proj_id, String)
    ).filter(
        models.ProjectList.proj_id.in_(allowed_community_ids)
    )
    
    items = query.all()
    
    unique_items = []
    seen = set()
    for item in items:
        key = f"{item.proj_id}|{item.item_name}"
        if key not in seen and item.item_name:
            seen.add(key)
            proj_name = item.proj_name or f"鍥尯{item.proj_id}"
            unique_items.append({
                "value": key,
                "label": f"{item.item_name} + {proj_name}"
            })
            
    return unique_items

@app.get("/api/bills/{bill_id}")
def get_bill(
    bill_id: str,
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    db: Session = Depends(get_db)
):
    bill = db.query(models.Bill).filter(models.Bill.id == bill_id).first()
    if not bill:
        raise HTTPException(status_code=404, detail="Bill not found")

    push_status = _get_bill_push_status_map(
        db,
        [{"bill_id": bill.id, "community_id": bill.community_id}],
        account_book_number=_decode_header_value(x_account_book_number) or None,
    ).get(
        (int(bill.id), int(bill.community_id)),
        _build_bill_push_status_entry(int(bill.id), int(bill.community_id)),
    )
    
    return {
        "id": bill.id,
        "community_id": bill.community_id,
        "charge_item_id": bill.charge_item_id,
        "charge_item_name": bill.charge_item_name,
        "category_name": bill.category_name,
        "asset_name": bill.asset_name,
        "full_house_name": bill.full_house_name,
        "start_time": bill.start_time,
        "end_time": bill.end_time,
        "pay_time": bill.pay_time,
        "create_time": bill.create_time,
        "amount": float(bill.amount) if bill.amount else 0,
        "bill_amount": float(bill.bill_amount) if bill.bill_amount else 0,
        "discount_amount": float(bill.discount_amount) if bill.discount_amount else 0,
        "late_money_amount": float(bill.late_money_amount) if bill.late_money_amount else 0,
        "deposit_amount": float(bill.deposit_amount) if bill.deposit_amount else 0,
        "pay_status": bill.pay_status,
        "pay_status_str": bill.pay_status_str,
        "bill_type_str": bill.bill_type_str,
        "pay_type_str": bill.pay_type_str,
        "in_month": bill.in_month,
        "remark": bill.remark,
        "receipt_id": bill.receipt_id,
        **push_status,
    }


# ===================== Receipt Bills (收款账单) =====================

RECEIPT_BILL_DEAL_TYPE_LABELS = {
    1: "预存款充值",
    2: "预存款退款",
    3: "账单实收",
    4: "账单退款",
    5: "收取押金",
    6: "退还押金",
}

PREPAYMENT_OPERATE_TYPE_LABELS = {
    1: "充值",
    2: "退款",
}

DEPOSIT_OPERATE_TYPE_LABELS = {
    1: "收取",
    2: "退还",
}


def _serialize_prepayment_record(record: models.PrepaymentRecord) -> Dict[str, Any]:
    operate_type = int(record.operate_type) if record.operate_type is not None else None
    return {
        "id": record.id,
        "community_id": record.community_id,
        "community_name": record.community_name or ("未匹配园区" if record.community_id is None else f"园区 {record.community_id}"),
        "account_id": record.account_id,
        "building_id": record.building_id,
        "unit_id": record.unit_id,
        "house_id": record.house_id,
        "house_name": record.house_name,
        "amount": float(record.amount) if record.amount is not None else 0,
        "balance_after_change": float(record.balance_after_change) if record.balance_after_change is not None else 0,
        "operate_type": operate_type,
        "operate_type_label": record.operate_type_label or PREPAYMENT_OPERATE_TYPE_LABELS.get(operate_type, "其他"),
        "pay_channel_id": record.pay_channel_id,
        "pay_channel_str": record.pay_channel_str,
        "operator": record.operator,
        "operator_name": record.operator_name,
        "operate_time": record.operate_time,
        "operate_date": record.operate_date,
        "source_updated_time": record.source_updated_time,
        "remark": record.remark,
        "deposit_order_id": record.deposit_order_id,
        "pay_time": record.pay_time,
        "pay_date": record.pay_date,
        "category_id": record.category_id,
        "category_name": record.category_name,
        "status": record.status,
        "payment_id": record.payment_id,
        "has_refund_receipt": bool(record.has_refund_receipt),
        "refund_receipt_id": record.refund_receipt_id,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def _serialize_deposit_record(record: models.DepositRecord) -> Dict[str, Any]:
    operate_type = int(record.operate_type) if record.operate_type is not None else None
    return {
        "id": record.id,
        "community_id": record.community_id,
        "community_name": record.community_name or ("未匹配园区" if record.community_id is None else f"园区 {record.community_id}"),
        "house_id": record.house_id,
        "house_name": record.house_name,
        "amount": float(record.amount) if record.amount is not None else 0,
        "operate_type": operate_type,
        "operate_type_label": DEPOSIT_OPERATE_TYPE_LABELS.get(operate_type, "其他"),
        "operator": record.operator,
        "operator_name": record.operator_name,
        "operate_time": record.operate_time,
        "operate_date": record.operate_date,
        "cash_pledge_name": record.cash_pledge_name,
        "remark": record.remark,
        "pay_time": record.pay_time,
        "pay_date": record.pay_date,
        "payment_id": record.payment_id,
        "has_refund_receipt": bool(record.has_refund_receipt),
        "refund_receipt_id": record.refund_receipt_id,
        "pay_channel_str": record.pay_channel_str,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def _build_house_resident_name_subquery(db: Session):
    resident_display = func.coalesce(
        func.nullif(models.HouseUser.owner_name, ""),
        func.nullif(models.HouseUser.name, ""),
    )
    return (
        db.query(
            models.House.house_id.label("house_id"),
            models.House.community_id.label("community_id"),
            func.string_agg(func.distinct(resident_display), ", ").label("resident_name"),
        )
        .join(models.HouseUser, models.HouseUser.house_fk == models.House.id)
        .filter(resident_display.isnot(None))
        .group_by(models.House.house_id, models.House.community_id)
        .subquery()
    )


@app.get("/api/prepayment-records")
def get_prepayment_records(
    search: Optional[str] = None,
    community_ids: Optional[str] = None,
    operate_type: Optional[int] = None,
    operate_date_start: Optional[str] = None,
    operate_date_end: Optional[str] = None,
    pay_date_start: Optional[str] = None,
    pay_date_end: Optional[str] = None,
    has_refund_receipt: Optional[bool] = None,
    skip: int = 0,
    limit: int = 25,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    from sqlalchemy import func as sa_func, cast, String as SAString

    if not allowed_community_ids:
        return {"total": 0, "total_amount": 0.00, "items": []}

    resident_subq = _build_house_resident_name_subquery(db)

    query = (
        db.query(models.PrepaymentRecord, resident_subq.c.resident_name)
        .outerjoin(
            resident_subq,
            and_(
                cast(models.PrepaymentRecord.house_id, SAString) == resident_subq.c.house_id,
                cast(models.PrepaymentRecord.community_id, SAString) == resident_subq.c.community_id,
            ),
        )
        .filter(models.PrepaymentRecord.community_id.in_(allowed_community_ids))
    )

    if community_ids:
        try:
            ids = [int(cid.strip()) for cid in community_ids.split(",") if cid.strip()]
            if ids:
                query = query.filter(models.PrepaymentRecord.community_id.in_(ids))
        except ValueError:
            pass

    if operate_type is not None:
        query = query.filter(models.PrepaymentRecord.operate_type == operate_type)

    if operate_date_start:
        try:
            start_dt = datetime.strptime(operate_date_start, "%Y-%m-%d").date()
            query = query.filter(models.PrepaymentRecord.operate_date >= start_dt)
        except ValueError:
            pass

    if operate_date_end:
        try:
            end_dt = datetime.strptime(operate_date_end, "%Y-%m-%d").date()
            query = query.filter(models.PrepaymentRecord.operate_date <= end_dt)
        except ValueError:
            pass

    if pay_date_start:
        try:
            start_dt = datetime.strptime(pay_date_start, "%Y-%m-%d").date()
            query = query.filter(models.PrepaymentRecord.pay_date >= start_dt)
        except ValueError:
            pass

    if pay_date_end:
        try:
            end_dt = datetime.strptime(pay_date_end, "%Y-%m-%d").date()
            query = query.filter(models.PrepaymentRecord.pay_date <= end_dt)
        except ValueError:
            pass

    if has_refund_receipt is not None:
        query = query.filter(models.PrepaymentRecord.has_refund_receipt == has_refund_receipt)

    if search:
        like = f"%{search}%"
        query = query.filter(
            or_(
                cast(models.PrepaymentRecord.id, SAString).ilike(like),
                cast(models.PrepaymentRecord.payment_id, SAString).ilike(like),
                cast(models.PrepaymentRecord.house_id, SAString).ilike(like),
                models.PrepaymentRecord.house_name.ilike(like),
                models.PrepaymentRecord.community_name.ilike(like),
                models.PrepaymentRecord.operator_name.ilike(like),
                models.PrepaymentRecord.category_name.ilike(like),
                models.PrepaymentRecord.remark.ilike(like),
                models.PrepaymentRecord.pay_channel_str.ilike(like),
                resident_subq.c.resident_name.ilike(like),
            )
        )

    total = query.count()
    total_amount = query.with_entities(sa_func.sum(models.PrepaymentRecord.amount)).scalar()
    rows = (
        query.order_by(models.PrepaymentRecord.operate_time.desc().nullslast(), models.PrepaymentRecord.id.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    items = []
    for row, resident_name in rows:
        item = _serialize_prepayment_record(row)
        item["resident_name"] = resident_name or ""
        items.append(item)

    return {
        "total": total,
        "total_amount": float(total_amount) if total_amount else 0.00,
        "items": items,
    }


@app.post("/api/prepayment-records/sync")
def sync_prepayment_records_endpoint(
    background_tasks: BackgroundTasks,
    request: Optional[schemas.PrepaymentRecordSyncRequest] = None,
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    if request and request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids

    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    str_ids = [str(cid) for cid in community_ids]
    task_id = tracker.create_task(str_ids)
    background_tasks.add_task(sync_prepayment_records, community_ids, task_id)

    return {
        "message": "Prepayment record synchronization started",
        "task_id": task_id,
        "community_ids": str_ids,
    }


@app.get("/api/prepayment-records/sync/status/{task_id}")
def get_prepayment_record_sync_status(task_id: str):
    status = tracker.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")
    return status


@app.get("/api/deposit-records")
def get_deposit_records(
    search: Optional[str] = None,
    community_ids: Optional[str] = None,
    operate_type: Optional[int] = None,
    operate_date_start: Optional[str] = None,
    operate_date_end: Optional[str] = None,
    pay_date_start: Optional[str] = None,
    pay_date_end: Optional[str] = None,
    has_refund_receipt: Optional[bool] = None,
    skip: int = 0,
    limit: int = 25,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    from sqlalchemy import func as sa_func, cast, String as SAString

    if not allowed_community_ids:
        return {"total": 0, "total_amount": 0.00, "items": []}

    resident_subq = _build_house_resident_name_subquery(db)

    query = (
        db.query(models.DepositRecord, resident_subq.c.resident_name)
        .outerjoin(
            resident_subq,
            and_(
                cast(models.DepositRecord.house_id, SAString) == resident_subq.c.house_id,
                cast(models.DepositRecord.community_id, SAString) == resident_subq.c.community_id,
            ),
        )
        .filter(models.DepositRecord.community_id.in_(allowed_community_ids))
    )

    if community_ids:
        try:
            ids = [int(cid.strip()) for cid in community_ids.split(",") if cid.strip()]
            if ids:
                query = query.filter(models.DepositRecord.community_id.in_(ids))
        except ValueError:
            pass

    if operate_type is not None:
        query = query.filter(models.DepositRecord.operate_type == operate_type)

    if operate_date_start:
        try:
            start_dt = datetime.strptime(operate_date_start, "%Y-%m-%d").date()
            query = query.filter(models.DepositRecord.operate_date >= start_dt)
        except ValueError:
            pass

    if operate_date_end:
        try:
            end_dt = datetime.strptime(operate_date_end, "%Y-%m-%d").date()
            query = query.filter(models.DepositRecord.operate_date <= end_dt)
        except ValueError:
            pass

    if pay_date_start:
        try:
            start_dt = datetime.strptime(pay_date_start, "%Y-%m-%d").date()
            query = query.filter(models.DepositRecord.pay_date >= start_dt)
        except ValueError:
            pass

    if pay_date_end:
        try:
            end_dt = datetime.strptime(pay_date_end, "%Y-%m-%d").date()
            query = query.filter(models.DepositRecord.pay_date <= end_dt)
        except ValueError:
            pass

    if has_refund_receipt is not None:
        query = query.filter(models.DepositRecord.has_refund_receipt == has_refund_receipt)

    if search:
        like = f"%{search}%"
        query = query.filter(
            or_(
                cast(models.DepositRecord.id, SAString).ilike(like),
                cast(models.DepositRecord.payment_id, SAString).ilike(like),
                cast(models.DepositRecord.house_id, SAString).ilike(like),
                models.DepositRecord.house_name.ilike(like),
                models.DepositRecord.community_name.ilike(like),
                models.DepositRecord.operator_name.ilike(like),
                models.DepositRecord.cash_pledge_name.ilike(like),
                models.DepositRecord.remark.ilike(like),
                models.DepositRecord.pay_channel_str.ilike(like),
                resident_subq.c.resident_name.ilike(like),
            )
        )

    total = query.count()
    total_amount = query.with_entities(sa_func.sum(models.DepositRecord.amount)).scalar()
    rows = (
        query.order_by(models.DepositRecord.operate_time.desc().nullslast(), models.DepositRecord.id.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    items = []
    for row, resident_name in rows:
        item = _serialize_deposit_record(row)
        item["resident_name"] = resident_name or ""
        items.append(item)

    return {
        "total": total,
        "total_amount": float(total_amount) if total_amount else 0.00,
        "items": items,
    }


@app.post("/api/deposit-records/sync")
def sync_deposit_records_endpoint(
    background_tasks: BackgroundTasks,
    request: Optional[schemas.DepositRecordSyncRequest] = None,
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    if request and request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids

    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    str_ids = [str(cid) for cid in community_ids]
    task_id = tracker.create_task(str_ids)
    background_tasks.add_task(sync_deposit_records, community_ids, task_id)

    return {
        "message": "Deposit record synchronization started",
        "task_id": task_id,
        "community_ids": str_ids,
    }


@app.get("/api/deposit-records/sync/status/{task_id}")
def get_deposit_record_sync_status(task_id: str):
    status = tracker.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")
    return status


@app.get("/api/receipt-bills")
def get_receipt_bills(
    search: Optional[str] = None,
    community_ids: Optional[str] = None,
    deal_date_start: Optional[str] = None,
    deal_date_end: Optional[str] = None,
    deal_type: Optional[int] = None,
    pay_channel_str: Optional[str] = None,
    payee: Optional[str] = None,
    skip: int = 0,
    limit: int = 25,
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    from sqlalchemy import func as sa_func, cast, String as SAString

    payer_subq = (
        db.query(
            models.ReceiptBillUser.receipt_bill_id,
            models.ReceiptBillUser.community_id,
            sa_func.string_agg(models.ReceiptBillUser.user_name, ", ").label("payer_name"),
        )
        .group_by(models.ReceiptBillUser.receipt_bill_id, models.ReceiptBillUser.community_id)
        .subquery()
    )

    query = (
        db.query(
            models.ReceiptBill,
            models.ProjectList.proj_name,
            payer_subq.c.payer_name,
        )
        .outerjoin(models.ProjectList, models.ReceiptBill.community_id == models.ProjectList.proj_id)
        .outerjoin(
            payer_subq,
            (models.ReceiptBill.id == payer_subq.c.receipt_bill_id)
            & (models.ReceiptBill.community_id == payer_subq.c.community_id),
        )
    )

    if allowed_community_ids:
        query = query.filter(models.ReceiptBill.community_id.in_(allowed_community_ids))
    else:
        return {"total": 0, "total_income_amount": 0.00, "items": []}

    if community_ids:
        try:
            ids = [int(cid.strip()) for cid in community_ids.split(",") if cid.strip()]
            if ids:
                query = query.filter(models.ReceiptBill.community_id.in_(ids))
        except ValueError:
            pass

    if deal_date_start:
        try:
            start_dt = datetime.strptime(deal_date_start, "%Y-%m-%d").date()
            query = query.filter(models.ReceiptBill.deal_date >= start_dt)
        except ValueError:
            pass

    if deal_date_end:
        try:
            end_dt = datetime.strptime(deal_date_end, "%Y-%m-%d").date()
            query = query.filter(models.ReceiptBill.deal_date <= end_dt)
        except ValueError:
            pass

    if deal_type is not None:
        query = query.filter(models.ReceiptBill.deal_type == int(deal_type))

    if pay_channel_str:
        query = query.filter(models.ReceiptBill.pay_channel_str.ilike(f"%{pay_channel_str}%"))

    if payee:
        query = query.filter(models.ReceiptBill.payee.ilike(f"%{payee}%"))

    if search:
        like = f"%{search}%"
        query = query.filter(
            or_(
                cast(models.ReceiptBill.id, SAString).ilike(like),
                models.ReceiptBill.receipt_id.ilike(like),
                models.ReceiptBill.asset_name.ilike(like),
                models.ReceiptBill.payee.ilike(like),
                payer_subq.c.payer_name.ilike(like),
                models.ProjectList.proj_name.ilike(like),
            )
        )

    total = query.count()
    total_income = query.with_entities(sa_func.sum(models.ReceiptBill.income_amount)).scalar()

    results = (
        query.order_by(models.ReceiptBill.deal_time.desc().nullslast(), models.ReceiptBill.id.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    receipt_refs = [
        {"receipt_bill_id": int(rb.id), "community_id": int(rb.community_id)}
        for rb, _, _ in results
    ]
    related_bill_map = _get_related_bill_refs_for_receipts(db, receipt_refs)

    flat_bill_refs: List[Dict[str, int]] = []
    seen_bill_keys = set()
    for refs in related_bill_map.values():
        for ref in refs:
            key = (ref["bill_id"], ref["community_id"])
            if key in seen_bill_keys:
                continue
            seen_bill_keys.add(key)
            flat_bill_refs.append(ref)

    bill_status_map = _get_bill_push_status_map(
        db,
        flat_bill_refs,
        account_book_number=_decode_header_value(x_account_book_number) or None,
    )

    items = []
    for rb, proj_name, payer_name in results:
        receipt_key = (int(rb.id), int(rb.community_id))
        related_refs = related_bill_map.get(receipt_key, [])
        related_statuses = [
            bill_status_map.get(
                (ref["bill_id"], ref["community_id"]),
                _build_bill_push_status_entry(ref["bill_id"], ref["community_id"]),
            )
            for ref in related_refs
        ]
        receipt_push_status = _aggregate_receipt_bill_push_status(related_statuses)
        drilldown_meta = _build_receipt_drilldown_meta(db, rb)

        items.append({
            "id": rb.id,
            "community_id": rb.community_id,
            "community_name": proj_name or f"园区 {rb.community_id}",
            "receipt_id": rb.receipt_id,
            "asset_name": rb.asset_name,
            "payee": rb.payee,
            "payer_name": payer_name or "",
            "income_amount": float(rb.income_amount) if rb.income_amount else 0,
            "amount": float(rb.amount) if rb.amount else 0,
            "bill_amount": float(rb.bill_amount) if rb.bill_amount else 0,
            "discount_amount": float(rb.discount_amount) if rb.discount_amount else 0,
            "late_money_amount": float(rb.late_money_amount) if rb.late_money_amount else 0,
            "deposit_amount": float(rb.deposit_amount) if rb.deposit_amount else 0,
            "pay_channel_str": rb.pay_channel_str,
            "deal_time": rb.deal_time,
            "deal_date": rb.deal_date,
            "deal_type": rb.deal_type,
            "deal_type_label": RECEIPT_BILL_DEAL_TYPE_LABELS.get(rb.deal_type, "其他"),
            **drilldown_meta,
            **receipt_push_status,
        })

    return {
        "total": total,
        "total_income_amount": float(total_income) if total_income else 0.00,
        "items": items,
    }


@app.post("/api/receipt-bills/sync")
def sync_receipt_bills_endpoint(
    background_tasks: BackgroundTasks,
    request: schemas.ReceiptBillSyncRequest = None,
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    if request and request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids

    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    str_ids = [str(cid) for cid in community_ids]
    task_id = tracker.create_task(str_ids)
    background_tasks.add_task(sync_receipt_bills, str_ids, task_id)

    return {
        "message": "Receipt bill synchronization started",
        "task_id": task_id,
        "community_ids": str_ids,
    }


@app.get("/api/receipt-bills/sync/status/{task_id}")
def get_receipt_bill_sync_status(task_id: str):
    status = tracker.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")
    return status


@app.get("/api/receipt-bills/{receipt_bill_id}")
def get_receipt_bill(
    receipt_bill_id: int,
    community_id: int = Query(..., description="Marki community ID"),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    if allowed_community_ids and int(community_id) not in set(allowed_community_ids):
        raise HTTPException(status_code=403, detail="Unauthorized community")

    rb = (
        db.query(models.ReceiptBill)
        .options(selectinload(models.ReceiptBill.users))
        .filter(models.ReceiptBill.id == int(receipt_bill_id), models.ReceiptBill.community_id == int(community_id))
        .first()
    )
    if not rb:
        raise HTTPException(status_code=404, detail="Receipt bill not found")

    related_bills = _load_receipt_to_bills_relation(db, rb)
    related_deposit_collect = _load_receipt_to_deposit_collect_relation(db, rb)
    deposit_refund_links = _load_receipt_deposit_refund_links(db, rb)
    deposit_refund_link_summary = _build_receipt_deposit_refund_link_summary(deposit_refund_links)
    related_deposit_refund = _load_receipt_to_deposit_refund_relation(db, rb)
    related_prepayment_recharge = _load_receipt_to_prepayment_recharge_relation(db, rb)
    related_prepayment_refund = _load_receipt_to_prepayment_refund_relation(db, rb)
    drilldown_sections = _build_receipt_drilldown_sections(
        rb,
        related_bills,
        related_deposit_collect,
        related_deposit_refund,
        related_prepayment_recharge,
        related_prepayment_refund,
    )
    drilldown_meta = _build_receipt_drilldown_meta(db, rb)

    return {
        "id": rb.id,
        "community_id": rb.community_id,
        "receipt_id": rb.receipt_id,
        "deal_type": rb.deal_type,
        "deal_type_label": RECEIPT_BILL_DEAL_TYPE_LABELS.get(rb.deal_type, "其他"),
        "asset_type": rb.asset_type,
        "asset_name": rb.asset_name,
        "asset_id": rb.asset_id,
        "income_amount": float(rb.income_amount) if rb.income_amount else 0,
        "amount": float(rb.amount) if rb.amount else 0,
        "discount_amount": float(rb.discount_amount) if rb.discount_amount else 0,
        "late_money_amount": float(rb.late_money_amount) if rb.late_money_amount else 0,
        "bill_amount": float(rb.bill_amount) if rb.bill_amount else 0,
        "deposit_amount": float(rb.deposit_amount) if rb.deposit_amount else 0,
        "pay_channel": rb.pay_channel,
        "pay_channel_list": rb.pay_channel_list,
        "pay_channel_str": rb.pay_channel_str,
        "deal_time": rb.deal_time,
        "deal_date": rb.deal_date,
        "remark": rb.remark,
        "fk_id": rb.fk_id,
        "receipt_record_id": rb.receipt_record_id,
        "receipt_version": rb.receipt_version,
        "invoice_number": rb.invoice_number,
        "invoice_urls": rb.invoice_urls,
        "invoice_status": rb.invoice_status,
        "open_invoice": rb.open_invoice,
        "payee": rb.payee,
        "bind_users_raw": rb.bind_users_raw,
        "users": [
            {
                "user_id": u.user_id,
                "user_name": u.user_name,
                "phone": u.phone,
            }
            for u in (rb.users or [])
        ],
        "deposit_refund_links": [
            _serialize_receipt_deposit_refund_link_model(link)
            for link in deposit_refund_links
        ],
        "deposit_refund_link_summary": deposit_refund_link_summary,
        "drilldown_enabled": drilldown_meta.get("drilldown_enabled", False),
        "drilldown_source": drilldown_meta.get("drilldown_source"),
        "drilldown_count": drilldown_meta.get("drilldown_count", 0),
        "drilldown_summary": drilldown_meta.get("drilldown_summary"),
        "drilldown_sections": drilldown_sections,
        "supports_bill_push_ops": drilldown_meta.get("supports_bill_push_ops", False),
        "related_bills": related_bills,
        "related_deposit_collect": related_deposit_collect,
        "related_deposit_refund": related_deposit_refund,
        "related_prepayment_recharge": related_prepayment_recharge,
        "related_prepayment_refund": related_prepayment_refund,
    }

# New endpoint: POST /api/projects/sync
@app.post("/api/projects/sync")
def sync_projects_endpoint(
    background_tasks: BackgroundTasks, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """Trigger project synchronization in background.
    Only administrators can trigger this.
    """
    _require_api_permission(db, current_user, "project.manage")
        
    def run_sync():
        try:
            logger.info("Project sync started")
            fetch_projects_main()
            logger.info("Project sync completed successfully")
        except Exception as e:
            logger.error(f"Project sync failed: {e}")
            raise
    background_tasks.add_task(run_sync)
    return {"detail": "Project synchronization started"}

# Reports
@app.get("/api/reports/income-trend")
def get_income_trend(
    period: str = "month", 
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    if not allowed_community_ids:
        return {"labels": [], "data": []}
    
    if period == "month":
        # Since pay_time is and Integer (unix timestamp), we need to convert it before extraction
        from sqlalchemy import cast, DateTime
        
        # PostgreSQL specific conversion: to_timestamp(pay_time)
        # Or generic SQL way if possible, but pay_time is int.
        
        data = db.query(
            func.extract('month', func.to_timestamp(models.Bill.pay_time)).label('month'),
            func.sum(models.Bill.amount).label('total')
        ).filter(
            models.Bill.pay_status_str == '已缴',
            models.Bill.pay_time != None,
            models.Bill.community_id.in_(allowed_community_ids)
        ).group_by(
            func.extract('month', func.to_timestamp(models.Bill.pay_time))
        ).all()
        
        months = {i: 0 for i in range(1, 13)}
        for row in data:
            months[int(row.month)] = float(row.total) if row.total else 0
        
        return {"labels": list(range(1, 13)), "data": list(months.values())}

    
    return {"labels": [], "data": []}

@app.get("/api/reports/charge-items-ranking")
def get_charge_items_ranking(
    limit: int = 10, 
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    if not allowed_community_ids:
        return []

    data = db.query(
        models.Bill.charge_item_name,
        func.sum(models.Bill.amount).label('total'),
        func.count(models.Bill.id).label('count')
    ).filter(
        models.Bill.charge_item_name != None,
        models.Bill.amount != None,
        models.Bill.community_id.in_(allowed_community_ids)
    ).group_by(
        models.Bill.charge_item_name
    ).order_by(
        func.sum(models.Bill.bill_amount).desc()
    ).limit(limit).all()
    
    total = sum(float(row.total) if row.total else 0 for row in data)
    
    return [{
        "item_name": row.charge_item_name,
        "amount": float(row.total) if row.total else 0,
        "count": row.count,
        "percentage": (float(row.total) / total * 100) if total > 0 else 0
    } for row in data]


# ===================== Organization Management =====================

def build_org_tree(orgs: List[models.Organization], parent_id=None):
    """Build organization tree structure recursively"""
    tree = []
    for org in orgs:
        if org.parent_id == parent_id:
            node = {
                "id": org.id,
                "name": org.name,
                "code": org.code,
                "parent_id": org.parent_id,
                "level": org.level,
                "sort_order": org.sort_order,
                "status": org.status,
                "description": org.description,
                "created_at": org.created_at,
                "updated_at": org.updated_at,
                "children": build_org_tree(orgs, org.id)
            }
            tree.append(node)
    return tree


# ===================== Voucher Template Category Management =====================

def build_template_category_tree(categories: List[models.VoucherTemplateCategory], parent_id=None, parent_path: str = ""):
    tree = []
    for cat in categories:
        if cat.parent_id == parent_id:
            path = f"{parent_path} / {cat.name}" if parent_path else cat.name
            node = {
                "id": cat.id,
                "name": cat.name,
                "parent_id": cat.parent_id,
                "sort_order": cat.sort_order,
                "status": cat.status,
                "description": cat.description,
                "path": path,
                "created_at": cat.created_at,
                "updated_at": cat.updated_at,
                "children": build_template_category_tree(categories, cat.id, path),
            }
            tree.append(node)
    return tree


def build_template_category_path_map(categories: List[models.VoucherTemplateCategory]) -> Dict[int, str]:
    by_id = {c.id: c for c in categories}
    cache: Dict[int, str] = {}

    def resolve(cat_id: int) -> Optional[str]:
        if cat_id in cache:
            return cache[cat_id]
        cat = by_id.get(cat_id)
        if not cat:
            return None
        if cat.parent_id and cat.parent_id in by_id:
            parent_path = resolve(cat.parent_id)
            path = f"{parent_path} / {cat.name}" if parent_path else cat.name
        else:
            path = cat.name
        cache[cat_id] = path
        return path

    for cid in by_id.keys():
        resolve(cid)
    return cache


@app.get("/api/organizations")
def get_organizations(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get all organizations as flat list"""
    _require_api_permission(db, current_user, "organization.manage")
    orgs = db.query(models.Organization).order_by(models.Organization.sort_order).offset(skip).limit(limit).all()
    return [{
        "id": org.id,
        "name": org.name,
        "code": org.code,
        "parent_id": org.parent_id,
        "level": org.level,
        "sort_order": org.sort_order,
        "status": org.status,
        "description": org.description,
        "created_at": org.created_at,
        "updated_at": org.updated_at
    } for org in orgs]


@app.get("/api/organizations/tree")
def get_organizations_tree(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get organizations as tree structure"""
    _require_api_permission(db, current_user, "organization.manage")
    orgs = db.query(models.Organization).order_by(models.Organization.sort_order).all()
    return build_org_tree(orgs, None)


@app.get("/api/organizations/{org_id}")
def get_organization(
    org_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "organization.manage")
    org = db.query(models.Organization).filter(models.Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return {
        "id": org.id,
        "name": org.name,
        "code": org.code,
        "parent_id": org.parent_id,
        "level": org.level,
        "sort_order": org.sort_order,
        "status": org.status,
        "description": org.description,
        "created_at": org.created_at,
        "updated_at": org.updated_at
    }


@app.post("/api/organizations")
def create_organization(
    org_data: schemas.OrganizationCreate, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    _require_api_permission(db, current_user, "organization.manage")
        
    # Check if code already exists
    if org_data.code:
        existing = db.query(models.Organization).filter(models.Organization.code == org_data.code).first()
        if existing:
            raise HTTPException(status_code=400, detail="Organization code already exists")
    
    org = models.Organization(
        name=org_data.name,
        code=org_data.code,
        parent_id=org_data.parent_id,
        level=org_data.level,
        sort_order=org_data.sort_order,
        status=org_data.status,
        description=org_data.description
    )
    db.add(org)
    db.commit()
    db.refresh(org)
    return {"id": org.id, "message": "Organization created successfully"}


@app.put("/api/organizations/{org_id}")
def update_organization(
    org_id: int, 
    org_data: schemas.OrganizationUpdate, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    _require_api_permission(db, current_user, "organization.manage")
        
    org = db.query(models.Organization).filter(models.Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    
    update_data = org_data.dict(exclude_unset=True)
    
    if "code" in update_data and update_data["code"] is not None:
        # Check uniqueness if code is being updated to a non-null value
        existing = db.query(models.Organization).filter(
            models.Organization.code == update_data["code"],
            models.Organization.id != org_id
        ).first()
        if existing:
            raise HTTPException(status_code=400, detail="Organization code already exists")

    for key, value in update_data.items():
        setattr(org, key, value)
    
    db.commit()
    return {"message": "Organization updated successfully"}


@app.delete("/api/organizations/{org_id}")
def delete_organization(
    org_id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    _require_api_permission(db, current_user, "organization.manage")
        
    org = db.query(models.Organization).filter(models.Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    
    # Check if has children
    children = db.query(models.Organization).filter(models.Organization.parent_id == org_id).first()
    if children:
        raise HTTPException(status_code=400, detail="Cannot delete organization with children")
    
    # Check if has users
    users = db.query(models.User).filter(models.User.org_id == org_id).first()
    if users:
        raise HTTPException(status_code=400, detail="Cannot delete organization with users")
    
    db.delete(org)
    db.commit()
    return {"message": "Organization deleted successfully"}


# ===================== Voucher Template Categories =====================

@app.get("/api/vouchers/template-categories")
def get_voucher_template_categories(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")
    categories = db.query(models.VoucherTemplateCategory).order_by(
        models.VoucherTemplateCategory.sort_order.asc(),
        models.VoucherTemplateCategory.id.asc()
    ).all()
    path_map = build_template_category_path_map(categories)
    return [{
        "id": c.id,
        "name": c.name,
        "parent_id": c.parent_id,
        "sort_order": c.sort_order,
        "status": c.status,
        "description": c.description,
        "path": path_map.get(c.id),
        "created_at": c.created_at,
        "updated_at": c.updated_at
    } for c in categories]


@app.get("/api/vouchers/template-categories/tree")
def get_voucher_template_categories_tree(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")
    categories = db.query(models.VoucherTemplateCategory).order_by(
        models.VoucherTemplateCategory.sort_order.asc(),
        models.VoucherTemplateCategory.id.asc()
    ).all()
    return build_template_category_tree(categories, None, "")


@app.post("/api/vouchers/template-categories")
def create_voucher_template_category(
    payload: schemas.VoucherTemplateCategoryCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    _require_api_permission(db, current_user, "voucher_template.manage")

    if payload.parent_id is not None:
        parent = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == payload.parent_id).first()
        if not parent:
            raise HTTPException(status_code=400, detail="Parent category not found")

    category = models.VoucherTemplateCategory(
        name=payload.name,
        parent_id=payload.parent_id,
        sort_order=payload.sort_order,
        status=payload.status,
        description=payload.description
    )
    db.add(category)
    db.commit()
    db.refresh(category)
    return {"id": category.id, "message": "Template category created successfully"}


@app.put("/api/vouchers/template-categories/{category_id}")
def update_voucher_template_category(
    category_id: int,
    payload: schemas.VoucherTemplateCategoryUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    _require_api_permission(db, current_user, "voucher_template.manage")

    category = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == category_id).first()
    if not category:
        raise HTTPException(status_code=404, detail="Template category not found")

    update_data = payload.dict(exclude_unset=True)
    if "parent_id" in update_data:
        next_parent_id = update_data["parent_id"]
        if next_parent_id == category_id:
            raise HTTPException(status_code=400, detail="Category cannot be its own parent")
        if next_parent_id is not None:
            parent = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == next_parent_id).first()
            if not parent:
                raise HTTPException(status_code=400, detail="Parent category not found")

            # prevent cycles
            cursor = parent
            while cursor and cursor.parent_id is not None:
                if cursor.parent_id == category_id:
                    raise HTTPException(status_code=400, detail="Invalid parent category (cycle detected)")
                cursor = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == cursor.parent_id).first()

    for key, value in update_data.items():
        setattr(category, key, value)

    db.commit()
    return {"message": "Template category updated successfully"}


@app.delete("/api/vouchers/template-categories/{category_id}")
def delete_voucher_template_category(
    category_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    _require_api_permission(db, current_user, "voucher_template.manage")

    category = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == category_id).first()
    if not category:
        raise HTTPException(status_code=404, detail="Template category not found")

    has_children = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.parent_id == category_id).first()
    if has_children:
        raise HTTPException(status_code=400, detail="Cannot delete category with children")

    bound_template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.category_id == category_id).first()
    if bound_template:
        raise HTTPException(status_code=400, detail="Cannot delete category with existing voucher templates")

    db.delete(category)
    db.commit()
    return {"message": "Template category deleted successfully"}


# ===================== User Management =====================

def hash_password(password: str) -> str:
    """Simple password hashing using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

from pydantic import BaseModel

class LoginRequest(BaseModel):
    username: str
    password: str

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)




@app.post("/api/auth/login")
def login(login_req: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.username == login_req.username).first()
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
        
    hashed_pwd = hash_password(login_req.password)
    if user.password_hash != hashed_pwd:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
        
    if user.status != 1:
        raise HTTPException(status_code=403, detail="Account is disabled")
        
    # Update last login
    user.last_login = datetime.now()
    db.commit()
    
    import utils.auth as auth_utils
    access_token = auth_utils.create_access_token({"sub": user.id})
    
    org_name = user.organization.name if user.organization else "未分配"
    menu_keys = _get_role_menu_keys(db, user.role)
    api_keys = _get_role_api_keys(db, user.role)
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "username": user.username,
            "real_name": user.real_name or user.username,
            "org_name": org_name,
            "avatar": user.avatar,
            "role": user.role,
            "menu_keys": menu_keys,
            "api_keys": api_keys,
        }
    }


@app.get("/api/users/me")
def get_me(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get current active user"""
    user = current_user
    org_name = user.organization.name if user.organization else "未分配"
    
    # get user authorized account books
    account_books = [{"id": b.id, "name": b.name, "number": b.number} for b in user.account_books]
    menu_keys = _get_role_menu_keys(db, user.role)
    api_keys = _get_role_api_keys(db, user.role)
    
    return {
        "id": user.id,
        "username": user.username,
        "real_name": user.real_name or user.username,
        "org_id": user.org_id,
        "org_name": org_name,
        "avatar": user.avatar,
        "role": user.role,
        "account_books": account_books,
        "menu_keys": menu_keys,
        "api_keys": api_keys,
    }


@app.get(
    "/api/users/me/table-column-preferences/{table_id}",
    response_model=schemas.UserTableColumnPreferenceResponse,
)
def get_my_table_column_preference(
    table_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    normalized_table_id = table_id.strip()
    if not normalized_table_id:
        raise HTTPException(status_code=400, detail="table_id is required")

    preference = (
        db.query(models.UserTableColumnPreference)
        .filter(
            models.UserTableColumnPreference.user_id == current_user.id,
            models.UserTableColumnPreference.table_id == normalized_table_id,
        )
        .first()
    )

    if not preference:
        return schemas.UserTableColumnPreferenceResponse(
            table_id=normalized_table_id,
            hidden=[],
            order=[],
            updated_at=None,
        )

    return schemas.UserTableColumnPreferenceResponse(
        table_id=normalized_table_id,
        hidden=_deserialize_column_preference(preference.hidden_columns),
        order=_deserialize_column_preference(preference.column_order),
        updated_at=preference.updated_at,
    )


@app.put(
    "/api/users/me/table-column-preferences/{table_id}",
    response_model=schemas.UserTableColumnPreferenceResponse,
)
def save_my_table_column_preference(
    table_id: str,
    payload: schemas.UserTableColumnPreferenceUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    normalized_table_id = table_id.strip()
    if not normalized_table_id:
        raise HTTPException(status_code=400, detail="table_id is required")

    hidden = _normalize_column_preference_items(payload.hidden)
    order = _normalize_column_preference_items(payload.order)

    preference = (
        db.query(models.UserTableColumnPreference)
        .filter(
            models.UserTableColumnPreference.user_id == current_user.id,
            models.UserTableColumnPreference.table_id == normalized_table_id,
        )
        .first()
    )

    if not preference:
        preference = models.UserTableColumnPreference(
            user_id=current_user.id,
            table_id=normalized_table_id,
        )
        db.add(preference)

    preference.hidden_columns = _serialize_column_preference(hidden)
    preference.column_order = _serialize_column_preference(order)
    db.commit()
    db.refresh(preference)

    return schemas.UserTableColumnPreferenceResponse(
        table_id=normalized_table_id,
        hidden=hidden,
        order=order,
        updated_at=preference.updated_at,
    )


@app.get("/api/users")
def get_users(
    skip: int = 0,
    limit: int = 100,
    org_id: int = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get all users with optional org filter"""
    _require_api_permission(db, current_user, "user.manage")
    query = db.query(models.User)
    if org_id:
        query = query.filter(models.User.org_id == org_id)
    users = query.order_by(models.User.created_at.desc()).offset(skip).limit(limit).all()
    
    result = []
    for user in users:
        org_name = None
        if user.organization:
            org_name = user.organization.name
        result.append({
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "phone": user.phone,
            "real_name": user.real_name,
            "org_id": user.org_id,
            "org_name": org_name,
            "status": user.status,
            "role": user.role,
            "avatar": user.avatar,
            "last_login": user.last_login,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
            "account_book_ids": [ab.id for ab in user.account_books] if user.account_books else []
        })
    return result


@app.get("/api/users/{user_id}")
def get_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "user.manage")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    org_name = None
    if user.organization:
        org_name = user.organization.name
    
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "phone": user.phone,
        "real_name": user.real_name,
        "org_id": user.org_id,
        "org_name": org_name,
        "status": user.status,
        "role": user.role,
        "avatar": user.avatar,
        "last_login": user.last_login,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
        "account_book_ids": [ab.id for ab in user.account_books] if user.account_books else []
    }


def _normalize_optional_user_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _normalize_user_role(value: Optional[str]) -> str:
    normalized_role = str(value or "user").strip() or "user"
    if normalized_role not in MENU_PERMISSION_ROLE_MAP:
        raise HTTPException(status_code=400, detail="Invalid user role")
    return normalized_role


def _normalize_user_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)

    if "username" in normalized and normalized["username"] is not None:
        normalized["username"] = str(normalized["username"]).strip()

    for field in ("email", "phone", "real_name"):
        if field in normalized:
            normalized[field] = _normalize_optional_user_text(normalized[field])

    if "role" in normalized and normalized["role"] is not None:
        normalized["role"] = _normalize_user_role(normalized["role"])

    if "org_id" in normalized and normalized["org_id"] in ("", "0", 0):
        normalized["org_id"] = None

    return normalized


def _build_user_integrity_error(exc: IntegrityError) -> HTTPException:
    error_text = str(getattr(exc, "orig", exc))
    lowered = error_text.lower()

    if "users_username_key" in lowered or "(username)" in lowered:
        return HTTPException(status_code=400, detail="Username already exists")

    if "users_email_key" in lowered or "(email)" in lowered:
        return HTTPException(status_code=400, detail="Email already exists")

    return HTTPException(status_code=400, detail="User save failed")


@app.post("/api/users")
def create_user(
    user_data: schemas.UserCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "user.manage")
    create_data = _normalize_user_payload(user_data.dict())

    if not create_data["username"]:
        raise HTTPException(status_code=400, detail="Username is required")

    # Check if username exists
    existing = db.query(models.User).filter(models.User.username == create_data["username"]).first()
    if existing:
        raise HTTPException(status_code=400, detail="Username already exists")

    # Check if email exists
    if create_data["email"]:
        existing_email = db.query(models.User).filter(models.User.email == create_data["email"]).first()
        if existing_email:
            raise HTTPException(status_code=400, detail="Email already exists")

    account_book_ids = create_data.pop("account_book_ids", None)
    user = models.User(
        username=create_data["username"],
        email=create_data["email"],
        phone=create_data["phone"],
        real_name=create_data["real_name"],
        password_hash=hash_password(user_data.password),
        org_id=create_data["org_id"],
        status=create_data["status"],
        role=create_data["role"],
    )
    db.add(user)

    if account_book_ids is not None:
        user.account_books = db.query(models.KingdeeAccountBook).filter(
            models.KingdeeAccountBook.id.in_(account_book_ids)
        ).all()

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise _build_user_integrity_error(exc) from exc

    db.refresh(user)
    return {"id": user.id, "message": "User created successfully"}


@app.put("/api/users/{user_id}")
def update_user(
    user_id: int,
    user_data: schemas.UserUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "user.manage")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    update_data = _normalize_user_payload(user_data.dict(exclude_unset=True))

    if "username" in update_data:
        if not update_data["username"]:
            raise HTTPException(status_code=400, detail="Username is required")
        existing = db.query(models.User).filter(
            models.User.username == update_data["username"],
            models.User.id != user_id
        ).first()
        if existing:
            raise HTTPException(status_code=400, detail="Username already exists")
    
    if "email" in update_data and update_data["email"]:
        existing = db.query(models.User).filter(
            models.User.email == update_data["email"],
            models.User.id != user_id
        ).first()
        if existing:
            raise HTTPException(status_code=400, detail="Email already exists")

    if "role" in update_data and update_data["role"] is not None:
        update_data["role"] = str(update_data["role"]).strip() or "user"

    if "password" in update_data and update_data["password"]:
         update_data["password_hash"] = hash_password(update_data.pop("password"))
    
    if "account_book_ids" in update_data:
        account_book_ids = update_data.pop("account_book_ids")
        if account_book_ids is not None:
            user.account_books = db.query(models.KingdeeAccountBook).filter(
                models.KingdeeAccountBook.id.in_(account_book_ids)
            ).all()
        else:
            user.account_books = []

    for key, value in update_data.items():
        setattr(user, key, value)

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise _build_user_integrity_error(exc) from exc

    return {"message": "User updated successfully"}


@app.delete("/api/users/{user_id}")
def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "user.manage")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.id == current_user.id:
        raise HTTPException(status_code=400, detail="Current user cannot be deleted")
    
    db.delete(user)
    db.commit()
    return {"message": "User deleted successfully"}


@app.get(
    "/api/menu-permissions",
    response_model=schemas.MenuPermissionOverviewResponse,
)
def get_menu_permissions(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_admin),
):
    roles = [_build_menu_permission_role_state(db, item["role"]) for item in MENU_PERMISSION_ROLE_DEFINITIONS]
    return {
        "menus": MENU_PERMISSION_DEFINITIONS,
        "apis": API_PERMISSION_DEFINITIONS,
        "roles": roles,
    }


@app.put(
    "/api/menu-permissions/{role}",
    response_model=schemas.MenuPermissionRoleState,
)
def update_menu_permissions(
    role: str,
    payload: schemas.MenuPermissionRoleUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_admin),
):
    normalized_role = str(role or "").strip()
    if not normalized_role:
        raise HTTPException(status_code=400, detail="Role is required")

    role_meta = MENU_PERMISSION_ROLE_MAP.get(normalized_role)
    if not role_meta:
        raise HTTPException(status_code=404, detail="Role not found")
    if not role_meta.get("editable", True):
        raise HTTPException(status_code=400, detail="This role uses fixed permissions")

    allowed_menu_keys = _get_allowed_permission_keys(MENU_PERMISSION_DEFINITIONS, normalized_role, "key")
    allowed_api_keys = _get_allowed_permission_keys(API_PERMISSION_DEFINITIONS, normalized_role, "key")
    required_menu_keys = _get_required_menu_keys(normalized_role)
    submitted_menu_keys = {
        key
        for key in payload.menu_keys
        if isinstance(key, str) and key in allowed_menu_keys
    }
    submitted_api_keys = {
        key
        for key in payload.api_keys
        if isinstance(key, str) and key in allowed_api_keys
    }
    final_menu_keys = submitted_menu_keys | required_menu_keys

    db.query(models.RoleMenuPermission).filter(
        models.RoleMenuPermission.role == normalized_role
    ).delete(synchronize_session=False)

    for menu_key in final_menu_keys:
        db.add(models.RoleMenuPermission(role=normalized_role, menu_key=menu_key))

    db.query(models.RoleApiPermission).filter(
        models.RoleApiPermission.role == normalized_role
    ).delete(synchronize_session=False)

    for api_key in submitted_api_keys:
        db.add(models.RoleApiPermission(role=normalized_role, api_key=api_key))

    db.commit()
    return _build_menu_permission_role_state(db, normalized_role)


# ===================== External Service Management =====================

@app.get("/api/external/services", response_model=List[schemas.ExternalServiceWithApis])
def get_external_services(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get all external services"""
    can_manage_credentials = _has_api_permission(db, current_user, "credential.manage")
    can_manage_apis = _has_api_permission(db, current_user, "api_registry.manage")
    if not (can_manage_credentials or can_manage_apis):
        raise HTTPException(status_code=403, detail="Permission denied")

    from utils.crypto import decrypt_value
    services = db.query(models.ExternalService).all()
    for s in services:
        if can_manage_credentials and s.app_secret:
            s.app_secret = decrypt_value(s.app_secret)
        elif not can_manage_credentials:
            s.app_secret = None
    return services


@app.post("/api/external/services", response_model=schemas.ExternalServiceResponse)
def create_external_service(
    service: schemas.ExternalServiceCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Create a new external service credential config"""
    _require_api_permission(db, current_user, "credential.manage")
    existing = db.query(models.ExternalService).filter(models.ExternalService.service_name == service.service_name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Service name already exists")
    
    service_data = service.dict()
    if service_data.get('app_secret'):
        service_data['app_secret'] = encrypt_value(service_data['app_secret'])
        
    new_service = models.ExternalService(**service_data)
    db.add(new_service)
    db.commit()
    db.refresh(new_service)
    
    # Decrypt for response
    from utils.crypto import decrypt_value
    if new_service.app_secret:
        new_service.app_secret = decrypt_value(new_service.app_secret)
        
    return new_service



@app.post("/api/external/services/test-connection")
def test_external_service_connection(
    service: schemas.ExternalServiceCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Test connection for a service configuration without saving it"""
    _require_api_permission(db, current_user, "credential.manage")
    from services.external_auth import ExternalAuthService
    from utils.crypto import encrypt_value
    
    service_data = service.dict()
    if service_data.get('app_secret'):
        service_data['app_secret'] = encrypt_value(service_data['app_secret'])
    
    # Create a transient model instance
    temp_service = models.ExternalService(**service_data)

    
    try:
        auth = ExternalAuthService(db=db, service_record=temp_service)
        # 1. Test Authentication
        token = auth.get_token()
        
        # 2. Test Base URL Connectivity (Optional, just a HEAD request)
        # If no base_url, we consider auth success as partial success
        connectivity_status = "Skipped (No Base URL)"
        if temp_service.base_url:
            try:
                headers = auth.get_auth_headers()
                resp = requests.get(temp_service.base_url, headers=headers, timeout=10)
                connectivity_status = f"Success (HTTP {resp.status_code})"
            except Exception as e:
                connectivity_status = f"Failed: {str(e)}"
        
        return {
            "success": True,
            "message": "Authentication successful",
            "token_preview": token[:10] + "..." if token else "N/A",
            "connectivity": connectivity_status
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Connection failed: {str(e)}"
        }


@app.put("/api/external/services/{service_id}", response_model=schemas.ExternalServiceResponse)
def update_external_service(
    service_id: int,
    service: schemas.ExternalServiceUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Update external service config"""
    _require_api_permission(db, current_user, "credential.manage")
    db_service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
    if not db_service:
        raise HTTPException(status_code=404, detail="Service not found")
        
    update_data = service.dict(exclude_unset=True)
    for key, value in update_data.items():
        if key == 'app_secret' and value:
            # Only encrypt if it's not already the one in DB
            # Note: This is a simple check. In a real system, you might use a flag or 'modified' state
            if value != db_service.app_secret:
                value = encrypt_value(value)
        setattr(db_service, key, value)
        
    db.commit()
    db.refresh(db_service)
    
    # Decrypt for response
    from utils.crypto import decrypt_value
    if db_service.app_secret:
        db_service.app_secret = decrypt_value(db_service.app_secret)
        
    return db_service



@app.delete("/api/external/services/{service_id}")
def delete_external_service(
    service_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Delete external service"""
    _require_api_permission(db, current_user, "credential.manage")
    db_service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
    if not db_service:
        raise HTTPException(status_code=404, detail="Service not found")
    
    db.delete(db_service)
    db.commit()
    return {"message": "Service deleted"}

# ===================== External API Management =====================

@app.post("/api/external/services/{service_id}/apis", response_model=schemas.ExternalApiResponse)
def create_external_api(
    service_id: int,
    api: schemas.ExternalApiCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Add an API definition to a service"""
    _require_api_permission(db, current_user, "api_registry.manage")
    service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")
    
    # Check duplicate name for this service
    existing = db.query(models.ExternalApi).filter(
        models.ExternalApi.service_id == service_id, 
        models.ExternalApi.name == api.name
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="API name already exists for this service")

    new_api = models.ExternalApi(
        **api.dict()
    )
    new_api.service_id = service_id # Override with path param
    db.add(new_api)
    db.commit()
    db.refresh(new_api)
    return new_api

@app.put("/api/external/apis/{api_id}", response_model=schemas.ExternalApiResponse)
def update_external_api(
    api_id: int,
    api: schemas.ExternalApiUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Update an external API"""
    _require_api_permission(db, current_user, "api_registry.manage")
    db_api = db.query(models.ExternalApi).filter(models.ExternalApi.id == api_id).first()
    if not db_api:
        raise HTTPException(status_code=404, detail="API not found")
    
    update_data = api.dict(exclude_unset=True)
    
    # Check name uniqueness if name is changed
    if "name" in update_data and update_data["name"] != db_api.name:
         existing = db.query(models.ExternalApi).filter(
            models.ExternalApi.service_id == db_api.service_id, 
            models.ExternalApi.name == update_data["name"]
        ).first()
         if existing:
             raise HTTPException(status_code=400, detail="API name already exists for this service")

    for key, value in update_data.items():
        setattr(db_api, key, value)
    
    db.commit()
    db.refresh(db_api)
    return db_api

@app.delete("/api/external/apis/{api_id}")
def delete_external_api(
    api_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Delete an external API"""
    _require_api_permission(db, current_user, "api_registry.manage")
    api = db.query(models.ExternalApi).filter(models.ExternalApi.id == api_id).first()
    if not api:
        raise HTTPException(status_code=404, detail="API not found")
    
    db.delete(api)
    db.commit()
    return {"message": "API deleted"}

# ===================== Legacy / Specific Status (Adapted) =====================

@app.get("/api/external/kingdee/status")
def get_kingdee_status(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get Kingdee token status (Adapted to use ExternalService)"""
    _require_api_permission(db, current_user, "credential.manage")
    # Assuming 'kingdee_oauth' is the service_name used by the service
    service = db.query(models.ExternalService).filter(models.ExternalService.service_name == "kingdee_oauth").first()
    
    if not service or not service.access_token:
        return {
            "status": "not_connected",
            "message": "尚未获取凭证",
            "expires_at": None,
            "has_refresh_token": False
        }
    
    now = datetime.now()
    is_expired = service.expires_at and service.expires_at < now
    
    return {
        "status": "expired" if is_expired else "connected",
        "message": "Token expired" if is_expired else "Connected",
        "expires_at": service.expires_at,
        "has_refresh_token": bool(service.refresh_token),
        "last_updated": service.updated_at
    }

@app.post("/api/external/kingdee/refresh")
def refresh_kingdee_token(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "credential.manage")
    try:
        service = KingdeeAuthService(db)
        token = service._login_and_save() # This updates ExternalService table now
        return {"success": True, "message": "刷新成功"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/external/marki/status")
def get_marki_status(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "credential.manage")
    service = db.query(models.ExternalService).filter(models.ExternalService.service_name == "marki").first()
    if not service or not service.app_id:
        return {
            "status": "not_connected",
            "message": "未配置 Marki 系统账户",
            "expires_at": None,
            "has_refresh_token": False
        }
        
    has_cookie = bool(service.extra_info)
    return {
        "status": "connected" if has_cookie else "expired",
        "message": "已连接" if has_cookie else "凭证已过期",
        "expires_at": None,
        "has_refresh_token": False,
        "last_updated": service.updated_at
    }

@app.post("/api/external/marki/config")
def update_marki_config(
    req: schemas.MarkiConfigRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "credential.manage")
    service = db.query(models.ExternalService).filter(models.ExternalService.service_name == "marki").first()
    if not service:
        service = models.ExternalService(
            service_name="marki", 
            display_name="Marki 物业系统",
            auth_url="https://sttc-os-lgn.markiapp.com/lgn/login/authorize.do",
            base_url="https://charge-api.markiapp.com"
        )
        db.add(service)
    service.app_id = req.app_id
    # update secret only if provided and not the dummy value
    if req.app_secret and req.app_secret != "********":
        service.app_secret = req.app_secret
    
    db.commit()
    return {"success": True, "message": "配置已保存"}

@app.post("/api/external/marki/refresh")
def refresh_marki_token(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "credential.manage")
    from utils.marki_client import marki_client
    # clear current cookie to force relogin
    service = db.query(models.ExternalService).filter(models.ExternalService.service_name == "marki").first()
    if service:
        service.extra_info = None
        db.commit()
        
    success = marki_client.login()
    if success:
        return {"success": True, "message": "鍒锋柊鎴愬姛"}
    else:
        raise HTTPException(status_code=400, detail="登录失败，请检查账号密码")

@app.post("/api/external/services/{service_id}/token")
def refresh_service_token(
    service_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Force refresh or acquire token for a specific service"""
    _require_api_permission(db, current_user, "credential.manage")
    from services.external_auth import ExternalAuthService
    
    db_service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
    if not db_service:
        raise HTTPException(status_code=404, detail="Service not found")
    
    try:
        auth = ExternalAuthService(db_service.service_name, db)
        # Attempt to get token (will login if needed)
        # To FORCE refresh, maybe we should add force flag?
        # For 'testing' connection, _login_and_save() is best as it verifies credentials.
        token = auth._login_and_save()
        
        return {
            "success": True, 
            "message": "Token acquired successfully", 
            "access_token_preview": token[:10] + "..." if token else None,
            "expires_at": auth.service_record.expires_at
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Authentication failed: {str(e)}")

# ===================== Global Variable Management =====================

def _build_settings_user_context(
    current_user: models.User,
    account_book_id: Optional[str] = None,
    account_book_name: Optional[str] = None,
) -> Dict[str, str]:
    org_name = current_user.organization.name if current_user.organization else "未分配"
    return {
        "current_user_id": str(current_user.id),
        "current_username": current_user.username,
        "current_user_realname": current_user.real_name or current_user.username,
        "current_org_id": str(current_user.org_id) if current_user.org_id else "",
        "current_org_name": org_name,
        "current_account_book_id": account_book_id or "",
        "current_account_book_name": account_book_name or "",
    }


def _validate_global_resource_key_conflicts(
    db: Session,
    key: str,
    *,
    exclude_variable_id: Optional[int] = None,
) -> List[str]:
    normalized_key = str(key or "").strip()
    if not normalized_key:
        return []

    errors: List[str] = []
    builtin_keys = get_builtin_variable_keys()
    function_names = set(get_public_expression_function_names())

    if normalized_key in builtin_keys:
        errors.append(f"Key conflicts with built-in variable: {normalized_key}")
    if normalized_key in function_names:
        errors.append(f"Key conflicts with built-in function: {normalized_key}")

    existing_variable = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == normalized_key).first()
    if existing_variable and (exclude_variable_id is None or int(existing_variable.id) != int(exclude_variable_id)):
        errors.append(f"Key already exists in global variables: {normalized_key}")

    return errors


@app.get("/api/settings/variables", response_model=List[schemas.GlobalVariableResponse])
def get_global_variables(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return db.query(models.GlobalVariable).all()


@app.get("/api/settings/variables/runtime")
def get_runtime_variables(
    account_book_id: Optional[str] = None,
    account_book_name: Optional[str] = None,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Return the full runtime variable map, including user context variables."""
    user_context = _build_settings_user_context(
        current_user,
        account_book_id=account_book_id,
        account_book_name=account_book_name,
    )
    var_map = build_variable_map(db, user_context=user_context)
    return var_map


@app.get("/api/settings/functions", response_model=List[schemas.ExpressionFunctionResponse])
def get_global_expression_functions(current_user: models.User = Depends(get_current_user)):
    return get_public_expression_functions()


@app.post("/api/settings/variables", response_model=schemas.GlobalVariableResponse)
def create_global_variable(
    variable: schemas.GlobalVariableCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    key_errors = _validate_global_resource_key_conflicts(db, variable.key)
    if key_errors:
        raise HTTPException(status_code=400, detail={"message": "Variable key is invalid", "errors": key_errors})

    new_variable = models.GlobalVariable(**variable.dict())
    db.add(new_variable)
    db.commit()
    db.refresh(new_variable)
    return new_variable


@app.put("/api/settings/variables/{variable_id}", response_model=schemas.GlobalVariableResponse)
def update_global_variable(
    variable_id: int,
    variable: schemas.GlobalVariableUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    db_variable = db.query(models.GlobalVariable).filter(models.GlobalVariable.id == variable_id).first()
    if not db_variable:
        raise HTTPException(status_code=404, detail="Variable not found")

    update_data = variable.dict(exclude_unset=True)
    if "key" in update_data and update_data["key"] is not None:
        key_errors = _validate_global_resource_key_conflicts(
            db,
            update_data["key"],
            exclude_variable_id=variable_id,
        )
        if key_errors:
            raise HTTPException(status_code=400, detail={"message": "Variable key is invalid", "errors": key_errors})

    for key, value in update_data.items():
        setattr(db_variable, key, value)

    db.commit()
    db.refresh(db_variable)
    return db_variable


@app.delete("/api/settings/variables/{variable_id}")
def delete_global_variable(
    variable_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    db_variable = db.query(models.GlobalVariable).filter(models.GlobalVariable.id == variable_id).first()
    if not db_variable:
        raise HTTPException(status_code=404, detail="Variable not found")

    db.delete(db_variable)
    db.commit()
    return {"message": "Variable deleted"}


# ===================== Voucher Template Management =====================

_PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")
_TRIGGER_OPERATORS = {"==", "!=", ">", ">=", "<", "<=", "contains", "not_contains", "startswith", "endswith"}
_TRIGGER_OPERATOR_ALIASES = {
    "=": "==",
    "eq": "==",
    "equals": "==",
    "equal": "==",
    "<>": "!=",
    "ne": "!=",
    "not_equal": "!=",
    "not_equals": "!=",
    "gt": ">",
    "greater_than": ">",
    "gte": ">=",
    "ge": ">=",
    "greater_or_equal": ">=",
    "greater_than_or_equal": ">=",
    "lt": "<",
    "less_than": "<",
    "lte": "<=",
    "le": "<=",
    "less_or_equal": "<=",
    "less_than_or_equal": "<=",
    "include": "contains",
    "includes": "contains",
    "notcontains": "not_contains",
    "not-contains": "not_contains",
    "exclude": "not_contains",
    "excludes": "not_contains",
    "starts_with": "startswith",
    "prefix": "startswith",
    "ends_with": "endswith",
    "suffix": "endswith",
}
_DATETIME_COMPARE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y-%m",
    "%Y/%m",
    "%Y%m%d",
    "%Y%m",
)


def _canonicalize_trigger_operator(operator: Any) -> Optional[str]:
    raw = "" if operator is None else str(operator).strip()
    if not raw:
        return None
    if raw in _TRIGGER_OPERATORS:
        return raw

    lower = raw.lower()
    if lower in _TRIGGER_OPERATORS:
        return lower

    normalized_keys = [
        lower,
        re.sub(r"\s+", "", lower),
        re.sub(r"[\s\-]+", "_", lower),
    ]
    for key in normalized_keys:
        mapped = _TRIGGER_OPERATOR_ALIASES.get(key)
        if mapped:
            return mapped
    return None


def _try_parse_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace(",", "")
    if normalized.endswith("%"):
        normalized = normalized[:-1].strip()
        if not normalized:
            return None
        try:
            return float(normalized) / 100.0
        except ValueError:
            return None

    try:
        return float(normalized)
    except ValueError:
        return None


def _try_parse_decimal(value: Any) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))

    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace(",", "")
    if normalized.endswith("%"):
        normalized = normalized[:-1].strip()
        if not normalized:
            return None
        try:
            return Decimal(normalized) / Decimal("100")
        except Exception:
            return None

    try:
        return Decimal(normalized)
    except Exception:
        return None


def _json_number(value: Any) -> float:
    if isinstance(value, Decimal):
        return float(value)
    parsed = _try_parse_decimal(value)
    return float(parsed) if parsed is not None else 0.0


def _validate_voucher_json_amounts(kingdee_json: Dict[str, Any]) -> None:
    data_rows = kingdee_json.get("data")
    if not isinstance(data_rows, list) or not data_rows:
        raise HTTPException(status_code=400, detail="kingdee_json.data is required")

    header = data_rows[0] or {}
    entries = header.get("entries")
    if not isinstance(entries, list) or not entries:
        raise HTTPException(status_code=400, detail="kingdee_json.data[0].entries is required")

    debit_ori = Decimal("0")
    credit_ori = Decimal("0")
    debit_local = Decimal("0")
    credit_local = Decimal("0")

    for idx, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise HTTPException(status_code=400, detail=f"kingdee_json entry #{idx} must be an object")

        debit_ori += _try_parse_decimal(entry.get("debitori")) or Decimal("0")
        credit_ori += _try_parse_decimal(entry.get("creditori")) or Decimal("0")
        debit_local += _try_parse_decimal(entry.get("debitlocal")) or Decimal("0")
        credit_local += _try_parse_decimal(entry.get("creditlocal")) or Decimal("0")

    tolerance = Decimal("0.000001")
    if abs(debit_ori - credit_ori) > tolerance:
        raise HTTPException(
            status_code=400,
            detail=f"Voucher JSON debit/credit not balanced: debitori={debit_ori} creditori={credit_ori}",
        )
    if abs(debit_local - credit_local) > tolerance:
        raise HTTPException(
            status_code=400,
            detail=f"Voucher JSON local debit/credit not balanced: debitlocal={debit_local} creditlocal={credit_local}",
        )


def _try_parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace("/", "-")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass

    for fmt in _DATETIME_COMPARE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _compare_ordered_values(actual: Any, expected: Any, operator: str):
    left_num = _try_parse_number(actual)
    right_num = _try_parse_number(expected)
    if left_num is not None and right_num is not None:
        left, right, mode = left_num, right_num, "numeric"
    else:
        left_dt = _try_parse_datetime(actual)
        right_dt = _try_parse_datetime(expected)
        if left_dt is not None and right_dt is not None:
            left, right, mode = left_dt, right_dt, "datetime"
        else:
            left = "" if actual is None else str(actual)
            right = "" if expected is None else str(expected)
            mode = "string"

    if operator == ">":
        return left > right, mode
    if operator == ">=":
        return left >= right, mode
    if operator == "<":
        return left < right, mode
    if operator == "<=":
        return left <= right, mode
    return False, mode


def _extract_placeholders(text: Any) -> Set[str]:
    if text is None:
        return set()
    if not isinstance(text, str):
        text = str(text)
    return {m.strip() for m in _PLACEHOLDER_RE.findall(text) if m and m.strip()}


def _format_placeholders(names: List[str]) -> str:
    return ", ".join(f"{{{name}}}" for name in names)


def _normalize_literal_account_code(expr: Any) -> Optional[str]:
    if expr is None:
        return None
    if not isinstance(expr, str):
        expr = str(expr)
    account_code = expr.strip()
    if not account_code or "{" in account_code or "}" in account_code:
        return None
    if extract_expression_function_names(account_code):
        return None
    if account_code.startswith("'") and account_code.endswith("'") and len(account_code) >= 2:
        account_code = account_code[1:-1].strip()
    return account_code or None


def _coerce_expression_result_to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float, Decimal)):
        return value != 0

    text_value = str(value).strip()
    if not text_value:
        return False

    normalized = text_value.lower()
    if normalized in {"false", "0", "0.0", "none", "null", "no", "off", "n", "f", "否", "假"}:
        return False
    if normalized in {"true", "1", "1.0", "yes", "on", "y", "t", "是", "真"}:
        return True

    try:
        return Decimal(text_value) != 0
    except Exception:
        return True


def _serialize_rule(rule: models.VoucherEntryRule) -> Dict[str, Any]:
    return {
        "line_no": rule.line_no,
        "dr_cr": rule.dr_cr,
        "account_code": rule.account_code,
        "display_condition_expr": rule.display_condition_expr,
        "amount_expr": rule.amount_expr,
        "summary_expr": rule.summary_expr,
        "currency_expr": rule.currency_expr,
        "localrate_expr": rule.localrate_expr,
        "aux_items": rule.aux_items,
        "main_cf_assgrp": rule.main_cf_assgrp,
    }


_BILL_RUNTIME_EXTRA_FIELDS: Set[str] = {"customer_name", "customer_id"}
_BILL_FIELD_LABELS: Dict[str, str] = {
    "id": "账单ID",
    "community_id": "园区ID",
    "charge_item_id": "收费项目ID",
    "ci_snapshot_id": "收费项目快照ID",
    "charge_item_name": "收费项目名称",
    "charge_item_type": "收费项目类型",
    "category_name": "类目名称",
    "asset_id": "资产ID",
    "asset_name": "资产名称",
    "asset_type": "资产类型",
    "asset_type_str": "资产类型(文本)",
    "house_id": "房屋ID",
    "full_house_name": "房屋全名",
    "bind_house_id": "绑定房屋ID",
    "bind_house_name": "绑定房屋名称",
    "park_id": "车位ID",
    "park_name": "车位名称",
    "bill_month": "账单月份",
    "in_month": "所属月份",
    "start_time": "计费开始时间",
    "end_time": "计费结束时间",
    "amount": "金额",
    "bill_amount": "账单金额",
    "discount_amount": "折扣金额",
    "late_money_amount": "滞纳金",
    "deposit_amount": "押金",
    "second_pay_amount": "二次支付金额",
    "pay_status": "支付状态编码",
    "pay_status_str": "支付状态",
    "pay_type": "支付方式编码",
    "pay_type_str": "支付方式",
    "pay_time": "支付时间戳",
    "second_pay_channel": "二次支付渠道",
    "bill_type": "账单类型编码",
    "bill_type_str": "账单类型",
    "deal_log_id": "交易日志ID",
    "receipt_id": "收据号",
    "sub_mch_id": "子商户ID",
    "sub_mch_name": "子商户名称",
    "bad_bill_state": "坏账状态",
    "is_bad_bill": "是否坏账",
    "has_split": "是否拆分",
    "split_desc": "拆分说明",
    "visible_type": "可见类型编码",
    "visible_desc_str": "可见描述",
    "can_revoke": "是否可撤销",
    "version": "版本",
    "meter_type": "表计类型",
    "snapshot_size": "快照大小",
    "now_size": "当前大小",
    "remark": "备注",
    "bind_toll": "收费项目快照(JSON)",
    "user_list": "客户列表(JSON)",
    "create_time": "创建时间",
    "last_op_time": "最后操作时间",
    "created_at": "创建时间(系统)",
    "updated_at": "更新时间(系统)",
    "kd_house_number": "金蝶房号编码",
    "kd_house_name": "金蝶房号名称",
    "kd_park_house_number": "车位映射房号编码",
    "kd_park_house_name": "车位映射房号名称",
    "kd_customer_number": "金蝶客户编码",
    "kd_customer_name": "金蝶客户名称",
    "kd_project_number": "金蝶项目编码",
    "kd_project_name": "金蝶项目名称",
    "kd_receive_bank_number": "收款银行账户编码",
    "kd_receive_bank_name": "收款银行账户名称",
    "kd_pay_bank_number": "付款银行账户编码",
    "kd_pay_bank_name": "付款银行账户名称",
    "customer_name": "账单关联客户名称",
    "customer_id": "账单关联客户ID",
    "receive_date": "支付日期",
}


_RECEIPT_BILL_RUNTIME_EXTRA_FIELDS: Set[str] = {"community_name", "payer_name", "deal_type_label"}
_RECEIPT_BILL_FIELD_LABELS: Dict[str, str] = {
    "id": "收款明细ID",
    "community_id": "园区ID",
    "community_name": "园区名称",
    "payer_name": "付款人",
    "deal_time": "交易时间",
    "deal_date": "交易日期",
    "income_amount": "实收金额",
    "amount": "收款金额",
    "bill_amount": "账单金额",
    "discount_amount": "折扣金额",
    "late_money_amount": "滞纳金",
    "deposit_amount": "押金",
    "pay_channel_str": "支付方式",
    "pay_channel": "支付方式编码",
    "pay_channel_list": "支付方式列表(JSON)",
    "payee": "收款人",
    "receipt_id": "收据号",
    "receipt_record_id": "收据记录ID",
    "receipt_version": "收据版本",
    "invoice_number": "发票号",
    "invoice_urls": "发票链接(JSON)",
    "invoice_status": "发票状态",
    "open_invoice": "是否开票",
    "asset_name": "资产名称",
    "asset_id": "资产ID",
    "asset_type": "资产类型",
    "deal_type": "收入类型",
    "remark": "备注",
    "fk_id": "FK_ID",
    "bind_users_raw": "关联住户备份(JSON)",
    "created_at": "创建时间(系统)",
    "updated_at": "更新时间(系统)",
    "kd_house_number": "金蝶房号编码",
    "kd_house_name": "金蝶房号名称",
    "kd_park_house_number": "车位映射房号编码",
    "kd_park_house_name": "车位映射房号名称",
    "kd_customer_number": "金蝶客户编码",
    "kd_customer_name": "金蝶客户名称",
    "kd_project_number": "金蝶项目编码",
    "kd_project_name": "金蝶项目名称",
    "kd_receive_bank_number": "收款银行账户编码",
    "kd_receive_bank_name": "收款银行账户名称",
    "kd_pay_bank_number": "付款银行账户编码",
    "kd_pay_bank_name": "付款银行账户名称",
}


_DEPOSIT_RECORD_RUNTIME_EXTRA_FIELDS: Set[str] = {"operate_type_label"}
_DEPOSIT_RECORD_FIELD_LABELS: Dict[str, str] = {
    "id": "押金记录ID",
    "community_id": "园区ID",
    "community_name": "园区名称",
    "house_id": "房屋ID",
    "house_name": "房屋名称",
    "amount": "押金金额",
    "operate_type": "操作类型编码",
    "operate_type_label": "操作类型",
    "operator": "操作人ID",
    "operator_name": "操作人",
    "operate_time": "操作时间戳",
    "operate_date": "操作日期",
    "cash_pledge_name": "押金类型",
    "remark": "备注",
    "pay_time": "支付时间戳",
    "pay_date": "支付日期",
    "payment_id": "关联收款单ID",
    "has_refund_receipt": "是否关联退款收据",
    "refund_receipt_id": "关联退款收款单ID",
    "pay_channel_str": "支付方式",
    "raw_data": "原始数据(JSON)",
    "created_at": "创建时间(系统)",
    "updated_at": "更新时间(系统)",
}


def _group_bills_field(field_name: str) -> str:
    field_options = mapping_build_source_field_options("bills")
    matched = next((item for item in field_options if item.get("value") == field_name), None)
    return str(matched.get("group")) if matched and matched.get("group") else "账单字段"


def _group_receipt_bills_field(field_name: str) -> str:
    field_options = mapping_build_source_field_options("receipt_bills")
    matched = next((item for item in field_options if item.get("value") == field_name), None)
    return str(matched.get("group")) if matched and matched.get("group") else "收款字段"


def _group_deposit_records_field(field_name: str) -> str:
    field_options = mapping_build_source_field_options("deposit_records")
    matched = next((item for item in field_options if item.get("value") == field_name), None)
    return str(matched.get("group")) if matched and matched.get("group") else "押金字段"


def _build_bills_fields() -> Set[str]:
    return mapping_build_source_fields("bills")


def _build_receipt_bills_fields() -> Set[str]:
    return mapping_build_source_fields("receipt_bills")


def _build_deposit_records_fields() -> Set[str]:
    return mapping_build_source_fields("deposit_records")


def _build_prepayment_records_fields() -> Set[str]:
    return mapping_build_source_fields("prepayment_records")


def _build_oa_fields() -> Set[str]:
    return set()


def _build_oa_field_options() -> List[Dict[str, str]]:
    return []


MODULE_REGISTRY: Dict[str, VoucherSourceModuleMeta] = {
    "marki": VoucherSourceModuleMeta(id="marki", label="马克系统"),
    "oa": VoucherSourceModuleMeta(id="oa", label="OA系统", note="仅预留扩展架构，暂未接入实体数据"),
}


SOURCE_REGISTRY: Dict[str, VoucherSourceMeta] = {
    "bills": VoucherSourceMeta(
        id="bills",
        module_id="marki",
        label="运营账单",
        source_type="bills",
        root_enabled=True,
        field_names_builder=_build_bills_fields,
        field_options_builder=lambda: _build_bills_field_options(),
    ),
    "receipt_bills": VoucherSourceMeta(
        id="receipt_bills",
        module_id="marki",
        label="收款账单",
        source_type="receipt_bills",
        root_enabled=True,
        field_names_builder=_build_receipt_bills_fields,
        field_options_builder=lambda: _build_receipt_bills_field_options(),
    ),
    "deposit_records": VoucherSourceMeta(
        id="deposit_records",
        module_id="marki",
        label="押金记录",
        source_type="deposit_records",
        root_enabled=False,
        field_names_builder=_build_deposit_records_fields,
        field_options_builder=lambda: _build_deposit_records_field_options(),
    ),
    "prepayment_records": VoucherSourceMeta(
        id="prepayment_records",
        module_id="marki",
        label="预存款记录",
        source_type="prepayment_records",
        root_enabled=False,
        field_names_builder=_build_prepayment_records_fields,
        field_options_builder=lambda: _build_prepayment_records_field_options(),
    ),
    "oa_forms": VoucherSourceMeta(
        id="oa_forms",
        module_id="oa",
        label="OA单据",
        source_type="oa_forms",
        root_enabled=False,
        note="仅预留扩展架构，暂未接入实体字段与关联关系",
        field_names_builder=_build_oa_fields,
        field_options_builder=_build_oa_field_options,
    ),
}


RELATION_REGISTRY: Dict[str, VoucherRelationMeta] = {
    "receipt_to_bills": VoucherRelationMeta(
        resolver="receipt_to_bills",
        label="关联运营账单",
        root_source="receipt_bills",
        target_source="bills",
        loader=_load_receipt_to_bills_relation,
    ),
    "receipt_to_deposit_collect": VoucherRelationMeta(
        resolver="receipt_to_deposit_collect",
        label="关联押金收取",
        root_source="receipt_bills",
        target_source="deposit_records",
        loader=_load_receipt_to_deposit_collect_relation,
    ),
    "receipt_to_deposit_refund": VoucherRelationMeta(
        resolver="receipt_to_deposit_refund",
        label="关联押金退款",
        root_source="receipt_bills",
        target_source="deposit_records",
        loader=_load_receipt_to_deposit_refund_relation,
    ),
    "receipt_to_prepayment_recharge": VoucherRelationMeta(
        resolver="receipt_to_prepayment_recharge",
        label="关联预存款收取",
        root_source="receipt_bills",
        target_source="prepayment_records",
        loader=_load_receipt_to_prepayment_recharge_relation,
    ),
    "receipt_to_prepayment_refund": VoucherRelationMeta(
        resolver="receipt_to_prepayment_refund",
        label="关联预存款退款",
        root_source="receipt_bills",
        target_source="prepayment_records",
        loader=_load_receipt_to_prepayment_refund_relation,
    ),
}


def _get_source_meta(source_type: Optional[str]) -> Optional[VoucherSourceMeta]:
    normalized_source = (source_type or "").strip().lower() or "bills"
    return SOURCE_REGISTRY.get(normalized_source)


def _get_module_source_types(module_id: Optional[str]) -> List[str]:
    normalized_module = (module_id or "").strip().lower()
    if not normalized_module:
        return []
    return [
        source_meta.source_type
        for source_meta in SOURCE_REGISTRY.values()
        if source_meta.module_id == normalized_module
    ]


def _build_source_fields(source_type: str) -> Set[str]:
    source_meta = _get_source_meta(source_type)
    if source_meta and source_meta.field_names_builder:
        return set(source_meta.field_names_builder())
    return set()


def _build_source_field_options(source_type: str) -> List[Dict[str, str]]:
    source_meta = _get_source_meta(source_type)
    if source_meta and source_meta.field_options_builder:
        return list(source_meta.field_options_builder())
    return []


def _build_bills_field_options() -> List[Dict[str, str]]:
    return mapping_build_source_field_options("bills")


def _build_receipt_bills_field_options() -> List[Dict[str, str]]:
    return mapping_build_source_field_options("receipt_bills")


def _build_deposit_records_field_options() -> List[Dict[str, str]]:
    return mapping_build_source_field_options("deposit_records")


def _build_prepayment_records_field_options() -> List[Dict[str, str]]:
    return mapping_build_source_field_options("prepayment_records")


def _build_legacy_source_field_options(source_type: str) -> List[Dict[str, str]]:
    normalized_source = (source_type or "").strip().lower()
    if normalized_source == "receipt_bills":
        return _build_receipt_bills_field_options()
    if normalized_source == "deposit_records":
        return _build_deposit_records_field_options()
    if normalized_source == "prepayment_records":
        return _build_prepayment_records_field_options()
    return _build_bills_field_options()


@app.get("/api/vouchers/source-fields")
def get_voucher_source_fields(source_type: str = Query("bills")):
    actual_source = (source_type or "").strip().lower() or "bills"
    return {"source_type": actual_source, "fields": _build_source_field_options(actual_source)}


@app.get("/api/vouchers/source-modules")
def get_voucher_source_modules():
    """
    Advanced source field selector metadata.

    - Top-level module split: Mark system vs OA system (OA is placeholder for now).
    - Mark system fields are loaded from backend data models (SQLAlchemy columns + runtime/derived fields).
    """

    return {
        "modules": build_source_modules_payload(MODULE_REGISTRY, SOURCE_REGISTRY),
        "relations": build_relation_payload(RELATION_REGISTRY),
    }


def _build_allowed_placeholders(source_type: Optional[str], source_module: Optional[str], db: Session) -> Set[str]:
    from utils.variable_parser import build_variable_map

    allowed = set()
    try:
        allowed.update(build_variable_map(db).keys())
    except Exception:
        allowed.update(v.key for v in db.query(models.GlobalVariable).all())

    # 鐢ㄦ埛涓婁笅鏂囧彉閲忥紙杩愯鏃剁敱褰撳墠鐧诲綍鐢ㄦ埛鍔ㄦ€佹敞鍏ワ紝鏍￠獙闃舵闇€棰勫厛鏀捐锛?
    allowed.update({
        "CURRENT_ACCOUNT_BOOK_NUMBER",
        "CURRENT_ACCOUNT_BOOK_NAME",
        "CURRENT_USER_REALNAME",
        "CURRENT_USERNAME",
        "CURRENT_USER_ID",
        "CURRENT_ORG_ID",
        "CURRENT_ORG_NAME",
    })

    normalized_source = (source_type or "").strip().lower()
    normalized_module = (source_module or "").strip().lower()

    source_meta = _get_source_meta(normalized_source) if normalized_source else None
    module_prefix = normalized_module or (source_meta.module_id if source_meta else "marki")

    source_types: Set[str] = set()
    if module_prefix:
        source_types.update(_get_module_source_types(module_prefix))

    if normalized_source:
        source_types.add(normalized_source)
    else:
        source_types.add("bills")

    for current_source in sorted(source_types):
        source_fields = _build_source_fields(current_source)
        if not source_fields:
            continue

        allowed.update(source_fields)
        allowed.update({f"{current_source}.{name}" for name in source_fields})
        if module_prefix:
            allowed.update({f"{module_prefix}.{current_source}.{name}" for name in source_fields})

        registered_meta = _get_source_meta(current_source)
        if registered_meta and registered_meta.module_id == "marki" and module_prefix != "marki":
            allowed.update({f"marki.{current_source}.{name}" for name in source_fields})
    return allowed


def _extract_required_check_dimensions(subject: Optional[models.AccountingSubject]) -> Set[str]:
    if not subject or not subject.check_items:
        return set()
    try:
        check_items = json.loads(subject.check_items)
    except (TypeError, json.JSONDecodeError):
        return set()
    if not isinstance(check_items, list):
        return set()

    required_dims = set()
    for item in check_items:
        if not isinstance(item, dict):
            continue
        dim_name = str(item.get("asstactitem_name") or item.get("asstactitem_number") or "").strip()
        if dim_name:
            required_dims.add(dim_name)
    return required_dims


def _validate_unknown_placeholders(expr: Any, field_path: str, allowed_placeholders: Set[str], errors: List[str]) -> None:
    unknown = sorted(_extract_placeholders(expr) - allowed_placeholders)
    if unknown:
        errors.append(f"{field_path} contains unknown placeholders: {_format_placeholders(unknown)}")


def _validate_unknown_functions(expr: Any, field_path: str, errors: List[str]) -> None:
    allowed_functions = set(get_public_expression_function_names())
    unknown = sorted({name for name in extract_expression_function_names(expr) if name not in allowed_functions})
    if unknown:
        errors.append(f"{field_path} contains unknown functions: {', '.join(unknown)}")


def _validate_dimension_mapping_json(
    raw_value: Any,
    field_path: str,
    allowed_placeholders: Set[str],
    errors: List[str],
) -> Dict[str, Dict[str, str]]:
    if raw_value in (None, ""):
        return {}

    mapping_obj: Any = raw_value
    if isinstance(raw_value, str):
        try:
            mapping_obj = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            errors.append(f"{field_path} must be a valid JSON object: {exc}")
            return {}

    if not isinstance(mapping_obj, dict):
        errors.append(f"{field_path} must be a JSON object")
        return {}

    normalized_mapping: Dict[str, Dict[str, str]] = {}
    for dim_key, dim_cfg in mapping_obj.items():
        dim_name = str(dim_key).strip()
        if not dim_name:
            errors.append(f"{field_path} has an empty dimension name")
            continue

        if not isinstance(dim_cfg, dict):
            errors.append(f"{field_path}.{dim_name} must be a JSON object")
            continue

        if not dim_cfg:
            errors.append(f"{field_path}.{dim_name} must have at least one property")
            continue

        normalized_mapping[dim_name] = {}
        for prop_key, expr in dim_cfg.items():
            prop_name = str(prop_key).strip()
            if not prop_name:
                errors.append(f"{field_path}.{dim_name} has an empty property name")
                continue

            expr_text = "" if expr is None else str(expr)
            normalized_mapping[dim_name][prop_name] = expr_text
            _validate_unknown_placeholders(
                expr_text,
                f"{field_path}.{dim_name}.{prop_name}",
                allowed_placeholders,
                errors,
            )
            _validate_unknown_functions(
                expr_text,
                f"{field_path}.{dim_name}.{prop_name}",
                errors,
            )

    return normalized_mapping


def _build_allowed_source_fields_for_type(source_type: Optional[str], module_prefix: str = "marki") -> Set[str]:
    normalized_source = (source_type or "").strip().lower()
    if not normalized_source:
        normalized_source = "bills"

    base_fields = _build_source_fields(normalized_source)
    allowed_fields = set(base_fields)
    allowed_fields.update({f"{normalized_source}.{name}" for name in base_fields})
    allowed_fields.update({f"{module_prefix}.{normalized_source}.{name}" for name in base_fields})
    if module_prefix != "marki":
        allowed_fields.update({f"marki.{normalized_source}.{name}" for name in base_fields})
    return allowed_fields


def _normalize_relation_group(node: Dict[str, Any]) -> Dict[str, Any]:
    children = node.get("children")
    if isinstance(children, list):
        return {
            "logic": str(node.get("logic", "AND")).upper(),
            "children": children,
        }

    where = node.get("where")
    if isinstance(where, dict):
        if str(where.get("type", "group")) == "group":
            return {
                "logic": str(where.get("logic", "AND")).upper(),
                "children": where.get("children", []),
            }
        return {
            "logic": "AND",
            "children": [where],
        }

    return {
        "logic": str(node.get("logic", "AND")).upper(),
        "children": [],
    }


def _validate_trigger_condition(
    trigger_condition: Optional[str],
    source_type: Optional[str],
    allowed_placeholders: Set[str],
    allowed_fields: Set[str],
    errors: List[str],
    field_path: str = "trigger_condition",
) -> None:
    if not trigger_condition:
        return

    try:
        root = json.loads(trigger_condition)
    except json.JSONDecodeError as exc:
        errors.append(f"{field_path} must be valid JSON: {exc}")
        return

    if not isinstance(root, dict):
        errors.append(f"{field_path} must be a JSON object")
        return

    normalized_source = (source_type or "").strip().lower() or "bills"
    enforce_field_check = _get_source_meta(normalized_source) is not None

    def walk(node: Any, path: str, current_source: str, current_fields: Set[str]) -> None:
        if not isinstance(node, dict):
            errors.append(f"{path} must be a JSON object")
            return

        node_type = node.get("type", "group")
        if node_type == "group":
            logic = str(node.get("logic", "AND")).upper()
            if logic not in {"AND", "OR"}:
                errors.append(f"{path}.logic must be AND or OR")
            children = node.get("children", [])
            if not isinstance(children, list):
                errors.append(f"{path}.children must be an array")
                return
            for idx, child in enumerate(children):
                walk(child, f"{path}.children[{idx}]", current_source, current_fields)
            return

        if node_type == "rule":
            field_name = str(node.get("field", "")).strip()
            if not field_name:
                errors.append(f"{path}.field is required")
            elif enforce_field_check and field_name not in current_fields:
                errors.append(f"{path}.field is not a supported field for source_type={current_source}: {field_name}")

            raw_operator = node.get("operator", "==")
            operator = _canonicalize_trigger_operator(raw_operator)
            if operator is None:
                errors.append(f"{path}.operator is not supported: {raw_operator}")

            _validate_unknown_placeholders(node.get("value", ""), f"{path}.value", allowed_placeholders, errors)
            _validate_unknown_functions(node.get("value", ""), f"{path}.value", errors)
            return

        if node_type == "relation":
            resolver = str(node.get("resolver", "")).strip()
            target_source = str(node.get("target_source", "")).strip().lower()
            quantifier = str(node.get("quantifier", "EXISTS")).upper()
            relation_meta = RELATION_REGISTRY.get(resolver)

            if quantifier not in {"EXISTS", "NOT_EXISTS"}:
                errors.append(f"{path}.quantifier must be EXISTS or NOT_EXISTS")

            if not relation_meta:
                errors.append(f"{path}.resolver is not supported: {resolver or '<empty>'}")
                expected_target = target_source
            else:
                expected_target = relation_meta.target_source
                if current_source != relation_meta.root_source:
                    errors.append(
                        f"{path}.resolver {resolver} is not supported under source_type={current_source}"
                    )

            if not target_source:
                errors.append(f"{path}.target_source is required")
            elif expected_target and target_source != expected_target:
                errors.append(f"{path}.target_source must be {expected_target} for resolver={resolver}")

            relation_group = _normalize_relation_group(node)
            if relation_group["logic"] not in {"AND", "OR"}:
                errors.append(f"{path}.logic must be AND or OR")
            if not isinstance(relation_group["children"], list):
                errors.append(f"{path}.children must be an array")
                return

            relation_fields = _build_allowed_source_fields_for_type(expected_target or target_source)
            for idx, child in enumerate(relation_group["children"]):
                walk(child, f"{path}.children[{idx}]", expected_target or target_source or current_source, relation_fields)
            return

        errors.append(f"{path}.type must be group, rule or relation")

    walk(root, field_path, normalized_source, allowed_fields)


def _validate_voucher_template_payload(payload: Dict[str, Any], db: Session) -> None:
    errors: List[str] = []
    category_id = payload.get("category_id")
    if category_id is not None:
        try:
            cat_id = int(category_id)
        except (TypeError, ValueError):
            errors.append("category_id must be an integer")
        else:
            exists = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == cat_id).first()
            if not exists:
                errors.append("category_id not found")
    source_type = payload.get("source_type")
    source_module = payload.get("source_module")
    allowed_placeholders = _build_allowed_placeholders(source_type, source_module, db)
    normalized_source = (source_type or "").strip().lower()
    allowed_source_fields = _build_allowed_source_fields_for_type(normalized_source or "bills")

    _validate_unknown_placeholders(payload.get("book_number_expr"), "book_number_expr", allowed_placeholders, errors)
    _validate_unknown_placeholders(payload.get("vouchertype_number_expr"), "vouchertype_number_expr", allowed_placeholders, errors)
    _validate_unknown_placeholders(payload.get("attachment_expr"), "attachment_expr", allowed_placeholders, errors)
    _validate_unknown_placeholders(payload.get("bizdate_expr"), "bizdate_expr", allowed_placeholders, errors)
    _validate_unknown_placeholders(payload.get("bookeddate_expr"), "bookeddate_expr", allowed_placeholders, errors)
    _validate_unknown_functions(payload.get("book_number_expr"), "book_number_expr", errors)
    _validate_unknown_functions(payload.get("vouchertype_number_expr"), "vouchertype_number_expr", errors)
    _validate_unknown_functions(payload.get("attachment_expr"), "attachment_expr", errors)
    _validate_unknown_functions(payload.get("bizdate_expr"), "bizdate_expr", errors)
    _validate_unknown_functions(payload.get("bookeddate_expr"), "bookeddate_expr", errors)

    _validate_trigger_condition(
        payload.get("trigger_condition"),
        source_type,
        allowed_placeholders,
        allowed_source_fields,
        errors,
    )

    rules = payload.get("rules")
    if not isinstance(rules, list):
        errors.append("rules must be an array")
        rules = []

    line_nos_seen: Set[int] = set()
    required_dims_cache: Dict[str, Set[str]] = {}

    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict):
            errors.append(f"rules[{idx}] must be an object")
            continue

        line_no = rule.get("line_no")
        if not isinstance(line_no, int) or line_no < 1:
            errors.append(f"rules[{idx}].line_no must be an integer >= 1")
            line_no = idx + 1
        if line_no in line_nos_seen:
            errors.append(f"rules[{idx}].line_no duplicates line number {line_no}")
        line_nos_seen.add(line_no)

        dr_cr = rule.get("dr_cr")
        if dr_cr not in {"D", "C"}:
            errors.append(f"rules[{idx}].dr_cr must be D or C")

        _validate_unknown_placeholders(rule.get("amount_expr"), f"rules[{idx}].amount_expr", allowed_placeholders, errors)
        _validate_unknown_placeholders(rule.get("summary_expr"), f"rules[{idx}].summary_expr", allowed_placeholders, errors)
        _validate_unknown_placeholders(rule.get("currency_expr"), f"rules[{idx}].currency_expr", allowed_placeholders, errors)
        _validate_unknown_placeholders(rule.get("localrate_expr"), f"rules[{idx}].localrate_expr", allowed_placeholders, errors)
        _validate_unknown_functions(rule.get("amount_expr"), f"rules[{idx}].amount_expr", errors)
        _validate_unknown_functions(rule.get("summary_expr"), f"rules[{idx}].summary_expr", errors)
        _validate_unknown_functions(rule.get("currency_expr"), f"rules[{idx}].currency_expr", errors)
        _validate_unknown_functions(rule.get("localrate_expr"), f"rules[{idx}].localrate_expr", errors)

        _validate_trigger_condition(
            rule.get("display_condition_expr"),
            source_type,
            allowed_placeholders,
            allowed_source_fields,
            errors,
            f"rules[{idx}].display_condition_expr",
        )

        aux_mapping = _validate_dimension_mapping_json(
            rule.get("aux_items"),
            f"rules[{idx}].aux_items",
            allowed_placeholders,
            errors,
        )
        _validate_dimension_mapping_json(
            rule.get("main_cf_assgrp"),
            f"rules[{idx}].main_cf_assgrp",
            allowed_placeholders,
            errors,
        )

        account_code = _normalize_literal_account_code(rule.get("account_code"))
        if not account_code:
            errors.append(f"rules[{idx}].account_code must be a static leaf account code; formulas are not allowed")
            continue

        if account_code not in required_dims_cache:
            subject = (
                db.query(models.AccountingSubject)
                .filter(models.AccountingSubject.number == account_code)
                .first()
            )
            required_dims_cache[account_code] = subject

        subject = required_dims_cache.get(account_code)
        if not subject:
            errors.append(f"rules[{idx}].account_code references a non-existent account: {account_code}")
            continue
        if not getattr(subject, "is_leaf", False):
            errors.append(f"rules[{idx}].account_code must be a leaf account: {account_code}")
            continue

        required_dims = _extract_required_check_dimensions(subject)
        if not required_dims:
            continue

        if not aux_mapping:
            errors.append(
                f"rules[{idx}].aux_items is required for account {account_code}; "
                f"missing dimensions: {', '.join(sorted(required_dims))}"
            )
            continue

        missing_dims = sorted(required_dims - set(aux_mapping.keys()))
        if missing_dims:
            errors.append(
                f"rules[{idx}].aux_items missing required dimensions for account {account_code}: "
                f"{', '.join(missing_dims)}"
            )

    if errors:
        raise HTTPException(
            status_code=400,
            detail={"message": "Voucher template validation failed", "errors": errors},
        )


def _normalize_rule_for_response(rule: models.VoucherEntryRule, fallback_line_no: int) -> None:
    dr_raw = (rule.dr_cr or "").strip().upper()
    dr_map = {
        "D": "D",
        "C": "C",
        "DEBIT": "D",
        "CREDIT": "C",
        "借": "D",
        "贷": "C",
        "1": "D",
        "-1": "C",
    }
    rule.dr_cr = dr_map.get(dr_raw, "D")
    rule.line_no = rule.line_no if isinstance(rule.line_no, int) and rule.line_no > 0 else fallback_line_no
    rule.account_code = (rule.account_code or "").strip()
    rule.display_condition_expr = rule.display_condition_expr if isinstance(rule.display_condition_expr, str) else ""
    rule.amount_expr = rule.amount_expr if isinstance(rule.amount_expr, str) and rule.amount_expr.strip() else "0"
    rule.summary_expr = rule.summary_expr if isinstance(rule.summary_expr, str) else ""
    rule.currency_expr = rule.currency_expr if isinstance(rule.currency_expr, str) and rule.currency_expr.strip() else "'CNY'"
    rule.localrate_expr = rule.localrate_expr if isinstance(rule.localrate_expr, str) and rule.localrate_expr.strip() else "1"


def _merge_selected_record_values(base_context: Dict[str, Any], selected_records: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = dict(base_context or {})
    for record in (selected_records or {}).values():
        if not isinstance(record, dict):
            continue
        for key, value in record.items():
            if isinstance(key, str) and "." in key:
                merged[key] = value
    return merged


def _evaluate_rule_display_condition(
    raw_condition: Optional[str],
    data: Dict[str, Any],
    global_context: Optional[Dict[str, Any]] = None,
    relation_context: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, Dict[str, Any]]:
    condition_text = str(raw_condition or "").strip()
    if not condition_text:
        return True, {}

    try:
        condition_root = json.loads(condition_text)
    except Exception:
        return False, {}

    scoped_relation_ctx = dict(relation_context or {})
    scoped_relation_ctx["selected_records"] = {}
    matched = _check_trigger_conditions(
        condition_root,
        data,
        [],
        global_context,
        scoped_relation_ctx if scoped_relation_ctx else None,
    )
    return matched, dict(scoped_relation_ctx.get("selected_records") or {})


def _normalize_template_for_response(template: models.VoucherTemplate) -> None:
    if template.priority is not None:
        try:
            template.priority = max(int(template.priority), 0)
        except (TypeError, ValueError):
            template.priority = 100
    if template.rules:
        for idx, rule in enumerate(template.rules, start=1):
            _normalize_rule_for_response(rule, idx)

@app.get("/api/vouchers/templates", response_model=List[schemas.VoucherTemplateResponse])
def get_voucher_templates(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get all voucher templates"""
    _require_api_permission(db, current_user, "voucher_template.manage")
    categories = db.query(models.VoucherTemplateCategory).all()
    category_path_map = build_template_category_path_map(categories)
    templates = db.query(models.VoucherTemplate).order_by(
        models.VoucherTemplate.priority.asc(),
        models.VoucherTemplate.template_id.asc()
    ).all()
    for template in templates:
        template.category_path = category_path_map.get(getattr(template, "category_id", None))
        _normalize_template_for_response(template)
    return templates

@app.get("/api/vouchers/templates/{template_id}", response_model=schemas.VoucherTemplateResponse)
def get_voucher_template(
    template_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get a specific voucher template"""
    _require_api_permission(db, current_user, "voucher_template.manage")
    template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template_id).first()
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    if getattr(template, "category_id", None) is not None:
        categories = db.query(models.VoucherTemplateCategory).all()
        category_path_map = build_template_category_path_map(categories)
        template.category_path = category_path_map.get(getattr(template, "category_id", None))
    _normalize_template_for_response(template)
    return template

@app.post("/api/vouchers/templates", response_model=schemas.VoucherTemplateResponse)
def create_voucher_template(
    template: schemas.VoucherTemplateCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Create a new voucher template with rules"""
    _require_api_permission(db, current_user, "voucher_template.manage")
    existing = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template.template_id).first()
    if existing:
        raise HTTPException(status_code=400, detail="Template ID already exists")
    
    template_data = template.dict()
    rules_data = template_data.pop('rules', [])
    _validate_voucher_template_payload({**template_data, "rules": rules_data}, db)
    
    new_template = models.VoucherTemplate(**template_data)
    db.add(new_template)
    
    for rule in rules_data:
        new_rule = models.VoucherEntryRule(**rule, template_id=new_template.template_id)
        db.add(new_rule)
        
    db.commit()
    db.refresh(new_template)
    return new_template

@app.put("/api/vouchers/templates/{template_id}", response_model=schemas.VoucherTemplateResponse)
def update_voucher_template(
    template_id: str,
    template: schemas.VoucherTemplateUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Update a voucher template and its rules"""
    _require_api_permission(db, current_user, "voucher_template.manage")
    db_template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template_id).first()
    if not db_template:
        raise HTTPException(status_code=404, detail="Template not found")
        
    update_data = template.dict(exclude_unset=True)
    rules_data = update_data.pop('rules', None)

    full_payload = {
        "source_module": update_data.get("source_module", getattr(db_template, "source_module", None)),
        "source_type": update_data.get("source_type", db_template.source_type),
        "trigger_condition": update_data.get("trigger_condition", db_template.trigger_condition),
        "category_id": update_data.get("category_id", getattr(db_template, "category_id", None)),
        "book_number_expr": update_data.get("book_number_expr", db_template.book_number_expr),
        "vouchertype_number_expr": update_data.get("vouchertype_number_expr", db_template.vouchertype_number_expr),
        "attachment_expr": update_data.get("attachment_expr", db_template.attachment_expr),
        "bizdate_expr": update_data.get("bizdate_expr", db_template.bizdate_expr),
        "bookeddate_expr": update_data.get("bookeddate_expr", db_template.bookeddate_expr),
        "rules": rules_data if rules_data is not None else [_serialize_rule(rule) for rule in db_template.rules],
    }
    _validate_voucher_template_payload(full_payload, db)
    
    for key, value in update_data.items():
        setattr(db_template, key, value)
    
    if rules_data is not None:
        # Simplest way: delete old rules and add new ones
        db.query(models.VoucherEntryRule).filter(models.VoucherEntryRule.template_id == template_id).delete()
        for rule in rules_data:
            new_rule = models.VoucherEntryRule(**rule, template_id=template_id)
            db.add(new_rule)
        
    db.commit()
    db.refresh(db_template)
    return db_template

@app.delete("/api/vouchers/templates/{template_id}")
def delete_voucher_template(
    template_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Delete a voucher template"""
    _require_api_permission(db, current_user, "voucher_template.manage")
    db_template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template_id).first()
    if not db_template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    # Note: Entry rules will be deleted because of relationship if cascade is set, 
    # but let's be explicit if not sure.
    db.query(models.VoucherEntryRule).filter(models.VoucherEntryRule.template_id == template_id).delete()
    db.delete(db_template)
    db.commit()
    return {"message": "Template deleted"}


@app.post("/api/vouchers/resolve-fields")
def resolve_voucher_fields(
    payload: dict,
    db: Session = Depends(get_db)
):
    """
    解析并补全账单数据中的金蝶衍生字段。

    输入示例:
    { "bill_data": { "house_id": "123", "park_id": "456" } }

    返回示例:
    { "enriched_data": { "kd_house_number": "H001" } }
    """
    bill_data = payload.get("bill_data", {})
    enriched = mapping_enrich_source_data("bills", bill_data, db=db)
    
    return {"enriched_data": enriched}


def _build_preview_user_context(
    current_user: models.User,
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
) -> Dict[str, str]:
    from urllib.parse import unquote

    org_name = current_user.organization.name if current_user.organization else "未分配"
    return {
        "current_user_id": str(current_user.id),
        "current_username": current_user.username,
        "current_user_realname": current_user.real_name or current_user.username,
        "current_org_id": str(current_user.org_id) if current_user.org_id else "",
        "current_org_name": org_name,
        "current_account_book_id": unquote(x_account_book_id) if x_account_book_id else "",
        "current_account_book_name": unquote(x_account_book_name) if x_account_book_name else "",
        "current_account_book_number": unquote(x_account_book_number) if x_account_book_number else "",
    }


def _parse_attachment_count(value: str) -> int:
    try:
        return max(int(float(value)), 0)
    except (TypeError, ValueError):
        return 0


def _collect_receipt_source_bills(
    db: Session,
    receipt_bill_id: int,
    community_id: int,
    account_book_number: Optional[str],
) -> List[Dict[str, Any]]:
    related_map = _get_related_bill_refs_for_receipts(
        db,
        [{"receipt_bill_id": int(receipt_bill_id), "community_id": int(community_id)}],
    )
    refs = related_map.get((int(receipt_bill_id), int(community_id)), [])
    if not refs:
        return []
    source_status_map = _get_bill_push_status_map(
        db,
        refs,
        account_book_number=account_book_number,
    )
    return [source_status_map[(ref["bill_id"], ref["community_id"])] for ref in refs]


def _build_bill_summary_payload(bill: models.Bill) -> Dict[str, Any]:
    return {
        "id": bill.id,
        "community_id": bill.community_id,
        "charge_item_name": bill.charge_item_name,
        "full_house_name": bill.full_house_name,
        "amount": _json_number(bill.amount),
        "asset_name": bill.asset_name,
    }


def _build_receipt_summary_payload(receipt_bill: models.ReceiptBill) -> Dict[str, Any]:
    return {
        "id": receipt_bill.id,
        "community_id": receipt_bill.community_id,
        "receipt_id": receipt_bill.receipt_id,
        "deal_type": receipt_bill.deal_type,
        "deal_type_label": RECEIPT_BILL_DEAL_TYPE_LABELS.get(receipt_bill.deal_type, "其他"),
        "income_amount": _json_number(receipt_bill.income_amount),
        "amount": _json_number(receipt_bill.amount),
        "asset_name": receipt_bill.asset_name,
    }


def _match_receipt_templates(
    receipt_bill: models.ReceiptBill,
    enriched: Dict[str, Any],
    runtime_vars: Dict[str, str],
    db: Session,
    scoped_relation_records: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    import json as json_mod

    templates = db.query(models.VoucherTemplate).filter(
        models.VoucherTemplate.active == True,
        models.VoucherTemplate.source_type == "receipt_bills",
    ).order_by(
        models.VoucherTemplate.priority.asc(),
        models.VoucherTemplate.template_id.asc(),
    ).all()

    matched_template = None
    matched_selected_records: Dict[str, Dict[str, Any]] = {}
    all_debug_logs = {}

    conditional_templates = [t for t in templates if t.trigger_condition]
    fallback_templates = [t for t in templates if not t.trigger_condition]

    for tmpl in conditional_templates:
        try:
            conditions = json_mod.loads(tmpl.trigger_condition)
            debug_logs = []
            relation_eval_ctx = {
                "db": db,
                "root_record": receipt_bill,
                "receipt_bill": receipt_bill,
                "cache": {},
                "selected_records": {},
            }
            if scoped_relation_records is not None:
                relation_eval_ctx["scoped_records"] = scoped_relation_records
            if _check_trigger_conditions(conditions, enriched, debug_logs, runtime_vars, relation_eval_ctx):
                matched_template = tmpl
                matched_selected_records = dict(relation_eval_ctx.get("selected_records") or {})
                break
            all_debug_logs[tmpl.template_name] = debug_logs
        except (json_mod.JSONDecodeError, Exception) as e:
            all_debug_logs[tmpl.template_name] = [f"JSON Parse Error: {e}"]
            continue

    if not matched_template and fallback_templates:
        matched_template = fallback_templates[0]

    return {
        "templates": templates,
        "matched_template": matched_template,
        "matched_selected_records": matched_selected_records,
        "debug_logs": all_debug_logs,
    }


def _preview_voucher_for_bill_via_receipt_templates(
    bill: models.Bill,
    enriched_bill: Dict[str, Any],
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
    current_user: models.User,
    db: Session,
) -> Optional[Dict[str, Any]]:
    from services.voucher_engine import evaluate_expression
    from utils.variable_parser import build_variable_map, resolve_variables

    if not bill.deal_log_id:
        return None

    receipt_bill = (
        db.query(models.ReceiptBill)
        .options(selectinload(models.ReceiptBill.users))
        .filter(
            models.ReceiptBill.id == int(bill.deal_log_id),
            models.ReceiptBill.community_id == int(bill.community_id),
        )
        .first()
    )
    if not receipt_bill:
        return None

    normalized_account_book_number = _decode_header_value(x_account_book_number) or None
    bill_ref = {"bill_id": int(bill.id), "community_id": int(bill.community_id)}
    source_status_map = _get_bill_push_status_map(
        db,
        [bill_ref],
        account_book_number=normalized_account_book_number,
    )
    source_bills = [source_status_map[(int(bill.id), int(bill.community_id))]]
    source_bill_push_summary = _summarize_bill_push_statuses(source_bills)
    push_conflicts = _find_bill_push_conflicts(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})" for item in push_conflicts[:10]]
        )
        push_block_reason = f"Current bill already has voucher push records: {conflict_preview}"

    receipt_data = _serialize_receipt_bill_model(receipt_bill, db)
    enriched_receipt = _enrich_receipt_bill_data(receipt_data, receipt_bill=receipt_bill, db=db)
    user_context = _build_preview_user_context(
        current_user,
        x_account_book_id=x_account_book_id,
        x_account_book_name=x_account_book_name,
        x_account_book_number=x_account_book_number,
    )
    runtime_vars = build_variable_map(db, user_context=user_context)
    scoped_relation_records = {"bills": [enriched_bill]}

    match_result = _match_receipt_templates(
        receipt_bill=receipt_bill,
        enriched=enriched_receipt,
        runtime_vars=runtime_vars,
        db=db,
        scoped_relation_records=scoped_relation_records,
    )
    matched_template = match_result["matched_template"]
    matched_selected_records = match_result["matched_selected_records"]
    all_debug_logs = match_result["debug_logs"]
    templates = match_result["templates"]

    if not matched_template:
        return {
            "matched": False,
            "message": "No applicable receipt-root voucher template matched",
            "matched_root_source": "receipt_bills",
            "matched_via_receipt": False,
            "receipt_summary": _build_receipt_summary_payload(receipt_bill),
            "bill_summary": _build_bill_summary_payload(bill),
            "receipt_data": enriched_receipt,
            "templates_checked": len(templates),
            "debug_logs": all_debug_logs,
            "selected_bills": source_bills,
            "selected_bill_push_summary": source_bill_push_summary,
            "source_bills": source_bills,
            "source_bill_push_summary": source_bill_push_summary,
            "push_blocked": push_blocked,
            "push_block_reason": push_block_reason,
        }

    expression_context = dict(enriched_receipt)
    for record in matched_selected_records.values():
        for key, value in (record or {}).items():
            if isinstance(key, str) and "." in key:
                expression_context[key] = value

    def resolve_expr(expr: Optional[str], ctx: Optional[Dict[str, Any]] = None) -> str:
        resolved_with_globals = resolve_variables(expr or "", db, preloaded_vars=runtime_vars)
        return evaluate_expression(resolved_with_globals, ctx or expression_context)

    now = datetime.now()
    book_number = resolve_expr(matched_template.book_number_expr or "'BU-35256'")
    vouchertype_number = resolve_expr(matched_template.vouchertype_number_expr or "'0001'")
    attachment = resolve_expr(matched_template.attachment_expr or "0")
    biz_date = resolve_expr(matched_template.bizdate_expr or "{CURRENT_DATE}") or now.strftime("%Y-%m-%d")
    booked_date = resolve_expr(matched_template.bookeddate_expr or "{CURRENT_DATE}") or biz_date
    period_number = biz_date[:7].replace("-", "") if len(biz_date) >= 7 else now.strftime("%Y%m")

    accounting_entries = []
    kingdee_entries = []
    subject_names_cache = {}
    subject_type_cache = {}
    rule_relation_base_ctx = {
        "db": db,
        "root_record": receipt_bill,
        "receipt_bill": receipt_bill,
        "cache": {},
    }
    if scoped_relation_records is not None:
        rule_relation_base_ctx["scoped_records"] = scoped_relation_records

    for rule in sorted(matched_template.rules, key=lambda r: r.line_no):
        visible, rule_selected_records = _evaluate_rule_display_condition(
            rule.display_condition_expr,
            enriched_receipt,
            runtime_vars,
            rule_relation_base_ctx,
        )
        if not visible:
            continue
        rule_expression_context = _merge_selected_record_values(expression_context, rule_selected_records)
        summary = resolve_expr(rule.summary_expr, rule_expression_context)
        account_code = _normalize_literal_account_code(rule.account_code) or (rule.account_code or "").strip()
        amount_str = resolve_expr(rule.amount_expr, rule_expression_context)
        currency = resolve_expr(rule.currency_expr or "'CNY'", rule_expression_context)
        localrate = resolve_expr(rule.localrate_expr or "1", rule_expression_context)

        account_display_name = account_code
        if account_code:
            if account_code not in subject_names_cache:
                subj = db.query(models.AccountingSubject).filter(models.AccountingSubject.number == account_code).first()
                if subj:
                    subject_names_cache[account_code] = subj.fullname or subj.name
                    subject_type_cache[account_code] = subj.account_type_number or ""
                else:
                    subject_names_cache[account_code] = account_code
                    subject_type_cache[account_code] = ""

            fullname = subject_names_cache[account_code]
            if fullname != account_code:
                account_display_name = f"{account_code} {fullname}"

        amount_val = _try_parse_decimal(amount_str) or Decimal("0")
        localrate_val = _try_parse_decimal(localrate) or Decimal("1")

        assgrp = {}
        if rule.aux_items:
            try:
                aux_obj = json.loads(rule.aux_items)
                for dim_key, dim_config in aux_obj.items():
                    assgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        assgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json.JSONDecodeError, Exception):
                pass

        maincfassgrp = {}
        if rule.main_cf_assgrp:
            try:
                mcf_obj = json.loads(rule.main_cf_assgrp)
                for dim_key, dim_config in mcf_obj.items():
                    maincfassgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        maincfassgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json.JSONDecodeError, Exception):
                pass

        accounting_entries.append({
            "line_no": rule.line_no,
            "summary": summary,
            "account_code": account_code,
            "account_name": subject_names_cache.get(account_code, ""),
            "account_type_number": subject_type_cache.get(account_code, ""),
            "account_display": account_display_name,
            "dr_cr": rule.dr_cr,
            "debit": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "credit": _json_number(amount_val) if rule.dr_cr == "C" else 0.0,
            "currency": currency,
            "localrate": _json_number(localrate_val),
            "assgrp": assgrp if assgrp else None,
            "maincfassgrp": maincfassgrp if maincfassgrp else None,
        })

        kd_entry = {
            "seq": rule.line_no,
            "edescription": summary,
            "account_number": account_code,
            "currency_number": currency,
            "localrate": _json_number(localrate_val),
            "debitori": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "creditori": 0.0 if rule.dr_cr == "D" else _json_number(amount_val),
            "debitlocal": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "creditlocal": 0.0 if rule.dr_cr == "D" else _json_number(amount_val),
        }
        if assgrp:
            kd_entry["assgrp"] = assgrp
        if maincfassgrp:
            kd_entry["maincfassgrp"] = maincfassgrp
        kingdee_entries.append(kd_entry)

    total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in accounting_entries)
    total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in accounting_entries)

    kingdee_json = {
        "data": [{
            "book_number": book_number,
            "bizdate": biz_date,
            "bookeddate": booked_date,
            "period_number": period_number,
            "vouchertype_number": vouchertype_number,
            "description": (matched_template.template_name or matched_template.template_id or "UnnamedTemplate"),
            "attachment": _parse_attachment_count(attachment),
            "entries": kingdee_entries,
        }]
    }

    return {
        "matched": True,
        "matched_root_source": "receipt_bills",
        "matched_via_receipt": True,
        "template_id": matched_template.template_id,
        "template_name": matched_template.template_name,
        "receipt_summary": _build_receipt_summary_payload(receipt_bill),
        "bill_summary": _build_bill_summary_payload(bill),
        "matched_relation_sources": sorted(matched_selected_records.keys()),
        "accounting_view": {
            "entries": accounting_entries,
            "total_debit": _json_number(total_debit),
            "total_credit": _json_number(total_credit),
            "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
        },
        "kingdee_json": kingdee_json,
        "selected_bills": source_bills,
        "selected_bill_push_summary": source_bill_push_summary,
        "source_bills": source_bills,
        "source_bill_push_summary": source_bill_push_summary,
        "push_blocked": push_blocked,
        "push_block_reason": push_block_reason,
    }


@app.post("/api/vouchers/preview-receipt/{receipt_bill_id}")
def preview_voucher_for_receipt(
    receipt_bill_id: int,
    community_id: int = Query(..., description="Marki community ID"),
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    allow_bill_fallback: bool = True,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    from services.voucher_engine import evaluate_expression
    from utils.variable_parser import build_variable_map, resolve_variables
    import json as json_mod

    if allowed_community_ids and int(community_id) not in set(allowed_community_ids):
        raise HTTPException(status_code=403, detail="Unauthorized community")

    receipt_bill = (
        db.query(models.ReceiptBill)
        .options(selectinload(models.ReceiptBill.users))
        .filter(
            models.ReceiptBill.id == int(receipt_bill_id),
            models.ReceiptBill.community_id == int(community_id),
        )
        .first()
    )
    if not receipt_bill:
        raise HTTPException(status_code=404, detail="Receipt bill not found")

    normalized_account_book_number = _decode_header_value(x_account_book_number) or None
    source_bills = _collect_receipt_source_bills(
        db,
        receipt_bill_id=int(receipt_bill.id),
        community_id=int(receipt_bill.community_id),
        account_book_number=normalized_account_book_number,
    )
    source_bill_push_summary = _summarize_bill_push_statuses(source_bills)
    push_conflicts = _find_bill_push_conflicts(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})" for item in push_conflicts[:10]]
        )
        push_block_reason = f"Related bills already have voucher push records: {conflict_preview}"

    if source_bills:
        related_bills = (
            db.query(models.Bill)
            .filter(
                models.Bill.deal_log_id == int(receipt_bill.id),
                models.Bill.community_id == int(receipt_bill.community_id),
            )
            .order_by(models.Bill.id.asc())
            .all()
        )

        previews: List[Dict[str, Any]] = []
        skipped_bills: List[Dict[str, Any]] = []

        for related_bill in related_bills:
            bill_data = {}
            for col in models.Bill.__table__.columns:
                val = getattr(related_bill, col.name, None)
                if val is not None:
                    bill_data[col.name] = val if not hasattr(val, "isoformat") else val.isoformat()
                else:
                    bill_data[col.name] = None

            from decimal import Decimal as PyDecimal
            for key, val in bill_data.items():
                if isinstance(val, PyDecimal):
                    bill_data[key] = float(val)

            enriched_bill = mapping_enrich_source_data("bills", bill_data, db=db)
            result = _preview_voucher_for_bill_via_receipt_templates(
                bill=related_bill,
                enriched_bill=enriched_bill,
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
            )

            if not result or not result.get("matched"):
                skipped_bills.append({
                    "bill_id": int(related_bill.id),
                    "community_id": int(related_bill.community_id),
                    "reason": "template not matched",
                })
                continue

            previews.append(result)

        if previews:
            first_preview = previews[0]
            first_header = ((first_preview.get("kingdee_json") or {}).get("data") or [{}])[0]
            header_keys = ["book_number", "bookeddate", "period_number", "vouchertype_number"]
            merged_bizdates = [str(first_header.get("bizdate") or "").strip()]

            header_compatible_previews: List[Dict[str, Any]] = [first_preview]
            for preview in previews[1:]:
                header = ((preview.get("kingdee_json") or {}).get("data") or [{}])[0]
                incompatible_keys = [k for k in header_keys if first_header.get(k) != header.get(k)]
                if incompatible_keys:
                    summary = preview.get("bill_summary") or {}
                    skipped_bills.append({
                        "bill_id": int(summary.get("id") or 0),
                        "community_id": int(summary.get("community_id") or 0),
                        "reason": f"inconsistent voucher header ({', '.join(incompatible_keys)}); skipped from merge",
                    })
                    continue
                merged_bizdates.append(str(header.get("bizdate") or "").strip())
                header_compatible_previews.append(preview)

            previews = header_compatible_previews
            merged_bizdate = max([d for d in merged_bizdates if d], default=str(first_header.get("bizdate") or ""))

            matched_source_bills: List[Dict[str, Any]] = []
            seen_source_keys = set()
            for preview in previews:
                for source_bill in preview.get("source_bills") or []:
                    key = (
                        int(source_bill.get("bill_id") or 0),
                        int(source_bill.get("community_id") or 0),
                    )
                    if key in seen_source_keys:
                        continue
                    seen_source_keys.add(key)
                    matched_source_bills.append(source_bill)

            merged_entries: List[Dict[str, Any]] = []
            merged_accounting_entries: List[Dict[str, Any]] = []
            seq = 1

            for preview in previews:
                kd_header = ((preview.get("kingdee_json") or {}).get("data") or [{}])[0]
                for entry in kd_header.get("entries") or []:
                    item = dict(entry)
                    item["seq"] = seq
                    merged_entries.append(item)
                    seq += 1

                acct_entries = ((preview.get("accounting_view") or {}).get("entries") or [])
                for entry in acct_entries:
                    item = dict(entry)
                    item["line_no"] = len(merged_accounting_entries) + 1
                    merged_accounting_entries.append(item)

            total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in merged_accounting_entries)
            total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in merged_accounting_entries)

            merged_template_ids = sorted({str(p.get("template_id") or "") for p in previews if p.get("template_id")})
            template_name = first_preview.get("template_name") or first_preview.get("template_id") or "ReceiptMerged"
            merged_kingdee_json = {
                "data": [{
                    "book_number": first_header.get("book_number"),
                    "bizdate": merged_bizdate,
                    "bookeddate": first_header.get("bookeddate"),
                    "period_number": first_header.get("period_number"),
                    "vouchertype_number": first_header.get("vouchertype_number"),
                    "description": template_name,
                    "attachment": first_header.get("attachment", 0),
                    "entries": merged_entries,
                }]
            }

            matched_source_bill_push_summary = _summarize_bill_push_statuses(matched_source_bills)
            matched_push_conflicts = _find_bill_push_conflicts(matched_source_bills)
            merged_push_blocked = len(matched_push_conflicts) > 0
            merged_push_block_reason = None
            if merged_push_blocked:
                conflict_preview = ", ".join(
                    [
                        f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})"
                        for item in matched_push_conflicts[:10]
                    ]
                )
                merged_push_block_reason = f"Related bills already have voucher push records: {conflict_preview}"

            return {
                "matched": True,
                "partial_matched": len(skipped_bills) > 0,
                "matched_root_source": "receipt_bills",
                "matched_via_receipt": False,
                "matched_bills": len(previews),
                "template_id": first_preview.get("template_id"),
                "template_name": first_preview.get("template_name"),
                "template_ids": merged_template_ids,
                "receipt_summary": _build_receipt_summary_payload(receipt_bill),
                "selected_bills": source_bills,
                "selected_bill_push_summary": source_bill_push_summary,
                "source_bills": matched_source_bills,
                "source_bill_push_summary": matched_source_bill_push_summary,
                "skipped_bills": skipped_bills,
                "push_blocked": merged_push_blocked,
                "push_block_reason": merged_push_block_reason,
                "accounting_view": {
                    "entries": merged_accounting_entries,
                    "total_debit": _json_number(total_debit),
                    "total_credit": _json_number(total_credit),
                    "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
                },
                "kingdee_json": merged_kingdee_json,
            }

    receipt_data = _serialize_receipt_bill_model(receipt_bill, db)
    enriched = _enrich_receipt_bill_data(receipt_data, receipt_bill=receipt_bill, db=db)

    user_context = _build_preview_user_context(
        current_user,
        x_account_book_id=x_account_book_id,
        x_account_book_name=x_account_book_name,
        x_account_book_number=x_account_book_number,
    )
    runtime_vars = build_variable_map(db, user_context=user_context)

    match_result = _match_receipt_templates(
        receipt_bill=receipt_bill,
        enriched=enriched,
        runtime_vars=runtime_vars,
        db=db,
    )
    templates = match_result["templates"]
    matched_template = match_result["matched_template"]
    matched_selected_records = match_result["matched_selected_records"]
    all_debug_logs = match_result["debug_logs"]

    if not matched_template:
        if allow_bill_fallback and source_bills:
            return preview_voucher_for_bills(
                payload=schemas.BatchVoucherPreviewRequest(
                    bills=[schemas.BillPreviewRef(bill_id=int(item["bill_id"]), community_id=int(item["community_id"])) for item in source_bills]
                ),
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
                allowed_community_ids=allowed_community_ids,
            )

        return {
            "matched": False,
            "message": "No applicable voucher template matched",
            "matched_root_source": "receipt_bills",
            "matched_via_receipt": False,
            "receipt_summary": {
                "id": receipt_bill.id,
                "community_id": receipt_bill.community_id,
                "receipt_id": receipt_bill.receipt_id,
                "deal_type": receipt_bill.deal_type,
                "deal_type_label": RECEIPT_BILL_DEAL_TYPE_LABELS.get(receipt_bill.deal_type, "其他"),
                "income_amount": _json_number(receipt_bill.income_amount),
                "amount": _json_number(receipt_bill.amount),
                "asset_name": receipt_bill.asset_name,
            },
            "receipt_summary": _build_receipt_summary_payload(receipt_bill),
            "receipt_data": enriched,
            "templates_checked": len(templates),
            "debug_logs": all_debug_logs,
            "selected_bills": source_bills,
            "selected_bill_push_summary": source_bill_push_summary,
            "source_bills": source_bills,
            "source_bill_push_summary": source_bill_push_summary,
            "push_blocked": push_blocked,
            "push_block_reason": push_block_reason,
        }

    expression_context = dict(enriched)
    for record in matched_selected_records.values():
        for key, value in (record or {}).items():
            if isinstance(key, str) and "." in key:
                expression_context[key] = value

    def resolve_expr(expr: Optional[str], ctx: Optional[Dict[str, Any]] = None) -> str:
        resolved_with_globals = resolve_variables(expr or "", db, preloaded_vars=runtime_vars)
        return evaluate_expression(resolved_with_globals, ctx or expression_context)

    now = datetime.now()
    book_number = resolve_expr(matched_template.book_number_expr or "'BU-35256'")
    vouchertype_number = resolve_expr(matched_template.vouchertype_number_expr or "'0001'")
    attachment = resolve_expr(matched_template.attachment_expr or "0")
    biz_date = resolve_expr(matched_template.bizdate_expr or "{CURRENT_DATE}") or now.strftime("%Y-%m-%d")
    booked_date = resolve_expr(matched_template.bookeddate_expr or "{CURRENT_DATE}") or biz_date
    period_number = biz_date[:7].replace("-", "") if len(biz_date) >= 7 else now.strftime("%Y%m")

    accounting_entries = []
    kingdee_entries = []
    subject_names_cache = {}
    subject_type_cache = {}
    rule_relation_base_ctx = {
        "db": db,
        "root_record": receipt_bill,
        "receipt_bill": receipt_bill,
        "cache": {},
    }

    for rule in sorted(matched_template.rules, key=lambda r: r.line_no):
        visible, rule_selected_records = _evaluate_rule_display_condition(
            rule.display_condition_expr,
            enriched,
            runtime_vars,
            rule_relation_base_ctx,
        )
        if not visible:
            continue
        rule_expression_context = _merge_selected_record_values(expression_context, rule_selected_records)
        summary = resolve_expr(rule.summary_expr, rule_expression_context)
        account_code = _normalize_literal_account_code(rule.account_code) or (rule.account_code or "").strip()
        amount_str = resolve_expr(rule.amount_expr, rule_expression_context)
        currency = resolve_expr(rule.currency_expr or "'CNY'", rule_expression_context)
        localrate = resolve_expr(rule.localrate_expr or "1", rule_expression_context)

        account_display_name = account_code
        if account_code:
            if account_code not in subject_names_cache:
                subj = db.query(models.AccountingSubject).filter(models.AccountingSubject.number == account_code).first()
                if subj:
                    subject_names_cache[account_code] = subj.fullname or subj.name
                    subject_type_cache[account_code] = subj.account_type_number or ""
                else:
                    subject_names_cache[account_code] = account_code
                    subject_type_cache[account_code] = ""

            fullname = subject_names_cache[account_code]
            if fullname != account_code:
                account_display_name = f"{account_code} {fullname}"

        amount_val = _try_parse_decimal(amount_str) or Decimal("0")
        localrate_val = _try_parse_decimal(localrate) or Decimal("1")

        assgrp = {}
        if rule.aux_items:
            try:
                aux_obj = json_mod.loads(rule.aux_items)
                for dim_key, dim_config in aux_obj.items():
                    assgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        assgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json_mod.JSONDecodeError, Exception):
                pass

        maincfassgrp = {}
        if rule.main_cf_assgrp:
            try:
                mcf_obj = json_mod.loads(rule.main_cf_assgrp)
                for dim_key, dim_config in mcf_obj.items():
                    maincfassgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        maincfassgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json_mod.JSONDecodeError, Exception):
                pass

        accounting_entries.append({
            "line_no": rule.line_no,
            "summary": summary,
            "account_code": account_code,
            "account_name": subject_names_cache.get(account_code, ""),
            "account_type_number": subject_type_cache.get(account_code, ""),
            "account_display": account_display_name,
            "dr_cr": rule.dr_cr,
            "debit": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "credit": _json_number(amount_val) if rule.dr_cr == "C" else 0.0,
            "currency": currency,
            "localrate": _json_number(localrate_val),
            "assgrp": assgrp if assgrp else None,
            "maincfassgrp": maincfassgrp if maincfassgrp else None,
        })

        kd_entry = {
            "seq": rule.line_no,
            "edescription": summary,
            "account_number": account_code,
            "currency_number": currency,
            "localrate": _json_number(localrate_val),
            "debitori": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "creditori": 0.0 if rule.dr_cr == "D" else _json_number(amount_val),
            "debitlocal": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "creditlocal": 0.0 if rule.dr_cr == "D" else _json_number(amount_val),
        }
        if assgrp:
            kd_entry["assgrp"] = assgrp
        if maincfassgrp:
            kd_entry["maincfassgrp"] = maincfassgrp
        kingdee_entries.append(kd_entry)

    total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in accounting_entries)
    total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in accounting_entries)

    kingdee_json = {
        "data": [{
            "book_number": book_number,
            "bizdate": biz_date,
            "bookeddate": booked_date,
            "period_number": period_number,
            "vouchertype_number": vouchertype_number,
            "description": (matched_template.template_name or matched_template.template_id or "UnnamedTemplate"),
            "attachment": _parse_attachment_count(attachment),
            "entries": kingdee_entries,
        }]
    }

    return {
        "matched": True,
        "matched_root_source": "receipt_bills",
        "matched_via_receipt": False,
        "template_id": matched_template.template_id,
        "template_name": matched_template.template_name,
        "receipt_summary": {
            "id": receipt_bill.id,
            "community_id": receipt_bill.community_id,
            "receipt_id": receipt_bill.receipt_id,
            "deal_type": receipt_bill.deal_type,
            "deal_type_label": RECEIPT_BILL_DEAL_TYPE_LABELS.get(receipt_bill.deal_type, "其他"),
            "income_amount": _json_number(receipt_bill.income_amount),
            "amount": _json_number(receipt_bill.amount),
            "asset_name": receipt_bill.asset_name,
        },
        "receipt_summary": _build_receipt_summary_payload(receipt_bill),
        "matched_relation_sources": sorted(matched_selected_records.keys()),
        "accounting_view": {
            "entries": accounting_entries,
            "total_debit": _json_number(total_debit),
            "total_credit": _json_number(total_credit),
            "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
        },
        "kingdee_json": kingdee_json,
        "selected_bills": source_bills,
        "selected_bill_push_summary": source_bill_push_summary,
        "source_bills": source_bills,
        "source_bill_push_summary": source_bill_push_summary,
        "push_blocked": push_blocked,
        "push_block_reason": push_block_reason,
    }


@app.post("/api/vouchers/preview-receipts")
def preview_voucher_for_receipts(
    payload: schemas.BatchReceiptVoucherPreviewRequest,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    if not payload.receipts:
        raise HTTPException(status_code=400, detail="No receipts selected")

    unique_refs = _normalize_receipt_refs(payload.receipts)
    if not allowed_community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    allowed_set = set(allowed_community_ids)
    unauthorized = [r for r in unique_refs if int(r["community_id"]) not in allowed_set]
    if unauthorized:
        bad = ", ".join([f"{r['community_id']}:{r['receipt_bill_id']}" for r in unauthorized[:10]])
        raise HTTPException(status_code=403, detail=f"Unauthorized receipt communities: {bad}")

    previews: List[Dict[str, Any]] = []
    skipped_bills: List[Dict[str, Any]] = []

    for ref in unique_refs:
        try:
            result = preview_voucher_for_receipt(
                receipt_bill_id=int(ref["receipt_bill_id"]),
                community_id=int(ref["community_id"]),
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
                allowed_community_ids=allowed_community_ids,
            )
            if not result.get("matched"):
                skipped_bills.append({
                    "bill_id": int(ref["receipt_bill_id"]),
                    "community_id": int(ref["community_id"]),
                    "reason": "template not matched",
                })
                continue
            previews.append(result)
        except HTTPException as exc:
            skipped_bills.append({
                "bill_id": int(ref["receipt_bill_id"]),
                "community_id": int(ref["community_id"]),
                "reason": exc.detail if isinstance(exc.detail, str) else str(exc.detail),
            })

    if not previews:
        details = "; ".join([f"{b['community_id']}:{b['bill_id']} -> {b['reason']}" for b in skipped_bills[:20]])
        raise HTTPException(
            status_code=400,
            detail=("No vouchers could be generated" + (f": {details}" if details else "")),
        )

    first_preview = previews[0]
    first_header = ((first_preview.get("kingdee_json") or {}).get("data") or [{}])[0]
    header_keys = ["book_number", "bookeddate", "period_number", "vouchertype_number"]
    merged_bizdates = [str(first_header.get("bizdate") or "").strip()]

    header_compatible_previews: List[Dict[str, Any]] = [first_preview]
    for preview in previews[1:]:
        header = ((preview.get("kingdee_json") or {}).get("data") or [{}])[0]
        incompatible_keys = [k for k in header_keys if first_header.get(k) != header.get(k)]
        if incompatible_keys:
            summary = preview.get("receipt_summary") or {}
            skipped_bills.append({
                "bill_id": int(summary.get("id") or 0),
                "community_id": int(summary.get("community_id") or 0),
                "reason": f"inconsistent voucher header ({', '.join(incompatible_keys)}); skipped from merge",
            })
            continue
        merged_bizdates.append(str(header.get("bizdate") or "").strip())
        header_compatible_previews.append(preview)

    previews = header_compatible_previews
    merged_bizdate = max([d for d in merged_bizdates if d], default=str(first_header.get("bizdate") or ""))

    source_bills: List[Dict[str, Any]] = []
    seen_source_keys = set()
    for preview in previews:
        for source_bill in preview.get("source_bills") or []:
            key = (int(source_bill.get("bill_id") or 0), int(source_bill.get("community_id") or 0))
            if key in seen_source_keys:
                continue
            seen_source_keys.add(key)
            source_bills.append(source_bill)

    source_bill_push_summary = _summarize_bill_push_statuses(source_bills)
    push_conflicts = _find_bill_push_conflicts(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})" for item in push_conflicts[:10]]
        )
        push_block_reason = f"Selected source bills already have pushed or pushing voucher records: {conflict_preview}"

    merged_entries: List[Dict[str, Any]] = []
    merged_accounting_entries: List[Dict[str, Any]] = []
    seq = 1
    for preview in previews:
        kd_header = ((preview.get("kingdee_json") or {}).get("data") or [{}])[0]
        for entry in kd_header.get("entries") or []:
            item = dict(entry)
            item["seq"] = seq
            merged_entries.append(item)
            seq += 1

        for entry in (preview.get("accounting_view") or {}).get("entries") or []:
            item = dict(entry)
            item["line_no"] = len(merged_accounting_entries) + 1
            merged_accounting_entries.append(item)

    total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in merged_accounting_entries)
    total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in merged_accounting_entries)
    merged_template_ids = sorted({str(p.get("template_id") or "") for p in previews if p.get("template_id")})
    template_name = first_preview.get("template_name") or first_preview.get("template_id") or "BatchMerged"
    merged_kingdee_json = {
        "data": [{
            "book_number": first_header.get("book_number"),
            "bizdate": merged_bizdate,
            "bookeddate": first_header.get("bookeddate"),
            "period_number": first_header.get("period_number"),
            "vouchertype_number": first_header.get("vouchertype_number"),
            "description": template_name,
            "attachment": first_header.get("attachment", 0),
            "entries": merged_entries,
        }]
    }

    return {
        "matched": True,
        "partial_matched": len(skipped_bills) > 0,
        "matched_bills": len(previews),
        "skipped_bills": skipped_bills,
        "template_id": first_preview.get("template_id"),
        "template_name": first_preview.get("template_name"),
        "template_ids": merged_template_ids,
        "source_bills": source_bills,
        "source_bill_push_summary": source_bill_push_summary,
        "push_blocked": push_blocked,
        "push_block_reason": push_block_reason,
        "accounting_view": {
            "entries": merged_accounting_entries,
            "total_debit": _json_number(total_debit),
            "total_credit": _json_number(total_credit),
            "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
        },
        "kingdee_json": merged_kingdee_json,
    }


@app.post("/api/vouchers/preview-bill/{bill_id}")
def preview_voucher_for_bill(
    bill_id: int,
    community_id: Optional[int] = None,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    allow_receipt_fallback: bool = True,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    """
    预览单笔账单对应的凭证内容。

    返回包含两个部分：
    1. `accounting_view`: 面向业务查看的会计凭证视图
    2. `kingdee_json`: 可直接用于 `voucherAdd` 的金蝶请求 JSON
    """
    from services.voucher_engine import evaluate_expression
    from utils.variable_parser import build_variable_map, resolve_variables
    import json as json_mod
    from datetime import datetime

    # 1. Query bill
    bill_query = db.query(models.Bill).filter(models.Bill.id == bill_id)
    if community_id is not None:
        bill = bill_query.filter(models.Bill.community_id == community_id).first()
        if not bill:
            raise HTTPException(
                status_code=404,
                detail=f"Bill not found: id={bill_id}, community_id={community_id}"
            )
    else:
        candidates = bill_query.limit(2).all()
        if not candidates:
            raise HTTPException(status_code=404, detail=f"Bill not found: id={bill_id}")
        if len(candidates) > 1:
            raise HTTPException(
                status_code=400,
                detail="bill_id is not unique across communities; please pass community_id"
            )
        bill = candidates[0]

    if not allowed_community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    if int(bill.community_id) not in set(allowed_community_ids):
        raise HTTPException(
            status_code=403,
            detail=f"Unauthorized bill community: {bill.community_id}"
        )

    normalized_account_book_number = _decode_header_value(x_account_book_number) or None
    source_refs = [{
        "bill_id": int(bill.id),
        "community_id": int(bill.community_id),
    }]
    source_status_map = _get_bill_push_status_map(
        db,
        source_refs,
        account_book_number=normalized_account_book_number,
    )
    source_bills = [source_status_map[(int(bill.id), int(bill.community_id))]]
    source_bill_push_summary = _summarize_bill_push_statuses(source_bills)
    push_conflicts = _find_bill_push_conflicts(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [
                f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})"
                for item in push_conflicts[:10]
            ]
        )
        push_block_reason = f"Current bill already has voucher push records: {conflict_preview}"

    # 鐏忓棜澶勯崡?ORM 鐎电钖勬潪璐熺€涙鍚€
    bill_data = {}
    for col in models.Bill.__table__.columns:
        val = getattr(bill, col.name, None)
        if val is not None:
            bill_data[col.name] = val if not hasattr(val, 'isoformat') else val.isoformat()
        else:
            bill_data[col.name] = None
    # Convert Decimal values to float for preview payload
    from decimal import Decimal as PyDecimal
    for k, v in bill_data.items():
        if isinstance(v, PyDecimal):
            bill_data[k] = float(v)

    # 2. 閹碘晛鐫嶉弫鐗堝祦閿涘牐鎷烽崝鐘诲櫨閾︽儼閻㈢喎鐡у▓纰夌礆
    enriched = mapping_enrich_source_data("bills", bill_data, db=db)

    # 3. Match candidate templates
    templates = db.query(models.VoucherTemplate).filter(
        models.VoucherTemplate.active == True,
        or_(
            models.VoucherTemplate.source_type == 'bills',
            models.VoucherTemplate.source_type.is_(None),
            models.VoucherTemplate.source_type == ''
        )
    ).order_by(
        models.VoucherTemplate.priority.asc(),
        models.VoucherTemplate.template_id.asc()
    ).all()

    # 鏋勫缓鐢ㄦ埛鍙婅处濂椾笂涓嬫枃
    from urllib.parse import unquote
    org_name = current_user.organization.name if current_user.organization else "未分配"
    user_context = {
        "current_user_id": str(current_user.id),
        "current_username": current_user.username,
        "current_user_realname": current_user.real_name or current_user.username,
        "current_org_id": str(current_user.org_id) if current_user.org_id else "",
        "current_org_name": org_name,
        "current_account_book_id": unquote(x_account_book_id) if x_account_book_id else "",
        "current_account_book_name": unquote(x_account_book_name) if x_account_book_name else "",
        "current_account_book_number": unquote(x_account_book_number) if x_account_book_number else "",
    }

    runtime_vars = build_variable_map(db, user_context=user_context)
    def resolve_expr(expr: Optional[str], ctx: Optional[Dict[str, Any]] = None) -> str:
        resolved_with_globals = resolve_variables(expr or '', db, preloaded_vars=runtime_vars)
        return evaluate_expression(resolved_with_globals, ctx or enriched)

    matched_template = None
    all_debug_logs = {}

    conditional_templates = [t for t in templates if t.trigger_condition]
    fallback_templates = [t for t in templates if not t.trigger_condition]

    for tmpl in conditional_templates:
        try:
            conditions = json_mod.loads(tmpl.trigger_condition)
            debug_logs = []
            if _check_trigger_conditions(conditions, enriched, debug_logs, runtime_vars):
                matched_template = tmpl
                break
            all_debug_logs[tmpl.template_name] = debug_logs
        except (json_mod.JSONDecodeError, Exception) as e:
            all_debug_logs[tmpl.template_name] = [f"JSON Parse Error: {e}"]
            continue

    if not matched_template and fallback_templates:
        matched_template = fallback_templates[0]

    if not matched_template:
        receipt_debug_logs = {}
        if allow_receipt_fallback and bill.deal_log_id:
            receipt_result = _preview_voucher_for_bill_via_receipt_templates(
                bill=bill,
                enriched_bill=enriched,
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
            )
            if receipt_result and receipt_result.get("matched"):
                return receipt_result
            if receipt_result:
                receipt_debug_logs = receipt_result.get("debug_logs") or {}

        combined_debug_logs = dict(all_debug_logs)
        for template_name, logs in receipt_debug_logs.items():
            combined_debug_logs[f"[receipt_bills] {template_name}"] = logs

        return {
            "matched": False,
            "message": "No applicable voucher template matched",
            "matched_root_source": "bills",
            "matched_via_receipt": False,
            "bill_summary": _build_bill_summary_payload(bill),
            "bill_data": enriched,
            "templates_checked": len(templates),
            "debug_logs": combined_debug_logs,
            "selected_bills": source_bills,
            "selected_bill_push_summary": source_bill_push_summary,
            "source_bills": source_bills,
            "source_bill_push_summary": source_bill_push_summary,
            "push_blocked": push_blocked,
            "push_block_reason": push_block_reason,
        }

    # 4. Resolve template header expressions
    now = datetime.now()
    book_number = resolve_expr(matched_template.book_number_expr or "'BU-35256'")
    vouchertype_number = resolve_expr(matched_template.vouchertype_number_expr or "'0001'")
    attachment = resolve_expr(matched_template.attachment_expr or "0")
    period_number = now.strftime("%Y%m")
    biz_date = resolve_expr(matched_template.bizdate_expr or "{CURRENT_DATE}") or now.strftime("%Y-%m-%d")
    booked_date = resolve_expr(matched_template.bookeddate_expr or "{CURRENT_DATE}") or now.strftime("%Y-%m-%d")

    def parse_attachment_count(value: str) -> int:
        try:
            return max(int(float(value)), 0)
        except (TypeError, ValueError):
            return 0

    # 5. 瑙ｆ瀽姣忔潯鍒嗗綍瑙勫垯
    accounting_entries = []
    kingdee_entries = []

    # Prepare subject naming cache
    subject_names_cache = {}
    subject_type_cache = {}
    expression_context = dict(enriched)

    for rule in sorted(matched_template.rules, key=lambda r: r.line_no):
        visible, rule_selected_records = _evaluate_rule_display_condition(
            rule.display_condition_expr,
            enriched,
            runtime_vars,
        )
        if not visible:
            continue
        rule_expression_context = _merge_selected_record_values(expression_context, rule_selected_records)
        summary = resolve_expr(rule.summary_expr, rule_expression_context)
        account_code = _normalize_literal_account_code(rule.account_code) or (rule.account_code or "").strip()
        amount_str = resolve_expr(rule.amount_expr, rule_expression_context)
        currency = resolve_expr(rule.currency_expr or "'CNY'", rule_expression_context)
        localrate = resolve_expr(rule.localrate_expr or "1", rule_expression_context)

        # Fetch subject name for display
        account_display_name = account_code
        if account_code:
            if account_code not in subject_names_cache:
                subj = db.query(models.AccountingSubject).filter(models.AccountingSubject.number == account_code).first()
                if subj:
                    # Use fullname if available, else name
                    subject_names_cache[account_code] = subj.fullname or subj.name
                    subject_type_cache[account_code] = subj.account_type_number or ""
                else:
                    subject_names_cache[account_code] = account_code
                    subject_type_cache[account_code] = ""
            
            fullname = subject_names_cache[account_code]
            if fullname != account_code:
                account_display_name = f"{account_code} {fullname}"

        amount_val = _try_parse_decimal(amount_str) or Decimal("0")
        localrate_val = _try_parse_decimal(localrate) or Decimal("1")

        # 瑙ｆ瀽杈呭姪鏍哥畻
        assgrp = {}
        if rule.aux_items:
            try:
                aux_obj = json_mod.loads(rule.aux_items)
                for dim_key, dim_config in aux_obj.items():
                    assgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        assgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json_mod.JSONDecodeError, Exception):
                pass

        # 瑙ｆ瀽涓昏〃鏍哥畻
        maincfassgrp = {}
        if rule.main_cf_assgrp:
            try:
                mcf_obj = json_mod.loads(rule.main_cf_assgrp)
                for dim_key, dim_config in mcf_obj.items():
                    maincfassgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        maincfassgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json_mod.JSONDecodeError, Exception):
                pass

        # 浼犵粺浼氳鍑瘉鏍煎紡
        accounting_entries.append({
            "line_no": rule.line_no,
            "summary": summary,
            "account_code": account_code,
            "account_name": subject_names_cache.get(account_code, ""),
            "account_type_number": subject_type_cache.get(account_code, ""),
            "account_display": account_display_name,
            "dr_cr": rule.dr_cr,
            "debit": _json_number(amount_val) if rule.dr_cr == 'D' else 0.0,
            "credit": _json_number(amount_val) if rule.dr_cr == 'C' else 0.0,
            "currency": currency,
            "localrate": _json_number(localrate_val),
            "assgrp": assgrp if assgrp else None,
            "maincfassgrp": maincfassgrp if maincfassgrp else None,
        })

        # 闁叉垼婢?API JSON 閺嶇厧绱?
        kd_entry = {
            "seq": rule.line_no,
            "edescription": summary,
            "account_number": account_code,
            "currency_number": currency,
            "localrate": _json_number(localrate_val),
            "debitori": _json_number(amount_val) if rule.dr_cr == 'D' else 0.0,
            "creditori": 0.0 if rule.dr_cr == 'D' else _json_number(amount_val),
            "debitlocal": _json_number(amount_val) if rule.dr_cr == 'D' else 0.0,
            "creditlocal": 0.0 if rule.dr_cr == 'D' else _json_number(amount_val),
        }
        if assgrp:
            kd_entry["assgrp"] = assgrp
        if maincfassgrp:
            kd_entry["maincfassgrp"] = maincfassgrp
        kingdee_entries.append(kd_entry)

    total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in accounting_entries)
    total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in accounting_entries)

    # 6. 缂佸嫯鐎瑰本鏆ｉ惃鍕櫨閾﹁埖甯归柅?JSON 缂佹挻鐎?
    kingdee_json = {
        "data": [{
            "book_number": book_number,
            "bizdate": biz_date,
            "bookeddate": booked_date,
            "period_number": period_number,
            "vouchertype_number": vouchertype_number,
            "description": (matched_template.template_name or matched_template.template_id or "未命名模板"),
            "attachment": _parse_attachment_count(attachment),
            "entries": kingdee_entries
        }]
    }

    return {
        "matched": True,
        "matched_root_source": "bills",
        "matched_via_receipt": False,
        "template_id": matched_template.template_id,
        "template_name": matched_template.template_name,
        "bill_summary": _build_bill_summary_payload(bill),
        "enriched_fields": {k: v for k, v in enriched.items() if k.startswith('kd_')},
        "accounting_view": {
            "entries": accounting_entries,
            "total_debit": _json_number(total_debit),
            "total_credit": _json_number(total_credit),
            "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
        },
        "kingdee_json": kingdee_json,
        "selected_bills": source_bills,
        "selected_bill_push_summary": source_bill_push_summary,
        "source_bills": source_bills,
        "source_bill_push_summary": source_bill_push_summary,
        "push_blocked": push_blocked,
        "push_block_reason": push_block_reason,
    }


@app.post("/api/vouchers/preview-bills")
def preview_voucher_for_bills(
    payload: schemas.BatchVoucherPreviewRequest,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    if not payload.bills:
        raise HTTPException(status_code=400, detail="No bills selected")

    unique_refs = _normalize_bill_refs(payload.bills)

    if not allowed_community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    allowed_set = set(allowed_community_ids)
    unauthorized = [r for r in unique_refs if int(r["community_id"]) not in allowed_set]
    if unauthorized:
        bad = ", ".join([f"{r['community_id']}:{r['bill_id']}" for r in unauthorized[:10]])
        raise HTTPException(status_code=403, detail=f"Unauthorized bill communities: {bad}")

    normalized_account_book_number = _decode_header_value(x_account_book_number) or None
    selected_status_map = _get_bill_push_status_map(
        db,
        unique_refs,
        account_book_number=normalized_account_book_number,
    )
    selected_bills = [
        selected_status_map[(ref["bill_id"], ref["community_id"])]
        for ref in unique_refs
    ]

    previews: List[Dict[str, Any]] = []
    skipped_bills: List[Dict[str, Any]] = []

    for ref in unique_refs:
        try:
            result = preview_voucher_for_bill(
                bill_id=int(ref["bill_id"]),
                community_id=int(ref["community_id"]),
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
                allowed_community_ids=allowed_community_ids,
            )
            if not result.get("matched"):
                skipped_bills.append({
                    "bill_id": int(ref["bill_id"]),
                    "community_id": int(ref["community_id"]),
                    "reason": "template not matched",
                })
                continue
            previews.append(result)
        except HTTPException as exc:
            skipped_bills.append({
                "bill_id": int(ref["bill_id"]),
                "community_id": int(ref["community_id"]),
                "reason": exc.detail if isinstance(exc.detail, str) else str(exc.detail),
            })

    if not previews:
        details = "; ".join([f"{b['community_id']}:{b['bill_id']} -> {b['reason']}" for b in skipped_bills[:20]])
        raise HTTPException(
            status_code=400,
            detail=("No vouchers could be generated" + (f": {details}" if details else ""))
        )

    # Merge across different templates; keep business date flexible so receipts from
    # different transaction days in the same batch can still produce a single voucher.
    first_preview = previews[0]
    first_header = ((first_preview.get("kingdee_json") or {}).get("data") or [{}])[0]
    header_keys = ["book_number", "bookeddate", "period_number", "vouchertype_number"]
    merged_bizdates = [
        str(first_header.get("bizdate") or "").strip()
    ]

    header_compatible_previews: List[Dict[str, Any]] = [first_preview]
    for p in previews[1:]:
        header = ((p.get("kingdee_json") or {}).get("data") or [{}])[0]
        incompatible_keys = [k for k in header_keys if first_header.get(k) != header.get(k)]
        if incompatible_keys:
            summary = p.get("bill_summary") or {}
            skipped_bills.append({
                "bill_id": int(summary.get("id") or 0),
                "community_id": int(summary.get("community_id") or 0),
                "reason": f"inconsistent voucher header ({', '.join(incompatible_keys)}); skipped from merge",
            })
            continue
        merged_bizdates.append(str(header.get("bizdate") or "").strip())
        header_compatible_previews.append(p)

    previews = header_compatible_previews
    merged_bizdate = max([d for d in merged_bizdates if d], default=str(first_header.get("bizdate") or ""))

    source_bills: List[Dict[str, Any]] = []
    seen_source_keys = set()
    for preview in previews:
        for source_bill in preview.get("source_bills") or []:
            key = (
                int(source_bill.get("bill_id") or 0),
                int(source_bill.get("community_id") or 0),
            )
            if key in seen_source_keys:
                continue
            seen_source_keys.add(key)
            source_bills.append(source_bill)

    selected_bill_push_summary = _summarize_bill_push_statuses(selected_bills)
    source_bill_push_summary = _summarize_bill_push_statuses(source_bills)
    push_conflicts = _find_bill_push_conflicts(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [
                f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})"
                for item in push_conflicts[:10]
            ]
        )
        push_block_reason = f"Selected bills already have pushed or pushing voucher records: {conflict_preview}"

    merged_entries: List[Dict[str, Any]] = []
    merged_accounting_entries: List[Dict[str, Any]] = []
    seq = 1

    for p in previews:
        kd_header = ((p.get("kingdee_json") or {}).get("data") or [{}])[0]
        kd_entries = kd_header.get("entries") or []
        for entry in kd_entries:
            e = dict(entry)
            e["seq"] = seq
            merged_entries.append(e)
            seq += 1

        acct_view = p.get("accounting_view") or {}
        acct_entries = acct_view.get("entries") or []
        for ae in acct_entries:
            entry = dict(ae)
            entry["line_no"] = len(merged_accounting_entries) + 1
            merged_accounting_entries.append(entry)

    total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in merged_accounting_entries)
    total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in merged_accounting_entries)

    merged_template_ids = sorted({str(p.get("template_id") or "") for p in previews if p.get("template_id")})
    template_name = first_preview.get("template_name") or first_preview.get("template_id") or "BatchMerged"
    merged_kingdee_json = {
        "data": [{
            "book_number": first_header.get("book_number"),
            "bizdate": merged_bizdate,
            "bookeddate": first_header.get("bookeddate"),
            "period_number": first_header.get("period_number"),
            "vouchertype_number": first_header.get("vouchertype_number"),
            "description": template_name,
            "attachment": first_header.get("attachment", 0),
            "entries": merged_entries,
        }]
    }

    return {
        "matched": True,
        "partial_matched": len(skipped_bills) > 0,
        "matched_bills": len(previews),
        "skipped_bills": skipped_bills,
        "template_id": first_preview.get("template_id"),
        "template_name": first_preview.get("template_name"),
        "template_ids": merged_template_ids,
        "selected_bills": selected_bills,
        "selected_bill_push_summary": selected_bill_push_summary,
        "source_bills": source_bills,
        "source_bill_push_summary": source_bill_push_summary,
        "push_blocked": push_blocked,
        "push_block_reason": push_block_reason,
        "accounting_view": {
            "entries": merged_accounting_entries,
            "total_debit": _json_number(total_debit),
            "total_credit": _json_number(total_credit),
            "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
        },
        "kingdee_json": merged_kingdee_json,
    }


@app.post("/api/vouchers/push")
def push_voucher_to_kingdee(
    payload: schemas.VoucherPushRequest,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    """
    Push previewed Kingdee voucher JSON to configured external API.
    Default target: active ExternalApi matching voucherAdd endpoint.
    """
    import requests
    import time
    from urllib.parse import unquote
    from services.external_auth import ExternalAuthService

    if not isinstance(payload.kingdee_json, dict) or not payload.kingdee_json:
        raise HTTPException(status_code=400, detail="kingdee_json is required")
    _validate_voucher_json_amounts(payload.kingdee_json)

    api_record: Optional[models.ExternalApi] = None
    if payload.api_id is not None:
        api_record = db.query(models.ExternalApi).filter(
            models.ExternalApi.id == payload.api_id,
            models.ExternalApi.is_active == True
        ).first()
        if not api_record:
            raise HTTPException(status_code=404, detail=f"External API not found or inactive: id={payload.api_id}")
    else:
        api_record = db.query(models.ExternalApi).filter(
            models.ExternalApi.is_active == True,
            or_(
                models.ExternalApi.name == "推送金蝶记账凭证",
                models.ExternalApi.url_path.ilike("%/gl/gl_voucher/voucherAdd%"),
                models.ExternalApi.url_path.ilike("%gl_voucher/voucherAdd%"),
            )
        ).order_by(models.ExternalApi.id.asc()).first()
        if not api_record:
            raise HTTPException(
                status_code=404,
                detail="No active voucher push external API found. Please configure it in 接口管理."
            )

    service = db.query(models.ExternalService).filter(models.ExternalService.id == api_record.service_id).first()
    if not service or not service.is_active:
        raise HTTPException(status_code=404, detail=f"External service not found or inactive: id={api_record.service_id}")

    account_book_id = _decode_header_value(x_account_book_id) or None
    account_book_name = _decode_header_value(x_account_book_name) or None
    account_book_number = _decode_header_value(x_account_book_number) or None
    tracked_refs = _normalize_bill_refs(payload.bills)
    push_batch_no = (
        f"VP{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:8].upper()}"
        if tracked_refs else None
    )
    request_payload_text = json.dumps(payload.kingdee_json, ensure_ascii=False)

    if tracked_refs:
        if not allowed_community_ids:
            raise HTTPException(status_code=403, detail="No authorized communities for this account book")

        allowed_set = set(allowed_community_ids)
        unauthorized = [
            ref for ref in tracked_refs
            if int(ref["community_id"]) not in allowed_set
        ]
        if unauthorized:
            preview = ", ".join([f"{ref['community_id']}:{ref['bill_id']}" for ref in unauthorized[:10]])
            raise HTTPException(status_code=403, detail=f"Unauthorized bill communities: {preview}")

        bill_conditions = [
            and_(
                models.Bill.id == ref["bill_id"],
                models.Bill.community_id == ref["community_id"],
            )
            for ref in tracked_refs
        ]
        locked_bills = db.query(models.Bill).filter(or_(*bill_conditions)).with_for_update().all()
        locked_keys = {(int(b.id), int(b.community_id)) for b in locked_bills}
        missing_refs = [
            ref for ref in tracked_refs
            if (int(ref["bill_id"]), int(ref["community_id"])) not in locked_keys
        ]
        if missing_refs:
            preview = ", ".join([f"{ref['community_id']}:{ref['bill_id']}" for ref in missing_refs[:10]])
            raise HTTPException(status_code=404, detail=f"Bills not found: {preview}")

        tracked_status_map = _get_bill_push_status_map(
            db,
            tracked_refs,
            account_book_number=account_book_number,
        )
        tracked_statuses = [
            tracked_status_map[(ref["bill_id"], ref["community_id"])]
            for ref in tracked_refs
        ]
        conflicts = _find_bill_push_conflicts(tracked_statuses)
        if conflicts and not payload.force_push:
            preview = ", ".join(
                [
                    f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})"
                    for item in conflicts[:10]
                ]
            )
            raise HTTPException(status_code=409, detail=f"Selected bills already pushed or pushing: {preview}")

        for ref in tracked_refs:
            db.add(models.BillVoucherPushRecord(
                bill_id=ref["bill_id"],
                community_id=ref["community_id"],
                push_batch_no=push_batch_no,
                push_status="pushing",
                account_book_id=account_book_id,
                account_book_name=account_book_name,
                account_book_number=account_book_number,
                api_id=api_record.id,
                api_name=api_record.name,
                pushed_by=current_user.id,
                message="Push request submitted",
                request_payload=request_payload_text,
            ))
        db.commit()

    org_name = current_user.organization.name if current_user.organization else "未分配"
    user_context = {
        "current_user_id": str(current_user.id),
        "current_username": current_user.username,
        "current_user_realname": current_user.real_name or current_user.username,
        "current_org_id": str(current_user.org_id) if current_user.org_id else "",
        "current_org_name": org_name,
        "current_account_book_id": account_book_id or "",
        "current_account_book_name": account_book_name or "",
        "current_account_book_number": account_book_number or "",
    }

    auth = ExternalAuthService(db=db, service_record=service, user_context=user_context)
    token = auth.get_token()
    base_headers = auth.get_auth_headers()

    custom_headers: Dict[str, Any] = {}
    if api_record.request_headers:
        try:
            parsed_headers = json.loads(api_record.request_headers) if isinstance(api_record.request_headers, str) else api_record.request_headers
            if isinstance(parsed_headers, dict):
                custom_headers = resolve_dict_variables(parsed_headers, db, user_context=user_context)
        except Exception:
            custom_headers = {}

    def _merge_headers(token_value: str) -> Dict[str, str]:
        merged = {k: str(v) for k, v in (base_headers or {}).items()}
        for k, v in (custom_headers or {}).items():
            val = str(v)
            if "{access_token}" in val:
                val = val.replace("{access_token}", token_value)
            merged[k] = val
        return merged

    headers = _merge_headers(token)
    method = (api_record.method or "POST").upper()
    raw_path = (api_record.url_path or "").strip()
    if not raw_path:
        raise HTTPException(status_code=400, detail=f"External API {api_record.id} url_path is empty")

    if raw_path.startswith("http://") or raw_path.startswith("https://"):
        full_url = raw_path
    else:
        base = (service.base_url or "").strip()
        if base and raw_path and not base.endswith("/") and not raw_path.startswith("/"):
            full_url = f"{base}/{raw_path}"
        else:
            full_url = f"{base}{raw_path}"

    if not full_url:
        raise HTTPException(status_code=400, detail="External API url is empty")

    request_started = time.time()

    for attempt in range(2):
        try:
            if attempt > 0:
                auth.invalidate_token()
                db.commit()
                token = auth.get_token()
                headers = _merge_headers(token)

            req_kwargs: Dict[str, Any] = {"headers": headers, "timeout": 30}
            content_type = next((v for k, v in headers.items() if k.lower() == "content-type"), "").lower()
            if method == "GET":
                req_kwargs["params"] = payload.kingdee_json
            elif "application/x-www-form-urlencoded" in content_type:
                req_kwargs["data"] = payload.kingdee_json
            else:
                req_kwargs["json"] = payload.kingdee_json

            resp = requests.request(method, full_url, **req_kwargs)
            try:
                resp_data = resp.json()
            except Exception:
                resp_data = {"raw": resp.text}

            auth_failed = resp.status_code in [401, 602]
            if not auth_failed and isinstance(resp_data, dict):
                err_code = str(resp_data.get("errorCode") or resp_data.get("code") or "").strip()
                if err_code in ["401", "602"]:
                    auth_failed = True

            if auth_failed and attempt == 0:
                continue

            success = bool(resp.ok)
            message = "Push successful" if success else "Push failed"
            if isinstance(resp_data, dict):
                status_flag = resp_data.get("status")
                if status_flag is False:
                    success = False
                error_code = str(resp_data.get("errorCode") or "").strip()
                if error_code not in ("", "0", "None", "null"):
                    success = False
                data_obj = resp_data.get("data")
                if isinstance(data_obj, dict):
                    fail_count = str(data_obj.get("failCount") or "").strip()
                    if fail_count not in ("", "0", "None", "null"):
                        success = False

            binding = _extract_kingdee_voucher_result(resp_data)
            if binding.get("bill_status") is False:
                success = False

            message = _extract_kingdee_push_message(resp_data, message)
            response_payload_text = json.dumps(resp_data, ensure_ascii=False) if isinstance(resp_data, (dict, list)) else str(resp_data)

            if tracked_refs and push_batch_no:
                _finalize_bill_push_records(
                    db=db,
                    push_batch_no=push_batch_no,
                    push_status="success" if success else "failed",
                    message=message,
                    response_payload=response_payload_text,
                    voucher_number=binding.get("voucher_number"),
                    voucher_id=binding.get("voucher_id"),
                )
                tracked_status_map = _get_bill_push_status_map(
                    db,
                    tracked_refs,
                    account_book_number=account_book_number,
                )
                tracked_statuses = [
                    tracked_status_map[(ref["bill_id"], ref["community_id"])]
                    for ref in tracked_refs
                ]
            else:
                tracked_statuses = []

            duration_ms = round((time.time() - request_started) * 1000, 2)
            return {
                "success": success,
                "message": message,
                "status_code": resp.status_code,
                "duration_ms": duration_ms,
                "api_id": api_record.id,
                "api_name": api_record.name,
                "api_url": full_url,
                "push_batch_no": push_batch_no,
                "voucher_number": binding.get("voucher_number"),
                "voucher_id": binding.get("voucher_id"),
                "tracked_bills": tracked_statuses,
                "response": resp_data,
            }
        except Exception as exc:
            if attempt == 1:
                if tracked_refs and push_batch_no:
                    _finalize_bill_push_records(
                        db=db,
                        push_batch_no=push_batch_no,
                        push_status="failed",
                        message=str(exc),
                        response_payload=str(exc),
                    )
                raise HTTPException(status_code=502, detail=f"Push voucher request failed: {str(exc)}")

    raise HTTPException(status_code=502, detail="Push voucher request failed")


def _check_trigger_conditions(
    node: dict,
    data: dict,
    debug_logs: list = None,
    global_context: Optional[dict] = None,
    relation_context: Optional[dict] = None,
) -> bool:
    """闁帒缍婂Λ鈧弻銉ㄐ曢崣鎴炴蒋娴犲墎绮ㄩ弸?"""
    if debug_logs is None:
        debug_logs = []

    def resolve_actual_candidates(field_name: str, actual_value: Any, ctx: dict) -> List[str]:
        candidates: List[str] = []
        primary_value = "" if actual_value is None else str(actual_value)
        candidates.append(primary_value)

        if isinstance(field_name, str) and field_name and not field_name.endswith("_label"):
            label_field = f"{field_name}_label"
            if label_field in ctx:
                label_value = "" if ctx.get(label_field) is None else str(ctx.get(label_field))
                if label_value not in candidates:
                    candidates.append(label_value)

        return candidates

    def resolve_value(val_str: str, ctx: dict) -> str:
        if not isinstance(val_str, str):
            return str(val_str)
        # 合并数据上下文和全局上下文，数据字段优先
        merged_ctx = dict(global_context or {})
        merged_ctx.update(ctx)
        # 使用 evaluate_expression 统一处理占位符替换和格式化函数
        from utils.expression_functions import evaluate_expression as _eval_expr
        return _eval_expr(val_str, merged_ctx)

    try:
        node_type = node.get("type", "group")
        
        if node_type in {"group", "relation"}:
            if node_type == "relation":
                resolver = str(node.get("resolver", "")).strip()
                quantifier = str(node.get("quantifier", "EXISTS")).upper()
                relation_meta = RELATION_REGISTRY.get(resolver)
                root_record = (relation_context or {}).get("root_record") or (relation_context or {}).get("receipt_bill")
                db = (relation_context or {}).get("db")
                relation_cache = (relation_context or {}).setdefault("cache", {})
                selected_records = (relation_context or {}).setdefault("selected_records", {})
                relation_group = _normalize_relation_group(node)
                logic = relation_group["logic"]
                children = relation_group["children"]

                if not relation_meta or not relation_meta.loader:
                    debug_logs.append(f"Relation resolver '{resolver}' is not registered")
                    return False

                if not db or root_record is None:
                    debug_logs.append(f"Relation resolver '{resolver}' is unavailable in current context")
                    return False

                scoped_records_map = (relation_context or {}).get("scoped_records") or {}
                if relation_meta.target_source in scoped_records_map:
                    records = list(scoped_records_map.get(relation_meta.target_source) or [])
                else:
                    cache_key = (
                        resolver,
                        int(getattr(root_record, "id", 0) or 0),
                        int(getattr(root_record, "community_id", 0) or 0),
                    )
                    if cache_key not in relation_cache:
                        relation_cache[cache_key] = relation_meta.loader(db, root_record)
                    records = relation_cache.get(cache_key, [])
                matched_record = None

                for idx, record in enumerate(records):
                    nested_logs: List[str] = []
                    record_globals = dict(global_context or {})
                    record_globals.update(data)
                    candidate = _check_trigger_conditions(
                        {
                            "type": "group",
                            "logic": logic,
                            "children": children,
                        },
                        record,
                        nested_logs,
                        record_globals,
                        relation_context,
                    )
                    debug_logs.append(
                        f"Relation resolver={resolver}, quantifier={quantifier}, candidate={idx + 1}/{len(records)}, match={candidate}"
                    )
                    debug_logs.extend([f"  {line}" for line in nested_logs])
                    if candidate:
                        matched_record = record
                        break

                matched = matched_record is not None
                if matched:
                    selected_records[relation_meta.target_source] = matched_record

                if quantifier == "NOT_EXISTS":
                    return not matched
                return matched

            logic = str(node.get("logic", "AND")).upper()
            children = node.get("children", [])
            if not children:
                return True

            results = [
                _check_trigger_conditions(c, data, debug_logs, global_context, relation_context)
                for c in children
            ]
            return all(results) if logic == "AND" else any(results)

        elif node_type == "rule":
            field = node.get("field", "")
            raw_operator = node.get("operator", "==")
            operator = _canonicalize_trigger_operator(raw_operator)
            if not operator:
                debug_logs.append(f"Unsupported operator '{raw_operator}' for field '{field}', treated as False")
                return False
            raw_value = str(node.get("value", ""))
            
            # 鐟欙絾鐎介崣姗€鍣?
            value = resolve_value(raw_value, data)
            actual_raw = data.get(field, "")
            raw_candidates = resolve_actual_candidates(field, actual_raw, data)
            actual = raw_candidates[0] if raw_candidates else ""

            # 字段侧格式化：如果配置了 field_format 模板，对字段原始值做变换
            field_format = node.get("field_format", "")
            if field_format and "__VALUE__" in field_format:
                from utils.expression_functions import evaluate_expression as _eval_expr
                # 用字段的实际值替换 __VALUE__ 后求值
                expr = field_format.replace("__VALUE__", actual)
                try:
                    actual = _eval_expr(expr, {})
                    debug_logs.append(f"Field format applied: {field_format} => {expr} => {actual}")
                except Exception as fmt_err:
                    debug_logs.append(f"Field format error for '{field}': {fmt_err}, falling back to raw value")

            actual_candidates = [actual]
            for candidate in raw_candidates[1:]:
                if candidate not in actual_candidates:
                    actual_candidates.append(candidate)

            compare_mode = "string"
            
            if operator == "==":
                res = any(candidate == value for candidate in actual_candidates)
            elif operator == "!=":
                res = all(candidate != value for candidate in actual_candidates)
            elif operator == ">":
                res, compare_mode = _compare_ordered_values(actual_raw, value, operator)
            elif operator == ">=":
                res, compare_mode = _compare_ordered_values(actual_raw, value, operator)
            elif operator == "<":
                res, compare_mode = _compare_ordered_values(actual_raw, value, operator)
            elif operator == "<=":
                res, compare_mode = _compare_ordered_values(actual_raw, value, operator)
            elif operator == "contains":
                res = any(value in candidate for candidate in actual_candidates)
            elif operator == "not_contains":
                res = all(value not in candidate for candidate in actual_candidates)
            elif operator == "startswith":
                res = any(candidate.startswith(value) for candidate in actual_candidates)
            elif operator == "endswith":
                res = any(candidate.endswith(value) for candidate in actual_candidates)
            else:
                res = False
                
            debug_logs.append(
                f"Field: {field}, OP: {operator} (raw={raw_operator}), CompareAs: {compare_mode}, "
                f"Expected: {value}, Actual: {actual}, ActualCandidates: {actual_candidates}, Match: {res}"
            )
            return res
            
        return True
    except Exception as e:
        debug_logs.append(f"Error checking condition: {e}")
        return False


@app.get("/api/archives/types")
def get_archive_types(db: Session = Depends(get_db)):
    """Get the list of registered archive types"""
    var = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == "ARCHIVE_TYPE_REGISTRY").first()
    if not var:
        # Default registry if not exists
        default_types = [
            {"key": "accounting-subjects", "label": "会计科目", "icon": "FileText"}
        ]
        import json
        return default_types
    import json
    try:
        return json.loads(var.value)
    except:
        return []

@app.post("/api/archives/types")
def save_archive_types(types: List[dict], db: Session = Depends(get_db)):
    """Update the list of registered archive types"""
    import json
    val = json.dumps(types)
    var = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == "ARCHIVE_TYPE_REGISTRY").first()
    if var:
        var.value = val
    else:
        var = models.GlobalVariable(
            key="ARCHIVE_TYPE_REGISTRY", 
            value=val, 
            description="归档类型注册表，用于维护归档接口管理中的归档类型清单",
            category="system"
        )
        db.add(var)
    db.commit()
    return {"message": "Archive types updated"}

@app.get("/api/archives/config/{archive_key}")
def get_archive_config(archive_key: str, db: Session = Depends(get_db)):
    """Get configuration for a specific archive type"""
    # Map old accounting-subjects to the specific key if needed, or just use the key
    storage_key = f"ARCHIVE_CONFIG_{archive_key.upper().replace('-', '_')}"
    
    # Handle legacy key for accounting subjects
    if archive_key == "accounting-subjects":
        storage_key = "ACCOUNTING_SUBJECT_CONFIG"

    config = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == storage_key).first()
    if not config:
        return {}
    import json
    try:
        return json.loads(config.value)
    except:
        return {}

@app.post("/api/archives/config/{archive_key}")
def save_archive_config(archive_key: str, config: dict, db: Session = Depends(get_db)):
    """Save configuration for a specific archive type"""
    storage_key = f"ARCHIVE_CONFIG_{archive_key.upper().replace('-', '_')}"
    
    if archive_key == "accounting-subjects":
        storage_key = "ACCOUNTING_SUBJECT_CONFIG"

    import json
    val = json.dumps(config)
    var = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == storage_key).first()
    if var:
        var.value = val
    else:
        config_description = "会计科目同步配置，用于维护会计科目归档与同步所需的接口参数" if archive_key == "accounting-subjects" else "归档数据拉取配置"
        var = models.GlobalVariable(
            key=storage_key, 
            value=val, 
            description=config_description,
            category="api_config"
        )
        db.add(var)
    db.commit()
    return {"message": "Config saved"}

@app.post("/api/archives/test")
def test_archive_config(
    config_data: dict, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Test a given archive configuration without saving it"""
    try:
        service_id = config_data.get("service_id")
        if not service_id:
            return {"success": False, "error": "未选择外部集成服务"}
            
        service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
        if not service:
            return {"success": False, "error": "所选服务不存在"}
            
        import requests
        import json
        from services.external_auth import ExternalAuthService
        
        # 1. Get Auth
        auth = None
        if service.service_name == "marki":
            from utils.marki_client import MarkiClient
            marki = MarkiClient()
            marki._load_config()
            headers = marki.headers.copy()
            token = marki.cookie or ""
            if not token:
                if marki.login():
                    headers = marki.headers.copy()
                    token = marki.cookie or ""
                else:
                    return {"success": False, "error": "Marki 系统自动登录失败，请检查集成配置"}
        else:
            auth = ExternalAuthService(db=db, service_record=service, user_context=user_ctx)
            try:
                token = auth.get_token()
                headers = auth.get_auth_headers()
            except Exception as e:
                return {"success": False, "error": f"认证失败: {str(e)}"}
            
        # 2. Add Custom Headers
        user_headers = config_data.get("request_headers", {})
        if isinstance(user_headers, str):
            try: user_headers = json.loads(user_headers)
            except: user_headers = {}
            
        # 瑙ｆ瀽 Headers 涓殑鍙橀噺
        user_headers = resolve_dict_variables(user_headers, db, user_context=user_ctx)
            
        for k, v in user_headers.items():
            if isinstance(v, str) and "{access_token}" in v:
                v = v.replace("{access_token}", token)
            if service.service_name == "marki" and k.lower() == "cookie":
                pass # let original marki cookie stand
            else:
                headers[k] = str(v)
            
        # 3. Prepare URL & Method
        url = config_data.get("url")
        if not url:
            path = config_data.get("url_path") or ""
            if path.startswith("http://") or path.startswith("https://"):
                url = path
            else:
                base = service.base_url or ""
                if base and path and not base.endswith("/") and not path.startswith("/"):
                    url = f"{base}/{path}"
                else:
                    url = base + path
        
        # 瑙ｆ瀽 URL 鍙橀噺
        url = resolve_variables(url or '', db, user_context=user_ctx)
            
        if not url:
            return {"success": False, "error": "璇锋眰鍦板潃涓嶈兘涓虹┖"}
            
        method = config_data.get("method", "POST").upper()
        
        # 4. Prepare Body
        body_template = config_data.get("request_body", "")
        body = None
        if body_template:
            if isinstance(body_template, str):
                try: body = json.loads(body_template)
                except: return {"success": False, "error": "璇锋眰浣?JSON 鏍煎紡閿欒"}
            else:
                body = body_template
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db, user_context=user_ctx)

        # 5. Execute Request with Retry
        start_time = __import__('time').time()
        for attempt in range(2):
            try:
                if attempt > 0:
                    if service.service_name == "marki":
                        from utils.marki_client import MarkiClient
                        marki = MarkiClient()
                        marki.login()
                        headers = marki.headers.copy()
                        token = marki.cookie or ""
                    elif auth:
                        auth.invalidate_token()
                        db.commit()
                        service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                        auth.service_record = service
                        token = auth.get_token()
                        headers = auth.get_auth_headers()
                        
                    for k, v in user_headers.items():
                        if isinstance(v, str) and "{access_token}" in v:
                            v = v.replace("{access_token}", token)
                        if service.service_name == "marki" and k.lower() == "cookie":
                            pass # skip
                        else:
                            headers[k] = str(v)

                is_get = method.upper() == "GET"
                
                # Setup kwargs based on method and headers
                req_kwargs = {"timeout": 15}
                if body is not None:
                    if is_get:
                        req_kwargs["params"] = body
                    else:
                        content_type = next((v for k, v in headers.items() if k.lower() == 'content-type'), '').lower()
                        if 'application/x-www-form-urlencoded' in content_type:
                            req_kwargs["data"] = body
                        else:
                            req_kwargs["json"] = body
                
                resp = requests.request(method, url, headers=headers, **req_kwargs)
                
                auth_failed = False
                if resp.status_code in [401, 602]:
                    auth_failed = True
                else:
                    try:
                        resp_json = resp.json()
                        err_code = str(resp_json.get("errorCode") or resp_json.get("code", ""))
                        if err_code in ["401", "602"]:
                            auth_failed = True
                    except:
                        pass
                
                if auth_failed and attempt == 0:
                    continue

                duration = round((__import__('time').time() - start_time) * 1000, 2)
                
                try:
                    response_json = resp.json()
                except:
                    response_json = {"raw": resp.text}
                    
                return {
                    "success": resp.ok and not auth_failed,
                    "status_code": resp.status_code,
                    "duration_ms": duration,
                    "data": response_json,
                    "headers": dict(resp.headers)
                }
            except Exception as e:
                if attempt == 1:
                    return {"success": False, "error": f"鐠囬攱鐪伴崣鎴︹偓浣搞亼鐠? {str(e)}"}
            
    except Exception as e:
        return {"success": False, "error": f"缁崵绮洪柨娆? {str(e)}"}

# ===================== Accounting Subject Management (Legacy/Specific) =====================

@app.get("/api/finance/accounting-subjects", response_model=schemas.PaginatedAccountingSubjectResponse)
def get_accounting_subjects(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    account_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get accounting subjects with pagination and search"""
    query = db.query(models.AccountingSubject)
    
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.AccountingSubject.number.ilike(search_filter)) |
            (models.AccountingSubject.name.ilike(search_filter)) |
            (models.AccountingSubject.fullname.ilike(search_filter))
        )
    
    if account_type:
        query = query.filter(models.AccountingSubject.account_type_number == account_type)
    
    total = query.count()
    subjects = query.order_by(models.AccountingSubject.number).offset(skip).limit(limit).all()
    
    return {"items": subjects, "total": total}

@app.get("/api/finance/accounting-subjects/config")
def get_accounting_subject_config(db: Session = Depends(get_db)):
    return get_archive_config("accounting-subjects", db)

@app.post("/api/finance/accounting-subjects/config")
def save_accounting_subject_config(config: dict, db: Session = Depends(get_db)):
    return save_archive_config("accounting-subjects", config, db)

@app.post("/api/finance/accounting-subjects/sync")
def sync_accounting_subjects(
    request: schemas.AccountingSubjectSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync accounting subjects using configured API"""
    
    from database import SessionLocal
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config (Try ExternalApi first, then fallback to GlobalVariable)
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            config = None
            service_id = None
            
            # Try to find the migrated API record
            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name == "会计科目同步接口").first()
            if api_record:
                config = {
                    "method": api_record.method,
                    "url": api_record.url_path,
                    "request_headers": api_record.request_headers,
                    "request_body": api_record.request_body,
                    "service_id": api_record.service_id
                }
                service_id = api_record.service_id
                logger.info("Using configuration from ExternalApi: 会计科目同步接口")
            else:
                # Fallback to legacy global variable
                config_var = db_session.query(models.GlobalVariable).filter(models.GlobalVariable.key == "ACCOUNTING_SUBJECT_CONFIG").first()
                if config_var:
                    config = json.loads(config_var.value)
                    service_id = config.get("service_id")
                    logger.info("Using legacy configuration from global_variables: ACCOUNTING_SUBJECT_CONFIG")
                else:
                    logger.error("No configuration found for accounting subjects (ExternalApi or GlobalVariable)")
                    return
                 
            if not service_id:
                 logger.error("Configuration missing service_id")
                 return
                 
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                 logger.error(f"Service with ID {service_id} not found")
                 return
                 
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            # Initial token fetch attempt
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 return
                 
            # 3. Prepare Request Parts
            full_url = config.get("url")
            if not full_url:
                full_url = (service.base_url or "") + config.get("url_path", "")
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            method = config.get("method", "POST")
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = config.get("request_headers", {})
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = config.get("request_body", "")
            body = {}
            try:
                if isinstance(body_template, str):
                    try:
                        body = json.loads(body_template) if body_template else {}
                    except:
                        body = {} 
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)

            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size

            # Incremental sync without truncating existing data
            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing accounting subjects page {page_no}...")
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(method, url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                if not success:
                    break
                    
                # Process Data
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                
                if isinstance(rows, list) and rows:
                    from sqlalchemy.dialects.postgresql import insert
                    count = 0
                    current_page_ids = []
                    unique_rows = {} # Python 娓氀勭壌閹?number 閸樺鍣?
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                            
                        number = str(row.get("number", ""))
                        if not number: continue
                        
                        subject_data = {
                            "id": number,
                            "number": number,
                            "name": row.get("name", ""),
                            "fullname": row.get("fullname", ""),
                            "long_number": row.get("longnumber", ""),
                            "level": row.get("level"),
                            "is_leaf": row.get("isleaf"),
                            "direction": str(row.get("dc", "")),
                            "is_active": (str(row.get("enable")) == "1"),
                            "is_cash": row.get("iscash", False),
                            "is_bank": row.get("isbank", False),
                            "is_cash_equivalent": row.get("iscashequivalent", False),
                            "acct_currency": row.get("acctcurrency", ""),
                            "account_type_number": row.get("accounttype_accounttype", ""),
                            "ac_check": row.get("accheck", False),
                            "is_qty": row.get("isqty", False),
                            "currency_entry": json.dumps(row["currencyentry"]) if "currencyentry" in row else None,
                            "raw_data": json.dumps(row),
                            "check_items": json.dumps(row["checkitementry"]) if "checkitementry" in row else None
                        }
                        unique_rows[number] = subject_data
                    
                    # 閹靛綊鍣洪崚鍡?Upsert
                    unique_list = list(unique_rows.values())
                    for i in range(0, len(unique_list), 50):
                        batch = unique_list[i : i + 50]
                        if not batch: continue
                        
                        stmt = insert(models.AccountingSubject).values(batch)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['id'],
                            set_={k: v for k, v in stmt.excluded.items() if k not in ['id', 'created_at']}
                        )
                        db_session.execute(stmt)
                        db_session.commit()
                        count += len(batch)

                    total_synced += count
                    
                    # 瀵板箚閻旀梹鏌?
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Accounting subjects pagination repeated, breaking.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            logger.info(f"Finished. Total synced accounting subjects: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync failed outer: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Sync started"}

# ===================== Customer Management =====================

@app.get("/api/finance/customers", response_model=schemas.PaginatedCustomerResponse)
def get_customers(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.Customer)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.Customer.number.ilike(search_filter)) |
            (models.Customer.name.ilike(search_filter))
        )
    total = query.count()
    customers = query.order_by(models.Customer.number).offset(skip).limit(limit).all()
    return {"items": customers, "total": total}

@app.post("/api/finance/customers/sync")
def sync_customers(
    request: schemas.CustomerSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync customers using configured API"""
    from database import SessionLocal
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name == "查询金蝶云星空客户").first()
            if not api_record:
                logger.error("No configuration found for customers: '查询金蝶云星空客户' external API not found.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            # Ensure pagination parameters are present for Kingdee
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            # Clear table before syncing
            try:
                db_session.query(models.Customer).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing customers page {page_no}...")
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                # Process Data
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        # Convert to boolean handling strict typing
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    from sqlalchemy.dialects.postgresql import insert
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {} # 閸?Python 娓氀勭壌閹?number 閸樺鍣?
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                            
                        number = str(row.get("number", ""))
                        if not number: continue
                        
                        # Build upsert payload
                        customer_data = {
                            "id": number,
                            "number": number,
                            "name": row.get("name", ""),
                            "status": str(row.get("status", "")),
                            "enable": str(row.get("enable", "")),
                            "type": str(row.get("type", "")),
                            "linkman": str(row.get("linkman", "")),
                            "bizpartner_phone": str(row.get("bizpartner_phone", "")),
                            "bizpartner_address": str(row.get("bizpartner_address", "")),
                            "societycreditcode": str(row.get("societycreditcode", "")),
                            "org_name": str(row.get("org_name", "")),
                            "createorg_name": str(row.get("createorg_name", "")),
                            "entry_bank": json.dumps(row["entry_bank"]) if "entry_bank" in row else None,
                            "entry_linkman": json.dumps(row["entry_linkman"]) if "entry_linkman" in row else None,
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[number] = customer_data
                    
                    # 閸戝棗閸掑棙澹?Upsert 閸忋儱绨?
                    unique_list = list(unique_rows.values())
                    for i in range(0, len(unique_list), 50):
                        batch = unique_list[i : i + 50]
                        if not batch: continue
                        
                        stmt = insert(models.Customer).values(batch)
                        # 鐠佸墽鐤嗛崘鑼崐鐟欏嫬鍨敍姘冲 ID/number 閸愯尙鐛婇敍灞藉灟閺囧瓨鏌婇幍鈧張澶夌炊閸ョ偟娈戞稉姘鐎涙
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['id'],
                            set_={k: v for k, v in stmt.excluded.items() if k not in ['id', 'created_at']}
                        )
                        db_session.execute(stmt)
                        db_session.commit()
                        count += len(batch)
                            
                    total_synced += count
                    
                    # Stop if API starts repeating the same page payload.
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            logger.info(f"Finished. Total synced customers: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync customers failed: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Customer sync started"}

# ===================== Supplier Management =====================

@app.get("/api/finance/suppliers", response_model=schemas.PaginatedSupplierResponse)
def get_suppliers(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.Supplier)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.Supplier.number.ilike(search_filter)) |
            (models.Supplier.name.ilike(search_filter))
        )
    total = query.count()
    suppliers = query.order_by(models.Supplier.number).offset(skip).limit(limit).all()
    return {"items": suppliers, "total": total}

@app.post("/api/finance/suppliers/sync")
def sync_suppliers(
    request: schemas.SupplierSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync suppliers using configured API"""
    from database import SessionLocal
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(
                models.ExternalApi.name.in_(["查询供应商", "查询金蝶云星空供应商"])
            ).first()
            if not api_record:
                logger.error("No configuration found for suppliers: '查询供应商' external API not found.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            # Ensure pagination parameters are present for Kingdee
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            # Clear table before syncing
            try:
                db_session.query(models.Supplier).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing suppliers page {page_no}...")
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                # Process Data
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    from sqlalchemy.dialects.postgresql import insert
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {} # 閸?Python 娓氀勭壌閹?number 閸樺鍣?
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                            
                        number = str(row.get("number", ""))
                        if not number: continue
                        
                        supplier_data = {
                            "id": number,
                            "number": number,
                            "name": row.get("name", ""),
                            "status": str(row.get("status", "")),
                            "enable": str(row.get("enable", "")),
                            "type": str(row.get("type", "")),
                            "createorg_number": str(row.get("createorg_number", "")),
                            "supplier_status_name": str(row.get("supplier_status_name", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[number] = supplier_data
                    
                    unique_list = list(unique_rows.values())
                    for i in range(0, len(unique_list), 50):
                        batch = unique_list[i : i + 50]
                        if not batch: continue
                        
                        stmt = insert(models.Supplier).values(batch)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['id'],
                            set_={k: v for k, v in stmt.excluded.items() if k not in ['id', 'created_at']}
                        )
                        db_session.execute(stmt)
                        db_session.commit()
                        count += len(batch)
                            
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            logger.info(f"Finished. Total synced suppliers: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync suppliers failed: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Supplier sync started"}

# ===================== Kingdee House Management =====================

@app.get("/api/finance/kd-houses", response_model=schemas.PaginatedKingdeeHouseResponse)
def get_kd_houses(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.KingdeeHouse)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.KingdeeHouse.number.ilike(search_filter)) |
            (models.KingdeeHouse.wtw8_number.ilike(search_filter)) |
            (models.KingdeeHouse.name.ilike(search_filter))
        )
    total = query.count()
    kd_houses = query.order_by(models.KingdeeHouse.wtw8_number).offset(skip).limit(limit).all()
    return {"items": kd_houses, "total": total}

@app.post("/api/finance/kd-houses/sync")
def sync_kd_houses(
    request: schemas.KingdeeHouseSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync kingdee houses using configured API"""
    from database import SessionLocal
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name == "查询金蝶系统房号信息").first()
            if not api_record:
                logger.error("No configuration found for kd_houses: '查询金蝶系统房号信息' external API not found.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            try:
                db_session.query(models.KingdeeHouse).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing kd_houses page {page_no}...")
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    from sqlalchemy.dialects.postgresql import insert
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {}
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                        else:
                            continue
                            
                        # Handle "Original Code" mapping
                        # Use number if available, fall back to wtw8_number
                        number = str(row.get("number", ""))
                        wtw8_num = str(row.get("wtw8_number", ""))
                        if not number:
                            number = wtw8_num
                            
                        kdhouse_data = {
                            "id": api_native_id,
                            "number": number,
                            "wtw8_number": wtw8_num,
                            "name": row.get("name", ""),
                            "tzqslx": str(row.get("wtw8_combofield_tzqslx", "")),
                            "splx": str(row.get("wtw8_combofield_splx", "")),
                            "createorg_name": str(row.get("createorg_name", "")),
                            "createorg_number": str(row.get("createorg_number", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[api_native_id] = kdhouse_data
                    
                    unique_list = list(unique_rows.values())
                    for i in range(0, len(unique_list), 50):
                        batch = unique_list[i : i + 50]
                        if not batch: continue
                        
                        stmt = insert(models.KingdeeHouse).values(batch)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['id'],
                            set_={k: v for k, v in stmt.excluded.items() if k not in ['id', 'created_at']}
                        )
                        db_session.execute(stmt)
                        db_session.commit()
                        count += len(batch)
                            
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            logger.info(f"Finished. Total synced kd_houses: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync kd_houses failed: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Kingdee House sync started"}

# ===================== Account Book Management =====================

@app.get("/api/finance/kd-account-books", response_model=schemas.PaginatedKingdeeAccountBookResponse)
def get_kd_account_books(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.KingdeeAccountBook)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.KingdeeAccountBook.number.ilike(search_filter)) |
            (models.KingdeeAccountBook.name.ilike(search_filter))
        )
    total = query.count()
    items = query.order_by(models.KingdeeAccountBook.number).offset(skip).limit(limit).all()
    return {"items": items, "total": total}

@app.post("/api/finance/kd-account-books/sync")
def sync_kd_account_books(
    request: schemas.KingdeeAccountBookSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync kingdee account books using configured API"""
    from database import SessionLocal
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(
                models.ExternalApi.name.in_(["账簿列表查询", "查询金蝶系统账簿信息"])
            ).first()
            if not api_record:
                logger.error("No configuration found for kd_account_books: '账簿列表查询' external API not found.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            try:
                db_session.query(models.KingdeeAccountBook).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing kd_account_books page {page_no}...")
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    from sqlalchemy.dialects.postgresql import insert
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {}
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                        else:
                            continue
                            
                        number = str(row.get("number", ""))
                            
                        account_book_data = {
                            "id": api_native_id,
                            "number": number,
                            "name": row.get("name", ""),
                            "org_number": str(row.get("org_number", "")),
                            "org_name": str(row.get("org_name", "")),
                            "accountingsys_number": str(row.get("accountingsys_number", "")),
                            "accountingsys_name": str(row.get("accountingsys_name", "")),
                            "booknature": str(row.get("booknature", "")),
                            "accounttable_name": str(row.get("accounttable_name", "")),
                            "basecurrency_name": str(row.get("basecurrency_name", "")),
                            "status": str(row.get("status", "")),
                            "enable": str(row.get("enable", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[api_native_id] = account_book_data
                    
                    unique_list = list(unique_rows.values())
                    for i in range(0, len(unique_list), 50):
                        batch = unique_list[i : i + 50]
                        if not batch: continue
                        
                        stmt = insert(models.KingdeeAccountBook).values(batch)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['id'],
                            set_={k: v for k, v in stmt.excluded.items() if k not in ['id', 'created_at']}
                        )
                        db_session.execute(stmt)
                        db_session.commit()
                        count += len(batch)
                            
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            logger.info(f"Finished. Total synced kd_account_books: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync kd_account_books failed: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Kingdee Account Book sync started"}

# ===================== Auxiliary Data Management =====================

@app.get("/api/finance/auxiliary-data", response_model=schemas.PaginatedAuxiliaryDataResponse)
def get_auxiliary_data(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    categories: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.AuxiliaryData)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.AuxiliaryData.number.ilike(search_filter)) |
            (models.AuxiliaryData.name.ilike(search_filter)) |
            (models.AuxiliaryData.group_name.ilike(search_filter))
        )
    if categories:
        cat_list = [c.strip() for c in categories.split(",") if c.strip()]
        if cat_list:
            query = query.filter(
                (models.AuxiliaryData.group_number.in_(cat_list)) | 
                (models.AuxiliaryData.group_name.in_(cat_list))
            )
    total = query.count()
    items = query.order_by(models.AuxiliaryData.number).offset(skip).limit(limit).all()
    return {"items": items, "total": total}

@app.post("/api/finance/auxiliary-data/sync")
def sync_auxiliary_data(
    request: schemas.AuxiliaryDataSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync auxiliary data using configured API"""
    from database import SessionLocal
    
    category_numbers = request.categories or []

    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name.in_(["辅助资料查询", "查询金蝶辅助资料"])).first()
            if not api_record:
                api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.url_path.like("%bos_assistantdata_detail/getList")).first()

            if not api_record:
                logger.error("No configuration found for auxiliary data: API configuration missing.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
                 
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            # Remove any specific group_number filter applied in template if we want to sync all or specific categories
            if "group_number" in body["data"]:
                del body["data"]["group_number"]
                
            local_category_numbers = list(category_numbers)
            if not local_category_numbers:
                # If no categories specified, fetch all from DB
                all_cats = db_session.query(models.AuxiliaryDataCategory.number).all()
                local_category_numbers = [c[0] for c in all_cats]
                
            if not local_category_numbers:
                logger.error("No categories available to sync. Please sync auxiliary data categories first.")
                return

            cat_str = "','".join(local_category_numbers)
            body["data"]["filter"] = f"group_number in ('{cat_str}')"
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            try:
                if category_numbers:
                    db_session.query(models.AuxiliaryData).filter(models.AuxiliaryData.group_number.in_(category_numbers)).delete(synchronize_session=False)
                else:
                    db_session.query(models.AuxiliaryData).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing auxiliary data page {page_no}...")
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    from sqlalchemy.dialects.postgresql import insert
                    count = 0
                    current_page_ids = []
                    unique_rows = {}
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                            
                        number = str(row.get("number", ""))
                        if not number: continue
                        
                        group_number = str(row.get("group_number", ""))
                        if local_category_numbers and group_number not in local_category_numbers:
                            continue
                        
                        aux_data = {
                            "id": api_native_id or number,
                            "number": number,
                            "name": row.get("name", ""),
                            "issyspreset": bool(row.get("issyspreset")),
                            "ctrlstrategy": str(row.get("ctrlstrategy", "")),
                            "enable": str(row.get("enable", "")),
                            "group_number": str(row.get("group_number", "")),
                            "group_name": str(row.get("group_name", "")),
                            "parent_number": str(row.get("parent_number", "")),
                            "parent_name": str(row.get("parent_name", "")),
                            "createorg_number": str(row.get("createorg_number", "")),
                            "createorg_name":  str(row.get("createorg_name", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[api_native_id or number] = aux_data
                    
                    unique_list = list(unique_rows.values())
                    for i in range(0, len(unique_list), 50):
                        batch = unique_list[i : i + 50]
                        if not batch: continue
                        
                        stmt = insert(models.AuxiliaryData).values(batch)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['id'],
                            set_={k: v for k, v in stmt.excluded.items() if k not in ['id', 'created_at']}
                        )
                        db_session.execute(stmt)
                        db_session.commit()
                        count += len(batch)
                            
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            logger.info(f"Finished. Total synced auxiliary_data: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync auxiliary data failed: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Auxiliary Data sync started"}

# ===================== Auxiliary Data Category Management =====================

@app.get("/api/finance/auxiliary-data-categories", response_model=schemas.PaginatedAuxiliaryDataCategoryResponse)
def get_auxiliary_data_categories(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.AuxiliaryDataCategory)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.AuxiliaryDataCategory.number.ilike(search_filter)) |
            (models.AuxiliaryDataCategory.name.ilike(search_filter))
        )
    total = query.count()
    items = query.order_by(models.AuxiliaryDataCategory.number).offset(skip).limit(limit).all()
    return {"items": items, "total": total}

@app.post("/api/finance/auxiliary-data-categories/sync")
def sync_auxiliary_data_categories(
    request: schemas.AuxiliaryDataCategorySyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync auxiliary data categories using configured API"""
    from database import SessionLocal
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name == "辅助资料分类").first()
            if not api_record:
                api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.url_path.like("%assistantdata/getList")).first()

            if not api_record:
                logger.error("No configuration found for auxiliary data categories: API configuration missing.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 1000
            else:
                body["pageSize"] = page_size
            
            try:
                db_session.query(models.AuxiliaryDataCategory).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing auxiliary data categories page {page_no}...")
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    from sqlalchemy.dialects.postgresql import insert
                    count = 0
                    current_page_ids = []
                    unique_rows = {}
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                            
                        number = str(row.get("number", ""))
                        if not number: continue
                        
                        cat_data = {
                            "id": api_native_id or number,
                            "number": number,
                            "name": row.get("name") or "",
                            "fissyspreset": bool(row.get("fissyspreset") or row.get("issyspreset")),
                            "description": str(row.get("description", "")),
                            "ctrlstrategy": str(row.get("ctrlstrategy", "")),
                            "createorg_name": str(row.get("createorg_name", "")),
                            "createorg_number": str(row.get("createorg_number", "")),
                            "createorg_id": str(row.get("createorg_id", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[api_native_id or number] = cat_data
                    
                    unique_list = list(unique_rows.values())
                    for i in range(0, len(unique_list), 100):
                        batch = unique_list[i : i + 100]
                        if not batch: continue
                        
                        stmt = insert(models.AuxiliaryDataCategory).values(batch)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['id'],
                            set_={k: v for k, v in stmt.excluded.items() if k not in ['id', 'created_at']}
                        )
                        db_session.execute(stmt)
                        db_session.commit()
                        count += len(batch)
                            
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            logger.info(f"Finished. Total synced auxiliary_data_categories: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync auxiliary data categories failed: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Auxiliary Data Category sync started"}

# ===================== Bank Account Management =====================

@app.get("/api/finance/kd-bank-accounts", response_model=schemas.PaginatedKingdeeBankAccountResponse)
def get_kd_bank_accounts(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.KingdeeBankAccount)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.KingdeeBankAccount.bankaccountnumber.ilike(search_filter)) |
            (models.KingdeeBankAccount.name.ilike(search_filter)) |
            (models.KingdeeBankAccount.acctname.ilike(search_filter)) |
            (models.KingdeeBankAccount.bank_name.ilike(search_filter))
        )
    total = query.count()
    items = query.order_by(models.KingdeeBankAccount.bankaccountnumber).offset(skip).limit(limit).all()
    return {"items": items, "total": total}

@app.post("/api/finance/kd-bank-accounts/sync")
def sync_kd_bank_accounts(
    request: schemas.KingdeeBankAccountSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync Kingdee bank accounts using configured API"""
    from database import SessionLocal
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(
                models.ExternalApi.name.in_(["银行账户查询", "查询金蝶银行账号"])
            ).first()
            if not api_record:
                api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.url_path.like("%cas_bankaccount/getList")).first()

            if not api_record:
                logger.error("No configuration found for bank accounts: API configuration missing.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            # 閸氬本閸撳秵绔荤粚楦裤€?
            try:
                db_session.query(models.KingdeeBankAccount).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing kd_bank_accounts page {page_no}...")
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    from sqlalchemy.dialects.postgresql import insert
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {}
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                        else:
                            continue
                            
                        bank_account_data = {
                            "id": api_native_id,
                            "bankaccountnumber": str(row.get("bankaccountnumber", "")),
                            "name": str(row.get("name", "")),
                            "acctname": str(row.get("acctname", "")),
                            "company_number": str(row.get("company_number", "")),
                            "company_name": str(row.get("company_name", "")),
                            "openorg_number": str(row.get("openorg_number", "")),
                            "openorg_name": str(row.get("openorg_name", "")),
                            "defaultcurrency_number": str(row.get("defaultcurrency_number", "")),
                            "defaultcurrency_name": str(row.get("defaultcurrency_name", "")),
                            "accttype": str(row.get("accttype", "")),
                            "acctstyle": str(row.get("acctstyle", "")),
                            "finorgtype": str(row.get("finorgtype", "")),
                            "banktype_number": str(row.get("banktype_number", "")),
                            "banktype_name": str(row.get("banktype_name", "")),
                            "bank_number": str(row.get("bank_number", "")),
                            "bank_name": str(row.get("bank_name", "")),
                            "acctproperty_number": str(row.get("acctproperty_number", "")),
                            "acctproperty_name": str(row.get("acctproperty_name", "")),
                            "status": str(row.get("status", "")),
                            "acctstatus": str(row.get("acctstatus", "")),
                            "isdefaultrec": bool(row.get("isdefaultrec", False)),
                            "isdefaultpay": bool(row.get("isdefaultpay", False)),
                            "comment": str(row.get("comment", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[api_native_id] = bank_account_data
                    
                    unique_list = list(unique_rows.values())
                    for i in range(0, len(unique_list), 50):
                        batch = unique_list[i : i + 50]
                        if not batch: continue
                        
                        stmt = insert(models.KingdeeBankAccount).values(batch)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['id'],
                            set_={k: v for k, v in stmt.excluded.items() if k not in ['id', 'created_at']}
                        )
                        db_session.execute(stmt)
                        db_session.commit()
                        count += len(batch)
                            
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            logger.info(f"Finished. Total synced kd_bank_accounts: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync bank accounts failed: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Bank account sync started"}


def _require_admin(user: models.User) -> None:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Only administrators can modify reporting resources")


def _serialize_reporting_connection(connection: models.ReportingDbConnection) -> schemas.ReportingDbConnectionResponse:
    return schemas.ReportingDbConnectionResponse(
        id=connection.id,
        name=connection.name,
        description=connection.description,
        db_type=connection.db_type,
        host=connection.host,
        port=connection.port,
        database_name=connection.database_name,
        schema_name=connection.schema_name,
        username=connection.username,
        connection_options=connection.connection_options,
        is_active=connection.is_active,
        has_password=bool(connection.password_enc),
        created_at=connection.created_at,
        updated_at=connection.updated_at,
    )


def _serialize_reporting_dataset(dataset: models.ReportingDataset) -> schemas.ReportingDatasetResponse:
    return schemas.ReportingDatasetResponse(
        id=dataset.id,
        connection_id=dataset.connection_id,
        connection_name=dataset.connection.name if dataset.connection else None,
        name=dataset.name,
        description=dataset.description,
        sql_text=dataset.sql_text,
        params_json=dataset.params_json,
        row_limit=dataset.row_limit,
        last_columns_json=dataset.last_columns_json,
        last_validated_at=dataset.last_validated_at,
        is_active=dataset.is_active,
        created_at=dataset.created_at,
        updated_at=dataset.updated_at,
    )


def _serialize_reporting_report(report: models.ReportingReport) -> schemas.ReportingReportResponse:
    return schemas.ReportingReportResponse(
        id=report.id,
        dataset_id=report.dataset_id,
        dataset_name=report.dataset.name if report.dataset else None,
        name=report.name,
        description=report.description,
        report_type=report.report_type,
        config_json=report.config_json,
        is_active=report.is_active,
        created_at=report.created_at,
        updated_at=report.updated_at,
    )


@app.get("/api/reporting/db-connections", response_model=List[schemas.ReportingDbConnectionResponse])
def list_reporting_db_connections(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    items = db.query(models.ReportingDbConnection).order_by(models.ReportingDbConnection.id.desc()).all()
    return [_serialize_reporting_connection(item) for item in items]


@app.post("/api/reporting/db-connections", response_model=schemas.ReportingDbConnectionResponse)
def create_reporting_db_connection(
    payload: schemas.ReportingDbConnectionCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    existing = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.name == payload.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Connection name already exists")

    connection = models.ReportingDbConnection(
        name=payload.name,
        description=payload.description,
        db_type=payload.db_type,
        host=payload.host,
        port=payload.port,
        database_name=payload.database_name,
        schema_name=payload.schema_name,
        username=payload.username,
        password_enc=encrypt_value(payload.password) if payload.password else None,
        connection_options=payload.connection_options,
        is_active=payload.is_active,
    )
    db.add(connection)
    db.commit()
    db.refresh(connection)
    return _serialize_reporting_connection(connection)


@app.post("/api/reporting/db-connections/test")
def test_reporting_db_connection(
    payload: schemas.ReportingDbConnectionCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    transient = models.ReportingDbConnection(
        name=payload.name,
        description=payload.description,
        db_type=payload.db_type,
        host=payload.host,
        port=payload.port,
        database_name=payload.database_name,
        schema_name=payload.schema_name,
        username=payload.username,
        password_enc=encrypt_value(payload.password) if payload.password else None,
        connection_options=payload.connection_options,
        is_active=payload.is_active,
    )
    try:
        return ReportingDatabaseService.test_connection(transient)
    except Exception as exc:
        return {"success": False, "message": str(exc)}


@app.put("/api/reporting/db-connections/{connection_id}", response_model=schemas.ReportingDbConnectionResponse)
def update_reporting_db_connection(
    connection_id: int,
    payload: schemas.ReportingDbConnectionUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    update_data = payload.dict(exclude_unset=True)
    if "name" in update_data and update_data["name"] != connection.name:
        exists = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.name == update_data["name"]).first()
        if exists:
            raise HTTPException(status_code=400, detail="Connection name already exists")

    password = update_data.pop("password", None) if "password" in update_data else None
    for key, value in update_data.items():
        setattr(connection, key, value)
    if "password" in payload.__fields_set__:
        connection.password_enc = encrypt_value(password) if password else None

    db.commit()
    db.refresh(connection)
    return _serialize_reporting_connection(connection)


@app.delete("/api/reporting/db-connections/{connection_id}")
def delete_reporting_db_connection(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    db.delete(connection)
    db.commit()
    return {"message": "Connection deleted"}


@app.get("/api/reporting/db-connections/{connection_id}/tables")
def list_reporting_db_tables(
    connection_id: int,
    schema_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        return {
            "connection_id": connection.id,
            "tables": ReportingDatabaseService.list_tables(connection, schema_name=schema_name),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/reporting/datasets", response_model=List[schemas.ReportingDatasetResponse])
def list_reporting_datasets(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    items = (
        db.query(models.ReportingDataset)
        .options(joinedload(models.ReportingDataset.connection))
        .order_by(models.ReportingDataset.id.desc())
        .all()
    )
    return [_serialize_reporting_dataset(item) for item in items]


@app.post("/api/reporting/datasets", response_model=schemas.ReportingDatasetResponse)
def create_reporting_dataset(
    payload: schemas.ReportingDatasetCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == payload.connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    dataset = models.ReportingDataset(**payload.dict())
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    dataset = (
        db.query(models.ReportingDataset)
        .options(joinedload(models.ReportingDataset.connection))
        .filter(models.ReportingDataset.id == dataset.id)
        .first()
    )
    return _serialize_reporting_dataset(dataset)


@app.put("/api/reporting/datasets/{dataset_id}", response_model=schemas.ReportingDatasetResponse)
def update_reporting_dataset(
    dataset_id: int,
    payload: schemas.ReportingDatasetUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    dataset = db.query(models.ReportingDataset).filter(models.ReportingDataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    update_data = payload.dict(exclude_unset=True)
    if "connection_id" in update_data:
        connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == update_data["connection_id"]).first()
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")

    for key, value in update_data.items():
        setattr(dataset, key, value)

    db.commit()
    db.refresh(dataset)
    dataset = (
        db.query(models.ReportingDataset)
        .options(joinedload(models.ReportingDataset.connection))
        .filter(models.ReportingDataset.id == dataset.id)
        .first()
    )
    return _serialize_reporting_dataset(dataset)


@app.delete("/api/reporting/datasets/{dataset_id}")
def delete_reporting_dataset(
    dataset_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    dataset = db.query(models.ReportingDataset).filter(models.ReportingDataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    db.delete(dataset)
    db.commit()
    return {"message": "Dataset deleted"}


@app.post("/api/reporting/datasets/{dataset_id}/preview")
def preview_reporting_dataset(
    dataset_id: int,
    payload: schemas.ReportingDatasetPreviewRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    user_ctx: Dict[str, str] = Depends(get_user_context),
):
    _require_api_permission(db, current_user, "reporting.manage")
    dataset = (
        db.query(models.ReportingDataset)
        .options(joinedload(models.ReportingDataset.connection))
        .filter(models.ReportingDataset.id == dataset_id)
        .first()
    )
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if not dataset.connection or not dataset.connection.is_active:
        raise HTTPException(status_code=400, detail="Dataset connection is inactive")

    try:
        result = ReportingDatabaseService.execute_dataset(
            dataset.connection,
            dataset,
            params=payload.params,
            limit=payload.limit,
            db_session=db,
            user_context=user_ctx,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return result


@app.post("/api/reporting/datasets/preview-draft")
def preview_reporting_dataset_draft(
    payload: schemas.ReportingDatasetDraftPreviewRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    user_ctx: Dict[str, str] = Depends(get_user_context),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = (
        db.query(models.ReportingDbConnection)
        .filter(models.ReportingDbConnection.id == payload.connection_id)
        .first()
    )
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    if not connection.is_active:
        raise HTTPException(status_code=400, detail="Connection is inactive")

    try:
        result = ReportingDatabaseService.execute_query(
            connection=connection,
            sql_text=payload.sql_text,
            params_json=payload.params_json,
            params=payload.params,
            limit=payload.limit,
            default_limit=payload.row_limit,
            db_session=db,
            user_context=user_ctx,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return result


@app.get("/api/reporting/reports", response_model=List[schemas.ReportingReportResponse])
def list_reporting_reports(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    items = db.query(models.ReportingReport).options(
        joinedload(models.ReportingReport.dataset)
    ).order_by(models.ReportingReport.id.desc()).all()
    return [_serialize_reporting_report(item) for item in items]


@app.post("/api/reporting/reports", response_model=schemas.ReportingReportResponse)
def create_reporting_report(
    payload: schemas.ReportingReportCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    dataset = db.query(models.ReportingDataset).filter(models.ReportingDataset.id == payload.dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    report = models.ReportingReport(**payload.dict())
    db.add(report)
    db.commit()
    db.refresh(report)
    report = (
        db.query(models.ReportingReport)
        .options(joinedload(models.ReportingReport.dataset))
        .filter(models.ReportingReport.id == report.id)
        .first()
    )
    return _serialize_reporting_report(report)


@app.put("/api/reporting/reports/{report_id}", response_model=schemas.ReportingReportResponse)
def update_reporting_report(
    report_id: int,
    payload: schemas.ReportingReportUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    report = db.query(models.ReportingReport).filter(models.ReportingReport.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    update_data = payload.dict(exclude_unset=True)
    if "dataset_id" in update_data:
        dataset = db.query(models.ReportingDataset).filter(models.ReportingDataset.id == update_data["dataset_id"]).first()
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")

    for key, value in update_data.items():
        setattr(report, key, value)

    db.commit()
    db.refresh(report)
    report = (
        db.query(models.ReportingReport)
        .options(joinedload(models.ReportingReport.dataset))
        .filter(models.ReportingReport.id == report.id)
        .first()
    )
    return _serialize_reporting_report(report)


@app.delete("/api/reporting/reports/{report_id}")
def delete_reporting_report(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    report = db.query(models.ReportingReport).filter(models.ReportingReport.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    db.delete(report)
    db.commit()
    return {"message": "Report deleted"}


@app.post("/api/reporting/reports/{report_id}/run")
def run_reporting_report(
    report_id: int,
    payload: schemas.ReportingReportRunRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    user_ctx: Dict[str, str] = Depends(get_user_context),
):
    _require_api_permission(db, current_user, "reporting.manage")
    report = (
        db.query(models.ReportingReport)
        .options(joinedload(models.ReportingReport.dataset).joinedload(models.ReportingDataset.connection))
        .filter(models.ReportingReport.id == report_id)
        .first()
    )
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    if not report.is_active:
        raise HTTPException(status_code=404, detail="Report not found")

    dataset = report.dataset
    if not dataset or not dataset.is_active:
        raise HTTPException(status_code=400, detail="Dataset is inactive")
    if not dataset.connection or not dataset.connection.is_active:
        raise HTTPException(status_code=400, detail="Dataset connection is inactive")

    report_config = {}
    if report.config_json:
        try:
            report_config = json.loads(report.config_json)
        except json.JSONDecodeError:
            report_config = {}

    effective_limit = payload.limit
    if effective_limit is None:
        try:
            effective_limit = int(report_config.get("default_limit")) if report_config.get("default_limit") is not None else None
        except (TypeError, ValueError):
            effective_limit = None

    try:
        raw_result = ReportingDatabaseService.execute_dataset(
            dataset.connection,
            dataset,
            params=payload.params,
            limit=effective_limit,
            db_session=db,
            user_context=user_ctx,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    config = report_config

    visible_columns = [
        str(col).strip()
        for col in (config.get("visible_columns") or [])
        if str(col).strip()
    ]
    if visible_columns:
        raw_result["columns"] = [col for col in raw_result["columns"] if col["name"] in visible_columns]
        raw_result["rows"] = [
            {key: value for key, value in row.items() if key in visible_columns}
            for row in raw_result["rows"]
        ]
        raw_result["numeric_summary"] = {
            key: value for key, value in raw_result["numeric_summary"].items() if key in visible_columns
        }

    return {
        "report": _serialize_reporting_report(report).dict(),
        "dataset": _serialize_reporting_dataset(dataset).dict(),
        **raw_result,
    }


SYNC_TARGET_DEFINITIONS = [
    {"code": "projects", "label": "马克园区档案", "system": "mark", "requires_community_ids": False},
    {"code": "charge_items", "label": "马克收费项目", "system": "mark", "requires_community_ids": True},
    {"code": "houses", "label": "马克房屋档案", "system": "mark", "requires_community_ids": True},
    {"code": "residents", "label": "马克住户档案", "system": "mark", "requires_community_ids": True},
    {"code": "parks", "label": "马克车位档案", "system": "mark", "requires_community_ids": True},
    {"code": "bills", "label": "马克运营账单", "system": "mark", "requires_community_ids": True},
    {"code": "receipt_bills", "label": "马克收款单", "system": "mark", "requires_community_ids": True},
    {"code": "deposit_records", "label": "马克押金记录", "system": "mark", "requires_community_ids": True},
    {"code": "prepayment_records", "label": "马克预存款记录", "system": "mark", "requires_community_ids": True},
    {"code": "accounting_subjects", "label": "金蝶会计科目", "system": "kingdee", "requires_community_ids": False},
    {"code": "customers", "label": "金蝶客户", "system": "kingdee", "requires_community_ids": False},
    {"code": "suppliers", "label": "金蝶供应商", "system": "kingdee", "requires_community_ids": False},
    {"code": "kd_houses", "label": "金蝶房号", "system": "kingdee", "requires_community_ids": False},
    {"code": "account_books", "label": "金蝶账簿", "system": "kingdee", "requires_community_ids": False},
    {"code": "auxiliary_data_categories", "label": "金蝶辅助资料分类", "system": "kingdee", "requires_community_ids": False},
    {"code": "auxiliary_data", "label": "金蝶辅助资料", "system": "kingdee", "requires_community_ids": False},
    {"code": "bank_accounts", "label": "金蝶银行账户", "system": "kingdee", "requires_community_ids": False},
]

SYNC_TARGET_MAP = {item["code"]: item for item in SYNC_TARGET_DEFINITIONS}


def _normalize_sync_time_text(value: Optional[str]) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        hour_str, minute_str = text.split(":", 1)
        hour = max(0, min(23, int(hour_str)))
        minute = max(0, min(59, int(minute_str)))
        return f"{hour:02d}:{minute:02d}"
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid time format, expected HH:MM")


def _normalize_schedule_community_ids(values: Optional[List[Any]]) -> List[int]:
    normalized: List[int] = []
    seen = set()
    for value in values or []:
        try:
            cid = int(value)
        except (TypeError, ValueError):
            continue
        if cid in seen:
            continue
        seen.add(cid)
        normalized.append(cid)
    return normalized


def _validate_sync_schedule_payload(payload: schemas.SyncScheduleBase | schemas.SyncScheduleUpdate) -> Dict[str, Any]:
    target_codes = [
        code for code in dict.fromkeys([str(code).strip() for code in (payload.target_codes or [])])
        if code in SYNC_TARGET_MAP
    ]
    if not target_codes:
        raise HTTPException(status_code=400, detail="At least one valid sync target must be selected")

    schedule_type = str(payload.schedule_type or "").strip()
    if schedule_type not in {"interval", "daily", "weekly"}:
        raise HTTPException(status_code=400, detail="Unsupported schedule type")

    interval_minutes = payload.interval_minutes
    daily_time = _normalize_sync_time_text(payload.daily_time)
    weekly_days = normalize_weekdays(payload.weekly_days)

    if schedule_type == "interval" and not interval_minutes:
        raise HTTPException(status_code=400, detail="Interval schedule requires interval_minutes")
    if schedule_type == "daily" and not daily_time:
        raise HTTPException(status_code=400, detail="Daily schedule requires daily_time")
    if schedule_type == "weekly":
        if not daily_time:
            raise HTTPException(status_code=400, detail="Weekly schedule requires daily_time")
        if not weekly_days:
            raise HTTPException(status_code=400, detail="Weekly schedule requires weekly_days")

    community_ids = _normalize_schedule_community_ids(payload.community_ids)
    requires_communities = any(SYNC_TARGET_MAP[code]["requires_community_ids"] for code in target_codes)
    if requires_communities and not community_ids:
        raise HTTPException(status_code=400, detail="Selected Mark targets require at least one community")

    timezone_name = str(payload.timezone or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    name = str(payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Schedule name is required")

    return {
        "name": name,
        "description": str(payload.description or "").strip() or None,
        "target_codes": target_codes,
        "community_ids": community_ids,
        "account_book_number": str(payload.account_book_number or "").strip() or None,
        "account_book_name": str(payload.account_book_name or "").strip() or None,
        "schedule_type": schedule_type,
        "interval_minutes": interval_minutes if schedule_type == "interval" else None,
        "daily_time": daily_time if schedule_type in {"daily", "weekly"} else None,
        "weekly_days": weekly_days if schedule_type == "weekly" else [],
        "timezone": timezone_name,
        "enabled": bool(payload.enabled),
    }


def _serialize_sync_schedule(schedule: models.SyncSchedule) -> Dict[str, Any]:
    creator_name = None
    updater_name = None
    if schedule.creator:
        creator_name = schedule.creator.real_name or schedule.creator.username
    if schedule.updater:
        updater_name = schedule.updater.real_name or schedule.updater.username

    return {
        "id": schedule.id,
        "name": schedule.name,
        "description": schedule.description,
        "target_codes": parse_json_list(schedule.target_codes),
        "community_ids": _normalize_schedule_community_ids(parse_json_list(schedule.community_ids)),
        "account_book_number": schedule.account_book_number,
        "account_book_name": schedule.account_book_name,
        "schedule_type": schedule.schedule_type,
        "interval_minutes": schedule.interval_minutes,
        "daily_time": schedule.daily_time,
        "weekly_days": normalize_weekdays(parse_json_list(schedule.weekly_days)),
        "timezone": schedule.timezone or DEFAULT_TIMEZONE,
        "enabled": bool(schedule.enabled),
        "is_running": bool(schedule.is_running),
        "current_execution_id": schedule.current_execution_id,
        "last_run_at": schedule.last_run_at,
        "last_status": schedule.last_status,
        "last_message": schedule.last_message,
        "next_run_at": schedule.next_run_at,
        "created_by": schedule.created_by,
        "updated_by": schedule.updated_by,
        "created_at": schedule.created_at,
        "updated_at": schedule.updated_at,
        "creator_name": creator_name,
        "updater_name": updater_name,
    }


def _serialize_sync_schedule_execution(execution: models.SyncScheduleExecution) -> Dict[str, Any]:
    result_payload = []
    if execution.result_payload:
        try:
            parsed_payload = json.loads(execution.result_payload)
            if isinstance(parsed_payload, list):
                result_payload = parsed_payload
        except (TypeError, ValueError):
            result_payload = []

    triggered_by_name = None
    if execution.triggered_by_user:
        triggered_by_name = execution.triggered_by_user.real_name or execution.triggered_by_user.username

    return {
        "id": execution.id,
        "schedule_id": execution.schedule_id,
        "trigger_type": execution.trigger_type,
        "triggered_by": execution.triggered_by,
        "triggered_by_name": triggered_by_name,
        "status": execution.status,
        "started_at": execution.started_at,
        "finished_at": execution.finished_at,
        "total_targets": execution.total_targets or 0,
        "success_targets": execution.success_targets or 0,
        "failed_targets": execution.failed_targets or 0,
        "summary": execution.summary,
        "error_message": execution.error_message,
        "result_payload": result_payload,
        "created_at": execution.created_at,
        "updated_at": execution.updated_at,
    }


def _resolve_schedule_community_ids(schedule_data: Dict[str, Any]) -> List[int]:
    community_ids = _normalize_schedule_community_ids(schedule_data.get("community_ids"))
    if community_ids:
        return community_ids

    db = database.SessionLocal()
    try:
        return [row[0] for row in db.query(models.ProjectList.proj_id).order_by(models.ProjectList.proj_id).all()]
    finally:
        db.close()


def _build_tracker_result(task_id: str, fallback_message: str) -> Dict[str, Any]:
    task_status = tracker.get_task_status(task_id) or {}
    tracker_status = str(task_status.get("status") or "").strip().lower()
    if tracker_status in {"completed"}:
        status = "success"
    elif tracker_status in {"failed", "partially_completed"}:
        status = "failed"
    else:
        status = "success"
    logs = task_status.get("logs") if isinstance(task_status.get("logs"), list) else []
    message = task_status.get("status") or fallback_message
    return {
        "status": status,
        "message": str(message),
        "task_id": task_id,
        "logs": logs,
    }


def _run_finance_sync_task(
    endpoint_callable,
    request_model,
    user_context: Dict[str, str],
) -> Dict[str, Any]:
    db = database.SessionLocal()
    try:
        background_tasks = BackgroundTasks()
        response = endpoint_callable(request_model, background_tasks, user_context, db)
        message = response.get("message") or response.get("detail") or "Sync started"
        logs = [{"type": "info", "message": str(message)}]
        for task in getattr(background_tasks, "tasks", []):
            task.func(*task.args, **task.kwargs)
        return {"status": "success", "message": str(message), "logs": logs}
    except Exception as exc:
        return {
            "status": "failed",
            "message": str(exc),
            "logs": [{"type": "error", "message": str(exc)}],
        }
    finally:
        db.close()


def _handle_projects_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    fetch_projects_main()
    return {
        "status": "success",
        "message": "Project sync completed",
        "logs": [{"type": "info", "message": "Project sync completed"}],
    }


def _handle_charge_items_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    sync_charge_items(community_ids)
    return {
        "status": "success",
        "message": f"Charge item sync completed for {len(community_ids)} communities",
        "logs": [{"type": "info", "message": f"Processed {len(community_ids)} communities"}],
    }


def _handle_houses_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    task_id = tracker.create_task(community_ids)
    sync_houses(community_ids, task_id)
    return _build_tracker_result(task_id, "House sync completed")


def _handle_residents_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    task_id = tracker.create_task(community_ids)
    sync_residents(community_ids, task_id)
    return _build_tracker_result(task_id, "Resident sync completed")


def _handle_parks_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    task_id = tracker.create_task(community_ids)
    sync_parks(community_ids, task_id)
    return _build_tracker_result(task_id, "Park sync completed")


def _handle_bills_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    task_id = tracker.create_task(community_ids)
    sync_bills(community_ids, task_id)
    return _build_tracker_result(task_id, "Bill sync completed")


def _handle_receipt_bills_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    task_id = tracker.create_task(community_ids)
    sync_receipt_bills(community_ids, task_id)
    return _build_tracker_result(task_id, "Receipt bill sync completed")


def _handle_deposit_records_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = _resolve_schedule_community_ids(schedule_data)
    task_id = tracker.create_task(community_ids)
    sync_deposit_records(community_ids, task_id)
    return _build_tracker_result(task_id, "Deposit record sync completed")


def _handle_prepayment_records_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = _resolve_schedule_community_ids(schedule_data)
    task_id = tracker.create_task(community_ids)
    sync_prepayment_records(community_ids, task_id)
    return _build_tracker_result(task_id, "Prepayment record sync completed")


def _handle_accounting_subjects_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        sync_accounting_subjects,
        schemas.AccountingSubjectSyncRequest(),
        user_context,
    )


def _handle_customers_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        sync_customers,
        schemas.CustomerSyncRequest(),
        user_context,
    )


def _handle_suppliers_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        sync_suppliers,
        schemas.SupplierSyncRequest(),
        user_context,
    )


def _handle_kd_houses_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        sync_kd_houses,
        schemas.KingdeeHouseSyncRequest(),
        user_context,
    )


def _handle_account_books_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        sync_kd_account_books,
        schemas.KingdeeAccountBookSyncRequest(),
        user_context,
    )


def _handle_auxiliary_data_categories_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        sync_auxiliary_data_categories,
        schemas.AuxiliaryDataCategorySyncRequest(),
        user_context,
    )


def _handle_auxiliary_data_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        sync_auxiliary_data,
        schemas.AuxiliaryDataSyncRequest(),
        user_context,
    )


def _handle_bank_accounts_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        sync_kd_bank_accounts,
        schemas.KingdeeBankAccountSyncRequest(),
        user_context,
    )

SYNC_TARGET_HANDLERS: Dict[str, Any] = {}


def _register_sync_target_handler(code: str, handler):
    SYNC_TARGET_HANDLERS[code] = handler
    sync_schedule_service.register_handler(code, handler)


def run_sync_target_handler(target_code: str, schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    handler = SYNC_TARGET_HANDLERS.get(target_code)
    if not handler:
        raise RuntimeError(f"No sync handler registered for target '{target_code}'.")
    return handler(schedule_data, user_context)


_register_sync_target_handler("projects", _handle_projects_sync)
_register_sync_target_handler("charge_items", _handle_charge_items_sync)
_register_sync_target_handler("houses", _handle_houses_sync)
_register_sync_target_handler("residents", _handle_residents_sync)
_register_sync_target_handler("parks", _handle_parks_sync)
_register_sync_target_handler("bills", _handle_bills_sync)
_register_sync_target_handler("receipt_bills", _handle_receipt_bills_sync)
_register_sync_target_handler("deposit_records", _handle_deposit_records_sync)
_register_sync_target_handler("prepayment_records", _handle_prepayment_records_sync)
_register_sync_target_handler("accounting_subjects", _handle_accounting_subjects_sync)
_register_sync_target_handler("customers", _handle_customers_sync)
_register_sync_target_handler("suppliers", _handle_suppliers_sync)
_register_sync_target_handler("kd_houses", _handle_kd_houses_sync)
_register_sync_target_handler("account_books", _handle_account_books_sync)
_register_sync_target_handler("auxiliary_data_categories", _handle_auxiliary_data_categories_sync)
_register_sync_target_handler("auxiliary_data", _handle_auxiliary_data_sync)
_register_sync_target_handler("bank_accounts", _handle_bank_accounts_sync)


@app.on_event("startup")
def start_sync_schedule_service():
    sync_schedule_service.start()


@app.on_event("shutdown")
def stop_sync_schedule_service():
    sync_schedule_service.stop()


@app.get("/api/sync-schedules/meta")
def get_sync_schedule_meta(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    return {
        "targets": SYNC_TARGET_DEFINITIONS,
        "schedule_types": [
            {"value": "interval", "label": "间隔执行"},
            {"value": "daily", "label": "每日执行"},
            {"value": "weekly", "label": "每周执行"},
        ],
        "weekdays": [
            {"value": "MON", "label": "周一"},
            {"value": "TUE", "label": "周二"},
            {"value": "WED", "label": "周三"},
            {"value": "THU", "label": "周四"},
            {"value": "FRI", "label": "周五"},
            {"value": "SAT", "label": "周六"},
            {"value": "SUN", "label": "周日"},
        ],
        "default_timezone": DEFAULT_TIMEZONE,
    }


@app.get("/api/sync-schedules")
def list_sync_schedules(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    schedules = (
        db.query(models.SyncSchedule)
        .options(
            joinedload(models.SyncSchedule.creator),
            joinedload(models.SyncSchedule.updater),
        )
        .order_by(models.SyncSchedule.created_at.desc(), models.SyncSchedule.id.desc())
        .all()
    )
    return [_serialize_sync_schedule(item) for item in schedules]


@app.post("/api/sync-schedules", response_model=schemas.SyncScheduleResponse)
def create_sync_schedule(
    payload: schemas.SyncScheduleCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    normalized = _validate_sync_schedule_payload(payload)
    schedule = models.SyncSchedule(
        name=normalized["name"],
        description=normalized["description"],
        target_codes=serialize_json_list(normalized["target_codes"]),
        community_ids=serialize_json_list(normalized["community_ids"]),
        account_book_number=normalized["account_book_number"],
        account_book_name=normalized["account_book_name"],
        schedule_type=normalized["schedule_type"],
        interval_minutes=normalized["interval_minutes"],
        daily_time=normalized["daily_time"],
        weekly_days=serialize_json_list(normalized["weekly_days"]),
        timezone=normalized["timezone"],
        enabled=normalized["enabled"],
        next_run_at=compute_next_run_at(
            schedule_type=normalized["schedule_type"],
            interval_minutes=normalized["interval_minutes"],
            daily_time=normalized["daily_time"],
            weekly_days=normalized["weekly_days"],
            timezone_name=normalized["timezone"],
            now_utc=utcnow_naive(),
        ) if normalized["enabled"] else None,
        created_by=current_user.id,
        updated_by=current_user.id,
    )
    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    return _serialize_sync_schedule(schedule)


@app.put("/api/sync-schedules/{schedule_id}", response_model=schemas.SyncScheduleResponse)
def update_sync_schedule(
    schedule_id: int,
    payload: schemas.SyncScheduleUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    schedule = db.query(models.SyncSchedule).filter(models.SyncSchedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Sync schedule not found")

    current_payload = schemas.SyncScheduleCreate(
        name=payload.name if payload.name is not None else schedule.name,
        description=payload.description if payload.description is not None else schedule.description,
        target_codes=payload.target_codes if payload.target_codes is not None else parse_json_list(schedule.target_codes),
        community_ids=payload.community_ids if payload.community_ids is not None else _normalize_schedule_community_ids(parse_json_list(schedule.community_ids)),
        account_book_number=payload.account_book_number if payload.account_book_number is not None else schedule.account_book_number,
        account_book_name=payload.account_book_name if payload.account_book_name is not None else schedule.account_book_name,
        schedule_type=payload.schedule_type if payload.schedule_type is not None else schedule.schedule_type,
        interval_minutes=payload.interval_minutes if payload.interval_minutes is not None else schedule.interval_minutes,
        daily_time=payload.daily_time if payload.daily_time is not None else schedule.daily_time,
        weekly_days=payload.weekly_days if payload.weekly_days is not None else normalize_weekdays(parse_json_list(schedule.weekly_days)),
        timezone=payload.timezone if payload.timezone is not None else (schedule.timezone or DEFAULT_TIMEZONE),
        enabled=payload.enabled if payload.enabled is not None else bool(schedule.enabled),
    )
    normalized = _validate_sync_schedule_payload(current_payload)

    schedule.name = normalized["name"]
    schedule.description = normalized["description"]
    schedule.target_codes = serialize_json_list(normalized["target_codes"])
    schedule.community_ids = serialize_json_list(normalized["community_ids"])
    schedule.account_book_number = normalized["account_book_number"]
    schedule.account_book_name = normalized["account_book_name"]
    schedule.schedule_type = normalized["schedule_type"]
    schedule.interval_minutes = normalized["interval_minutes"]
    schedule.daily_time = normalized["daily_time"]
    schedule.weekly_days = serialize_json_list(normalized["weekly_days"])
    schedule.timezone = normalized["timezone"]
    schedule.enabled = normalized["enabled"]
    schedule.updated_by = current_user.id

    if schedule.enabled:
        schedule.next_run_at = compute_next_run_at(
            schedule_type=normalized["schedule_type"],
            interval_minutes=normalized["interval_minutes"],
            daily_time=normalized["daily_time"],
            weekly_days=normalized["weekly_days"],
            timezone_name=normalized["timezone"],
            now_utc=utcnow_naive(),
        )
    else:
        schedule.next_run_at = None

    db.commit()
    db.refresh(schedule)
    return _serialize_sync_schedule(schedule)


@app.delete("/api/sync-schedules/{schedule_id}")
def delete_sync_schedule(
    schedule_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    schedule = db.query(models.SyncSchedule).filter(models.SyncSchedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Sync schedule not found")
    if schedule.is_running:
        raise HTTPException(status_code=400, detail="Running schedule cannot be deleted")

    db.delete(schedule)
    db.commit()
    return {"message": "Sync schedule deleted"}


@app.post("/api/sync-schedules/{schedule_id}/run")
def run_sync_schedule_now(
    schedule_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    try:
        payload = sync_schedule_service.trigger_execution(
            schedule_id=schedule_id,
            trigger_type="manual",
            user_id=current_user.id,
            advance_schedule=False,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return payload


@app.post("/api/sync-schedules/{schedule_id}/toggle", response_model=schemas.SyncScheduleResponse)
def toggle_sync_schedule(
    schedule_id: int,
    enabled: bool = Query(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    schedule = db.query(models.SyncSchedule).filter(models.SyncSchedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Sync schedule not found")

    schedule.enabled = enabled
    schedule.updated_by = current_user.id
    if enabled:
        schedule.next_run_at = compute_next_run_at(
            schedule_type=schedule.schedule_type,
            interval_minutes=schedule.interval_minutes,
            daily_time=schedule.daily_time,
            weekly_days=parse_json_list(schedule.weekly_days),
            timezone_name=schedule.timezone,
            now_utc=utcnow_naive(),
        )
    else:
        schedule.next_run_at = None

    db.commit()
    db.refresh(schedule)
    return _serialize_sync_schedule(schedule)


@app.get("/api/sync-schedules/{schedule_id}/executions")
def list_sync_schedule_executions(
    schedule_id: int,
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    schedule = db.query(models.SyncSchedule).filter(models.SyncSchedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Sync schedule not found")

    executions = (
        db.query(models.SyncScheduleExecution)
        .options(joinedload(models.SyncScheduleExecution.triggered_by_user))
        .filter(models.SyncScheduleExecution.schedule_id == schedule_id)
        .order_by(models.SyncScheduleExecution.started_at.desc(), models.SyncScheduleExecution.id.desc())
        .limit(limit)
        .all()
    )
    return [_serialize_sync_schedule_execution(item) for item in executions]


@app.get("/api/sync-schedules/executions/latest")
def list_latest_sync_schedule_executions(
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    executions = (
        db.query(models.SyncScheduleExecution)
        .options(
            joinedload(models.SyncScheduleExecution.triggered_by_user),
            joinedload(models.SyncScheduleExecution.schedule),
        )
        .order_by(models.SyncScheduleExecution.started_at.desc(), models.SyncScheduleExecution.id.desc())
        .limit(limit)
        .all()
    )

    result = []
    for execution in executions:
        item = _serialize_sync_schedule_execution(execution)
        item["schedule_name"] = execution.schedule.name if execution.schedule else ""
        result.append(item)
    return result

if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", 8100))
    
    uvicorn.run("main:app", host=host, port=port, reload=True)
