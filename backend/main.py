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
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session, joinedload, selectinload
from sqlalchemy import and_, desc, func, extract, or_, inspect, text, cast, String
from sqlalchemy.exc import IntegrityError
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple
import models, schemas, database
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
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")
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
from api import frontend as frontend_module
from api import voucher_preview_handlers as voucher_preview_handlers_module
from api import voucher_push_handlers as voucher_push_handlers_module
from api import voucher_template_handlers as voucher_template_handlers_module
from api import voucher_helpers as voucher_helper_module
from api.routers import archives as archives_router_module
from api.routers import bills as bills_router_module
from api.routers import external_services as external_services_router_module
from api.routers import finance as finance_router_module
from api.routers import master_data as master_data_router_module
from api.routers import oa_journals as oa_journals_router_module
from api.routers import organization_template_categories as org_tpl_router_module
from api.routers import project_reports as project_reports_router_module
from api.routers import records as records_router_module
from api.routers import reporting as reporting_router_module
from api.routers import settings as settings_router_module
from api.routers import sync_schedules as sync_schedules_router_module
from api.routers import users as users_router_module
from api.routers import voucher_preview_push as voucher_preview_push_router_module
from api.routers import voucher_templates as voucher_templates_router_module

# Configure logger for project sync
logger = logging.getLogger('project_sync')
if not logger.handlers:
    log_path = os.path.join(os.path.dirname(__file__), 'scripts', 'fetch_projects.log')
    handler = logging.FileHandler(log_path)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _is_mssql() -> bool:
    return database.engine.dialect.name == "mssql"


def _index_names(table_name: str) -> set[str]:
    inspector_obj = inspect(database.engine)
    try:
        return {idx["name"] for idx in inspector_obj.get_indexes(table_name)}
    except Exception:
        return set()


def _create_index_if_missing(conn, table_name: str, index_name: str, columns_sql: str) -> None:
    if index_name in _index_names(table_name):
        return
    conn.execute(text(f"CREATE INDEX {index_name} ON {table_name} ({columns_sql})"))


def _drop_index_if_exists(conn, table_name: str, index_name: str) -> None:
    if index_name not in _index_names(table_name):
        return
    if _is_mssql():
        conn.execute(text(f"DROP INDEX {index_name} ON {table_name}"))
    else:
        conn.execute(text(f"DROP INDEX IF EXISTS {index_name}"))


def _upsert_rows(
    db_session: Session,
    model: Any,
    rows: List[Dict[str, Any]],
    conflict_fields: List[str],
    immutable_fields: Optional[Set[str]] = None,
    batch_size: int = 100,
) -> int:
    immutable = set(immutable_fields or set())
    processed = 0

    for row in rows:
        if not isinstance(row, dict):
            continue
        if any(row.get(field) is None for field in conflict_fields):
            continue

        filters = [getattr(model, field) == row[field] for field in conflict_fields]
        existing = db_session.query(model).filter(and_(*filters)).one_or_none()

        if existing:
            for key, value in row.items():
                if key in immutable:
                    continue
                setattr(existing, key, value)
        else:
            db_session.add(model(**row))

        processed += 1
        if processed % batch_size == 0:
            db_session.commit()

    if processed % batch_size != 0:
        db_session.commit()

    return processed

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
            if _is_mssql():
                conn.execute(text("ALTER TABLE houses ADD disable BIT DEFAULT 0"))
            else:
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

    columns_info = {c["name"]: c for c in inspector.get_columns("bills")}
    existing_cols = set(columns_info.keys())
    added_receive_date = False

    with database.engine.begin() as conn:
        if "receive_date" not in existing_cols:
            if _is_mssql():
                conn.execute(text("ALTER TABLE bills ADD receive_date DATETIME2 NULL"))
            else:
                conn.execute(text("ALTER TABLE bills ADD COLUMN receive_date TIMESTAMP"))
            added_receive_date = True
        elif _is_mssql():
            receive_type = str(columns_info["receive_date"].get("type", "")).upper()
            if receive_type.startswith("DATE") and "TIME" not in receive_type:
                conn.execute(text("ALTER TABLE bills ALTER COLUMN receive_date DATETIME2 NULL"))

        if "bill_month" in existing_cols and _is_mssql():
            bill_month_type = str(columns_info["bill_month"].get("type", "")).upper()
            if "DATETIME" in bill_month_type or bill_month_type.startswith("TIMESTAMP"):
                conn.execute(text("ALTER TABLE bills ALTER COLUMN bill_month DATE NULL"))

        # Keep bigint-width fields consistent with PostgreSQL to avoid 32-bit truncation risk.
        if _is_mssql():
            bigint_cols = ["start_time", "end_time", "pay_time", "deal_log_id", "create_time"]
            for col_name in bigint_cols:
                if col_name not in existing_cols:
                    continue
                col_type = str(columns_info[col_name].get("type", "")).upper()
                if "BIGINT" in col_type:
                    continue
                conn.execute(text(f"ALTER TABLE bills ALTER COLUMN [{col_name}] BIGINT NULL"))

        # Backfill receive_date from pay_time (unix timestamp seconds)
        if added_receive_date or "receive_date" in existing_cols:
            rows = conn.execute(text("""
                SELECT id, community_id, pay_time
                FROM bills
                WHERE receive_date IS NULL
                  AND pay_time IS NOT NULL
                  AND pay_time > 0
            """)).fetchall()
            for row in rows:
                try:
                    dt = datetime.fromtimestamp(int(row.pay_time))
                except Exception:
                    continue
                conn.execute(
                    text("UPDATE bills SET receive_date = :d WHERE id = :id AND community_id = :cid"),
                    {"d": dt, "id": row.id, "cid": row.community_id},
                )


_ensure_bill_columns()


def _ensure_charge_and_project_columns():
    inspector = inspect(database.engine)
    tables = set(inspector.get_table_names())

    with database.engine.begin() as conn:
        if "charge_items" in tables:
            charge_cols = {c["name"] for c in inspector.get_columns("charge_items")}
            if "updated_at" not in charge_cols:
                if _is_mssql():
                    conn.execute(text("ALTER TABLE charge_items ADD updated_at DATETIME2 NULL"))
                else:
                    conn.execute(text("ALTER TABLE charge_items ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))

        if "projects_lists" in tables:
            project_cols = {c["name"] for c in inspector.get_columns("projects_lists")}
            if "updated_at" not in project_cols:
                if _is_mssql():
                    conn.execute(text("ALTER TABLE projects_lists ADD updated_at DATETIME2 NULL"))
                else:
                    conn.execute(text("ALTER TABLE projects_lists ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))


_ensure_charge_and_project_columns()


def _ensure_deposit_record_columns():
    inspector = inspect(database.engine)
    tables = inspector.get_table_names()
    if "deposit_records" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("deposit_records")}
    with database.engine.begin() as conn:
        if "payment_id" not in existing_cols:
            conn.execute(text("ALTER TABLE deposit_records ADD COLUMN payment_id BIGINT"))
        _create_index_if_missing(conn, "deposit_records", "ix_deposit_records_payment_id", "payment_id")


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
        _create_index_if_missing(conn, "prepayment_records", "ix_prepayment_records_payment_id", "payment_id")


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
            _drop_index_if_exists(conn, "users", "ix_users_email")
            _create_index_if_missing(conn, "users", "ix_users_email", "email")


_ensure_user_email_index()

BILL_VOUCHER_PUSH_STATUS_LABELS = {
    "not_pushed": "未推送",
    "pushing": "推送中",
    "success": "已推送",
    "failed": "推送失败",
}

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

def _jsonify_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _prefix_source_fields(data: Dict[str, Any], source_type: str, module_prefix: str = "marki") -> Dict[str, Any]:
    return mapping_prefix_source_fields(data, source_type, module_prefix=module_prefix)


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

# Route handlers below are intentionally rebound to the shared helper module.
# This keeps behavior aligned while we reduce voucher-specific logic in main.py.
_decode_header_value = voucher_helper_module._decode_header_value
_normalize_bill_refs = voucher_helper_module._normalize_bill_refs
_get_bill_push_status_map = voucher_helper_module._get_bill_push_status_map
_summarize_bill_push_statuses = voucher_helper_module._summarize_bill_push_statuses
_get_related_bill_refs_for_receipts = voucher_helper_module._get_related_bill_refs_for_receipts
_normalize_receipt_refs = voucher_helper_module._normalize_receipt_refs
_serialize_receipt_bill_model = voucher_helper_module._serialize_receipt_bill_model
_enrich_receipt_bill_data = voucher_helper_module._enrich_receipt_bill_data
_load_receipt_to_bills_relation = voucher_helper_module._load_receipt_to_bills_relation
_load_receipt_to_deposit_collect_relation = voucher_helper_module._load_receipt_to_deposit_collect_relation
_load_receipt_to_deposit_refund_relation = voucher_helper_module._load_receipt_to_deposit_refund_relation
_load_receipt_to_prepayment_recharge_relation = voucher_helper_module._load_receipt_to_prepayment_recharge_relation
_load_receipt_to_prepayment_refund_relation = voucher_helper_module._load_receipt_to_prepayment_refund_relation
_find_bill_push_conflicts = voucher_helper_module._find_bill_push_conflicts
_extract_kingdee_voucher_result = voucher_helper_module._extract_kingdee_voucher_result
_extract_kingdee_push_message = voucher_helper_module._extract_kingdee_push_message
_finalize_bill_push_records = voucher_helper_module._finalize_bill_push_records


app = FastAPI(title="FinFlow Middleware")
app.include_router(archives_router_module.router)
app.include_router(bills_router_module.router)
app.include_router(external_services_router_module.router)
app.include_router(finance_router_module.router)
app.include_router(master_data_router_module.router)
app.include_router(oa_journals_router_module.router)
app.include_router(org_tpl_router_module.router)
app.include_router(project_reports_router_module.router)
app.include_router(records_router_module.router)
app.include_router(reporting_router_module.router)
app.include_router(settings_router_module.router)
app.include_router(sync_schedules_router_module.router)
app.include_router(users_router_module.router)
app.include_router(voucher_preview_push_router_module.router)
app.include_router(voucher_templates_router_module.router)

build_template_category_path_map = org_tpl_router_module.build_template_category_path_map
_normalize_template_for_response = voucher_template_handlers_module._normalize_template_for_response
_validate_trigger_condition = voucher_template_handlers_module._validate_trigger_condition
_validate_voucher_template_payload = voucher_template_handlers_module._validate_voucher_template_payload
get_voucher_templates = voucher_template_handlers_module.get_voucher_templates
get_voucher_template = voucher_template_handlers_module.get_voucher_template
create_voucher_template = voucher_template_handlers_module.create_voucher_template
update_voucher_template = voucher_template_handlers_module.update_voucher_template
delete_voucher_template = voucher_template_handlers_module.delete_voucher_template
resolve_voucher_fields = voucher_template_handlers_module.resolve_voucher_fields
_check_trigger_conditions = voucher_preview_handlers_module._check_trigger_conditions
_preview_voucher_for_bill_via_receipt_templates = voucher_preview_handlers_module._preview_voucher_for_bill_via_receipt_templates
preview_voucher_for_bill = voucher_preview_handlers_module.preview_voucher_for_bill
preview_voucher_for_receipt = voucher_preview_handlers_module.preview_voucher_for_receipt
preview_voucher_for_receipts = voucher_preview_handlers_module.preview_voucher_for_receipts
preview_voucher_for_bills = voucher_preview_handlers_module.preview_voucher_for_bills
push_voucher_to_kingdee = voucher_push_handlers_module.push_voucher_to_kingdee


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
allow_lan_origins = (
    os.getenv("ALLOW_LAN_ORIGINS", "").strip().lower() in {"1", "true", "yes", "on"}
)
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
    allow_origin_regex=(
        None
        if allow_all_origins or not allow_lan_origins
        else r"^http://(localhost|127\.0\.0\.1|192\.168\.\d+\.\d+|10\.\d+\.\d+\.\d+)(:\d+)?$"
    ),
    # This project uses Authorization header (Bearer token) instead of cookies.
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
frontend_module.mount_frontend_assets(app)

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

# Dashboard Stats - Removed Globally



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
    if _is_mssql():
        # SQL Server 2016 has no STRING_AGG; keep one representative resident name.
        return (
            db.query(
                models.House.house_id.label("house_id"),
                models.House.community_id.label("community_id"),
                func.max(resident_display).label("resident_name"),
            )
            .join(models.HouseUser, models.HouseUser.house_fk == models.House.id)
            .filter(resident_display.isnot(None))
            .group_by(models.House.house_id, models.House.community_id)
            .subquery()
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


def get_voucher_source_fields(source_type: str = Query("bills")):
    actual_source = (source_type or "").strip().lower() or "bills"
    return {"source_type": actual_source, "fields": _build_source_field_options(actual_source)}


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


def get_archive_config(archive_key: str, db: Session = Depends(get_db)):
    return archives_router_module.get_archive_config(archive_key, db)


def save_archive_config(archive_key: str, config: dict, db: Session = Depends(get_db)):
    return archives_router_module.save_archive_config(archive_key, config, db)

app.include_router(frontend_module.router)

if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", 8100))
    reload_enabled = os.getenv("APP_RELOAD", "").strip().lower() in {"1", "true", "yes", "on"}
    
    uvicorn.run("main:app", host=host, port=port, reload=reload_enabled)

