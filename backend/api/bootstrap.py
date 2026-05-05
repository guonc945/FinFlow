# -*- coding: utf-8 -*-
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import and_, inspect, text
from sqlalchemy.orm import Session

import database
import models

def _is_mssql() -> bool:
    return database.engine.dialect.name == "mssql"


def _table_names() -> set[str]:
    if _is_mssql():
        with database.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT name
                    FROM sys.tables
                    WHERE is_ms_shipped = 0
                    """
                )
            ).fetchall()
        return {str(row[0]) for row in rows if row and row[0]}

    inspector_obj = inspect(database.engine)
    try:
        return set(inspector_obj.get_table_names())
    except Exception:
        return set()


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

def _ensure_voucher_columns():
    inspector = inspect(database.engine)
    tables = _table_names()
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

def _ensure_house_columns():
    inspector = inspect(database.engine)
    tables = _table_names()
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

def _ensure_park_columns():
    inspector = inspect(database.engine)
    tables = _table_names()
    if "parks" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("parks")}
    with database.engine.begin() as conn:
        if "house_id" not in existing_cols:
            conn.execute(text("ALTER TABLE parks ADD COLUMN house_id VARCHAR(50)"))
        if "house_fk" not in existing_cols:
            conn.execute(text("ALTER TABLE parks ADD COLUMN house_fk INTEGER"))

def _ensure_bill_columns():
    inspector = inspect(database.engine)
    tables = _table_names()
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
        if "last_seen_at" not in existing_cols:
            if _is_mssql():
                conn.execute(text("ALTER TABLE bills ADD last_seen_at DATETIME2 NULL"))
            else:
                conn.execute(text("ALTER TABLE bills ADD COLUMN last_seen_at TIMESTAMP"))
        if "source_deleted" not in existing_cols:
            if _is_mssql():
                conn.execute(text("ALTER TABLE bills ADD source_deleted BIT NOT NULL CONSTRAINT DF_bills_source_deleted DEFAULT 0"))
            else:
                conn.execute(text("ALTER TABLE bills ADD COLUMN source_deleted BOOLEAN DEFAULT FALSE"))
        if "source_deleted_at" not in existing_cols:
            if _is_mssql():
                conn.execute(text("ALTER TABLE bills ADD source_deleted_at DATETIME2 NULL"))
            else:
                conn.execute(text("ALTER TABLE bills ADD COLUMN source_deleted_at TIMESTAMP"))
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

        conn.execute(text("""
            UPDATE bills
            SET source_deleted = 0
            WHERE source_deleted IS NULL
        """))

        conn.execute(text("""
            UPDATE bills
            SET last_seen_at = COALESCE(last_seen_at, updated_at, created_at, GETDATE())
            WHERE last_seen_at IS NULL
        """) if _is_mssql() else text("""
            UPDATE bills
            SET last_seen_at = COALESCE(last_seen_at, updated_at, created_at, CURRENT_TIMESTAMP)
            WHERE last_seen_at IS NULL
        """))

        # Ensure hot-path relation index exists for receipt drilldown / voucher preview.
        _create_index_if_missing(conn, "bills", "ix_bills_community_deal_log", "community_id, deal_log_id")
        _create_index_if_missing(conn, "bills", "ix_bills_community_source_deleted", "community_id, source_deleted")


def _ensure_receipt_bill_user_indexes():
    inspector = inspect(database.engine)
    tables = _table_names()
    if "receipt_bill_users" not in tables:
        return

    with database.engine.begin() as conn:
        _create_index_if_missing(
            conn,
            "receipt_bill_users",
            "ix_receipt_bill_users_receipt_community",
            "receipt_bill_id, community_id",
        )

def _ensure_charge_and_project_columns():
    inspector = inspect(database.engine)
    tables = _table_names()

    with database.engine.begin() as conn:
        if "charge_items" in tables:
            charge_cols = {c["name"] for c in inspector.get_columns("charge_items")}
            if "updated_at" not in charge_cols:
                if _is_mssql():
                    conn.execute(text("ALTER TABLE charge_items ADD updated_at DATETIME2 NULL"))
                else:
                    conn.execute(text("ALTER TABLE charge_items ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
            if "kingdee_tax_rate_id" not in charge_cols:
                if _is_mssql():
                    conn.execute(text("ALTER TABLE charge_items ADD kingdee_tax_rate_id NVARCHAR(50) NULL"))
                else:
                    conn.execute(text("ALTER TABLE charge_items ADD COLUMN kingdee_tax_rate_id VARCHAR(50)"))

        if "projects_lists" in tables:
            project_cols = {c["name"] for c in inspector.get_columns("projects_lists")}
            if "updated_at" not in project_cols:
                if _is_mssql():
                    conn.execute(text("ALTER TABLE projects_lists ADD updated_at DATETIME2 NULL"))
                else:
                    conn.execute(text("ALTER TABLE projects_lists ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))

def _ensure_deposit_record_columns():
    inspector = inspect(database.engine)
    tables = _table_names()
    if "deposit_records" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("deposit_records")}
    with database.engine.begin() as conn:
        if "payment_id" not in existing_cols:
            conn.execute(text("ALTER TABLE deposit_records ADD COLUMN payment_id BIGINT"))
        _create_index_if_missing(conn, "deposit_records", "ix_deposit_records_payment_id", "payment_id")

def _ensure_prepayment_record_columns():
    inspector = inspect(database.engine)
    tables = _table_names()
    if "prepayment_records" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("prepayment_records")}
    with database.engine.begin() as conn:
        if "payment_id" not in existing_cols:
            conn.execute(text("ALTER TABLE prepayment_records ADD COLUMN payment_id BIGINT"))
        _create_index_if_missing(conn, "prepayment_records", "ix_prepayment_records_payment_id", "payment_id")

def _ensure_user_email_index():
    inspector = inspect(database.engine)
    tables = _table_names()
    if "users" not in tables:
        return

    indexes = {idx["name"]: idx for idx in inspector.get_indexes("users")}
    email_index = indexes.get("ix_users_email")

    with database.engine.begin() as conn:
        if email_index and email_index.get("unique"):
            _drop_index_if_exists(conn, "users", "ix_users_email")
            _create_index_if_missing(conn, "users", "ix_users_email", "email")


def _ensure_business_dictionary_tables():
    models.BusinessDictionary.__table__.create(bind=database.engine, checkfirst=True)
    models.BusinessDictionaryItem.__table__.create(bind=database.engine, checkfirst=True)

def _ensure_data_dictionary_columns():
    inspector = inspect(database.engine)
    tables = _table_names()
    if "data_dictionaries" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("data_dictionaries")}
    with database.engine.begin() as conn:
        if "dict_type" not in existing_cols:
            if _is_mssql():
                conn.execute(text("ALTER TABLE data_dictionaries ADD dict_type NVARCHAR(20) NOT NULL CONSTRAINT DF_data_dictionaries_dict_type DEFAULT 'enum'"))
            else:
                conn.execute(text("ALTER TABLE data_dictionaries ADD COLUMN dict_type VARCHAR(20) NOT NULL DEFAULT 'enum'"))


def _ensure_sync_module_status_table() -> None:
    models.SyncModuleStatus.__table__.create(bind=database.engine, checkfirst=True)
    inspector = inspect(database.engine)
    tables = _table_names()
    if "sync_module_status" not in tables:
        return

    existing_cols = {c["name"] for c in inspector.get_columns("sync_module_status")}
    with database.engine.begin() as conn:
        if "status" not in existing_cols:
            if _is_mssql():
                conn.execute(text("ALTER TABLE sync_module_status ADD status NVARCHAR(20) NOT NULL CONSTRAINT DF_sync_module_status_status DEFAULT 'idle'"))
            else:
                conn.execute(text("ALTER TABLE sync_module_status ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'idle'"))
        if "message" not in existing_cols:
            if _is_mssql():
                conn.execute(text("ALTER TABLE sync_module_status ADD message NVARCHAR(500) NULL"))
            else:
                conn.execute(text("ALTER TABLE sync_module_status ADD COLUMN message VARCHAR(500)"))
        if "started_at" not in existing_cols:
            if _is_mssql():
                conn.execute(text("ALTER TABLE sync_module_status ADD started_at DATETIME2 NULL"))
            else:
                conn.execute(text("ALTER TABLE sync_module_status ADD COLUMN started_at TIMESTAMP"))
        if "finished_at" not in existing_cols:
            if _is_mssql():
                conn.execute(text("ALTER TABLE sync_module_status ADD finished_at DATETIME2 NULL"))
            else:
                conn.execute(text("ALTER TABLE sync_module_status ADD COLUMN finished_at TIMESTAMP"))

def initialize_database() -> None:
    startup_bootstrap_enabled = os.getenv("FINFLOW_ENABLE_STARTUP_BOOTSTRAP", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if _is_mssql() and not startup_bootstrap_enabled:
        _ensure_sync_module_status_table()
        _ensure_business_dictionary_tables()
        _ensure_data_dictionary_columns()
        return

    existing_tables = _table_names()

    # SQL Server remote instances can become extremely slow when create_all()
    # checks every mapped table with per-table has_table probes. For an already
    # initialized database, prefer the fast incremental bootstrap path.
    if not (_is_mssql() and existing_tables):
        models.Base.metadata.create_all(bind=database.engine)
    _ensure_sync_module_status_table()
    _ensure_business_dictionary_tables()
    _ensure_data_dictionary_columns()
    _ensure_voucher_columns()
    _ensure_house_columns()
    _ensure_park_columns()
    _ensure_bill_columns()
    _ensure_receipt_bill_user_indexes()
    _ensure_charge_and_project_columns()
    _ensure_deposit_record_columns()
    _ensure_prepayment_record_columns()
    _ensure_user_email_index()
