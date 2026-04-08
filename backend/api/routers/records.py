# -*- coding: utf-8 -*-
from datetime import datetime
from importlib import import_module
import logging
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query
from sqlalchemy import String, and_, cast, func, or_
from sqlalchemy.orm import Session, joinedload

import models
import schemas
from api.bootstrap import _is_mssql
from api.dependencies import get_allowed_community_ids, get_db
from fetch_bills import sync_bills
from fetch_deposit_records import sync_deposit_records
from fetch_prepayment_records import sync_prepayment_records
from fetch_receipt_bills import sync_receipt_bills
from sync_tracker import tracker

router = APIRouter()
logger = logging.getLogger("receipt_drilldown_perf")
ENABLE_RECEIPT_DRILLDOWN_PERF_LOG = os.getenv("ENABLE_RECEIPT_DRILLDOWN_PERF_LOG", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _main_attr(name: str):
    return getattr(import_module("main"), name)


def _get_related_bill_refs_for_receipts(*args, **kwargs):
    return _main_attr("_get_related_bill_refs_for_receipts")(*args, **kwargs)


def _get_bill_push_status_map(*args, **kwargs):
    return _main_attr("_get_bill_push_status_map")(*args, **kwargs)


def _decode_header_value(*args, **kwargs):
    return _main_attr("_decode_header_value")(*args, **kwargs)


def _build_bill_push_status_entry(*args, **kwargs):
    return _main_attr("_build_bill_push_status_entry")(*args, **kwargs)


def _aggregate_receipt_bill_push_status(*args, **kwargs):
    return _main_attr("_aggregate_receipt_bill_push_status")(*args, **kwargs)


def _build_receipt_drilldown_meta(*args, **kwargs):
    return _main_attr("_build_receipt_drilldown_meta")(*args, **kwargs)


def _load_receipt_to_bills_relation(*args, **kwargs):
    return _main_attr("_load_receipt_to_bills_relation")(*args, **kwargs)


def _load_receipt_to_bills_relation_light(*args, **kwargs):
    return _main_attr("_load_receipt_to_bills_relation_light")(*args, **kwargs)


def _load_receipt_to_deposit_collect_relation(*args, **kwargs):
    return _main_attr("_load_receipt_to_deposit_collect_relation")(*args, **kwargs)


def _load_receipt_deposit_refund_links(*args, **kwargs):
    return _main_attr("_load_receipt_deposit_refund_links")(*args, **kwargs)


def _build_receipt_deposit_refund_link_summary(*args, **kwargs):
    return _main_attr("_build_receipt_deposit_refund_link_summary")(*args, **kwargs)


def _load_receipt_to_deposit_refund_relation(*args, **kwargs):
    return _main_attr("_load_receipt_to_deposit_refund_relation")(*args, **kwargs)


def _load_receipt_to_prepayment_recharge_relation(*args, **kwargs):
    return _main_attr("_load_receipt_to_prepayment_recharge_relation")(*args, **kwargs)


def _load_receipt_to_prepayment_refund_relation(*args, **kwargs):
    return _main_attr("_load_receipt_to_prepayment_refund_relation")(*args, **kwargs)


def _normalize_sync_community_ids(values: Optional[List[int]]) -> List[int]:
    normalized: List[int] = []
    seen = set()
    for value in values or []:
        try:
            community_id = int(value)
        except (TypeError, ValueError):
            continue
        if community_id in seen:
            continue
        seen.add(community_id)
        normalized.append(community_id)
    return normalized


def _empty_receipt_drilldown_meta() -> Dict[str, Any]:
    return {
        "drilldown_enabled": False,
        "drilldown_source": None,
        "drilldown_count": 0,
        "drilldown_summary": "鏆傛棤鍏宠仈鏁版嵁",
        "drilldown_sections": [],
        "supports_bill_push_ops": False,
    }


def _build_receipt_drilldown_meta_from_sections(sections: List[Dict[str, Any]]) -> Dict[str, Any]:
    normalized_sections = [
        {
            "relation_key": section["relation_key"],
            "source_type": section["source_type"],
            "label": section["label"],
            "count": int(section.get("count") or 0),
        }
        for section in sections
        if int(section.get("count") or 0) > 0
    ]
    if not normalized_sections:
        return _empty_receipt_drilldown_meta()

    total_count = sum(section["count"] for section in normalized_sections)
    unique_sources = {section["source_type"] for section in normalized_sections}
    if len(unique_sources) == 1:
        primary_source = next(iter(unique_sources))
    else:
        primary_source = "mixed"

    return {
        "drilldown_enabled": True,
        "drilldown_source": primary_source,
        "drilldown_count": total_count,
        "drilldown_summary": " / ".join(
            f"{section['label']} {section['count']} 鏉?" for section in normalized_sections
        ),
        "drilldown_sections": normalized_sections,
        "supports_bill_push_ops": any(section["source_type"] == "bills" for section in normalized_sections),
    }


def _build_receipt_drilldown_meta_map(
    db: Session,
    receipt_bills: List[models.ReceiptBill],
) -> Dict[tuple, Dict[str, Any]]:
    meta_map: Dict[tuple, Dict[str, Any]] = {}
    if not receipt_bills:
        return meta_map

    receipts_by_type: Dict[int, List[models.ReceiptBill]] = {}
    for receipt_bill in receipt_bills:
        key = (int(receipt_bill.id), int(receipt_bill.community_id))
        meta_map[key] = _empty_receipt_drilldown_meta()
        receipts_by_type.setdefault(int(receipt_bill.deal_type or 0), []).append(receipt_bill)

    def _query_group_count(model, group_fields, *filters):
        rows = (
            db.query(*group_fields, func.count().label("count"))
            .filter(*filters)
            .group_by(*group_fields)
            .all()
        )
        return {
            tuple(int(value) for value in row[:-1]): int(row[-1] or 0)
            for row in rows
        }

    def _community_receipt_count_map(model, id_field, receipts, *extra_filters):
        if not receipts:
            return {}
        community_ids = sorted({int(receipt.community_id) for receipt in receipts})
        receipt_ids = sorted({int(receipt.id) for receipt in receipts})
        return _query_group_count(
            model,
            [model.community_id, id_field],
            model.community_id.in_(community_ids),
            id_field.in_(receipt_ids),
            *extra_filters,
        )

    def _community_house_time_count_map(model, receipts, *extra_filters):
        candidates = [
            receipt
            for receipt in receipts
            if receipt.asset_id is not None and receipt.deal_time is not None
        ]
        if not candidates:
            return {}
        community_ids = sorted({int(receipt.community_id) for receipt in candidates})
        house_ids = sorted({int(receipt.asset_id) for receipt in candidates})
        pay_times = sorted({int(receipt.deal_time) for receipt in candidates})
        return _query_group_count(
            model,
            [model.community_id, model.house_id, model.pay_time],
            model.community_id.in_(community_ids),
            model.house_id.in_(house_ids),
            model.pay_time.in_(pay_times),
            *extra_filters,
        )

    bill_count_map = _community_receipt_count_map(models.Bill, models.Bill.deal_log_id, receipts_by_type.get(3, []) + receipts_by_type.get(4, []))
    deposit_collect_count_map = _community_receipt_count_map(
        models.DepositRecord,
        models.DepositRecord.payment_id,
        receipts_by_type.get(5, []),
        models.DepositRecord.operate_type == 1,
    )
    prepayment_recharge_count_map = _community_receipt_count_map(
        models.PrepaymentRecord,
        models.PrepaymentRecord.payment_id,
        receipts_by_type.get(1, []),
        models.PrepaymentRecord.operate_type == 1,
    )

    refund_receipts = receipts_by_type.get(2, []) + receipts_by_type.get(6, [])
    deposit_refund_link_count_map: Dict[tuple, int] = {}
    if receipts_by_type.get(6):
        community_ids = sorted({int(receipt.community_id) for receipt in receipts_by_type[6]})
        receipt_ids = sorted({int(receipt.id) for receipt in receipts_by_type[6]})
        deposit_refund_link_count_map = _query_group_count(
            models.ReceiptBillDepositRefundLink,
            [
                models.ReceiptBillDepositRefundLink.community_id,
                models.ReceiptBillDepositRefundLink.receipt_bill_id,
            ],
            models.ReceiptBillDepositRefundLink.community_id.in_(community_ids),
            models.ReceiptBillDepositRefundLink.receipt_bill_id.in_(receipt_ids),
            models.ReceiptBillDepositRefundLink.deposit_record_id.isnot(None),
        )

    deposit_refund_direct_count_map = _community_house_time_count_map(
        models.DepositRecord,
        receipts_by_type.get(6, []),
        models.DepositRecord.operate_type == 2,
    )
    deposit_refund_fallback_count_map = _community_receipt_count_map(
        models.DepositRecord,
        models.DepositRecord.refund_receipt_id,
        receipts_by_type.get(6, []),
        models.DepositRecord.operate_type == 2,
    )
    prepayment_transfer_count_map = _community_house_time_count_map(
        models.PrepaymentRecord,
        receipts_by_type.get(6, []),
        models.PrepaymentRecord.operate_type == 1,
        or_(
            models.PrepaymentRecord.pay_channel_str.ilike("%押金转预存%"),
            models.PrepaymentRecord.remark.ilike("%押金转入预存款%"),
            models.PrepaymentRecord.remark.ilike("%押金转预存%"),
        ),
    )
    prepayment_refund_direct_count_map = _community_house_time_count_map(
        models.PrepaymentRecord,
        receipts_by_type.get(2, []),
        models.PrepaymentRecord.operate_type == 2,
    )
    prepayment_refund_fallback_count_map = _community_receipt_count_map(
        models.PrepaymentRecord,
        models.PrepaymentRecord.refund_receipt_id,
        receipts_by_type.get(2, []),
        models.PrepaymentRecord.operate_type == 2,
    )

    for deal_type, typed_receipts in receipts_by_type.items():
        for receipt_bill in typed_receipts:
            key = (int(receipt_bill.id), int(receipt_bill.community_id))
            sections: List[Dict[str, Any]] = []

            if deal_type in {3, 4}:
                bill_count = bill_count_map.get((int(receipt_bill.community_id), int(receipt_bill.id)), 0)
                if bill_count > 0:
                    sections.append({
                        "relation_key": "receipt_to_bills",
                        "source_type": "bills",
                        "label": "杩愯惀璐﹀崟",
                        "count": bill_count,
                    })
            elif deal_type == 5:
                deposit_collect_count = deposit_collect_count_map.get((int(receipt_bill.community_id), int(receipt_bill.id)), 0)
                if deposit_collect_count > 0:
                    sections.append({
                        "relation_key": "receipt_to_deposit_collect",
                        "source_type": "deposit_records",
                        "label": "鎶奸噾鏀跺彇",
                        "count": deposit_collect_count,
                    })
            elif deal_type == 6:
                deposit_refund_count = deposit_refund_link_count_map.get((int(receipt_bill.community_id), int(receipt_bill.id)), 0)
                if deposit_refund_count <= 0 and receipt_bill.asset_id is not None and receipt_bill.deal_time is not None:
                    deposit_refund_count = deposit_refund_direct_count_map.get(
                        (int(receipt_bill.community_id), int(receipt_bill.asset_id), int(receipt_bill.deal_time)),
                        0,
                    )
                if deposit_refund_count <= 0:
                    deposit_refund_count = deposit_refund_fallback_count_map.get((int(receipt_bill.community_id), int(receipt_bill.id)), 0)
                if deposit_refund_count > 0:
                    sections.append({
                        "relation_key": "receipt_to_deposit_refund",
                        "source_type": "deposit_records",
                        "label": "鎶奸噾閫€娆?",
                        "count": deposit_refund_count,
                    })

                prepayment_transfer_count = 0
                if receipt_bill.asset_id is not None and receipt_bill.deal_time is not None:
                    prepayment_transfer_count = prepayment_transfer_count_map.get(
                        (int(receipt_bill.community_id), int(receipt_bill.asset_id), int(receipt_bill.deal_time)),
                        0,
                    )
                if prepayment_transfer_count > 0:
                    sections.append({
                        "relation_key": "receipt_to_prepayment_transfer",
                        "source_type": "prepayment_records",
                        "label": "杞叆棰勫瓨",
                        "count": prepayment_transfer_count,
                    })
            elif deal_type == 1:
                prepayment_recharge_count = prepayment_recharge_count_map.get((int(receipt_bill.community_id), int(receipt_bill.id)), 0)
                if prepayment_recharge_count > 0:
                    sections.append({
                        "relation_key": "receipt_to_prepayment_recharge",
                        "source_type": "prepayment_records",
                        "label": "棰勫瓨娆惧厖鍊?",
                        "count": prepayment_recharge_count,
                    })
            elif deal_type == 2:
                prepayment_refund_count = 0
                if receipt_bill.asset_id is not None and receipt_bill.deal_time is not None:
                    prepayment_refund_count = prepayment_refund_direct_count_map.get(
                        (int(receipt_bill.community_id), int(receipt_bill.asset_id), int(receipt_bill.deal_time)),
                        0,
                    )
                if prepayment_refund_count <= 0:
                    prepayment_refund_count = prepayment_refund_fallback_count_map.get((int(receipt_bill.community_id), int(receipt_bill.id)), 0)
                if prepayment_refund_count > 0:
                    sections.append({
                        "relation_key": "receipt_to_prepayment_refund",
                        "source_type": "prepayment_records",
                        "label": "棰勫瓨娆鹃€€娆?",
                        "count": prepayment_refund_count,
                    })

            meta_map[key] = _build_receipt_drilldown_meta_from_sections(sections)

    return meta_map


def _apply_receipt_bill_filters(
    query,
    *,
    allowed_community_ids: List[int],
    community_ids: Optional[str],
    deal_date_start: Optional[str],
    deal_date_end: Optional[str],
    deal_type: Optional[int],
    pay_channel_str: Optional[str],
    payee: Optional[str],
):
    if allowed_community_ids:
        query = query.filter(models.ReceiptBill.community_id.in_(allowed_community_ids))
    else:
        return query, False

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

    return query, True


def sync_receipt_bills_with_related_modules(community_ids: List[int], task_id: str) -> None:
    normalized_ids = _normalize_sync_community_ids(community_ids)
    if not normalized_ids:
        tracker.add_log(task_id, "No communities available for receipt sync.", "error")
        tracker.update_status(task_id, "failed")
        return

    tracker.update_status(task_id, "running")
    tracker.add_log(
        task_id,
        "为保证收款单关联完整性，本次将依次同步运营账单、押金、预存款、收款单据。",
        "info",
    )

    str_ids = [str(cid) for cid in normalized_ids]
    stages = [
        ("运营账单", lambda: sync_bills(str_ids)),
        ("押金记录", lambda: sync_deposit_records(normalized_ids)),
        ("预存款记录", lambda: sync_prepayment_records(normalized_ids)),
        ("收款单据", lambda: sync_receipt_bills(str_ids)),
    ]

    try:
        for index, (stage_name, stage_runner) in enumerate(stages, start=1):
            tracker.update_progress(task_id, min(index - 1, len(normalized_ids)), stage_name)
            tracker.add_log(task_id, f"开始同步{stage_name}", "info")
            stage_runner()
            tracker.add_log(task_id, f"{stage_name}同步完成", "info")

        tracker.update_progress(task_id, len(normalized_ids), "sync completed")
        tracker.update_status(task_id, "completed")
        tracker.add_log(task_id, "收款单据及其关联模块同步完成。", "info")
    except Exception as exc:
        tracker.add_log(task_id, f"收款单组合同步失败: {exc}", "error")
        tracker.update_status(task_id, "failed")


def _build_receipt_drilldown_sections(*args, **kwargs):
    return _main_attr("_build_receipt_drilldown_sections")(*args, **kwargs)


def _serialize_receipt_deposit_refund_link_model(*args, **kwargs):
    return _main_attr("_serialize_receipt_deposit_refund_link_model")(*args, **kwargs)

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


@router.get("/api/prepayment-records")
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
        query.order_by(models.PrepaymentRecord.operate_time.desc(), models.PrepaymentRecord.id.desc())
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


@router.post("/api/prepayment-records/sync")
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


@router.get("/api/prepayment-records/sync/status/{task_id}")
def get_prepayment_record_sync_status(task_id: str):
    status = tracker.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")
    return status


@router.get("/api/deposit-records")
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
        query.order_by(models.DepositRecord.operate_time.desc(), models.DepositRecord.id.desc())
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


@router.post("/api/deposit-records/sync")
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


@router.get("/api/deposit-records/sync/status/{task_id}")
def get_deposit_record_sync_status(task_id: str):
    status = tracker.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")
    return status


@router.get("/api/receipt-bills")
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

    receipt_query, has_scope = _apply_receipt_bill_filters(
        db.query(models.ReceiptBill),
        allowed_community_ids=allowed_community_ids,
        community_ids=community_ids,
        deal_date_start=deal_date_start,
        deal_date_end=deal_date_end,
        deal_type=deal_type,
        pay_channel_str=pay_channel_str,
        payee=payee,
    )
    if not has_scope:
        return {"total": 0, "total_income_amount": 0.00, "items": []}

    if _is_mssql():
        payer_subq = (
            db.query(
                models.ReceiptBillUser.receipt_bill_id,
                models.ReceiptBillUser.community_id,
                sa_func.max(models.ReceiptBillUser.user_name).label("payer_name"),
            )
            .group_by(models.ReceiptBillUser.receipt_bill_id, models.ReceiptBillUser.community_id)
            .subquery()
        )
    else:
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
    query, _ = _apply_receipt_bill_filters(
        query,
        allowed_community_ids=allowed_community_ids,
        community_ids=community_ids,
        deal_date_start=deal_date_start,
        deal_date_end=deal_date_end,
        deal_type=deal_type,
        pay_channel_str=pay_channel_str,
        payee=payee,
    )

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
    else:
        total = receipt_query.count()
        total_income = receipt_query.with_entities(sa_func.sum(models.ReceiptBill.income_amount)).scalar()

    results = (
        query.order_by(models.ReceiptBill.deal_time.desc(), models.ReceiptBill.id.desc())
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
    drilldown_meta_map = _build_receipt_drilldown_meta_map(
        db,
        [rb for rb, _, _ in results],
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
        drilldown_meta = drilldown_meta_map.get(receipt_key) or _empty_receipt_drilldown_meta()

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


@router.post("/api/receipt-bills/sync")
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
    background_tasks.add_task(sync_receipt_bills_with_related_modules, community_ids, task_id)

    return {
        "message": "Receipt bill synchronization started with related bills, deposits and prepayments",
        "task_id": task_id,
        "community_ids": str_ids,
    }


@router.get("/api/receipt-bills/sync/status/{task_id}")
def get_receipt_bill_sync_status(task_id: str):
    status = tracker.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")
    return status


@router.get("/api/receipt-bills/{receipt_bill_id}")
def get_receipt_bill(
    receipt_bill_id: int,
    community_id: int = Query(..., description="Marki community ID"),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    t0 = time.perf_counter()
    if allowed_community_ids and int(community_id) not in set(allowed_community_ids):
        raise HTTPException(status_code=403, detail="Unauthorized community")

    rb = (
        db.query(models.ReceiptBill)
        .options(joinedload(models.ReceiptBill.users))
        .filter(models.ReceiptBill.id == int(receipt_bill_id), models.ReceiptBill.community_id == int(community_id))
        .first()
    )
    if not rb:
        raise HTTPException(status_code=404, detail="Receipt bill not found")

    t_load_receipt = time.perf_counter()
    deal_type = int(rb.deal_type or 0)
    related_bills = _load_receipt_to_bills_relation_light(db, rb) if deal_type in {3, 4} else []
    related_deposit_collect = _load_receipt_to_deposit_collect_relation(db, rb) if deal_type == 5 else []
    deposit_refund_links = _load_receipt_deposit_refund_links(db, rb) if deal_type == 6 else []
    deposit_refund_link_summary = _build_receipt_deposit_refund_link_summary(deposit_refund_links)
    related_deposit_refund = _load_receipt_to_deposit_refund_relation(db, rb) if deal_type == 6 else []
    related_prepayment_recharge = _load_receipt_to_prepayment_recharge_relation(db, rb) if deal_type in {1, 6} else []
    related_prepayment_refund = _load_receipt_to_prepayment_refund_relation(db, rb) if deal_type == 2 else []
    t_load_relations = time.perf_counter()
    drilldown_sections = _build_receipt_drilldown_sections(
        rb,
        related_bills,
        related_deposit_collect,
        related_deposit_refund,
        related_prepayment_recharge,
        related_prepayment_refund,
    )
    drilldown_meta = _build_receipt_drilldown_meta(db, rb)
    t_build_response = time.perf_counter()

    if ENABLE_RECEIPT_DRILLDOWN_PERF_LOG:
        logger.info(
            "receipt_drilldown_perf receipt_bill_id=%s community_id=%s deal_type=%s users=%s bills=%s sections=%s t_receipt_ms=%.1f t_relations_ms=%.1f t_response_ms=%.1f t_total_ms=%.1f",
            receipt_bill_id,
            community_id,
            deal_type,
            len(rb.users or []),
            len(related_bills),
            len(drilldown_sections),
            (t_load_receipt - t0) * 1000,
            (t_load_relations - t_load_receipt) * 1000,
            (t_build_response - t_load_relations) * 1000,
            (t_build_response - t0) * 1000,
        )

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
