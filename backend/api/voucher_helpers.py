# -*- coding: utf-8 -*-
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from importlib import import_module
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

import models
from voucher_field_mapping import enrich_source_data as mapping_enrich_source_data


def _main_constant(name: str, default: Any) -> Any:
    try:
        return getattr(import_module("main"), name)
    except Exception:
        return default


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
    labels = _main_constant(
        "BILL_VOUCHER_PUSH_STATUS_LABELS",
        {
            "not_pushed": "Not Pushed",
            "pushing": "Pushing",
            "success": "Success",
            "failed": "Failed",
        },
    )
    normalized_status = push_status if push_status in labels else "not_pushed"
    return {
        "bill_id": int(bill_id),
        "community_id": int(community_id),
        "push_status": normalized_status,
        "push_status_label": labels.get(normalized_status, labels.get("not_pushed", "Not Pushed")),
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

    BATCH_SIZE = 100
    all_rows = []

    for i in range(0, len(normalized_refs), BATCH_SIZE):
        batch_refs = normalized_refs[i:i + BATCH_SIZE]

        conditions = [
            and_(
                models.BillVoucherPushRecord.bill_id == ref["bill_id"],
                models.BillVoucherPushRecord.community_id == ref["community_id"],
            )
            for ref in batch_refs
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

        batch_rows = latest_query.order_by(
            models.BillVoucherPushRecord.bill_id.asc(),
            models.BillVoucherPushRecord.community_id.asc(),
            models.BillVoucherPushRecord.created_at.desc(),
            models.BillVoucherPushRecord.id.desc(),
        ).all()

        all_rows.extend(batch_rows)

    seen_keys = set()
    latest_rows = []
    for row in all_rows:
        key = (int(row.bill_id), int(row.community_id))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        latest_rows.append(row)

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


_MONEY_QUANTIZER = Decimal("0.01")


def _decimal_text(value: Any) -> str:
    if value is None:
        return "0"
    parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    text = format(parsed, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _money_text(value: Any) -> str:
    parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(parsed.quantize(_MONEY_QUANTIZER, rounding=ROUND_HALF_UP), ".2f")


def _is_money_field(field_name: Optional[str]) -> bool:
    normalized = str(field_name or "").strip().lower()
    return normalized == "amount" or normalized.endswith("_amount") or normalized == "income_amount" or normalized == "balance_after_change"


def _jsonify_scalar(value: Any, field_name: Optional[str] = None) -> Any:
    if isinstance(value, Decimal):
        return _money_text(value) if _is_money_field(field_name) else _decimal_text(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _serialize_receipt_bill_model(
    receipt_bill: models.ReceiptBill,
    db: Session,
) -> Dict[str, Any]:
    deal_type_labels = _main_constant("RECEIPT_BILL_DEAL_TYPE_LABELS", {})
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
        "deal_type_label": deal_type_labels.get(receipt_bill.deal_type, "Other"),
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
    return {key: _jsonify_scalar(value, key) for key, value in data.items()}


def _enrich_receipt_bill_data(
    receipt_data: Dict[str, Any],
    receipt_bill: Optional[models.ReceiptBill] = None,
    db: Optional[Session] = None,
) -> Dict[str, Any]:
    return mapping_enrich_source_data("receipt_bills", receipt_data, db=db, record=receipt_bill)


def _serialize_deposit_record_model(record: models.DepositRecord) -> Dict[str, Any]:
    operate_type_labels = _main_constant("DEPOSIT_OPERATE_TYPE_LABELS", {})
    data = {
        "id": record.id,
        "community_id": record.community_id,
        "community_name": record.community_name,
        "house_id": record.house_id,
        "house_name": record.house_name,
        "amount": record.amount,
        "operate_type": record.operate_type,
        "operate_type_label": operate_type_labels.get(record.operate_type, "Other"),
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
    return {key: _jsonify_scalar(value, key) for key, value in data.items()}


def _enrich_deposit_record_data(record_data: Dict[str, Any], db: Optional[Session] = None) -> Dict[str, Any]:
    return mapping_enrich_source_data("deposit_records", record_data, db=db)


def _serialize_prepayment_record_model(record: models.PrepaymentRecord) -> Dict[str, Any]:
    operate_type_labels = _main_constant("PREPAYMENT_OPERATE_TYPE_LABELS", {})
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
        "operate_type_label": record.operate_type_label or operate_type_labels.get(record.operate_type, "Other"),
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
    return {key: _jsonify_scalar(value, key) for key, value in data.items()}


def _enrich_prepayment_record_data(record_data: Dict[str, Any], db: Optional[Session] = None) -> Dict[str, Any]:
    return mapping_enrich_source_data("prepayment_records", record_data, db=db)


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
            {col.name: _jsonify_scalar(getattr(bill, col.name, None), col.name) for col in models.Bill.__table__.columns},
            db,
        )
        for bill in bills
    ]


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
