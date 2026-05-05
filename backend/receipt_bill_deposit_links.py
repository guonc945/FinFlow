# -*- coding: utf-8 -*-
from typing import Dict, Iterable, List, Optional

import database
import models
from utils.db_compat import upsert_model_row


TRANSFER_TO_PREPAYMENT = "transfer_to_prepayment"
ACTUAL_REFUND = "actual_refund"


def _normalize_community_ids(community_ids: Optional[Iterable[int]]) -> List[int]:
    normalized: List[int] = []
    seen = set()

    for raw in community_ids or []:
        try:
            community_id = int(raw)
        except (TypeError, ValueError):
            continue
        if community_id in seen:
            continue
        seen.add(community_id)
        normalized.append(community_id)

    return normalized


def _contains_any(text: Optional[str], keywords: List[str]) -> bool:
    if not text:
        return False
    return any(keyword in text for keyword in keywords)


def rebuild_receipt_bill_deposit_refund_links(
    community_ids: Optional[Iterable[int]] = None,
) -> Dict[str, int]:
    models.ReceiptBillDepositRefundLink.__table__.create(bind=database.engine, checkfirst=True)

    normalized_ids = _normalize_community_ids(community_ids)
    db = database.SessionLocal()

    transfer_keywords = ["押金转预存", "押金转入预存款", "押金转"]

    try:
        link_query = db.query(models.ReceiptBillDepositRefundLink)
        if normalized_ids:
            link_query = link_query.filter(models.ReceiptBillDepositRefundLink.community_id.in_(normalized_ids))
        link_query.delete(synchronize_session=False)

        receipt_query = db.query(
            models.ReceiptBill.id,
            models.ReceiptBill.community_id,
            models.ReceiptBill.asset_id,
            models.ReceiptBill.deal_time,
        ).filter(
            models.ReceiptBill.deal_type == 6,
            models.ReceiptBill.asset_id.isnot(None),
            models.ReceiptBill.deal_time.isnot(None),
        )
        if normalized_ids:
            receipt_query = receipt_query.filter(models.ReceiptBill.community_id.in_(normalized_ids))
        receipt_rows = receipt_query.all()

        if not receipt_rows:
            db.commit()
            return {
                "total_links": 0,
                "transfer_to_prepayment_links": 0,
                "actual_refund_links": 0,
            }

        community_scope = sorted({int(row.community_id) for row in receipt_rows})
        deal_times = sorted({int(row.deal_time) for row in receipt_rows})
        asset_ids = sorted({int(row.asset_id) for row in receipt_rows})

        deposit_rows = (
            db.query(
                models.DepositRecord.id,
                models.DepositRecord.community_id,
                models.DepositRecord.house_id,
                models.DepositRecord.pay_time,
                models.DepositRecord.amount,
            )
            .filter(
                models.DepositRecord.operate_type == 2,
                models.DepositRecord.community_id.in_(community_scope),
                models.DepositRecord.pay_time.in_(deal_times),
                models.DepositRecord.house_id.in_(asset_ids),
            )
            .order_by(models.DepositRecord.id.desc())
            .all()
        )

        deposit_map = {}
        for row in deposit_rows:
            key = (int(row.community_id), int(row.house_id), int(row.pay_time))
            if key not in deposit_map:
                deposit_map[key] = {
                    "deposit_record_id": int(row.id),
                    "deposit_amount": float(row.amount) if row.amount is not None else 0.0,
                }

        prepayment_rows = (
            db.query(
                models.PrepaymentRecord.id,
                models.PrepaymentRecord.community_id,
                models.PrepaymentRecord.house_id,
                models.PrepaymentRecord.pay_time,
                models.PrepaymentRecord.amount,
                models.PrepaymentRecord.pay_channel_str,
                models.PrepaymentRecord.remark,
            )
            .filter(
                models.PrepaymentRecord.operate_type == 1,
                models.PrepaymentRecord.community_id.in_(community_scope),
                models.PrepaymentRecord.pay_time.in_(deal_times),
                models.PrepaymentRecord.house_id.in_(asset_ids),
            )
            .all()
        )

        prepayment_map = {}
        for row in prepayment_rows:
            pay_channel_str = row.pay_channel_str or ""
            remark = row.remark or ""
            if not (_contains_any(pay_channel_str, transfer_keywords) or _contains_any(remark, transfer_keywords)):
                continue
            key = (int(row.community_id), int(row.house_id), int(row.pay_time))
            prepayment_map.setdefault(key, []).append(row)

        total_links = 0
        transfer_links = 0
        actual_links = 0

        for receipt in receipt_rows:
            key = (int(receipt.community_id), int(receipt.asset_id), int(receipt.deal_time))
            matched_deposit = deposit_map.get(key)
            if not matched_deposit:
                continue

            deposit_amount = matched_deposit["deposit_amount"]
            prepayment_candidates = prepayment_map.get(key, [])
            selected_prepayment_id = None

            if prepayment_candidates:
                ranked = sorted(
                    prepayment_candidates,
                    key=lambda row: (
                        0 if _contains_any(row.pay_channel_str or "", ["押金转预存"]) else 1,
                        0 if _contains_any(row.remark or "", transfer_keywords) else 1,
                        abs((float(row.amount) if row.amount is not None else 0.0) - deposit_amount),
                        -int(row.id),
                    ),
                )
                selected_prepayment_id = int(ranked[0].id)

            if selected_prepayment_id is None:
                link_type = ACTUAL_REFUND
                match_rule = "community+asset_id+deal_time=deposit.pay_time"
                match_confidence = 0.92
                actual_links += 1
            else:
                link_type = TRANSFER_TO_PREPAYMENT
                match_rule = "community+asset_id+deal_time=deposit.pay_time=prepayment.pay_time"
                match_confidence = 0.98
                transfer_links += 1

            upsert_model_row(
                db,
                models.ReceiptBillDepositRefundLink,
                {
                    "receipt_bill_id": int(receipt.id),
                    "community_id": int(receipt.community_id),
                },
                {
                    "deposit_record_id": int(matched_deposit["deposit_record_id"]),
                    "prepayment_record_id": selected_prepayment_id,
                    "link_type": link_type,
                    "match_rule": match_rule,
                    "match_confidence": match_confidence,
                },
            )
            total_links += 1

        db.commit()

        return {
            "total_links": total_links,
            "transfer_to_prepayment_links": transfer_links,
            "actual_refund_links": actual_links,
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
