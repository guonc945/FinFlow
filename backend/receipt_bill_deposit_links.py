from typing import Dict, Iterable, List, Optional

from sqlalchemy import text

import database
import models


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


def _build_in_clause(column_name: str, community_ids: List[int]) -> str:
    if not community_ids:
        return ""
    ids_sql = ", ".join(str(community_id) for community_id in community_ids)
    return f" AND {column_name} IN ({ids_sql})"


def rebuild_receipt_bill_deposit_refund_links(
    community_ids: Optional[Iterable[int]] = None,
) -> Dict[str, int]:
    models.ReceiptBillDepositRefundLink.__table__.create(bind=database.engine, checkfirst=True)

    normalized_ids = _normalize_community_ids(community_ids)
    receipt_filter = _build_in_clause("rb.community_id", normalized_ids)
    link_filter = _build_in_clause("community_id", normalized_ids)

    delete_sql = "DELETE FROM receipt_bill_deposit_refund_links WHERE 1 = 1"
    if link_filter:
        delete_sql += link_filter

    insert_sql = f"""
        WITH ranked_deposit AS (
            SELECT
                rb.id AS receipt_bill_id,
                rb.community_id,
                rb.asset_id,
                rb.deal_time,
                d.id AS deposit_record_id,
                d.amount AS deposit_amount,
                ROW_NUMBER() OVER (
                    PARTITION BY rb.id, rb.community_id
                    ORDER BY d.id DESC
                ) AS rn
            FROM receipt_bills rb
            JOIN deposit_records d
              ON d.community_id = rb.community_id
             AND d.house_id = rb.asset_id
             AND d.pay_time = rb.deal_time
             AND d.operate_type = 2
            WHERE rb.deal_type = 6
              AND rb.asset_id IS NOT NULL
              AND rb.deal_time IS NOT NULL
              {receipt_filter}
        ),
        matched_deposit AS (
            SELECT
                receipt_bill_id,
                community_id,
                asset_id,
                deal_time,
                deposit_record_id,
                deposit_amount
            FROM ranked_deposit
            WHERE rn = 1
        ),
        ranked_prepayment AS (
            SELECT
                md.receipt_bill_id,
                md.community_id,
                md.deposit_record_id,
                p.id AS prepayment_record_id,
                ROW_NUMBER() OVER (
                    PARTITION BY md.receipt_bill_id, md.community_id, md.deposit_record_id
                    ORDER BY
                        CASE WHEN COALESCE(p.pay_channel_str, '') LIKE '%%押金转预存%%' THEN 0 ELSE 1 END,
                        CASE WHEN COALESCE(p.remark, '') LIKE '%%押金转%%' THEN 0 ELSE 1 END,
                        ABS(COALESCE(p.amount, 0) - COALESCE(md.deposit_amount, 0)),
                        p.id DESC
                ) AS rn
            FROM matched_deposit md
            JOIN prepayment_records p
              ON p.community_id = md.community_id
             AND p.house_id = md.asset_id
             AND p.pay_time = md.deal_time
             AND p.operate_type = 1
            WHERE
                COALESCE(p.pay_channel_str, '') LIKE '%%押金转预存%%'
                OR COALESCE(p.remark, '') LIKE '%%押金转入预存款%%'
                OR COALESCE(p.remark, '') LIKE '%%押金转预存%%'
        ),
        selected_links AS (
            SELECT
                md.receipt_bill_id,
                md.community_id,
                md.deposit_record_id,
                rp.prepayment_record_id,
                CASE
                    WHEN rp.prepayment_record_id IS NULL THEN '{ACTUAL_REFUND}'
                    ELSE '{TRANSFER_TO_PREPAYMENT}'
                END AS link_type,
                CASE
                    WHEN rp.prepayment_record_id IS NULL THEN 'community+asset_id+deal_time=deposit.pay_time'
                    ELSE 'community+asset_id+deal_time=deposit.pay_time=prepayment.pay_time'
                END AS match_rule,
                CASE
                    WHEN rp.prepayment_record_id IS NULL THEN 0.92
                    ELSE 0.98
                END AS match_confidence
            FROM matched_deposit md
            LEFT JOIN ranked_prepayment rp
              ON rp.receipt_bill_id = md.receipt_bill_id
             AND rp.community_id = md.community_id
             AND rp.deposit_record_id = md.deposit_record_id
             AND rp.rn = 1
        )
        INSERT INTO receipt_bill_deposit_refund_links (
            receipt_bill_id,
            community_id,
            deposit_record_id,
            prepayment_record_id,
            link_type,
            match_rule,
            match_confidence
        )
        SELECT
            receipt_bill_id,
            community_id,
            deposit_record_id,
            prepayment_record_id,
            link_type,
            match_rule,
            match_confidence
        FROM selected_links
        ON CONFLICT (receipt_bill_id, community_id) DO UPDATE SET
            deposit_record_id = EXCLUDED.deposit_record_id,
            prepayment_record_id = EXCLUDED.prepayment_record_id,
            link_type = EXCLUDED.link_type,
            match_rule = EXCLUDED.match_rule,
            match_confidence = EXCLUDED.match_confidence,
            updated_at = NOW()
    """

    count_sql = "SELECT COUNT(*) FROM receipt_bill_deposit_refund_links WHERE 1 = 1"
    if link_filter:
        count_sql += link_filter

    transfer_sql = count_sql + f" AND link_type = '{TRANSFER_TO_PREPAYMENT}'"
    actual_sql = count_sql + f" AND link_type = '{ACTUAL_REFUND}'"

    with database.engine.begin() as conn:
        conn.execute(text(delete_sql))
        conn.execute(text(insert_sql))

        total_links = int(conn.execute(text(count_sql)).scalar() or 0)
        transfer_links = int(conn.execute(text(transfer_sql)).scalar() or 0)
        actual_links = int(conn.execute(text(actual_sql)).scalar() or 0)

    return {
        "total_links": total_links,
        "transfer_to_prepayment_links": transfer_links,
        "actual_refund_links": actual_links,
    }
