from decimal import Decimal
from pathlib import Path
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import models
from api.routers import records
from database import Base


TEST_TABLES = [
    models.ProjectList.__table__,
    models.ReceiptBill.__table__,
    models.ReceiptBillUser.__table__,
    models.Bill.__table__,
    models.DepositRecord.__table__,
    models.PrepaymentRecord.__table__,
    models.ReceiptBillDepositRefundLink.__table__,
]


def make_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine, tables=TEST_TABLES)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def seed_receipt_bill_list_data(db):
    db.add(models.ProjectList(proj_id=101, proj_name="Test Project"))

    receipts = [
        models.ReceiptBill(
            id=1001,
            community_id=101,
            deal_type=3,
            deal_time=1710000001,
            income_amount=Decimal("10.00"),
        ),
        models.ReceiptBill(
            id=1002,
            community_id=101,
            deal_type=5,
            deal_time=1710000002,
            income_amount=Decimal("20.00"),
        ),
        models.ReceiptBill(
            id=1003,
            community_id=101,
            deal_type=1,
            deal_time=1710000003,
            income_amount=Decimal("30.00"),
        ),
        models.ReceiptBill(
            id=1004,
            community_id=101,
            deal_type=2,
            asset_id=501,
            deal_time=1710000004,
            income_amount=Decimal("40.00"),
        ),
        models.ReceiptBill(
            id=1005,
            community_id=101,
            deal_type=6,
            asset_id=502,
            deal_time=1710000005,
            income_amount=Decimal("50.00"),
        ),
    ]
    db.add_all(receipts)

    db.add(models.Bill(id=2001, community_id=101, deal_log_id=1001, amount=Decimal("10.00")))
    db.add(models.DepositRecord(id=3001, community_id=101, payment_id=1002, operate_type=1, amount=Decimal("20.00")))
    db.add(models.PrepaymentRecord(id=4001, community_id=101, payment_id=1003, operate_type=1, amount=Decimal("30.00")))
    db.add(models.PrepaymentRecord(id=4002, community_id=101, house_id=501, pay_time=1710000004, operate_type=2, amount=Decimal("40.00")))
    db.add(models.DepositRecord(id=3002, community_id=101, house_id=502, pay_time=1710000005, operate_type=2, amount=Decimal("50.00")))
    db.add(
        models.ReceiptBillDepositRefundLink(
            id=1,
            receipt_bill_id=1005,
            community_id=101,
            deposit_record_id=3002,
            prepayment_record_id=4003,
            link_type="transfer_to_prepayment",
        )
    )
    db.add(
        models.PrepaymentRecord(
            id=4003,
            community_id=101,
            house_id=502,
            pay_time=1710000005,
            operate_type=1,
            amount=Decimal("50.00"),
            remark="押金转入预存款",
        )
    )
    db.commit()


def test_get_receipt_bills_returns_expected_drilldown_metadata():
    db = make_session()
    seed_receipt_bill_list_data(db)

    original_related = records._get_related_bill_refs_for_receipts
    original_status_map = records._get_bill_push_status_map
    original_build_status = records._build_bill_push_status_entry
    original_aggregate = records._aggregate_receipt_bill_push_status
    original_decode = records._decode_header_value

    records._get_related_bill_refs_for_receipts = lambda db, refs: {
        (1001, 101): [{"bill_id": 2001, "community_id": 101}],
        (1002, 101): [],
        (1003, 101): [],
        (1004, 101): [],
        (1005, 101): [],
    }
    records._get_bill_push_status_map = lambda db, refs, account_book_number=None: {}
    records._build_bill_push_status_entry = lambda bill_id, community_id, **kwargs: {
        "bill_id": int(bill_id),
        "community_id": int(community_id),
        "push_status": "not_pushed",
        "push_status_label": "未推送",
    }
    records._aggregate_receipt_bill_push_status = lambda statuses: {
        "related_bill_count": len(statuses),
        "related_bill_push_summary": {
            "total": len(statuses),
            "not_pushed": len(statuses),
            "pushing": 0,
            "success": 0,
            "failed": 0,
        },
        "push_status": "not_pushed" if statuses else "unbound",
        "push_status_label": "未推送" if statuses else "未关联账单",
    }
    records._decode_header_value = lambda value: value

    try:
        result = records.get_receipt_bills(
            search=None,
            community_ids=None,
            deal_date_start=None,
            deal_date_end=None,
            deal_type=None,
            pay_channel_str=None,
            payee=None,
            skip=0,
            limit=25,
            x_account_book_number=None,
            db=db,
            allowed_community_ids=[101],
        )
    finally:
        records._get_related_bill_refs_for_receipts = original_related
        records._get_bill_push_status_map = original_status_map
        records._build_bill_push_status_entry = original_build_status
        records._aggregate_receipt_bill_push_status = original_aggregate
        records._decode_header_value = original_decode

    assert result["total"] == 5
    assert result["total_income_amount"] == 150.0

    items_by_id = {int(item["id"]): item for item in result["items"]}

    assert items_by_id[1001]["drilldown_enabled"] is True
    assert items_by_id[1001]["supports_bill_push_ops"] is True
    assert items_by_id[1001]["drilldown_sections"][0]["count"] == 1

    assert items_by_id[1002]["drilldown_sections"][0]["relation_key"] == "receipt_to_deposit_collect"
    assert items_by_id[1002]["drilldown_sections"][0]["count"] == 1

    assert items_by_id[1003]["drilldown_sections"][0]["relation_key"] == "receipt_to_prepayment_recharge"
    assert items_by_id[1003]["drilldown_sections"][0]["count"] == 1

    assert items_by_id[1004]["drilldown_sections"][0]["relation_key"] == "receipt_to_prepayment_refund"
    assert items_by_id[1004]["drilldown_sections"][0]["count"] == 1

    assert len(items_by_id[1005]["drilldown_sections"]) == 2
    assert {section["relation_key"] for section in items_by_id[1005]["drilldown_sections"]} == {
        "receipt_to_deposit_refund",
        "receipt_to_prepayment_transfer",
    }
