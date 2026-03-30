from importlib import import_module
from typing import List, Optional

from fastapi import APIRouter, Depends, Header, Query
from sqlalchemy.orm import Session

import models
import schemas
from api.dependencies import get_allowed_community_ids, get_current_user, get_db

router = APIRouter()


def _main_attr(name: str):
    return getattr(import_module("main"), name)


@router.post("/api/vouchers/preview-receipt/{receipt_bill_id}")
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
    return _main_attr("preview_voucher_for_receipt")(
        receipt_bill_id,
        community_id,
        x_account_book_id,
        x_account_book_name,
        x_account_book_number,
        allow_bill_fallback,
        current_user,
        db,
        allowed_community_ids,
    )


@router.post("/api/vouchers/preview-receipts")
def preview_voucher_for_receipts(
    payload: schemas.BatchReceiptVoucherPreviewRequest,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    return _main_attr("preview_voucher_for_receipts")(
        payload,
        x_account_book_id,
        x_account_book_name,
        x_account_book_number,
        current_user,
        db,
        allowed_community_ids,
    )


@router.post("/api/vouchers/preview-bill/{bill_id}")
def preview_voucher_for_bill(
    bill_id: int,
    community_id: Optional[int] = None,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    allow_receipt_fallback: bool = True,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    return _main_attr("preview_voucher_for_bill")(
        bill_id,
        community_id,
        x_account_book_id,
        x_account_book_name,
        x_account_book_number,
        allow_receipt_fallback,
        current_user,
        db,
        allowed_community_ids,
    )


@router.post("/api/vouchers/preview-bills")
def preview_voucher_for_bills(
    payload: schemas.BatchVoucherPreviewRequest,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    return _main_attr("preview_voucher_for_bills")(
        payload,
        x_account_book_id,
        x_account_book_name,
        x_account_book_number,
        current_user,
        db,
        allowed_community_ids,
    )


@router.post("/api/vouchers/push")
def push_voucher_to_kingdee(
    payload: schemas.VoucherPushRequest,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    return _main_attr("push_voucher_to_kingdee")(
        payload,
        x_account_book_id,
        x_account_book_name,
        x_account_book_number,
        current_user,
        db,
        allowed_community_ids,
    )
