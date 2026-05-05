# -*- coding: utf-8 -*-
import os
from typing import Optional


API_ID_DEFAULTS = {
    "KINGDEE_VOUCHER_PUSH_API_ID": 1,
    "KINGDEE_ACCOUNTING_SUBJECT_API_ID": 2,
    "KINGDEE_CUSTOMER_API_ID": 3,
    "KINGDEE_SUPPLIER_API_ID": 4,
    "KINGDEE_HOUSE_API_ID": 5,
    "KINGDEE_AUXILIARY_DATA_API_ID": 6,
    "KINGDEE_AUXILIARY_DATA_CATEGORY_API_ID": 7,
    "KINGDEE_ACCOUNT_BOOK_API_ID": 8,
    "KINGDEE_BANK_ACCOUNT_API_ID": 9,
    "MARKI_PROJECT_API_ID": 22,
    "MARKI_RESIDENT_API_ID": 23,
    "MARKI_HOUSE_API_ID": 24,
    "MARKI_PARK_API_ID": 25,
    "MARKI_CHARGE_ITEM_API_ID": 26,
    "MARKI_BILL_API_ID": 27,
    "KINGDEE_VOUCHER_QUERY_API_ID": 28,
    "MARKI_RECEIPT_BILL_API_ID": 29,
    "MARKI_DEPOSIT_RECORD_API_ID": 30,
    "MARKI_PREPAYMENT_RECORD_API_ID": 31,
    "KINGDEE_TAX_RATE_API_ID": 32,
}


def get_api_id(env_key: str, fallback: Optional[int] = None) -> Optional[int]:
    raw_value = os.getenv(env_key, "").strip()
    if not raw_value:
        default_value = API_ID_DEFAULTS.get(env_key, fallback)
        return int(default_value) if default_value is not None else None
    return int(raw_value)


def require_api_id(env_key: str, fallback: Optional[int] = None) -> int:
    api_id = get_api_id(env_key, fallback=fallback)
    if api_id is None:
        raise ValueError(f"Missing API id configuration: {env_key}")
    return int(api_id)
