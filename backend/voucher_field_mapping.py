# -*- coding: utf-8 -*-
import json
import re
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import func
from sqlalchemy.orm import Session

import models


_MOJIBAKE_HINT_RE = re.compile(
    r"[ÃÂÐÑØÞ�]|(?:鍚|鐨|鎴|涓|浠|鍙|鏁|瀹|缁|璁|鏍|鏉|闂|锛|锟|銆)"
)


BILL_RUNTIME_EXTRA_FIELDS: Set[str] = {"customer_name", "customer_id"}
BILL_FIELD_LABELS: Dict[str, str] = {
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
    "kd_tax_rate_number": "金蝶税率编码",
    "kd_tax_rate_name": "金蝶税率名称",
    "customer_name": "账单关联客户名称",
    "customer_id": "账单关联客户ID",
    "receive_date": "支付日期",
}

RECEIPT_BILL_KD_DERIVED_FIELDS: Set[str] = {
    "kd_house_number",
    "kd_house_name",
    "kd_park_house_number",
    "kd_park_house_name",
    "kd_customer_number",
    "kd_customer_name",
    "kd_project_number",
    "kd_project_name",
    "kd_receive_bank_number",
    "kd_receive_bank_name",
    "kd_pay_bank_number",
    "kd_pay_bank_name",
    "kd_tax_rate_number",
    "kd_tax_rate_name",
}

RECEIPT_BILL_RUNTIME_EXTRA_FIELDS: Set[str] = {"community_name", "payer_name", "deal_type_label"}
RECEIPT_BILL_FIELD_LABELS: Dict[str, str] = {
    "id": "缴费ID",
    "community_id": "园区ID",
    "community_name": "园区",
    "payer_name": "付款人",
    "deal_time": "收款时间",
    "deal_date": "收款日期",
    "income_amount": "入账金额",
    "amount": "实收金额",
    "bill_amount": "账单金额",
    "discount_amount": "折扣金额",
    "late_money_amount": "滞纳金",
    "deposit_amount": "押金",
    "pay_channel_str": "收款渠道",
    "pay_channel": "收款渠道编码",
    "pay_channel_list": "收款渠道列表(JSON)",
    "payee": "收款人",
    "receipt_id": "收据号",
    "receipt_record_id": "收据记录ID",
    "receipt_version": "收据版本",
    "invoice_number": "发票号",
    "invoice_urls": "发票链接(JSON)",
    "invoice_status": "发票状态",
    "open_invoice": "是否开票",
    "asset_name": "资产/房号",
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
    "kd_tax_rate_number": "金蝶税率编码",
    "kd_tax_rate_name": "金蝶税率名称",
}

DEPOSIT_RECORD_RUNTIME_EXTRA_FIELDS: Set[str] = {"operate_type_label", "resident_name"}
DEPOSIT_RECORD_FIELD_LABELS: Dict[str, str] = {
    "id": "记录ID",
    "community_id": "园区ID",
    "community_name": "园区",
    "house_id": "房屋ID",
    "house_name": "房号",
    "resident_name": "住户",
    "amount": "金额",
    "operate_type": "变动类型编码",
    "operate_type_label": "变动类型",
    "operator": "操作人ID",
    "operator_name": "操作人",
    "operate_time": "操作时间",
    "operate_date": "业务日期",
    "cash_pledge_name": "押金类型",
    "remark": "备注",
    "pay_time": "支付时间",
    "pay_date": "支付日期",
    "payment_id": "缴费ID",
    "has_refund_receipt": "是否关联退款收据",
    "refund_receipt_id": "退款收据ID",
    "pay_channel_str": "支付渠道",
    "raw_data": "原始数据(JSON)",
    "created_at": "创建时间(系统)",
    "updated_at": "更新时间(系统)",
}


PREPAYMENT_RECORD_RUNTIME_EXTRA_FIELDS: Set[str] = {"operate_type_label", "resident_name"}
PREPAYMENT_RECORD_FIELD_LABELS: Dict[str, str] = {
    "id": "记录ID",
    "community_id": "园区ID",
    "community_name": "园区",
    "account_id": "账户ID",
    "building_id": "楼栋ID",
    "unit_id": "单元ID",
    "house_id": "房屋ID",
    "house_name": "房号",
    "resident_name": "住户",
    "amount": "变动金额",
    "balance_after_change": "变动后余额",
    "operate_type": "变动类型编码",
    "operate_type_label": "变动类型",
    "pay_channel_id": "支付渠道编码",
    "pay_channel_str": "支付渠道",
    "operator": "操作人ID",
    "operator_name": "操作人",
    "operate_time": "操作时间",
    "operate_date": "业务日期",
    "source_updated_time": "源数据更新时间",
    "remark": "备注",
    "deposit_order_id": "押金单ID",
    "pay_time": "支付时间",
    "pay_date": "支付日期",
    "category_id": "分类ID",
    "category_name": "预存款类别",
    "status": "状态",
    "payment_id": "缴费ID",
    "has_refund_receipt": "是否关联退款收据",
    "refund_receipt_id": "退款收款单ID",
    "raw_data": "原始数据(JSON)",
    "created_at": "创建时间(系统)",
    "updated_at": "更新时间(系统)",
}


def prefix_source_fields(data: Dict[str, Any], source_type: str, module_prefix: str = "marki") -> Dict[str, Any]:
    enriched = dict(data)
    for key, val in list(data.items()):
        if not isinstance(key, str) or "." in key:
            continue
        enriched[f"{source_type}.{key}"] = val
        enriched[f"{module_prefix}.{source_type}.{key}"] = val
    return enriched


def _contains_mojibake_hint(value: str) -> bool:
    return bool(value and _MOJIBAKE_HINT_RE.search(value))


def _text_quality_score(value: str) -> int:
    score = 0
    for ch in value:
        code = ord(ch)
        if ch == "\ufffd":
            score -= 6
        elif ch in "\r\n\t":
            score += 1
        elif ch.isascii():
            score += 1 if ch.isprintable() else -2
        elif 0x4E00 <= code <= 0x9FFF:
            score += 3
        elif 0x3400 <= code <= 0x4DBF or 0xF900 <= code <= 0xFAFF:
            score += 2
        elif 0x3000 <= code <= 0x303F:
            score += 2
        else:
            score += 1
    if _contains_mojibake_hint(value):
        score -= 8
    return score


def _repair_mojibake_text(value: str) -> str:
    if not isinstance(value, str):
        return value
    if not value.strip() or not _contains_mojibake_hint(value):
        return value

    candidates = [value]
    for source_encoding, target_encoding in (
        ("latin1", "utf-8"),
        ("cp1252", "utf-8"),
        ("gbk", "utf-8"),
        ("gb18030", "utf-8"),
    ):
        try:
            repaired = value.encode(source_encoding).decode(target_encoding)
        except Exception:
            continue
        if repaired and repaired not in candidates:
            candidates.append(repaired)

    best = max(candidates, key=_text_quality_score)
    if best != value and _text_quality_score(best) >= _text_quality_score(value) + 4:
        return best
    return value


def _sanitize_text_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_text_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_text_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_text_payload(item) for item in value)
    if isinstance(value, str):
        repaired = _repair_mojibake_text(value)
        stripped = repaired.strip()
        if stripped.startswith("{") and stripped.endswith("}") or stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(repaired)
            except Exception:
                return repaired
            sanitized = _sanitize_text_payload(parsed)
            return json.dumps(sanitized, ensure_ascii=False)
        return repaired
    return value


def _normalize_lookup_id(value: Any) -> Optional[str]:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        return str(int(text))
    except (TypeError, ValueError):
        pass

    try:
        numeric_value = Decimal(text)
        if numeric_value == numeric_value.to_integral_value():
            return str(int(numeric_value))
    except Exception:
        pass

    return text


def _build_receipt_bill_user_list(
    receipt_data: Dict[str, Any],
    receipt_bill: Optional[models.ReceiptBill] = None,
) -> List[Dict[str, Any]]:
    raw_users = receipt_data.get("bind_users_raw")
    parsed_users: List[Dict[str, Any]] = []

    if isinstance(raw_users, list):
        parsed_users = [item for item in raw_users if isinstance(item, dict)]
    elif isinstance(raw_users, str) and raw_users.strip():
        try:
            loaded_users = json.loads(raw_users)
            if isinstance(loaded_users, list):
                parsed_users = [item for item in loaded_users if isinstance(item, dict)]
        except Exception:
            parsed_users = []

    normalized_users: List[Dict[str, Any]] = []
    for user in parsed_users:
        user_id = user.get("id") or user.get("userId") or user.get("user_id")
        user_name = user.get("name") or user.get("userName") or user.get("user_name")
        normalized_users.append({
            "id": user_id,
            "user_id": user_id,
            "name": user_name,
            "user_name": user_name,
            "phone": user.get("phone") or user.get("mobile"),
        })

    if normalized_users:
        return normalized_users

    if receipt_bill is None:
        return []

    return [
        {
            "id": getattr(user, "user_id", None),
            "user_id": getattr(user, "user_id", None),
            "name": getattr(user, "user_name", ""),
            "user_name": getattr(user, "user_name", ""),
            "phone": getattr(user, "phone", ""),
        }
        for user in list(getattr(receipt_bill, "users", None) or [])
    ]


def _build_receipt_bill_kd_lookup_data(
    receipt_data: Dict[str, Any],
    receipt_bill: Optional[models.ReceiptBill] = None,
    db: Optional[Session] = None,
) -> Dict[str, Any]:
    lookup_data = dict(receipt_data)

    user_list = _build_receipt_bill_user_list(receipt_data, receipt_bill=receipt_bill)
    if user_list:
        lookup_data["user_list"] = user_list

    if db is None:
        return lookup_data

    community_id = _normalize_lookup_id(receipt_data.get("community_id"))
    asset_id = _normalize_lookup_id(receipt_data.get("asset_id"))
    if not asset_id:
        return lookup_data

    house_query = db.query(models.House).filter(models.House.house_id == asset_id)
    if community_id:
        house_query = house_query.filter(models.House.community_id == community_id)
    house = house_query.first()
    if house:
        lookup_data["house_id"] = house.house_id
        if not lookup_data.get("full_house_name"):
            lookup_data["full_house_name"] = house.house_name or receipt_data.get("asset_name") or ""
        if not lookup_data.get("asset_type_str"):
            lookup_data["asset_type_str"] = "房屋"

    park_query = db.query(models.Park).filter(models.Park.park_id == asset_id)
    if community_id:
        park_query = park_query.filter(models.Park.community_id == community_id)
    park = park_query.first()
    if park:
        lookup_data["park_id"] = park.park_id
        if not lookup_data.get("park_name"):
            lookup_data["park_name"] = park.name or receipt_data.get("asset_name") or ""
        if park.house_id and not lookup_data.get("house_id"):
            lookup_data["house_id"] = park.house_id
        if park.house_name and not lookup_data.get("full_house_name"):
            lookup_data["full_house_name"] = park.house_name
        if not lookup_data.get("asset_type_str"):
            lookup_data["asset_type_str"] = "车位"

    return lookup_data


def _enrich_receipt_bill_data(
    receipt_data: Dict[str, Any],
    receipt_bill: Optional[models.ReceiptBill] = None,
    db: Optional[Session] = None,
) -> Dict[str, Any]:
    enriched = _build_receipt_bill_kd_lookup_data(
        receipt_data,
        receipt_bill=receipt_bill,
        db=db,
    )

    if db is not None:
        try:
            from services.voucher_engine import KD_DERIVED_FIELDS, resolve_kd_derived_field

            for field_name in KD_DERIVED_FIELDS:
                if field_name not in enriched:
                    enriched[field_name] = resolve_kd_derived_field(field_name, enriched, db)
        except Exception:
            for field_name in RECEIPT_BILL_KD_DERIVED_FIELDS:
                enriched.setdefault(field_name, "")

    return prefix_source_fields(_sanitize_text_payload(enriched), "receipt_bills")


def _lookup_house_resident_name(
    data: Dict[str, Any],
    db: Optional[Session] = None,
) -> str:
    if db is None:
        return ""

    community_id = _normalize_lookup_id(data.get("community_id"))
    house_id = _normalize_lookup_id(data.get("house_id"))
    if not house_id:
        return ""

    resident_display = func.coalesce(
        func.nullif(models.HouseUser.owner_name, ""),
        func.nullif(models.HouseUser.name, ""),
    )
    dialect_name = ""
    try:
        dialect_name = (db.bind.dialect.name or "").lower() if db and db.bind else ""
    except Exception:
        dialect_name = ""

    if dialect_name == "mssql":
        query = (
            db.query(func.max(resident_display))
            .select_from(models.House)
            .join(models.HouseUser, models.HouseUser.house_fk == models.House.id)
            .filter(models.House.house_id == house_id)
            .filter(resident_display.isnot(None))
        )
    else:
        query = (
            db.query(func.string_agg(func.distinct(resident_display), ", "))
            .select_from(models.House)
            .join(models.HouseUser, models.HouseUser.house_fk == models.House.id)
            .filter(models.House.house_id == house_id)
            .filter(resident_display.isnot(None))
        )
    if community_id:
        query = query.filter(models.House.community_id == community_id)
    return str(query.scalar() or "").strip()


def _enrich_deposit_record_data(record_data: Dict[str, Any], db: Optional[Session] = None) -> Dict[str, Any]:
    enriched = dict(record_data)
    if "resident_name" not in enriched:
        enriched["resident_name"] = _lookup_house_resident_name(enriched, db=db)
    return prefix_source_fields(_sanitize_text_payload(enriched), "deposit_records")


def _enrich_prepayment_record_data(record_data: Dict[str, Any], db: Optional[Session] = None) -> Dict[str, Any]:
    enriched = dict(record_data)
    if "resident_name" not in enriched:
        enriched["resident_name"] = _lookup_house_resident_name(enriched, db=db)
    return prefix_source_fields(_sanitize_text_payload(enriched), "prepayment_records")


def _group_bills_field(field_name: str) -> str:
    if field_name.startswith("kd_"):
        return "银行账户" if "_bank_" in field_name else "金蝶关联"
    if field_name in BILL_RUNTIME_EXTRA_FIELDS:
        return "运行时字段"
    if field_name == "amount" or field_name.endswith("_amount"):
        return "金额信息"
    if field_name.startswith("pay_") or field_name.startswith("bill_") or field_name in {"receipt_id", "in_month", "receive_date"}:
        return "支付与状态"
    if field_name in {"id", "community_id", "charge_item_id", "asset_id", "house_id", "park_id", "bind_house_id", "deal_log_id"}:
        return "关联ID"
    return "账单字段"


def _group_receipt_bills_field(field_name: str) -> str:
    if field_name.startswith("kd_"):
        return "银行账户" if "_bank_" in field_name else "金蝶关联"
    if field_name in RECEIPT_BILL_RUNTIME_EXTRA_FIELDS:
        return "运行时字段"
    if field_name == "income_amount" or field_name == "amount" or field_name.endswith("_amount"):
        return "金额信息"
    if field_name.startswith("pay_") or field_name in {"receipt_id"}:
        return "支付信息"
    if field_name.startswith("invoice_") or field_name in {"open_invoice"}:
        return "发票信息"
    if field_name.startswith("deal_"):
        return "交易时间"
    if field_name in {"id", "community_id", "asset_id", "receipt_record_id", "receipt_version"}:
        return "关联ID"
    if field_name.startswith("asset_"):
        return "资产信息"
    return "收款字段"


def _group_deposit_records_field(field_name: str) -> str:
    if field_name in DEPOSIT_RECORD_RUNTIME_EXTRA_FIELDS:
        return "运行时字段"
    if field_name == "amount" or field_name.endswith("_amount"):
        return "金额信息"
    if field_name.startswith("operate_"):
        return "操作信息"
    if field_name.startswith("pay_"):
        return "支付信息"
    if field_name in {"id", "community_id", "house_id", "payment_id", "refund_receipt_id"}:
        return "关联ID"
    return "押金字段"


def _group_prepayment_records_field(field_name: str) -> str:
    if field_name in PREPAYMENT_RECORD_RUNTIME_EXTRA_FIELDS:
        return "运行时字段"
    if field_name == "amount" or field_name.endswith("_amount") or field_name == "balance_after_change":
        return "金额信息"
    if field_name.startswith("operate_") or field_name in {"operator", "operator_name", "status"}:
        return "操作信息"
    if field_name.startswith("pay_"):
        return "支付信息"
    if field_name in {"id", "community_id", "account_id", "building_id", "unit_id", "house_id", "payment_id", "refund_receipt_id", "deposit_order_id", "category_id"}:
        return "关联ID"
    return "预存款字段"


def build_source_fields(source_type: str) -> Set[str]:
    normalized_source = (source_type or "").strip().lower() or "bills"

    if normalized_source == "bills":
        fields = {col.name for col in models.Bill.__table__.columns}
        try:
            from services.voucher_engine import KD_DERIVED_FIELDS

            fields.update(KD_DERIVED_FIELDS.keys())
        except Exception:
            pass
        fields.update(BILL_RUNTIME_EXTRA_FIELDS)
        return fields

    if normalized_source == "receipt_bills":
        fields = {col.name for col in models.ReceiptBill.__table__.columns}
        fields.update(RECEIPT_BILL_KD_DERIVED_FIELDS)
        fields.update(RECEIPT_BILL_RUNTIME_EXTRA_FIELDS)
        return fields

    if normalized_source == "deposit_records":
        fields = {col.name for col in models.DepositRecord.__table__.columns}
        fields.update(DEPOSIT_RECORD_RUNTIME_EXTRA_FIELDS)
        return fields

    if normalized_source == "prepayment_records":
        fields = {col.name for col in models.PrepaymentRecord.__table__.columns}
        fields.update(PREPAYMENT_RECORD_RUNTIME_EXTRA_FIELDS)
        return fields

    return set()


def build_source_field_options(source_type: str) -> List[Dict[str, str]]:
    normalized_source = (source_type or "").strip().lower() or "bills"
    fields = build_source_fields(normalized_source)

    if normalized_source == "bills":
        labels = BILL_FIELD_LABELS
        grouper = _group_bills_field
    elif normalized_source == "receipt_bills":
        labels = RECEIPT_BILL_FIELD_LABELS
        grouper = _group_receipt_bills_field
    elif normalized_source == "deposit_records":
        labels = DEPOSIT_RECORD_FIELD_LABELS
        grouper = _group_deposit_records_field
    elif normalized_source == "prepayment_records":
        labels = PREPAYMENT_RECORD_FIELD_LABELS
        grouper = _group_prepayment_records_field
    else:
        labels = {}
        grouper = lambda _field_name: "账单字段"

    options = []
    for field_name in sorted(fields):
        display_name = labels.get(field_name)
        label = display_name or field_name
        options.append({
            "label": label,
            "value": field_name,
            "group": grouper(field_name),
        })
    return options


def enrich_source_data(
    source_type: str,
    data: Dict[str, Any],
    db: Optional[Session] = None,
    record: Optional[Any] = None,
) -> Dict[str, Any]:
    normalized_source = (source_type or "").strip().lower() or "bills"

    if normalized_source == "bills":
        if db is not None:
            from services.voucher_engine import enrich_bill_data

            return _sanitize_text_payload(enrich_bill_data(data, db))
        return prefix_source_fields(_sanitize_text_payload(data), "bills")

    if normalized_source == "receipt_bills":
        return _enrich_receipt_bill_data(data, receipt_bill=record, db=db)

    if normalized_source == "deposit_records":
        return _enrich_deposit_record_data(data, db=db)

    if normalized_source == "prepayment_records":
        return _enrich_prepayment_record_data(data, db=db)

    return prefix_source_fields(data, normalized_source)
