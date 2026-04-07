import json
import json as json_mod
import re
import uuid
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import and_, cast, func, or_, String
from sqlalchemy.orm import Session, joinedload

import models
import schemas
from api.dependencies import (
    _require_api_permission,
    get_allowed_community_ids,
    get_current_user,
    get_db,
    get_user_context,
)
from api.voucher_helpers import (
    _decode_header_value,
    _enrich_receipt_bill_data,
    _extract_kingdee_push_message,
    _extract_kingdee_voucher_result,
    _finalize_bill_push_records,
    _find_bill_push_conflicts,
    _get_bill_push_status_map,
    _get_related_bill_refs_for_receipts,
    _load_receipt_to_bills_relation,
    _load_receipt_to_deposit_collect_relation,
    _load_receipt_to_deposit_refund_relation,
    _load_receipt_to_prepayment_recharge_relation,
    _load_receipt_to_prepayment_refund_relation,
    _normalize_bill_refs,
    _normalize_receipt_refs,
    _serialize_receipt_bill_model,
    _summarize_bill_push_statuses,
)
from services.voucher_engine import evaluate_expression
from utils.expression_functions import (
    extract_expression_function_names,
    get_public_expression_function_names,
    get_public_expression_functions,
)
from utils.variable_parser import (
    build_variable_map,
    get_builtin_variable_keys,
    resolve_dict_variables,
    resolve_variables,
)
from voucher_field_mapping import (
    build_source_field_options as mapping_build_source_field_options,
    build_source_fields as mapping_build_source_fields,
    enrich_source_data as mapping_enrich_source_data,
    prefix_source_fields as mapping_prefix_source_fields,
)
from voucher_source_registry import (
    VoucherRelationMeta,
    VoucherSourceMeta,
    VoucherSourceModuleMeta,
    build_relation_payload,
    build_source_modules_payload,
)

router = APIRouter()


def build_template_category_tree(
    categories: List[models.VoucherTemplateCategory],
    parent_id=None,
    parent_path: str = "",
):
    tree = []
    for cat in categories:
        if cat.parent_id == parent_id:
            path = f"{parent_path} / {cat.name}" if parent_path else cat.name
            node = {
                "id": cat.id,
                "name": cat.name,
                "parent_id": cat.parent_id,
                "sort_order": cat.sort_order,
                "status": cat.status,
                "description": cat.description,
                "path": path,
                "created_at": cat.created_at,
                "updated_at": cat.updated_at,
                "children": build_template_category_tree(categories, cat.id, path),
            }
            tree.append(node)
    return tree


def build_template_category_path_map(
    categories: List[models.VoucherTemplateCategory],
) -> Dict[int, str]:
    by_id = {c.id: c for c in categories}
    cache: Dict[int, str] = {}

    def resolve(cat_id: int) -> Optional[str]:
        if cat_id in cache:
            return cache[cat_id]
        cat = by_id.get(cat_id)
        if not cat:
            return None
        if cat.parent_id and cat.parent_id in by_id:
            parent_path = resolve(cat.parent_id)
            path = f"{parent_path} / {cat.name}" if parent_path else cat.name
        else:
            path = cat.name
        cache[cat_id] = path
        return path

    for cid in by_id.keys():
        resolve(cid)
    return cache


@router.get("/api/vouchers/template-categories")
def get_voucher_template_categories(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")
    categories = db.query(models.VoucherTemplateCategory).order_by(
        models.VoucherTemplateCategory.sort_order.asc(),
        models.VoucherTemplateCategory.id.asc(),
    ).all()
    path_map = build_template_category_path_map(categories)
    return [{
        "id": c.id,
        "name": c.name,
        "parent_id": c.parent_id,
        "sort_order": c.sort_order,
        "status": c.status,
        "description": c.description,
        "path": path_map.get(c.id),
        "created_at": c.created_at,
        "updated_at": c.updated_at,
    } for c in categories]


@router.get("/api/vouchers/template-categories/tree")
def get_voucher_template_categories_tree(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")
    categories = db.query(models.VoucherTemplateCategory).order_by(
        models.VoucherTemplateCategory.sort_order.asc(),
        models.VoucherTemplateCategory.id.asc(),
    ).all()
    return build_template_category_tree(categories, None, "")


@router.post("/api/vouchers/template-categories")
def create_voucher_template_category(
    payload: schemas.VoucherTemplateCategoryCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")

    if payload.parent_id is not None:
        parent = db.query(models.VoucherTemplateCategory).filter(
            models.VoucherTemplateCategory.id == payload.parent_id
        ).first()
        if not parent:
            raise HTTPException(status_code=400, detail="Parent category not found")

    category = models.VoucherTemplateCategory(
        name=payload.name,
        parent_id=payload.parent_id,
        sort_order=payload.sort_order,
        status=payload.status,
        description=payload.description,
    )
    db.add(category)
    db.commit()
    db.refresh(category)
    return {"id": category.id, "message": "Template category created successfully"}


@router.put("/api/vouchers/template-categories/{category_id}")
def update_voucher_template_category(
    category_id: int,
    payload: schemas.VoucherTemplateCategoryUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")

    category = db.query(models.VoucherTemplateCategory).filter(
        models.VoucherTemplateCategory.id == category_id
    ).first()
    if not category:
        raise HTTPException(status_code=404, detail="Template category not found")

    update_data = payload.dict(exclude_unset=True)
    if "parent_id" in update_data:
        next_parent_id = update_data["parent_id"]
        if next_parent_id == category_id:
            raise HTTPException(status_code=400, detail="Category cannot be its own parent")
        if next_parent_id is not None:
            parent = db.query(models.VoucherTemplateCategory).filter(
                models.VoucherTemplateCategory.id == next_parent_id
            ).first()
            if not parent:
                raise HTTPException(status_code=400, detail="Parent category not found")

            cursor = parent
            while cursor and cursor.parent_id is not None:
                if cursor.parent_id == category_id:
                    raise HTTPException(status_code=400, detail="Invalid parent category (cycle detected)")
                cursor = db.query(models.VoucherTemplateCategory).filter(
                    models.VoucherTemplateCategory.id == cursor.parent_id
                ).first()

    for key, value in update_data.items():
        setattr(category, key, value)

    db.commit()
    return {"message": "Template category updated successfully"}


@router.delete("/api/vouchers/template-categories/{category_id}")
def delete_voucher_template_category(
    category_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")

    category = db.query(models.VoucherTemplateCategory).filter(
        models.VoucherTemplateCategory.id == category_id
    ).first()
    if not category:
        raise HTTPException(status_code=404, detail="Template category not found")

    has_children = db.query(models.VoucherTemplateCategory).filter(
        models.VoucherTemplateCategory.parent_id == category_id
    ).first()
    if has_children:
        raise HTTPException(status_code=400, detail="Cannot delete category with children")

    bound_template = db.query(models.VoucherTemplate).filter(
        models.VoucherTemplate.category_id == category_id
    ).first()
    if bound_template:
        raise HTTPException(status_code=400, detail="Cannot delete category with existing voucher templates")

    db.delete(category)
    db.commit()
    return {"message": "Template category deleted successfully"}

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


def _decimal_text(value: Any) -> str:
    parsed = _try_parse_decimal(value)
    if parsed is None:
        return "0"
    text = format(parsed, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


MONEY_QUANTIZER = Decimal("0.01")


def _money_text(value: Any) -> str:
    parsed = _try_parse_decimal(value) or Decimal("0")
    rounded = parsed.quantize(MONEY_QUANTIZER, rounding=ROUND_HALF_UP)
    return format(rounded, ".2f")


def _entry_decimal(entry: Dict[str, Any], exact_field: str, fallback_field: str) -> Decimal:
    return (
        _try_parse_decimal(entry.get(exact_field))
        or _try_parse_decimal(entry.get(fallback_field))
        or Decimal("0")
    )


def _allocate_money_amounts(values: List[Decimal]) -> List[Decimal]:
    if not values:
        return []
    scale_factor = Decimal("100")
    target_total = sum(values, Decimal("0")).quantize(MONEY_QUANTIZER, rounding=ROUND_HALF_UP)
    target_cents = int((target_total * scale_factor).to_integral_value(rounding=ROUND_HALF_UP))

    base_cents: List[int] = []
    remainders: List[Tuple[int, Decimal]] = []
    for idx, value in enumerate(values):
        scaled = value * scale_factor
        base = int(scaled.to_integral_value(rounding=ROUND_DOWN))
        remainder = scaled - Decimal(base)
        base_cents.append(base)
        remainders.append((idx, remainder))

    delta = target_cents - sum(base_cents)
    if delta > 0:
        for idx, _ in sorted(remainders, key=lambda item: (-item[1], item[0]))[:delta]:
            base_cents[idx] += 1

    return [Decimal(cents) / scale_factor for cents in base_cents]


def _normalize_voucher_money_fields(accounting_entries: List[Dict[str, Any]], kingdee_entries: List[Dict[str, Any]]) -> None:
    debit_values = [_entry_decimal(entry, "debit_exact", "debit") for entry in accounting_entries]
    credit_values = [_entry_decimal(entry, "credit_exact", "credit") for entry in accounting_entries]
    rounded_debits = _allocate_money_amounts(debit_values)
    rounded_credits = _allocate_money_amounts(credit_values)

    for idx, accounting_entry in enumerate(accounting_entries):
        debit_value = rounded_debits[idx]
        credit_value = rounded_credits[idx]
        debit_text = format(debit_value, ".2f")
        credit_text = format(credit_value, ".2f")

        accounting_entry["debit_exact"] = debit_text
        accounting_entry["credit_exact"] = credit_text
        accounting_entry["debit"] = float(debit_value)
        accounting_entry["credit"] = float(credit_value)

        if idx >= len(kingdee_entries):
            continue

        kingdee_entry = kingdee_entries[idx]
        kingdee_entry["debitori"] = debit_text
        kingdee_entry["creditori"] = credit_text
        kingdee_entry["debitlocal"] = debit_text
        kingdee_entry["creditlocal"] = credit_text


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

    right_diff_ori = debit_ori - credit_ori
    left_diff_ori = credit_ori - debit_ori
    if right_diff_ori != Decimal("0") or left_diff_ori != Decimal("0"):
        raise HTTPException(
            status_code=400,
            detail=(
                "Voucher JSON debit/credit not balanced "
                f"(right_diff={right_diff_ori}, left_diff={left_diff_ori}): "
                f"debitori={debit_ori} creditori={credit_ori}"
            ),
        )
    right_diff_local = debit_local - credit_local
    left_diff_local = credit_local - debit_local
    if right_diff_local != Decimal("0") or left_diff_local != Decimal("0"):
        raise HTTPException(
            status_code=400,
            detail=(
                "Voucher JSON local debit/credit not balanced "
                f"(right_diff={right_diff_local}, left_diff={left_diff_local}): "
                f"debitlocal={debit_local} creditlocal={credit_local}"
            ),
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
    if normalized in {"false", "0", "0.0", "none", "null", "no", "off", "n", "f", "否", "不"}:
        return False
    if normalized in {"true", "1", "1.0", "yes", "on", "y", "t", "是", "对"}:
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
    "community_id": "社区ID",
    "charge_item_id": "收费项目ID",
    "charge_item_name": "收费项目名称",
    "charge_item_type": "收费项目类型",
    "category_name": "收费分类",
    "asset_id": "资产ID",
    "asset_name": "资产名称",
    "asset_type": "资产类型",
    "asset_type_str": "资产类型说明",
    "house_id": "房屋ID",
    "house_name": "房屋名称",
    "building_name": "楼栋名称",
    "room_name": "房号",
    "area": "面积",
    "owner_name": "业主名称",
    "customer_id": "客户ID",
    "customer_name": "客户名称",
    "bill_id": "账单编号",
    "receipt_id": "收款ID",
    "status": "状态",
    "status_str": "状态说明",
    "amount": "金额",
    "bill_amount": "应收金额",
    "paid_amount": "已收金额",
    "unpaid_amount": "未收金额",
    "in_month": "账期",
    "start_date": "开始日期",
    "end_date": "结束日期",
    "pay_date": "收款日期",
    "pay_time": "收款时间",
    "created_at": "创建时间",
    "updated_at": "更新时间",
}

_RECEIPT_BILL_RUNTIME_EXTRA_FIELDS: Set[str] = {"community_name", "payer_name", "deal_type_label"}
_RECEIPT_BILL_FIELD_LABELS: Dict[str, str] = {
    "id": "收款流水ID",
    "community_id": "社区ID",
    "community_name": "社区名称",
    "payer_name": "付款人",
    "deal_time": "交易时间",
    "deal_date": "交易日期",
    "income_amount": "入账金额",
    "amount": "金额",
    "bill_amount": "账单金额",
    "discount_amount": "优惠金额",
    "deposit_amount": "押金金额",
    "pay_channel": "支付渠道",
    "pay_channel_str": "支付渠道说明",
    "payee": "收款人",
    "receipt_id": "收据ID",
    "receipt_record_id": "收据记录ID",
    "receipt_version": "收据版本",
    "invoice_number": "发票号码",
    "invoice_urls": "发票链接",
    "invoice_status": "发票状态",
    "open_invoice": "是否开票",
    "asset_name": "资产名称",
    "asset_id": "资产ID",
    "asset_type": "资产类型",
    "deal_type": "交易类型",
    "deal_type_label": "交易类型说明",
    "remark": "备注",
    "fk_id": "FK_ID",
    "bind_users_raw": "绑定用户原始数据",
    "created_at": "创建时间",
    "updated_at": "更新时间",
    "kd_house_number": "金蝶房屋编码",
    "kd_house_name": "金蝶房屋名称",
    "kd_park_house_number": "金蝶园区房屋编码",
    "kd_park_house_name": "金蝶园区房屋名称",
    "kd_customer_number": "金蝶客户编码",
    "kd_customer_name": "金蝶客户名称",
    "kd_project_number": "金蝶项目编码",
    "kd_project_name": "金蝶项目名称",
    "kd_receive_bank_number": "金蝶收款银行编码",
    "kd_receive_bank_name": "金蝶收款银行名称",
    "kd_pay_bank_number": "金蝶付款银行编码",
    "kd_pay_bank_name": "金蝶付款银行名称",
}

_DEPOSIT_RECORD_RUNTIME_EXTRA_FIELDS: Set[str] = {"operate_type_label"}
_DEPOSIT_RECORD_FIELD_LABELS: Dict[str, str] = {
    "id": "押金记录ID",
    "community_id": "社区ID",
    "community_name": "社区名称",
    "house_id": "房屋ID",
    "house_name": "房屋名称",
    "amount": "金额",
    "operate_type": "操作类型",
    "operate_type_label": "操作类型说明",
    "operator": "操作人",
    "operator_name": "操作人姓名",
    "operate_time": "操作时间",
    "operate_date": "操作日期",
    "cash_pledge_name": "押金类型",
    "remark": "备注",
    "pay_time": "支付时间",
    "pay_date": "支付日期",
    "payment_id": "支付流水号",
    "has_refund_receipt": "是否有退款收据",
    "refund_receipt_id": "退款收据ID",
    "pay_channel_str": "支付渠道",
    "raw_data": "原始数据",
    "created_at": "创建时间",
    "updated_at": "更新时间",
}
def _group_bills_field(field_name: str) -> str:
    field_options = mapping_build_source_field_options("bills")
    matched = next((item for item in field_options if item.get("value") == field_name), None)
    return str(matched.get("group")) if matched and matched.get("group") else "placeholder"


def _group_receipt_bills_field(field_name: str) -> str:
    field_options = mapping_build_source_field_options("receipt_bills")
    matched = next((item for item in field_options if item.get("value") == field_name), None)
    return str(matched.get("group")) if matched and matched.get("group") else "placeholder"


def _group_deposit_records_field(field_name: str) -> str:
    field_options = mapping_build_source_field_options("deposit_records")
    matched = next((item for item in field_options if item.get("value") == field_name), None)
    return str(matched.get("group")) if matched and matched.get("group") else "placeholder"


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
    "marki": VoucherSourceModuleMeta(id="marki", label="placeholder"),
    "oa": VoucherSourceModuleMeta(
        id="oa",
        label="OA",
        note="OA source module metadata.",
    ),
}


SOURCE_REGISTRY: Dict[str, VoucherSourceMeta] = {
    "bills": VoucherSourceMeta(
        id="bills",
        module_id="marki",
        label="placeholder",
        source_type="bills",
        root_enabled=True,
        field_names_builder=_build_bills_fields,
        field_options_builder=lambda: _build_bills_field_options(),
    ),
    "receipt_bills": VoucherSourceMeta(
        id="receipt_bills",
        module_id="marki",
        label="placeholder",
        source_type="receipt_bills",
        root_enabled=True,
        field_names_builder=_build_receipt_bills_fields,
        field_options_builder=lambda: _build_receipt_bills_field_options(),
    ),
    "deposit_records": VoucherSourceMeta(
        id="deposit_records",
        module_id="marki",
        label="placeholder",
        source_type="deposit_records",
        root_enabled=False,
        field_names_builder=_build_deposit_records_fields,
        field_options_builder=lambda: _build_deposit_records_field_options(),
    ),
    "prepayment_records": VoucherSourceMeta(
        id="prepayment_records",
        module_id="marki",
        label="濠电姷鏁告慨鐑藉极閸涘﹥鍙忛柣鎴濐潟閳ь剙鍊块幐濠冪珶閳哄绉€规洏鍔戝鍫曞箣閻欏懐骞㈤梻鍌欑閹测剝绗熷Δ鍛煑閹兼番鍔嶉崑鍕煕閳╁厾鑲╂崲閸℃ǜ浜滈柡宥冨妽閻ㄦ垶鎱ㄩ敐鍥т槐闁哄本绋撻埀顒婄秵閸嬪懐浜搁鐔翠簻妞ゆ劧绲跨粻鐐烘煙椤旂懓澧查柟顖涙閸╋箓鍩€椤掑嫬绠い鎰堕檮閳锋帒霉閿濆洨鎽傞柛銈呭暣閺屾盯鎮╅幇浣圭暥濡炪値浜滈崯鎾箠閻愬搫唯闁靛牆娲ㄩ悾楣冩⒒娴ｅ摜绉烘俊顐ユ硶缁牊鎷呴崨濠冩闂傚倸鍊风欢姘焽瑜庨〃銉ㄧ疀閺囩噥娼熼梺鍝勬储閸ㄥ綊鎮為崹顐犱簻闁瑰搫妫楁禍鎯ь渻閵堝骸骞栭柛銏＄叀閿濈偠绠涘☉娆愬劒闁荤喐鐟ョ€氼剟宕㈣ぐ鎺撯拺闁告稑锕﹂惌鍡涙煟閹虹偛顩紒杈╁仱瀹曞ジ濡烽敂瑙勫闂備礁鎲＄粙鎴︽晝閵夆敡澶愬冀椤愩倗锛滈梺缁橆焾濞呮洜浜搁銏＄厽闁挎繂娲ら崢瀛樸亜閵忊槅娈滅€规洜鍠栭、鏇㈠Χ鎼粹懣銈嗙節閻㈤潧浠╅柟娲讳簽瀵板﹪宕稿Δ鈧粻鐘绘煙閹规劦鍤欓柛?",
        source_type="prepayment_records",
        root_enabled=False,
        field_names_builder=_build_prepayment_records_fields,
        field_options_builder=lambda: _build_prepayment_records_field_options(),
    ),
    "oa_forms": VoucherSourceMeta(
        id="oa_forms",
        module_id="oa",
        label="placeholder",
        source_type="oa_forms",
        root_enabled=False,
        note="濠电姷鏁告慨鐑藉极閸涘﹥鍙忛柣鎴ｆ閺嬩線鏌熼梻瀵割槮缁惧墽绮换娑㈠箣濞嗗繒鍔撮梺杞扮椤戝棝濡甸崟顖氱閻犺櫣鍎ら悗楣冩⒑閸涘﹦鎳冪紒缁橈耿瀵鎮㈤搹鍦紲闂侀潧绻掓慨鐢告倶閸垻纾藉ù锝呮惈鍟告繝娈垮枤閺佸鐛崼銉ノ╅柕澶樺枟鐎靛矂鏌ｉ悩鍙夌┛鐎殿喗鎸荤粩鐔煎即閵忊檧鎷绘繛鎾村焹閸嬫挻绻涙担鍐插悩濞戞鏃堝川椤撶媴绱梻浣侯潒閸曞灚鐣堕梺鍝勫閸庣敻寮婚妸鈺傚亜闁告繂瀚呴姀鈶╁亾閻熺増鍟炵紒璇插暣婵＄敻宕熼姘辩潉闂佸壊鍋嗛崳锔炬閻㈠憡鈷戦悹鍥ｂ偓铏彲缂備焦褰冩晶浠嬪箲閵忕姭鏀介悗锝庡亜娴犳椽姊婚崒姘卞缂佸鍔楅崚鎺旀崉鐞涒剝鏂€闂佸疇妫勫Λ妤佺濠靛鐓熼柣鏂垮级濞呭懘鏌ｉ敐鍛Щ闁宠鍨归埀顒婄秵娴滄粌鐣甸崱娑欌拺闂傚牊绋撶粻鍐测攽椤旀儳鍘寸€规洏鍨介弻鍡楊吋閸″繑瀚奸梻浣告啞缁诲倻鈧凹鍣ｉ崺銏″緞閹邦厾鍘卞┑鈽嗗灠濠€閬嶆儗濞嗘垟鍋撶憴鍕闁告梹鐟ラ悾閿嬬附缁嬪灝宓嗛梺缁樻煥閹碱偊鐛Δ鍛拻濞达絽鎽滅粔娲煕鐎ｎ亷韬€规洘绮岄埥澶愬閻樻鍟堥梻浣虹帛椤洭寮崫銉ヮ棜濠靛倸鎲￠悡鍐喐濠婂牆绀堥柣鏃堫棑閺嗭附鎱ㄥ璇蹭壕濡炪們鍨洪悷鈺呭箖閸撗傛勃闁芥ê顦遍鎴︽⒒閸屾艾鈧悂鈥﹂鍕；闁告洦鍊嬪ú顏勵潊闁靛牆鎳愰敍鐔兼⒑鐟欏嫬鍔跺┑顔哄€濆畷鎴﹀煛閸涱喚鍘介梺闈涚箞閸╁嫰寮抽鐐寸厱閻庯綆鍋呯亸顓㈡煃缂佹ɑ宕岀€规洖缍婇、娆撴偩鐏炲吋鍠氶梻鍌氬€峰ù鍥敋閺嶎厼绐楅柡宥庡幖绾惧綊鏌熼梻瀵稿妽闁稿鏅犻弻锝夊箣閿濆憛鎾绘煕鐎ｎ亜顏柡宀嬬秮楠炴﹢鎼归锝呴棷婵＄偑鍊х€靛矂宕㈤悾灞惧床婵炴垶鐟︾紞鍥煕閹炬鍠氶崵銈夋⒑閼姐倕袥闁稿鎹囬弻銊╁即濡も偓娴滈箖鏌涘Δ鍛喚闁哄矉缍侀獮瀣晲閸涘懏鎹囬弻宥夋煥鐎ｎ亞鐟ㄩ梻鍥ь樀閺屻劌鈹戦崱妯烘闂佸摜鍠撻崑銈夊蓟閻旂⒈鏁嶆慨妯哄船椤も偓濠电儑绲藉ú銈夋晝椤忓牄鈧線寮撮姀鈩冩珳闁瑰吋鐣崹濠氬级瑜版帗鐓熼幖娣€ゅ鎰箾閸欏鑰跨€规洘绻傞埢搴ㄥ箻瀹曞洨鏆┑锛勫仜椤戝懐鈧稈鏅犲鍐差煥閸曗晙绨婚梺鍝勫€藉▔鏇烆潩閵娾晜鐓曢悗锝庡亝瀹曞本顨ラ悙鏉戝闁诡垱鏌ㄩ埥澶娢旈崘顏呮櫒闂傚倸鍊峰ù鍥綖婢舵劕纾块柛鎰皺閺嗭附銇勯幒鎴濐仾闁稿﹤鐖奸弻宥夊传閸曨偅娈剁紒鐐劤閵堟悂寮婚敐鍛傜喖宕崟顒佺槪闂備礁纾划顖毭洪悢鐓庤摕闁挎稑瀚▽顏堟煟閹伴潧澧版い锔哄姂閹宕归锝囧嚒闁诲孩纰嶅姗€鎮鹃悜鑺ュ亗閹煎瓨蓱閺傗偓闂備胶纭堕崜婵嬨€冭箛鎿冪劷婵炴垯鍨洪埛鎴︽偣閸ャ劌绲绘い鎺嬪灪閵囧嫰寮埀顒勬偋閻樿尙鏆﹂柡澶庮嚦閺冣偓閹峰懘鎼圭拠鈥虫櫗婵犵數濮烽弫鍛婃叏閺夋嚚娲Χ閸モ晙绗夐梺鍝勭Р閸斿鎹㈤崱娑欑厪闁割偅绻冮崳娲煕閿濆懐绉洪柡宀€鍠撳☉鐢稿川椤撶姴甯块梻渚€娼уú銈団偓姘嵆閻涱喖螣閸忕厧纾柡澶屽仦婢瑰棝鏌﹂悽鐢电＝闁稿本鐟х拹浼存煕閻曚礁浜扮€规洏鍔戦、娑橆煥閹邦噣妫烽梻鍌氬€烽懗鍫曗€﹂崼銉ュ珘妞ゆ巻鍋撴い顐ｇ箞婵℃悂濡疯閹冲啯绻濈喊澶岀？闁稿鍨垮畷鎰板冀椤撶偛鐎梻鍌氱墛閼拌棄煤椤忓秵鏅ｉ梺闈涚箳婵潧危椤掑嫭鈷戦梺顐ゅ仜閼活垱鏅堕鐐寸厪闁搞儜鍐句純濡ょ姷鍋炵敮鎺楊敇婵傜閱囨繝闈涙閼垫劙姊婚崒娆戝妽闁告挻宀稿畷褰掑础閻忚鎼～婊堝焵椤掍椒绻嗛柣銏㈩焾缁€瀣亜閺嶎煈鍤ら柍鍝勬噺閻撳繐顭块懜鐢碘槈闁伙附绮嶆穱濠囶敃椤掑倻鏆犲銈庝簻閸熷瓨淇婇崼鏇炵闁靛ě鍌滄／闂傚倷娴囧畷鐢稿疮濞嗘挸绀嬫い鎾楀嫮銈梻鍌欑劍鐎笛兠洪弽顓炵９闁告縿鍎抽惌鍡涙煕椤愮姴鐏柛鐘冲姇椤潡鎳滈棃娑橆潓闂佸憡鍨规繛鈧柡灞糕偓鎰佸悑閹肩补鈧磭顔戦梻浣虹帛閹稿鎮烽埡鍛摕闁靛牆鎮块崷顓涘亾閿濆骸浜濋柣婵囶殜閺岋箑螣閻撳孩鐏堝┑顔硷攻濡炰粙寮婚崨瀛樺€烽柤鑹版硾椤忣厽绻濋埛鈧仦鑺ョ彎闂佸搫鏈惄顖炲箖閵忋垻纾兼俊顖滎儠閳ь剙锕︾槐鎾存媴閹存帒鎯堥梺绋款儐閻╊垶鏁愰悙宸叆闁割偅绻勯崝锕€顪冮妶鍡楀潑闁稿鎸婚妵鍕敃閵忋垻顔囬柣鎾卞€栭妵鍕疀閹炬潙娅ｅ┑鐐茬墢閸嬬偟鎹㈠☉銏犵骇闁瑰瓨绻冮崐顖氣攽閻愭彃鎮戦柣鐔叉櫊閻涱喖螖閸涱喖浠洪梺鍛婄☉鑹岄柟閿嬫そ濮婃椽宕ㄦ繝鍕暤闁诲孩鍑归崹鍫曟晲閻愮儤鏅濋柛灞剧〒閸橀亶姊洪崫鍕殜闁稿鎹囬弻娑㈠Ω瑜庨弳顒勬煙椤曗偓缁犳牠骞冨鍫熷殟闁靛闄勯悵鎶芥⒒娴ｇ顥忛柛瀣浮瀹曟垿宕ㄧ€电硶鍋撻悜鑺モ拻?",
        field_names_builder=_build_oa_fields,
        field_options_builder=_build_oa_field_options,
    ),
}


RELATION_REGISTRY: Dict[str, VoucherRelationMeta] = {
    "receipt_to_bills": VoucherRelationMeta(
        resolver="receipt_to_bills",
        label="placeholder",
        root_source="receipt_bills",
        target_source="bills",
        loader=_load_receipt_to_bills_relation,
    ),
    "receipt_to_deposit_collect": VoucherRelationMeta(
        resolver="receipt_to_deposit_collect",
        label="placeholder",
        root_source="receipt_bills",
        target_source="deposit_records",
        loader=_load_receipt_to_deposit_collect_relation,
    ),
    "receipt_to_deposit_refund": VoucherRelationMeta(
        resolver="receipt_to_deposit_refund",
        label="闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌ｉ幋锝呅撻柛銈呭閺屾盯顢曢敐鍡欙紩闂侀€炲苯澧剧紒鐘虫尭閻ｉ攱绺界粙娆炬綂闂佺偨鍎遍崯璺ㄨ姳閵夆晜鈷掑ù锝囩摂濞兼劕顭块悷鐗堫棡闁哄懓娉涜灃闁告侗鍘鹃敍娑橆渻閵堝懐绠伴柣妤€锕幃锟犲即閵忥紕鍘繝銏ｅ煐缁嬫捇宕氶弶搴撴斀闁斥晛鍟婊堟煏閸パ冾伃闁糕晪绻濆畷姗€濡搁妷銉ь吅闂傚倷绀侀幖顐﹀嫉椤掑嫭鍎庢い鏍亼閳ь兛绶氬鎾閳ュ厖姹楅梻浣告贡缁垳鏁幒妤嬬稏闁规儳澧庣壕浠嬫煕鐏炲墽鎳嗛柛蹇撹嫰閳规垿顢涢敐鍛睄閻庤娲戦崡鍐差嚕娴犲鏁囬柣鎰版涧楠炲姊绘担渚敯闁规椿浜炵划濠氬箣閻樺樊妫滈梺绋跨箺閸嬫劗寮ч埀顒佺節閻㈤潧孝闁稿﹤缍婇獮鎴︽晲婢跺鍘遍梺鍝勫暙閸嬪棝鎮炴ィ鍐╃厓閻熸瑥瀚悘鎾煙椤旂晫鎳囩€规洩绲惧鍕節閸愬彞娌梻鍌氬€风粈渚€骞夐垾鎰佹綎鐟滅増甯掗崹鍌炴煟閵忋倖浜ょ紓宥嗙墵閻擃偊宕堕妸锕€顎涘┑鐐叉▕娴滃爼寮崒婧惧亾楠炲灝鍔氭俊顐ｇ懃閳诲秹宕堕浣叉嫽婵炶揪缍€濞咃絿鏁☉娆庣箚妞ゆ劧绱曢ˇ锕傛煏閸℃ê绗ч柟鍙夋尦瀹曠喖顢橀悩鏌ョ崕闂傚倷绀侀幖顐⒚洪妸鈺佺獥闁规崘娉涙慨顒勬煃?",
        root_source="receipt_bills",
        target_source="deposit_records",
        loader=_load_receipt_to_deposit_refund_relation,
    ),
    "receipt_to_prepayment_recharge": VoucherRelationMeta(
        resolver="receipt_to_prepayment_recharge",
        label="闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌ｉ幋锝呅撻柛銈呭閺屾盯顢曢敐鍡欙紩闂侀€炲苯澧剧紒鐘虫尭閻ｉ攱绺界粙娆炬綂闂佺偨鍎遍崯璺ㄨ姳閵夆晜鈷掑ù锝囩摂濞兼劕顭块悷鐗堫棡闁哄懓娉涜灃闁告侗鍘鹃敍娑橆渻閵堝懐绠伴柣妤€锕幃锟犲即閵忥紕鍘繝銏ｅ煐缁嬫捇宕氶弶搴撴斀闁斥晛鍟婊堟煏閸パ冾伃闁糕晪绻濆畷姗€濡搁妷銉ь吅闂傚倷绀侀幖顐﹀嫉椤掑嫭鍎庢い鏍亼閳ь兛绶氬鎾閻樻爠鍥ㄧ厱婵炴垵宕弸銈囨喐閻楀牊鐨戠紒杈ㄦ崌瀹曟帒鈻庨幋婵嗩瀴闂備礁鎽滈崑鐘茬暦閻㈠灚顫曢柣鎰嚟閻熷綊鏌嶈閸撴瑩顢氶敐鍥ㄥ珰婵炴潙顑嗛～宥夋⒑闂堟稓绠冲┑顔芥綑閻ｇ敻宕卞☉娆屾嫼闂傚倸鐗婃笟妤呮倿妤ｅ啯鐓曢幖娣灩椤ュ鏌ゆウ鍧楀摵缂佸倹甯為埀顒婄秵閸嬧偓闁归攱妞藉娲閳轰胶妲ｉ梺鍛婎焾濡嫰鍩㈠澶婂嵆闁绘劏鏅滈弬鈧俊鐐€栧濠氬磻閹捐姹查柍鍝勬噺閻撴瑦顨ラ悙鑼虎闁诲繆鏅犻弻宥夋寠婢舵ɑ鈻堟繝娈垮枓閸嬫捇姊虹紒姗嗙劸閻忓浚浜崺鈧い鎺嗗亾闁诲繑绻堥崺鐐哄箣閿旇棄鈧兘鏌涘▎蹇ｆ▓婵☆偆鍋熺槐鎾存媴缁涘娈銈嗗灥濡稓鍒掗埡鍛亜闁绘挸楠稿畵鍡涙⒑闂堟稓绠氭俊鐙欏洤绠繛宸簼閻撴稑顭跨捄楦垮濞寸媴绠戦…鍧楁偡閻楀牜妫﹂柦妯荤箞閺屻劑寮崹顔规寖缂佹儳澧介弲顐﹀焵椤掆偓缁犲秹宕曢柆宥呯闁瑰瓨绻嶉崯鍛存煏婢跺棙娅嗛柣?",
        root_source="receipt_bills",
        target_source="prepayment_records",
        loader=_load_receipt_to_prepayment_recharge_relation,
    ),
    "receipt_to_prepayment_refund": VoucherRelationMeta(
        resolver="receipt_to_prepayment_refund",
        label="闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌ｉ幋锝呅撻柛銈呭閺屾盯顢曢敐鍡欙紩闂侀€炲苯澧剧紒鐘虫尭閻ｉ攱绺界粙娆炬綂闂佺偨鍎遍崯璺ㄨ姳閵夆晜鈷掑ù锝囩摂濞兼劕顭块悷鐗堫棡闁哄懓娉涜灃闁告侗鍘鹃敍娑橆渻閵堝懐绠伴柣妤€锕幃锟犲即閵忥紕鍘繝銏ｅ煐缁嬫捇宕氶弶搴撴斀闁斥晛鍟婊堟煏閸パ冾伃闁糕晪绻濆畷姗€濡搁妷銉ь吅闂傚倷绀侀幖顐﹀嫉椤掑嫭鍎庢い鏍亼閳ь兛绶氬鎾閻樻爠鍥ㄧ厱婵炴垵宕弸銈囨喐閻楀牊鐨戠紒杈ㄦ崌瀹曟帒鈻庨幋婵嗩瀴闂備礁鎽滈崑鐘茬暦閻㈠灚顫曢柣鎰嚟閻熷綊鏌嶈閸撴瑩顢氶敐鍥ㄥ珰婵炴潙顑嗛～宥夋⒑闂堟稓绠冲┑顔芥綑閻ｇ敻宕卞☉娆屾嫼闂傚倸鐗婃笟妤呮倿妤ｅ啯鐓曢幖娣灩椤ュ鏌ゆウ鍧楀摵缂佸倹甯為埀顒婄秵閸嬧偓闁归攱妞藉娲閳轰胶妲ｉ梺鍛婎焾濡嫰鍩㈠澶婂嵆闁绘劏鏅滈弬鈧俊鐐€栧濠氬磻閹捐姹查柍鍝勬噺閻撴瑦顨ラ悙鑼虎闁诲繆鏅犻弻宥夋寠婢舵ɑ鈻堟繝娈垮枓閸嬫捇姊虹紒姗嗙劸閻忓浚浜崺鈧い鎺嗗亾闁诲繑绻堥崺鐐哄箣閿旇棄鈧兘鏌℃径瀣仼濞寸姷顭堥埞鎴︻敊鐟欐帗绮撳畷婵嬪箣濠垫劕娈ㄩ梺鐟板閻℃棃寮崘顔界叆闁哄啫娉﹂幒妤€绠繛宸簼閳锋帒銆掑顒佹悙濞存粎鍋ら弻娑氣偓锝庡亞濞叉挳鏌熷畷鍥ф灈妞ゃ垺鐩幃娆撳箵閹烘垳鎲鹃梻鍌欑濠€閬嶆惞鎼淬劌绐楁俊銈呮噺閸嬪倿鏌￠崶銉ョ仾闁稿﹦鏁婚弻銊モ攽閸℃侗鈧霉濠婂嫮绠為柟顔筋焾缁犳盯寮▎鐐棆缂?",
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


@router.get("/api/vouchers/source-fields")
def get_voucher_source_fields(source_type: str = Query("bills")):
    actual_source = (source_type or "").strip().lower() or "bills"
    return {"source_type": actual_source, "fields": _build_source_field_options(actual_source)}


@router.get("/api/vouchers/source-modules")
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

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻锝夊箣閿濆憛鎾绘煕婵犲倹鍋ラ柡灞诲姂瀵潙螖閳ь剚绂嶉幆褜娓婚柕鍫濈凹缁ㄥ鏌涢悢鍛婄稇闁伙絿鏌夐妵鎰板箳濠靛洦娅旈梻浣告啞娓氭宕归悧鍫熷弿婵炲樊浜濋埛鎺懨归敐鍕劅婵炲吋甯￠弻娑㈠即閻愬吀绮甸梺浼欑到婢у海妲愰幘瀛樺闁圭粯甯婃竟鏇炩攽閻橆喖鐏辨繛澶嬬⊕瀵板嫰宕堕鈧壕濠氭煙閹规劕鍚圭€规挷绶氶弻娑⑩€﹂幋婵囩亪婵犳鍠栨鎼佲€旈崘顔嘉ч煫鍥ㄦ尵濡诧綁姊洪幖鐐插婵炲鐩、姘舵晲閸℃鍤ら梺鍝勵槹閸わ箓濡搁埡鍌滃弳闂佸搫鍊搁悘婵嬪煕閺冣偓閵囧嫰寮埀顒€煤閻斿娼栨繛宸簼閸嬶繝姊洪銊х暠婵炲牆鐖煎铏圭矙濞嗘儳鍓抽梺鍝ュУ閻楃娀鎮伴鐣岀瘈闁搞儜鍐崺闂佽瀛╃粙鎺椻€﹂崶顒€鍌ㄩ柟鎯у绾句粙鏌涚仦鍓ф噮闁告柨绉归幏鎴︽焼瀹ュ棛鍘介梺瑙勫劤瀹曨剟宕濋敃鍌涚厸閻忕偟鏅倴闂佺懓寮堕幐鍐茬暦閻旂⒈鏁冮柕鍫濆缁€澶愭⒒閸屾瑦绁版い鏇熺墵瀹曚即骞掑Δ鈧悿鐐節婵犲倸鏋ら柣鎺嶇矙閻擃偊宕堕妸銈囩箒闂佽桨绀侀崯鎾蓟閺囷紕鐤€闁哄洨鍊姀銈嗙厾闁割煈鍋勬慨宥嗘叏婵犲偆鐓肩€规洘甯掗埢搴ㄥ箛椤斿搫浠掗梺璇叉唉椤煤濠婂牆鏋侀悹鍥皺閺嗭箑霉閸忓吋缍戠紒鐘崇洴閺屸剝寰勬惔銏€婂銈嗘煥椤﹂潧顫忓ú顏勭闁绘劖褰冩慨鍫曟⒑閸涘﹥灏伴柣鐔叉櫊楠炲繘骞嬮敂钘変簻闂佺绻楅崑鎰板矗閸℃稒鈷戠紓浣股戦悡銉╂煙绾板崬浜滈柣鈽嗗幘缁辨挻鎷呴崫鍕闂佺瀛╂繛濠傜暦閵壯€鍋撻敐搴℃灈缂佺姷鍋ら弻鏇熺箾閸喖濮跺┑鐐殿儠閸旀垿寮诲☉銏犵労闁告劗鍋撻悾鍓佺磽娴ｅ搫校婵＄偠妫勯～蹇涙惞鐟欏嫬鐝伴梺鐐藉劥濞呮洟鎮樺澶嬧拺闁煎鍊曢弳閬嶆煛閸涱垰鈻堥柣娑卞櫍瀹曟﹢濡歌濞堟繈姊绘笟鍥у伎濠碘€虫喘瀹曪綁宕卞☉娆屾嫼闂佸憡绋戦敃銉﹀緞閸曨垱鐓曢柟鎯ь嚟濞插鈧娲橀悷鈺呭垂妤ｅ啫绠涘ù锝囨嚀閻︽粓姊绘笟鈧褔鎮ч崱娆愬床闁规媽鍩囬埀顒€鎳橀、妤呭礋椤掑倸骞愬┑鐐舵彧缁插潡鎮洪弮鍫濆惞婵炲棙鍔戞禍婊堟煛閸ユ湹绨介柟顔笺偢閺岀喐顦版惔鈾€鏋呴梺鐟扮－婵炩偓妞ゃ垺顨嗛幏鍛村捶椤撶喐顔曢梻鍌氬€搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼槷闂佹寧姊婚弲顐︺€呴弻銉︾厽闁归偊鍓氶幆鍫㈢磼閻欐瑥娲﹂悡娆撴⒒閸屾凹鍤熼柛鏂跨Ч閺屽秹鏌ㄧ€ｎ亞浼屽┑顔硷工椤嘲鐣锋總鍛婂亜鐎瑰嫮澧楅悘鍡欑磽閸屾瑧鍔嶉柛鐐跺吹閹广垹鈹戠€ｎ亣鎽曞┑鐐村灦閿曗晠鎮￠妷鈺傜厓鐟滄粓宕滈悢鐓庣畺闁绘劗鍎ら弲婵喢归敐鍡楃祷闁诡喕绶氬濠氬磼濮橆兘鍋撻幖浣哥９闁归棿绀佺壕鐟邦渻鐎ｎ亝鎹ｉ柣顓炴閵嗘帒顫濋敐鍛婵°倗濮烽崑鐐烘偋閻樻眹鈧線寮村杈┬㈤梻浣规偠閸庢椽宕滈敃鍌氭瀬闁告劦鍠楅悡銉╂煛閸ヮ煈娈斿ù婊堢畺濮婂搫效閸パ€鍋撳Δ鍛；闁规崘鍩栧畷鍙夌節闂堟稒宸濈紒鈾€鍋撻梻浣侯焾閺堫剛鍒掑畝鍕┾偓鍌炴惞閸︻厾锛濇繛杈剧稻瑜板啯绂嶉悙顒傜瘈闁靛骏绲剧涵鐐亜閹存繃顥㈢€规洩缍€缁犳盯寮崜褏鐣鹃梻浣虹帛閸旀瑩路閸岀偛鍚规繛鍡楃贩閻熼偊鐓ラ柛顐犲灮閺嗩偄鈹戦纭锋敾婵＄偠妫勮灋闁告劑鍔夊Σ鍫熺箾閸℃〞鎴λ夐悩鐢电＝闁稿本鐟︾粊浼存煕閳哄倻澧柣锝嗙箞楠炴牗鎷呴悷棰佺钵婵＄偑鍊栧Λ鍐极椤曗偓瀹曟垿骞樼紒妯绘珳闁硅偐琛ラ崜婵嬫倶瀹ュ鈷戦柛婵嗗閸ｈ櫣绱掔拠鑼ⅵ鐎殿喛顕ч濂稿醇椤愶綆鈧洭姊绘担鍛婂暈闁圭顭烽幆鍕敍閻愯尙鐣哄銈嗘磵閸嬫挻顨ラ悙鍙夘棥妞ぱ傜窔閺屾稑顫濋澶婂壈濡炪値鍙€濞夋洟骞戦崟顒傜懝妞ゆ牗鑹炬竟瀣⒒娴ｅ憡鍟為柨鏇樺灪缁傚秶鎹勬笟顖涚稁闂佹儳绻楅～澶愬窗閸℃稒鐓曢柡鍥ュ妼娴滅偞銇勯敂鍝勫闁哄矉缍€缁犳盯骞橀崜渚囧敼闂備浇宕甸崯鍧楀疾濞戙垺鍋╃€瑰嫭澹嬮弨浠嬫煕閳锯偓閺呮盯骞冮幋鐐电瘈闁靛骏绲剧涵鐐亜閹存繃鍠樼€规洏鍨介獮鏍ㄦ媴閸忓瀚肩紓鍌欑贰閸ㄥ崬煤閺嶃劍娅犻柡灞诲劜閻撶喖鏌ｉ弮鈧娆撳礉濮樿埖鐓涢柛鈥崇箲濞呭﹥鎱ㄦ繝鍕笡闁瑰嘲鎳橀幃婊兾熼悜妯兼殮闂佽瀛╅鏍窗閺嶎厽鍋夊┑鍌氭憸瀹撲線鏌涢幇闈涙灈缁炬儳銈搁弻锝夊箛椤栨氨姣㈤梺浼欑稻濡炰粙骞冨畡閭︾叆闁告洦鍓涢崣鍡涙⒑娴兼瑧绉柡鈧潏鈺冪处濞寸姴顑呭婵嗏攽閻樻彃鈧鑺辩拠娴嬫斀闁挎稑瀚禒婊勩亜閹存繃鍣介柍褜鍓欓悘姘辨暜閳ユ剚鍤曢柟鎯版閻掓椽鏌涢幇鍏哥敖闁伙箑鐗撳鍝勑ч崶褏浼堝┑鐐板尃閸″繐褰洪梻鍌氬€烽懗鍫曞箠閹捐搴婇柡灞诲劚閻ゎ噣鏌℃径瀣仼闁哄棴绠撻弻鏇熺箾閻愵剚鐝﹂梺杞扮濞诧妇鎹㈠☉銏犵闁绘劕鐏氶崳顕€姊洪柅鐐茶嫰閸樻悂鏌涘Δ鈧崯鍧楋綖韫囨洜纾兼俊顖濐嚙椤庢挾绱撴担鍦槈妞ゆ垵鎳庨埢鎾诲箛閻楀牃鎷洪梺纭呭亹閸嬫稒淇婂ú顏呯厱婵°倐鍋撻柛鐔锋健閹箖鎮滈挊澶岀杸濡炪倖鎸炬慨鐑芥儊閸績鏀芥い鏃€鏋绘笟娑㈡煕閹惧娲存い銏＄懇瀹曞崬鈽夊▎灞惧缂傚倸鍊烽悞锕傛晝閳哄懏鍊块柣锝呯灱绾惧吋銇勯弴鐐村櫣闁诲骏濡囬埀顒冾潐濞叉粓寮繝姘卞祦閻庯綆鍠楅崐閿嬨亜韫囨挸鏆熼柣鈺侀叄濮婄粯鎷呴崨濠呯闂佺绨洪崐婵嗙暦瑜版帗鍋ㄧ紒瀣硶閸旓箑顪冮妶鍡楃瑐缂佲偓娴ｈ鍙忕€广儱妫庢禍婊堟煏韫囧﹥顫夐柤鏉跨仢閳规垿顢欓悷棰佸闂傚倷鐒︽繛濠囧极椤曗偓瀹曟垿骞樼紒妯煎幍閻庣懓瀚晶妤呭箚閸儲鐓冮悷娆忓閻忔挳鏌熼鐣屾噮闁圭懓瀚伴幖鍦喆閸曢潧娈戠紓鍌氬€搁崐椋庢閿熺姴闂い鏇楀亾鐎规洝顫夊蹇涘煛閸屾艾绨ユ繝鐢靛仦閸垶宕硅ぐ鎺撳€峰┑鐘插暔娴滄粓鏌熼崫鍕ラ柛蹇撶焸閺屾稑顫滈埀顒佺鐠轰警娼栨繛宸簻娴肩娀鏌涢弴銊ヤ簮闁稿鎹囬幐濠冨緞閸℃浜欓梻浣告惈濞层劑宕伴幇顑芥瀺闊洦绋掗埛鎴︽偣閸パ冪骇闁哥偛顦伴妵鍕敃閵忊晜笑闁绘挶鍊濋弻鈥愁吋鎼粹€崇缂備胶濮鹃～澶愬Φ閸曨垰绠涢柛鎾茶兌閺嗙娀姊虹紒妯诲皑闁逞屽墯閸撴岸鎮㈤崱娑欑厾闁归棿鐒﹀☉褍鈹戦鍏煎闁靛洤瀚伴、鏇㈠閳ヨ櫕鐣婚梻浣告惈閻寰婇崐鐔轰航婵犵數鍋犵亸顏堫敋瑜斿鏌ュ础閻愨晜鏂€闂傚嫬娲ㄦ禍绋库枎閹惧磭顦у┑鈽嗗灟鐠€锕€顭囬弽顐ょ＝濞达綀顕栭悞浠嬫煟閻旈绉洪柡灞界Х椤т線鏌涢幘瀵告噰闁炽儻绠撳畷鍫曨敂瀹ュ棌鏋岄梻鍌欒兌閸嬨劑宕曢懡銈囦笉闁圭偓鐪归埀顑跨椤粓鍩€椤掍焦鍙忛柍褜鍓熼弻宥夊Ψ閵婏妇褰ч梺浼欑到閵堟悂寮婚敐澶嬪亜闁告縿鍎抽悡鍌涚節閳封偓閸曞灚鐣奸梺杞扮贰閸ｏ綁鐛幒妤€妫樻繛鍡欏亾椤モ€斥攽閻樺灚鏆╁┑顔炬暩閸犲﹤顓兼径瀣簵濠电偛妫欓幐濠氭偂濞嗘劑浜滈柡宓嫷妫為梺鐟板暱缁绘﹢鐛径鎰闁兼祴鏅濋惁鍫濃攽閻愯尙澧曢柣蹇旂箞閹﹢鏁撻悩宕囧幍闂佸憡绋戦敃銈夊煝閺囩噥娈介柣鎰絻閺嗘瑩鏌ｉ敐蹇曠瘈妤犵偛绉归幖褰掝敃閵堝倸浜惧┑鐘崇椤ュ﹥銇勯幇鈺佺仾濠㈣泛瀚伴弻鐔轰焊閺嶃劍鐝曠紓浣稿€圭敮锟犵嵁閸℃凹妲烽梺琛″亾濞寸姴顑嗛悡鐔兼煙闁箑鐏犻柣銊ユ惈椤儻顦抽柣鈺婂灦瀵濡搁埡浣稿祮濠碘槅鍨靛▍锝嗙閻撳寒娓婚柕鍫濋娴滄粎绱掔紒姗堣€挎鐐寸墳閵囨劙骞掗幋鐐茬ザ婵＄偑鍊栭幐鐐叏閻戣棄鍌ㄦい鏍仦閳锋垿鏌ゆ慨鎰偓鏇熺墡濠电偛鐡ㄧ划鍫㈠垝濞嗗繒鏆︽繝闈涙处閸庣喖鏌曟繝蹇曠暠濞寸娀绠栧娲川婵犲啫顦╅梺鎼炲姂娴滃爼宕哄☉銏犵闁绘鏁搁敍婊堟煟鎼搭垳绉甸柛瀣椤㈡艾顭ㄩ崼鐔哄幈闂佺粯鍔曞Ο濠偽ｅú顏呯厓鐟滄粓宕滃┑瀣剁稏濠㈣泛鈯曞ú顏呭亜濠靛倸顦遍崝鐑芥偡濠婂啰孝闁伙絿鏁诲畷鐔碱敍濞戞帗瀚奸柣鐔哥矌婢ф鏁幒鎾额洸濞寸厧鐡ㄩ悡鏇㈢叓閸ャ劍顥栭柤鎷屾硶閳ь剚顔栭崰鎾诲礉瀹ュ洨鐭夐柟鐑樻煛閸嬫捇鏁愭惔鈥茬盎闂侀€炲苯澧伴柡浣割煼瀵濡搁妷銏℃杸闂佺硶鍓濋悷銉╁吹椤掑倻纾藉ù锝夋涧婵″吋銇勯鐘插幋鐎殿喛顕ч埥澶愬閳ユ枼鍋撻柨瀣ㄤ簻闊洦鎸搁鈺傘亜椤愩垺鎼愰柍瑙勫灴椤㈡瑧娑甸悜鐣屽弽婵犵數鍋涢幏鎴犵礊娓氣偓閻涱噣骞嬮敃鈧粈瀣亜閺嶎煈鍤ら柍鍝勬噺閻撳繐顭块懜鐢碘槈妞も晩鍓欓湁婵犲﹤瀚晶顏堟煃鐟欏嫬鐏撮柟顔规櫊楠炲洦鎷呴崨濠冪彵闂傚倷绀侀幗婊勬叏閻㈠憡鍋嬮柣妯垮皺閺嗭箓鏌＄仦璇插姎闁藉啰鍠栭弻鏇熷緞濞戞氨鏆犳繛瀵稿Т閵堢顫忛搹鍦＜婵☆垵娅ｆ导鍥ㄧ節濞堝灝鏋旈柛濠冪箓椤曪綁寮婚妷銉ь唽闂佸湱鍎ょ换鍕船閻㈠憡鐓熼柣妯煎劋閵嗗啴鎮归埀顒勬晝閸屾氨锛熼悗鍏夊亾闁告洦鍓涢崢鐢告倵閻熸澘顏柛瀣躬閹繝宕楅崗鐓庡伎婵犵數濮撮崯顖炲Φ濠靛牃鍋撶憴鍕８闁告柨绉堕幑銏犫攽鐎ｎ亞锛滃┑顕嗙稻鐎笛兠洪悢濂夋綎婵炲樊浜滃婵嗏攽閻愭潙淇ù婊庝簼娣囧﹪宕奸弴鐐殿啇婵炶揪绲介幗婊堟偩濞差亝鈷戦悹鎭掑妼閺嬫柨鈹戦纰卞殶缂侇喛顕ч埥澶娾枎瀹ュ嫮鐩庨梻浣筋潐閸庢娊顢氶銏″剹闁糕剝鐟х壕濂告煏婵炲灝鍔撮柣鎾冲悑閹便劍绻濋崘鈹夸虎閻庤娲﹂崑濠傜暦閻旂⒈鏁冮柣鏃囨腹婢规洟姊洪懞銉冾亪藝椤愶箑鍑犻柡宥庡幗閻撴盯鏌涢妷锝呭姎闁诲浚浜弻娑欑節閸屾稑浠撮梺鍝勬湰缁嬫垿鍩ユ径濠庢建闁割偆鍣ラ弳顓犵磽閸屾瑦绁版い鏇嗗吘娑樷枎閹炬緞锕傛煕閺囥劌鐏犵紒鐘崇洴閺屾盯骞橀懠璺哄帯闂佹寧绋撻崰鎾舵閹惧瓨濯撮柣鐔告緲椤秴鈹戦埥鍡椾簻闁哥喐娼欓锝囨嫚濞村顫嶉梺闈涚箳婵兘宕㈤幘顔解拺閻犲洠鈧磭鈧鏌涚仦鎯у毈婵☆偅绮岄埞鎴︽偐濞堟寧娈扮紓浣介哺濞茬喎鐣烽幋锕€绠ｉ柨鏇楀亾缁炬儳缍婇弻鈥愁吋鎼粹€茬凹闂佸搫妫欑划宀勫煘閹达附鍋愰柟缁樺俯娴犳儳顪冮妶鍡楀缂侇喗鐟╁濠氬即閵忕娀鍞跺┑鐘绘涧濞层倝藝閳轰緡娓婚柕鍫濋娴滄繃绻涢懠顒€鏋涚€殿喖顭锋俊鎼佸Ψ閵忊剝鏉搁梻浣虹《濡狙囧疾濠婂嫭娅忛梻鍌氬€烽懗鍓佸垝椤栫偛绠伴悹鍥梿濞差亝鍋勯柣鎾虫唉閹芥洟姊虹捄銊ユ灁濠殿喗鎸抽幃娆愮節閸ャ劎鍘繝銏ｆ硾閻楀棝宕濆顓滀簻妞ゆ巻鍋撻柣妤€锕﹂幑銏犫槈濮橈絽浜炬繛鎴炵懐閻掍粙鏌ｉ鐑嗗剳缂佽鲸甯￠、娆撴嚍閵夈儳锛撴俊銈囧Х閸嬫盯宕婊呯焿闁圭儤鏌￠崑鎾绘晲閸涱垯绮甸梺鍝勬媼閸撴瑩鍩為幋锕€鐓￠柛鈩冾殘娴犫晠姊洪崷顓涙嫛闁稿鎳橀獮鍫ュΩ閳哄倹娅囬梺绋挎湰缁嬪牓骞忓ú顏呪拺闁告稑锕︾粻鎾绘倵濮樼厧娅嶉柟顔惧仧閹瑰嫰濡歌閿涙粎绱撻崒娆戝妽妞ゎ厼娲ㄧ划濠氭倷绾版ê浜鹃悷娆忓缁€鍐╀繆閻愭壆鐭欑€规洘妞介崺鈧い鎺嶉檷娴滄粓鏌熼崫鍕ф俊鎯у槻闇夋繝濠傚閻帡鏌″畝鈧崰鏍箖濠婂吘鐔兼惞闁稒妯婂┑锛勫亼閸婃洜鎹㈤崱娑樼柧婵犻潧鐗婇～鏇㈡煙閻戞﹩娈曢柛瀣姉閳ь剝顫夊ú鏍洪妶鍜佸殨闁靛ň鏅滈埛鎴︽煕濠靛棗顏柣蹇涗憾閺屾盯鎮╁畷鍥р吂濡炪倖娲╃紞浣哥暦濠婂嫭濯撮柣鐔稿閿涘繘姊绘笟鈧褔鈥﹂崼銉ョ９婵°倓闄嶆禒姘舵煙閹澘袚闁抽攱甯掗湁闁挎繂鎳忛崯鐐烘煙椤栨氨澧﹂柡灞剧⊕缁绘繈宕熼浣圭槗闁诲氦顫夊ú姗€宕归崸妤冨祦婵☆垵鍋愮壕鍏间繆椤栫偞鏁遍悗?
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


def _validate_trigger_condition(
    trigger_condition: Optional[str],
    source_type: Optional[str],
    allowed_placeholders: Set[str],
    allowed_fields: Set[str],
    errors: List[str],
    field_path: str = "trigger_condition",
) -> None:
    if not trigger_condition:
        return

    try:
        root = json.loads(trigger_condition)
    except json.JSONDecodeError as exc:
        errors.append(f"{field_path} must be valid JSON: {exc}")
        return

    if not isinstance(root, dict):
        errors.append(f"{field_path} must be a JSON object")
        return

    normalized_source = (source_type or "").strip().lower() or "bills"
    enforce_field_check = _get_source_meta(normalized_source) is not None

    def walk(node: Any, path: str, current_source: str, current_fields: Set[str]) -> None:
        if not isinstance(node, dict):
            errors.append(f"{path} must be a JSON object")
            return

        node_type = node.get("type", "group")
        if node_type == "group":
            logic = str(node.get("logic", "AND")).upper()
            if logic not in {"AND", "OR"}:
                errors.append(f"{path}.logic must be AND or OR")
            children = node.get("children", [])
            if not isinstance(children, list):
                errors.append(f"{path}.children must be an array")
                return
            for idx, child in enumerate(children):
                walk(child, f"{path}.children[{idx}]", current_source, current_fields)
            return

        if node_type == "rule":
            field_name = str(node.get("field", "")).strip()
            if not field_name:
                errors.append(f"{path}.field is required")
            elif enforce_field_check and field_name not in current_fields:
                errors.append(f"{path}.field is not a supported field for source_type={current_source}: {field_name}")

            raw_operator = node.get("operator", "==")
            operator = _canonicalize_trigger_operator(raw_operator)
            if operator is None:
                errors.append(f"{path}.operator is not supported: {raw_operator}")

            _validate_unknown_placeholders(node.get("value", ""), f"{path}.value", allowed_placeholders, errors)
            _validate_unknown_functions(node.get("value", ""), f"{path}.value", errors)
            return

        if node_type == "relation":
            resolver = str(node.get("resolver", "")).strip()
            target_source = str(node.get("target_source", "")).strip().lower()
            quantifier = str(node.get("quantifier", "EXISTS")).upper()
            relation_meta = RELATION_REGISTRY.get(resolver)

            if quantifier not in {"EXISTS", "NOT_EXISTS"}:
                errors.append(f"{path}.quantifier must be EXISTS or NOT_EXISTS")

            if not relation_meta:
                errors.append(f"{path}.resolver is not supported: {resolver or '<empty>'}")
                expected_target = target_source
            else:
                expected_target = relation_meta.target_source
                if current_source != relation_meta.root_source:
                    errors.append(
                        f"{path}.resolver {resolver} is not supported under source_type={current_source}"
                    )

            if not target_source:
                errors.append(f"{path}.target_source is required")
            elif expected_target and target_source != expected_target:
                errors.append(f"{path}.target_source must be {expected_target} for resolver={resolver}")

            relation_group = _normalize_relation_group(node)
            if relation_group["logic"] not in {"AND", "OR"}:
                errors.append(f"{path}.logic must be AND or OR")
            if not isinstance(relation_group["children"], list):
                errors.append(f"{path}.children must be an array")
                return

            relation_fields = _build_allowed_source_fields_for_type(expected_target or target_source)
            for idx, child in enumerate(relation_group["children"]):
                walk(child, f"{path}.children[{idx}]", expected_target or target_source or current_source, relation_fields)
            return

        errors.append(f"{path}.type must be group, rule or relation")

    walk(root, field_path, normalized_source, allowed_fields)


def _validate_voucher_template_payload(payload: Dict[str, Any], db: Session) -> None:
    errors: List[str] = []
    category_id = payload.get("category_id")
    if category_id is not None:
        try:
            cat_id = int(category_id)
        except (TypeError, ValueError):
            errors.append("category_id must be an integer")
        else:
            exists = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == cat_id).first()
            if not exists:
                errors.append("category_id not found")
    source_type = payload.get("source_type")
    source_module = payload.get("source_module")
    allowed_placeholders = _build_allowed_placeholders(source_type, source_module, db)
    normalized_source = (source_type or "").strip().lower()
    allowed_source_fields = _build_allowed_source_fields_for_type(normalized_source or "bills")

    _validate_unknown_placeholders(payload.get("book_number_expr"), "book_number_expr", allowed_placeholders, errors)
    _validate_unknown_placeholders(payload.get("vouchertype_number_expr"), "vouchertype_number_expr", allowed_placeholders, errors)
    _validate_unknown_placeholders(payload.get("attachment_expr"), "attachment_expr", allowed_placeholders, errors)
    _validate_unknown_placeholders(payload.get("bizdate_expr"), "bizdate_expr", allowed_placeholders, errors)
    _validate_unknown_placeholders(payload.get("bookeddate_expr"), "bookeddate_expr", allowed_placeholders, errors)
    _validate_unknown_functions(payload.get("book_number_expr"), "book_number_expr", errors)
    _validate_unknown_functions(payload.get("vouchertype_number_expr"), "vouchertype_number_expr", errors)
    _validate_unknown_functions(payload.get("attachment_expr"), "attachment_expr", errors)
    _validate_unknown_functions(payload.get("bizdate_expr"), "bizdate_expr", errors)
    _validate_unknown_functions(payload.get("bookeddate_expr"), "bookeddate_expr", errors)

    _validate_trigger_condition(
        payload.get("trigger_condition"),
        source_type,
        allowed_placeholders,
        allowed_source_fields,
        errors,
    )

    rules = payload.get("rules")
    if not isinstance(rules, list):
        errors.append("rules must be an array")
        rules = []

    line_nos_seen: Set[int] = set()
    required_dims_cache: Dict[str, Set[str]] = {}

    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict):
            errors.append(f"rules[{idx}] must be an object")
            continue

        line_no = rule.get("line_no")
        if not isinstance(line_no, int) or line_no < 1:
            errors.append(f"rules[{idx}].line_no must be an integer >= 1")
            line_no = idx + 1
        if line_no in line_nos_seen:
            errors.append(f"rules[{idx}].line_no duplicates line number {line_no}")
        line_nos_seen.add(line_no)

        dr_cr = rule.get("dr_cr")
        if dr_cr not in {"D", "C"}:
            errors.append(f"rules[{idx}].dr_cr must be D or C")

        _validate_unknown_placeholders(rule.get("amount_expr"), f"rules[{idx}].amount_expr", allowed_placeholders, errors)
        _validate_unknown_placeholders(rule.get("summary_expr"), f"rules[{idx}].summary_expr", allowed_placeholders, errors)
        _validate_unknown_placeholders(rule.get("currency_expr"), f"rules[{idx}].currency_expr", allowed_placeholders, errors)
        _validate_unknown_placeholders(rule.get("localrate_expr"), f"rules[{idx}].localrate_expr", allowed_placeholders, errors)
        _validate_unknown_functions(rule.get("amount_expr"), f"rules[{idx}].amount_expr", errors)
        _validate_unknown_functions(rule.get("summary_expr"), f"rules[{idx}].summary_expr", errors)
        _validate_unknown_functions(rule.get("currency_expr"), f"rules[{idx}].currency_expr", errors)
        _validate_unknown_functions(rule.get("localrate_expr"), f"rules[{idx}].localrate_expr", errors)

        _validate_trigger_condition(
            rule.get("display_condition_expr"),
            source_type,
            allowed_placeholders,
            allowed_source_fields,
            errors,
            f"rules[{idx}].display_condition_expr",
        )

        aux_mapping = _validate_dimension_mapping_json(
            rule.get("aux_items"),
            f"rules[{idx}].aux_items",
            allowed_placeholders,
            errors,
        )
        _validate_dimension_mapping_json(
            rule.get("main_cf_assgrp"),
            f"rules[{idx}].main_cf_assgrp",
            allowed_placeholders,
            errors,
        )

        account_code = _normalize_literal_account_code(rule.get("account_code"))
        if not account_code:
            errors.append(f"rules[{idx}].account_code must be a static leaf account code; formulas are not allowed")
            continue

        if account_code not in required_dims_cache:
            subject = (
                db.query(models.AccountingSubject)
                .filter(models.AccountingSubject.number == account_code)
                .first()
            )
            required_dims_cache[account_code] = subject

        subject = required_dims_cache.get(account_code)
        if not subject:
            errors.append(f"rules[{idx}].account_code references a non-existent account: {account_code}")
            continue
        if not getattr(subject, "is_leaf", False):
            errors.append(f"rules[{idx}].account_code must be a leaf account: {account_code}")
            continue

        required_dims = _extract_required_check_dimensions(subject)
        if not required_dims:
            continue

        if not aux_mapping:
            errors.append(
                f"rules[{idx}].aux_items is required for account {account_code}; "
                f"missing dimensions: {', '.join(sorted(required_dims))}"
            )
            continue

        missing_dims = sorted(required_dims - set(aux_mapping.keys()))
        if missing_dims:
            errors.append(
                f"rules[{idx}].aux_items missing required dimensions for account {account_code}: "
                f"{', '.join(missing_dims)}"
            )

    if errors:
        raise HTTPException(
            status_code=400,
            detail={"message": "Voucher template validation failed", "errors": errors},
        )


def _normalize_rule_for_response(rule: models.VoucherEntryRule, fallback_line_no: int) -> None:
    dr_raw = (rule.dr_cr or "").strip().upper()
    dr_map = {
        "D": "D",
        "C": "C",
        "DEBIT": "D",
        "CREDIT": "C",
        "闂?": "D",
        "闂?": "C",
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


def _normalize_template_for_response(template: models.VoucherTemplate) -> None:
    if template.priority is not None:
        try:
            template.priority = max(int(template.priority), 0)
        except (TypeError, ValueError):
            template.priority = 100
    if template.rules:
        for idx, rule in enumerate(template.rules, start=1):
            _normalize_rule_for_response(rule, idx)

@router.get("/api/vouchers/templates", response_model=List[schemas.VoucherTemplateResponse])
def get_voucher_templates(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get all voucher templates"""
    _require_api_permission(db, current_user, "voucher_template.manage")
    categories = db.query(models.VoucherTemplateCategory).all()
    category_path_map = build_template_category_path_map(categories)
    templates = db.query(models.VoucherTemplate).order_by(
        models.VoucherTemplate.priority.asc(),
        models.VoucherTemplate.template_id.asc()
    ).all()
    for template in templates:
        template.category_path = category_path_map.get(getattr(template, "category_id", None))
        _normalize_template_for_response(template)
    return templates

@router.get("/api/vouchers/templates/{template_id}", response_model=schemas.VoucherTemplateResponse)
def get_voucher_template(
    template_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get a specific voucher template"""
    _require_api_permission(db, current_user, "voucher_template.manage")
    template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template_id).first()
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    if getattr(template, "category_id", None) is not None:
        categories = db.query(models.VoucherTemplateCategory).all()
        category_path_map = build_template_category_path_map(categories)
        template.category_path = category_path_map.get(getattr(template, "category_id", None))
    _normalize_template_for_response(template)
    return template

@router.post("/api/vouchers/templates", response_model=schemas.VoucherTemplateResponse)
def create_voucher_template(
    template: schemas.VoucherTemplateCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Create a new voucher template with rules"""
    _require_api_permission(db, current_user, "voucher_template.manage")
    existing = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template.template_id).first()
    if existing:
        raise HTTPException(status_code=400, detail="Template ID already exists")
    
    template_data = template.dict()
    rules_data = template_data.pop('rules', [])
    _validate_voucher_template_payload({**template_data, "rules": rules_data}, db)
    
    new_template = models.VoucherTemplate(**template_data)
    db.add(new_template)
    
    for rule in rules_data:
        new_rule = models.VoucherEntryRule(**rule, template_id=new_template.template_id)
        db.add(new_rule)
        
    db.commit()
    db.refresh(new_template)
    return new_template

@router.put("/api/vouchers/templates/{template_id}", response_model=schemas.VoucherTemplateResponse)
def update_voucher_template(
    template_id: str,
    template: schemas.VoucherTemplateUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Update a voucher template and its rules"""
    _require_api_permission(db, current_user, "voucher_template.manage")
    db_template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template_id).first()
    if not db_template:
        raise HTTPException(status_code=404, detail="Template not found")
        
    update_data = template.dict(exclude_unset=True)
    rules_data = update_data.pop('rules', None)

    full_payload = {
        "source_module": update_data.get("source_module", getattr(db_template, "source_module", None)),
        "source_type": update_data.get("source_type", db_template.source_type),
        "trigger_condition": update_data.get("trigger_condition", db_template.trigger_condition),
        "category_id": update_data.get("category_id", getattr(db_template, "category_id", None)),
        "book_number_expr": update_data.get("book_number_expr", db_template.book_number_expr),
        "vouchertype_number_expr": update_data.get("vouchertype_number_expr", db_template.vouchertype_number_expr),
        "attachment_expr": update_data.get("attachment_expr", db_template.attachment_expr),
        "bizdate_expr": update_data.get("bizdate_expr", db_template.bizdate_expr),
        "bookeddate_expr": update_data.get("bookeddate_expr", db_template.bookeddate_expr),
        "rules": rules_data if rules_data is not None else [_serialize_rule(rule) for rule in db_template.rules],
    }
    _validate_voucher_template_payload(full_payload, db)
    
    for key, value in update_data.items():
        setattr(db_template, key, value)
    
    if rules_data is not None:
        # Simplest way: delete old rules and add new ones
        db.query(models.VoucherEntryRule).filter(models.VoucherEntryRule.template_id == template_id).delete()
        for rule in rules_data:
            new_rule = models.VoucherEntryRule(**rule, template_id=template_id)
            db.add(new_rule)
        
    db.commit()
    db.refresh(db_template)
    return db_template

@router.delete("/api/vouchers/templates/{template_id}")
def delete_voucher_template(
    template_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Delete a voucher template"""
    _require_api_permission(db, current_user, "voucher_template.manage")
    db_template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template_id).first()
    if not db_template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    # Note: Entry rules will be deleted because of relationship if cascade is set, 
    # but let's be explicit if not sure.
    db.query(models.VoucherEntryRule).filter(models.VoucherEntryRule.template_id == template_id).delete()
    db.delete(db_template)
    db.commit()
    return {"message": "Template deleted"}


@router.post("/api/vouchers/resolve-fields")
def resolve_voucher_fields(
    payload: dict,
    db: Session = Depends(get_db)
):
    """
    闂傚倸鍊搁崐鎼佸磹閹间礁纾圭€瑰嫭鍣磋ぐ鎺戠倞鐟滃繘寮抽敃鍌涚厽闁靛繈鍩勯悞鍓х磼閹邦収娈滈柡宀€鍠栭獮宥夘敊绾拌鲸姣夐梻浣侯焾椤戞垹鎹㈠┑鍡╁殨闁割偅娲栭柋鍥ㄦ叏濮楀棗骞楅柣婵囨⒒缁辨挻鎷呴悾灞界墯闂佺锕ュú婵嬫倶閸愵喗鈷戦柟绋挎捣缁犳捇鏌熼崘鏌ュ弰鐎殿喗濞婇幃娆撴倻濡厧骞堝┑鐘垫暩婵潙煤閿曗偓閳藉顦规俊顐＄劍瀵板嫰骞囬鐘插箺婵犵妲呴崹鎶藉磿閵堝鐓濋柡鍥ュ灪閻撴洟骞栧ǎ顒€鐏╅柣蹇婃櫊閺屽秷顧侀柛鎾寸箞閿濈偞寰勯幇顒傤唶婵＄偛顑呯€涒晠顢曢懞銉﹀弿婵☆垱瀵х涵楣冩煢閸愵亜鏋涢柡宀嬬秮瀵噣宕掑Δ浣哥彵闂備浇妗ㄧ粈浣虹矓閻㈢绠為柕濞垮労濞笺劑鏌涢埄鍐炬當妞ゎ偀鏅濈槐鎾存媴閻熸壆绁烽梺鍛婅壘椤戝洭骞戦姀鐘斀閻庯綆浜跺濠囨⒑閹稿海绠撻柟铏姍瀹曘垽顢曢敂瑙ｆ嫼闂佸憡绋戦敃銉╁煕閹邦厾绠鹃柛娆忣槺閻矂鏌涢幒鎾虫诞鐎规洖銈告俊鐑藉Ψ瑜濈槐鏌ユ⒒娴ｈ櫣甯涢柛鏃€娲熼、姘额敇閵忕姴鍋嶅銈呯箰閹虫劗寮ч埀顒勬⒑濮瑰洤鐏叉繛浣冲嫮顩锋繝濠傜墛閻撶喖鐓崶銊︹拻缂佺姵鐗曡彁闁搞儜宥堝惈閻庤娲橀〃鍫㈠垝椤撶喐濮滈柟娈垮枤濞夊潡姊婚崒娆戝妽閻庣瑳鍏炬稒绗熼埀顒勬晲閻愭潙绶炲┑鐐靛亾閻庣儤绻濋悽闈浶㈡繛璇х畵閹€斥槈閵忊€斥偓鍫曟煟閹邦厼绲婚柍閿嬫閺屽秹鎸婃径妯烩枅闂佺妫勯悥鐓庮嚕娴犲鈧牠鍨惧畷鍥┬ㄩ梺杞扮劍閸旀瑥鐣烽妸鈺婃晩閻犱警鐓堥弨銊モ攽閿涘嫬浜奸柛濞у懏瀚婚柣鏃傚帶缁€澶愭煙鐎涙绠ユ繛鍏肩墬缁绘稑顔忛鑽ょ泿婵炵鍋愭繛鈧柡灞剧洴瀵挳濡搁妷銉㈡嫽婵犵妲呴崑渚€宕愬┑鍡╂綎婵炲樊浜滄导鐘绘煏婢跺牆鍓鹃柨婵嗩槹閻撶喐淇婇妶鍛殭闁宠鐗撻弻鐔碱敊鐠囨彃绁銈冨灪瀹€鎼佸春閳ь剚銇勯幒鎴濐伀鐎规挷绶氶幃妤呮晲鎼粹剝鐏嶉梺鍝勬噺缁海妲愰幘瀛樺闁兼祴鍓濋崹鐢稿煝瀹ュ绠荤紓浣骨氶幏缁樼箾鏉堝墽鍒伴柟璇х節楠炲棝宕奸妷锕€鈧灚鎱ㄥΟ鐓庡付濠⒀傚嵆閺岀喖鐛崹顔句紙濡ょ姷鍋涘ú顓㈢嵁瀹ュ鏁婇柤娴嬫杺閸嬪﹪姊婚崒娆戠獢闁逞屽墰閸嬫盯鎳熼娑欐珷閻庢稒蓱閸欏繐鈹戦悩鎻掝伀閻㈩垱鐩弻鐔风暋閻楀牆娈楅悗瑙勬礈閸忔﹢銆佸鈧幃銏㈡嫚閹绘帒绁梻鍌氬€搁崐椋庣矆娓氣偓楠炲鏁撻悩鑼舵憰闂侀潧艌閺呮粓宕曢弬搴撴斀闁稿本绮犻悞楣冩煟閹烘垹浠涢柕鍥у楠炴帒顓奸崼婵嗗腐闂備胶绮幖鈺呭垂閸洖钃熸繛鎴烆焸閺冨牆鐒垫い鎺戝閻ゎ噣鏌涜椤ㄥ棝宕戦埡鍛厽闁硅揪绲借闂佽鍨抽崑鐐哄Φ閸曨垰绠涙い鏂裤仚閵忕妴鐟邦煥閸曨厾鐤勫┑顔硷攻濡炶棄鐣烽妸锔剧瘈闁稿本鍑瑰濠囨⒒娴ｅ憡鎯堥柟鍐茬箳閸掓帡骞樺畷鍥ㄦ婵炴潙鍚嬪娆戠不閾忣偂绻嗛柕鍫濆椤斿鏌熼柨瀣仢闁哄矉缍侀幃鈺呭矗婢跺矈妲洪梻渚€鈧偛鑻晶顔剧棯缂併垹骞楃紒鍌涘浮楠炴牗鎷呴崗澶嬪缂傚倷绀侀鍡涱敄濞嗘挸纾块煫鍥ㄧ⊕閻撱儵鏌￠崶銉ュ缂併劎绮妵鍕即椤忓棛袦濡炪們鍨哄ú鐔煎极閸愵喖鐒垫い鎺戝閸婂潡鏌涢…鎴濅簴濞存粍绮撻弻鐔兼倻濡櫣浠村銈呮禋娴滎亪寮诲澶嬬叆閻庯綆浜炴禒鎼佹⒑闁稓鈹掗柛鏃€鍨块獮鍐倻閼恒儱浜遍梺鍓插亞閸犳劙宕愰悜鑺モ拻濞达絿鐡旈崵娆愮箾鐎涙ê鍝虹€规洘濞婇弫鎰緞婵犲懏鎲版繝鐢靛仦閸垶宕瑰ú顏勭；闁规壆澧楅悡娑㈡倶閻愰鍤欓柛鏃€姘ㄩ埀顒侇問閸犳洟宕￠崘宸綎闁惧繗顫夌€氭岸鏌嶉妷銊︾彧闁绘繃绻勭槐鎾诲磼濮橆兘鍋撻悷鎵殕婵繂鏈ˉ銈夋⒒娴ｅ憡鎯堥悶姘煎亰瀹曟洟寮婚妷锕€浜楅梺鍛婂姦娴滅偟澹曢挊澹濆綊鏁愰崼婵呯暗闂佺懓鐡ㄧ换鍕汲閿旈敮鍋撻崗澶婁壕闂佸憡娲﹂崜娑㈠储闂堟侗娓婚柕鍫濇閳锋帡鏌￠崪浣镐喊妤犵偛锕畷鍫曞煘閹傚闁荤喐鐟ョ€氼厾绮堥崘鈹夸簻闁靛鍎查ˉ婊勩亜閺囨ê鍔︾€规洜鍠栭、妤呭磼濠婂懏顫屾繝鐢靛仩閹活亞寰婇懞銉х彾濠电姴浼?

    闂傚倸鍊搁崐鎼佸磹閹间礁纾瑰瀣捣閻棗銆掑锝呬壕濡ょ姷鍋為悧鐘汇€侀弴姘辩Т闂佹悶鍎洪崜锕傚极瀹ュ鐓熼柟閭﹀幗缂嶆垵霉濠婂棝鍝虹紒缁樼箞閹粙妫冨ù韬插灪缁绘稓浠﹂崒姘ｅ亾濡ゅ啫鍨濈紓浣姑閬嶆倵濞戞瑯鐒介柛妯兼暬濮婂宕掑顑藉亾閹间緡鏁嬫い鎾嚍閸ヮ剚鏅濋柛灞剧〒閸樿棄顪冮妶鍡樺暗闁革絻鍎遍埢鎾愁煥閸喓鍘搁柣蹇曞仜婢ц棄煤閹绢喗鐓曢柍杞扮椤忣厾鈧娲橀敃銏ゃ€侀弮鍫濆窛妞ゆ挾鍠撹ぐ顖炴⒒閸屾艾鈧兘鎮為敃鈧—鍐锤濡も偓閸屻劑鏌ｉ幘鍐差唫婵炴垶菤閺€浠嬫煕閳锯偓閺呮稑鈻撻妸鈺傗拺闁告挻褰冩禍婵囩箾閸欏鑰跨€规洘鍔欓幃婊堟嚍閵壯冨箰濠电姰鍨煎▔娑㈩敄閸涘瓨鍊块柣鎰靛厸缁诲棙銇勯幇鈺佺仾妞ゃ儳鍋熺槐?
    { "bill_data": { "house_id": "123", "park_id": "456" } }

    闂傚倸鍊搁崐鎼佸磹閹间礁纾瑰瀣捣閻棗銆掑锝呬壕濡ょ姷鍋為悧鐘汇€侀弴姘辩Т闂佹悶鍎洪崜锕傚极閸愵喗鐓ラ柡鍥殔娴滈箖姊哄Ч鍥р偓妤呭磻閹捐埖宕叉繝闈涙川缁♀偓闂佺鏈划宀勩€傚ú顏呪拺闁芥ê顦弳鐔兼煕閻樺磭澧电€殿喖顭峰鎾偄閾忚鍟庨梻浣稿閻撳牓宕伴弽銊х彾闁告洦鍋€閺€浠嬫煟閹邦剙绾ч柍缁樻礀闇夋繝濠傚閻帞鈧娲橀敃銏ゅ春閳ь剚銇勯幒鍡椾壕濡炪値浜滈崯瀛樹繆閸洖骞㈡俊銈傚亾妞ゎ偄娲铏圭矙閸栤剝鏁惧銈冨妼閿曨亪鐛崘顔藉€烽悗鍨倐濡绢噣姊洪崨濠勨槈闁挎洩绠撳畷銏ゅ川婵犲嫮鐦堥梺闈涢獜缁插墽娑垫ィ鍐╃厾婵炶尪顕ч悘锛勭磼閸屾稑娴柡浣稿暣閸┾偓妞ゆ帒瀚ч埀顒婄畵瀹曠螖娴ｉ晲鐢婚梻浣虹帛椤ㄥ懘鎮у鍏炬盯鏁撻悩宕囧幗?
    { "enriched_data": { "kd_house_number": "H001" } }
    """
    bill_data = payload.get("bill_data", {})
    enriched = mapping_enrich_source_data("bills", bill_data, db=db)
    
    return {"enriched_data": enriched}


def _build_preview_user_context(
    current_user: models.User,
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
) -> Dict[str, str]:
    from urllib.parse import unquote

    org_name = current_user.organization.name if current_user.organization else "闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾剧懓顪冪€ｎ亝鎹ｉ柣顓炴閵嗘帒顫濋敐鍛婵°倗濮烽崑鐐烘偋閻樻眹鈧線寮撮姀鈩冩珕闂佽姤锚椤︻喚绱旈弴鐔虹瘈闁汇垽娼у瓭闂佹寧娲忛崐妤呭焵椤掍礁鍤柛锝忕秮婵℃挳宕ㄩ弶鎴犵厬婵犮垼娉涢惉濂告儊閸喓绡€闁汇垽娼у瓭闂佺锕︾划顖炲疾閸洖鍗抽柕蹇ョ磿閸橀亶姊洪棃娑辩劸闁稿酣浜堕崺鈧い鎺嗗亾婵炵》绻濋幃浼搭敋閳ь剙顕ｆ禒瀣р偓鏍Ψ閵夆晛寮板銈冨灪椤ㄥ﹪宕洪埀顒併亜閹哄秵顦风紒璇叉闇夐柣妯烘▕閸庢劙鏌ｉ幘璺烘灈闁哄瞼鍠撶槐鎺楀閻樺吀鍝楀┑?"
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
        "deal_type_label": RECEIPT_BILL_DEAL_TYPE_LABELS.get(receipt_bill.deal_type, "placeholder"),
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


def _preview_voucher_for_bill_via_receipt_templates(
    bill: models.Bill,
    enriched_bill: Dict[str, Any],
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
    current_user: models.User,
    db: Session,
) -> Optional[Dict[str, Any]]:
    from services.voucher_engine import evaluate_expression
    from utils.variable_parser import build_variable_map, resolve_variables

    if not bill.deal_log_id:
        return None

    receipt_bill = (
        db.query(models.ReceiptBill)
        .options(joinedload(models.ReceiptBill.users))
        .filter(
            models.ReceiptBill.id == int(bill.deal_log_id),
            models.ReceiptBill.community_id == int(bill.community_id),
        )
        .first()
    )
    if not receipt_bill:
        return None

    normalized_account_book_number = _decode_header_value(x_account_book_number) or None
    bill_ref = {"bill_id": int(bill.id), "community_id": int(bill.community_id)}
    source_status_map = _get_bill_push_status_map(
        db,
        [bill_ref],
        account_book_number=normalized_account_book_number,
    )
    source_bills = [source_status_map[(int(bill.id), int(bill.community_id))]]
    source_bill_push_summary = _summarize_bill_push_statuses(source_bills)
    push_conflicts = _find_bill_push_conflicts(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})" for item in push_conflicts[:10]]
        )
        push_block_reason = f"Current bill already has voucher push records: {conflict_preview}"

    receipt_data = _serialize_receipt_bill_model(receipt_bill, db)
    enriched_receipt = _enrich_receipt_bill_data(receipt_data, receipt_bill=receipt_bill, db=db)
    user_context = _build_preview_user_context(
        current_user,
        x_account_book_id=x_account_book_id,
        x_account_book_name=x_account_book_name,
        x_account_book_number=x_account_book_number,
    )
    runtime_vars = build_variable_map(db, user_context=user_context)
    scoped_relation_records = {"bills": [enriched_bill]}

    match_result = _match_receipt_templates(
        receipt_bill=receipt_bill,
        enriched=enriched_receipt,
        runtime_vars=runtime_vars,
        db=db,
        scoped_relation_records=scoped_relation_records,
    )
    matched_template = match_result["matched_template"]
    matched_selected_records = match_result["matched_selected_records"]
    all_debug_logs = match_result["debug_logs"]
    templates = match_result["templates"]

    if not matched_template:
        return {
            "matched": False,
            "message": "No applicable receipt-root voucher template matched",
            "matched_root_source": "receipt_bills",
            "matched_via_receipt": False,
            "receipt_summary": _build_receipt_summary_payload(receipt_bill),
            "bill_summary": _build_bill_summary_payload(bill),
            "receipt_data": enriched_receipt,
            "templates_checked": len(templates),
            "debug_logs": all_debug_logs,
            "selected_bills": source_bills,
            "selected_bill_push_summary": source_bill_push_summary,
            "source_bills": source_bills,
            "source_bill_push_summary": source_bill_push_summary,
            "push_blocked": push_blocked,
            "push_block_reason": push_block_reason,
        }

    expression_context = dict(enriched_receipt)
    for record in matched_selected_records.values():
        for key, value in (record or {}).items():
            if isinstance(key, str) and "." in key:
                expression_context[key] = value

    def resolve_expr(expr: Optional[str], ctx: Optional[Dict[str, Any]] = None) -> str:
        resolved_with_globals = resolve_variables(expr or "", db, preloaded_vars=runtime_vars)
        return evaluate_expression(resolved_with_globals, ctx or expression_context)

    now = datetime.now()
    book_number = resolve_expr(matched_template.book_number_expr or "'BU-35256'")
    vouchertype_number = resolve_expr(matched_template.vouchertype_number_expr or "'0001'")
    attachment = resolve_expr(matched_template.attachment_expr or "0")
    biz_date = resolve_expr(matched_template.bizdate_expr or "{CURRENT_DATE}") or now.strftime("%Y-%m-%d")
    booked_date = resolve_expr(matched_template.bookeddate_expr or "{CURRENT_DATE}") or biz_date
    period_number = biz_date[:7].replace("-", "") if len(biz_date) >= 7 else now.strftime("%Y%m")

    accounting_entries = []
    kingdee_entries = []
    subject_names_cache = {}
    subject_type_cache = {}
    rule_relation_base_ctx = {
        "db": db,
        "root_record": receipt_bill,
        "receipt_bill": receipt_bill,
        "cache": {},
    }
    if scoped_relation_records is not None:
        rule_relation_base_ctx["scoped_records"] = scoped_relation_records

    for rule in sorted(matched_template.rules, key=lambda r: r.line_no):
        visible, rule_selected_records = _evaluate_rule_display_condition(
            rule.display_condition_expr,
            enriched_receipt,
            runtime_vars,
            rule_relation_base_ctx,
        )
        if not visible:
            continue
        rule_expression_context = _merge_selected_record_values(expression_context, rule_selected_records)
        summary = resolve_expr(rule.summary_expr, rule_expression_context)
        account_code = _normalize_literal_account_code(rule.account_code) or (rule.account_code or "").strip()
        amount_str = resolve_expr(rule.amount_expr, rule_expression_context)
        currency = resolve_expr(rule.currency_expr or "'CNY'", rule_expression_context)
        localrate = resolve_expr(rule.localrate_expr or "1", rule_expression_context)

        account_display_name = account_code
        if account_code:
            if account_code not in subject_names_cache:
                subj = db.query(models.AccountingSubject).filter(models.AccountingSubject.number == account_code).first()
                if subj:
                    subject_names_cache[account_code] = subj.fullname or subj.name
                    subject_type_cache[account_code] = subj.account_type_number or ""
                else:
                    subject_names_cache[account_code] = account_code
                    subject_type_cache[account_code] = ""

            fullname = subject_names_cache[account_code]
            if fullname != account_code:
                account_display_name = f"{account_code} {fullname}"

        amount_val = _try_parse_decimal(amount_str) or Decimal("0")
        localrate_val = _try_parse_decimal(localrate) or Decimal("1")

        assgrp = {}
        if rule.aux_items:
            try:
                aux_obj = json.loads(rule.aux_items)
                for dim_key, dim_config in aux_obj.items():
                    assgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        assgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json.JSONDecodeError, Exception):
                pass

        maincfassgrp = {}
        if rule.main_cf_assgrp:
            try:
                mcf_obj = json.loads(rule.main_cf_assgrp)
                for dim_key, dim_config in mcf_obj.items():
                    maincfassgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        maincfassgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json.JSONDecodeError, Exception):
                pass

        accounting_entries.append({
            "line_no": rule.line_no,
            "summary": summary,
            "account_code": account_code,
            "account_name": subject_names_cache.get(account_code, ""),
            "account_type_number": subject_type_cache.get(account_code, ""),
            "account_display": account_display_name,
            "dr_cr": rule.dr_cr,
            "debit": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "credit": _json_number(amount_val) if rule.dr_cr == "C" else 0.0,
            "debit_exact": _decimal_text(amount_val) if rule.dr_cr == "D" else "0",
            "credit_exact": _decimal_text(amount_val) if rule.dr_cr == "C" else "0",
            "currency": currency,
            "localrate": _json_number(localrate_val),
            "localrate_exact": _decimal_text(localrate_val),
            "assgrp": assgrp if assgrp else None,
            "maincfassgrp": maincfassgrp if maincfassgrp else None,
        })

        kd_entry = {
            "seq": rule.line_no,
            "edescription": summary,
            "account_number": account_code,
            "currency_number": currency,
            "localrate": _decimal_text(localrate_val),
            "debitori": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "creditori": "0.00" if rule.dr_cr == "D" else _money_text(amount_val),
            "debitlocal": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "creditlocal": "0.00" if rule.dr_cr == "D" else _money_text(amount_val),
        }
        if assgrp:
            kd_entry["assgrp"] = assgrp
        if maincfassgrp:
            kd_entry["maincfassgrp"] = maincfassgrp
        kingdee_entries.append(kd_entry)

    _normalize_voucher_money_fields(accounting_entries, kingdee_entries)
    total_debit = sum((_entry_decimal(e, "debit_exact", "debit")) for e in accounting_entries)
    total_credit = sum((_entry_decimal(e, "credit_exact", "credit")) for e in accounting_entries)

    kingdee_json = {
        "data": [{
            "book_number": book_number,
            "bizdate": biz_date,
            "bookeddate": booked_date,
            "period_number": period_number,
            "vouchertype_number": vouchertype_number,
            "description": (matched_template.template_name or matched_template.template_id or "UnnamedTemplate"),
            "attachment": _parse_attachment_count(attachment),
            "entries": kingdee_entries,
        }]
    }

    return {
        "matched": True,
        "matched_root_source": "receipt_bills",
        "matched_via_receipt": True,
        "template_id": matched_template.template_id,
        "template_name": matched_template.template_name,
        "receipt_summary": _build_receipt_summary_payload(receipt_bill),
        "bill_summary": _build_bill_summary_payload(bill),
        "matched_relation_sources": sorted(matched_selected_records.keys()),
        "accounting_view": {
            "entries": accounting_entries,
            "total_debit": _json_number(total_debit),
            "total_credit": _json_number(total_credit),
            "total_debit_exact": _decimal_text(total_debit),
            "total_credit_exact": _decimal_text(total_credit),
            "is_balanced": (total_debit - total_credit) == Decimal("0") and (total_credit - total_debit) == Decimal("0"),
        },
        "kingdee_json": kingdee_json,
        "selected_bills": source_bills,
        "selected_bill_push_summary": source_bill_push_summary,
        "source_bills": source_bills,
        "source_bill_push_summary": source_bill_push_summary,
        "push_blocked": push_blocked,
        "push_block_reason": push_block_reason,
    }


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
    from services.voucher_engine import evaluate_expression
    from utils.variable_parser import build_variable_map, resolve_variables
    import json as json_mod

    if allowed_community_ids and int(community_id) not in set(allowed_community_ids):
        raise HTTPException(status_code=403, detail="Unauthorized community")

    receipt_bill = (
        db.query(models.ReceiptBill)
        .options(joinedload(models.ReceiptBill.users))
        .filter(
            models.ReceiptBill.id == int(receipt_bill_id),
            models.ReceiptBill.community_id == int(community_id),
        )
        .first()
    )
    if not receipt_bill:
        raise HTTPException(status_code=404, detail="Receipt bill not found")

    normalized_account_book_number = _decode_header_value(x_account_book_number) or None
    source_bills = _collect_receipt_source_bills(
        db,
        receipt_bill_id=int(receipt_bill.id),
        community_id=int(receipt_bill.community_id),
        account_book_number=normalized_account_book_number,
    )
    source_bill_push_summary = _summarize_bill_push_statuses(source_bills)
    push_conflicts = _find_bill_push_conflicts(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})" for item in push_conflicts[:10]]
        )
        push_block_reason = f"Related bills already have voucher push records: {conflict_preview}"

    if source_bills:
        related_bills = (
            db.query(models.Bill)
            .filter(
                models.Bill.deal_log_id == int(receipt_bill.id),
                models.Bill.community_id == int(receipt_bill.community_id),
            )
            .order_by(models.Bill.id.asc())
            .all()
        )

        previews: List[Dict[str, Any]] = []
        skipped_bills: List[Dict[str, Any]] = []

        for related_bill in related_bills:
            bill_data = {}
            for col in models.Bill.__table__.columns:
                val = getattr(related_bill, col.name, None)
                if val is not None:
                    bill_data[col.name] = val if not hasattr(val, "isoformat") else val.isoformat()
                else:
                    bill_data[col.name] = None

            from decimal import Decimal as PyDecimal
            for key, val in bill_data.items():
                if isinstance(val, PyDecimal):
                    bill_data[key] = float(val)

            enriched_bill = mapping_enrich_source_data("bills", bill_data, db=db)
            result = _preview_voucher_for_bill_via_receipt_templates(
                bill=related_bill,
                enriched_bill=enriched_bill,
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
            )

            if not result or not result.get("matched"):
                skipped_bills.append({
                    "bill_id": int(related_bill.id),
                    "community_id": int(related_bill.community_id),
                    "reason": "template not matched",
                })
                continue

            previews.append(result)

        if previews:
            first_preview = previews[0]
            first_header = ((first_preview.get("kingdee_json") or {}).get("data") or [{}])[0]
            header_keys = ["book_number", "bookeddate", "period_number", "vouchertype_number"]
            merged_bizdates = [str(first_header.get("bizdate") or "").strip()]

            header_compatible_previews: List[Dict[str, Any]] = [first_preview]
            for preview in previews[1:]:
                header = ((preview.get("kingdee_json") or {}).get("data") or [{}])[0]
                incompatible_keys = [k for k in header_keys if first_header.get(k) != header.get(k)]
                if incompatible_keys:
                    summary = preview.get("bill_summary") or {}
                    skipped_bills.append({
                        "bill_id": int(summary.get("id") or 0),
                        "community_id": int(summary.get("community_id") or 0),
                        "reason": f"inconsistent voucher header ({', '.join(incompatible_keys)}); skipped from merge",
                    })
                    continue
                merged_bizdates.append(str(header.get("bizdate") or "").strip())
                header_compatible_previews.append(preview)

            previews = header_compatible_previews
            merged_bizdate = max([d for d in merged_bizdates if d], default=str(first_header.get("bizdate") or ""))

            matched_source_bills: List[Dict[str, Any]] = []
            seen_source_keys = set()
            for preview in previews:
                for source_bill in preview.get("source_bills") or []:
                    key = (
                        int(source_bill.get("bill_id") or 0),
                        int(source_bill.get("community_id") or 0),
                    )
                    if key in seen_source_keys:
                        continue
                    seen_source_keys.add(key)
                    matched_source_bills.append(source_bill)

            merged_entries: List[Dict[str, Any]] = []
            merged_accounting_entries: List[Dict[str, Any]] = []
            seq = 1

            for preview in previews:
                kd_header = ((preview.get("kingdee_json") or {}).get("data") or [{}])[0]
                for entry in kd_header.get("entries") or []:
                    item = dict(entry)
                    item["seq"] = seq
                    merged_entries.append(item)
                    seq += 1

                acct_entries = ((preview.get("accounting_view") or {}).get("entries") or [])
                for entry in acct_entries:
                    item = dict(entry)
                    item["line_no"] = len(merged_accounting_entries) + 1
                    merged_accounting_entries.append(item)

            total_debit = sum((_entry_decimal(e, "debit_exact", "debit")) for e in merged_accounting_entries)
            total_credit = sum((_entry_decimal(e, "credit_exact", "credit")) for e in merged_accounting_entries)

            merged_template_ids = sorted({str(p.get("template_id") or "") for p in previews if p.get("template_id")})
            template_name = first_preview.get("template_name") or first_preview.get("template_id") or "ReceiptMerged"
            merged_kingdee_json = {
                "data": [{
                    "book_number": first_header.get("book_number"),
                    "bizdate": merged_bizdate,
                    "bookeddate": first_header.get("bookeddate"),
                    "period_number": first_header.get("period_number"),
                    "vouchertype_number": first_header.get("vouchertype_number"),
                    "description": template_name,
                    "attachment": first_header.get("attachment", 0),
                    "entries": merged_entries,
                }]
            }

            matched_source_bill_push_summary = _summarize_bill_push_statuses(matched_source_bills)
            matched_push_conflicts = _find_bill_push_conflicts(matched_source_bills)
            merged_push_blocked = len(matched_push_conflicts) > 0
            merged_push_block_reason = None
            if merged_push_blocked:
                conflict_preview = ", ".join(
                    [
                        f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})"
                        for item in matched_push_conflicts[:10]
                    ]
                )
                merged_push_block_reason = f"Related bills already have voucher push records: {conflict_preview}"

            return {
                "matched": True,
                "partial_matched": len(skipped_bills) > 0,
                "matched_root_source": "receipt_bills",
                "matched_via_receipt": False,
                "matched_bills": len(previews),
                "template_id": first_preview.get("template_id"),
                "template_name": first_preview.get("template_name"),
                "template_ids": merged_template_ids,
                "receipt_summary": _build_receipt_summary_payload(receipt_bill),
                "selected_bills": source_bills,
                "selected_bill_push_summary": source_bill_push_summary,
                "source_bills": matched_source_bills,
                "source_bill_push_summary": matched_source_bill_push_summary,
                "skipped_bills": skipped_bills,
                "push_blocked": merged_push_blocked,
                "push_block_reason": merged_push_block_reason,
                "accounting_view": {
                    "entries": merged_accounting_entries,
                    "total_debit": _json_number(total_debit),
                    "total_credit": _json_number(total_credit),
                    "total_debit_exact": _decimal_text(total_debit),
                    "total_credit_exact": _decimal_text(total_credit),
                    "is_balanced": (total_debit - total_credit) == Decimal("0") and (total_credit - total_debit) == Decimal("0"),
                },
                "kingdee_json": merged_kingdee_json,
            }

    receipt_data = _serialize_receipt_bill_model(receipt_bill, db)
    enriched = _enrich_receipt_bill_data(receipt_data, receipt_bill=receipt_bill, db=db)

    user_context = _build_preview_user_context(
        current_user,
        x_account_book_id=x_account_book_id,
        x_account_book_name=x_account_book_name,
        x_account_book_number=x_account_book_number,
    )
    runtime_vars = build_variable_map(db, user_context=user_context)

    match_result = _match_receipt_templates(
        receipt_bill=receipt_bill,
        enriched=enriched,
        runtime_vars=runtime_vars,
        db=db,
    )
    templates = match_result["templates"]
    matched_template = match_result["matched_template"]
    matched_selected_records = match_result["matched_selected_records"]
    all_debug_logs = match_result["debug_logs"]

    if not matched_template:
        if allow_bill_fallback and source_bills:
            return preview_voucher_for_bills(
                payload=schemas.BatchVoucherPreviewRequest(
                    bills=[schemas.BillPreviewRef(bill_id=int(item["bill_id"]), community_id=int(item["community_id"])) for item in source_bills]
                ),
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
                allowed_community_ids=allowed_community_ids,
            )

        return {
            "matched": False,
            "message": "No applicable voucher template matched",
            "matched_root_source": "receipt_bills",
            "matched_via_receipt": False,
            "receipt_summary": {
                "id": receipt_bill.id,
                "community_id": receipt_bill.community_id,
                "receipt_id": receipt_bill.receipt_id,
                "deal_type": receipt_bill.deal_type,
                "deal_type_label": RECEIPT_BILL_DEAL_TYPE_LABELS.get(receipt_bill.deal_type, "placeholder"),
                "income_amount": _json_number(receipt_bill.income_amount),
                "amount": _json_number(receipt_bill.amount),
                "asset_name": receipt_bill.asset_name,
            },
            "receipt_summary": _build_receipt_summary_payload(receipt_bill),
            "receipt_data": enriched,
            "templates_checked": len(templates),
            "debug_logs": all_debug_logs,
            "selected_bills": source_bills,
            "selected_bill_push_summary": source_bill_push_summary,
            "source_bills": source_bills,
            "source_bill_push_summary": source_bill_push_summary,
            "push_blocked": push_blocked,
            "push_block_reason": push_block_reason,
        }

    expression_context = dict(enriched)
    for record in matched_selected_records.values():
        for key, value in (record or {}).items():
            if isinstance(key, str) and "." in key:
                expression_context[key] = value

    def resolve_expr(expr: Optional[str], ctx: Optional[Dict[str, Any]] = None) -> str:
        resolved_with_globals = resolve_variables(expr or "", db, preloaded_vars=runtime_vars)
        return evaluate_expression(resolved_with_globals, ctx or expression_context)

    now = datetime.now()
    book_number = resolve_expr(matched_template.book_number_expr or "'BU-35256'")
    vouchertype_number = resolve_expr(matched_template.vouchertype_number_expr or "'0001'")
    attachment = resolve_expr(matched_template.attachment_expr or "0")
    biz_date = resolve_expr(matched_template.bizdate_expr or "{CURRENT_DATE}") or now.strftime("%Y-%m-%d")
    booked_date = resolve_expr(matched_template.bookeddate_expr or "{CURRENT_DATE}") or biz_date
    period_number = biz_date[:7].replace("-", "") if len(biz_date) >= 7 else now.strftime("%Y%m")

    accounting_entries = []
    kingdee_entries = []
    subject_names_cache = {}
    subject_type_cache = {}
    rule_relation_base_ctx = {
        "db": db,
        "root_record": receipt_bill,
        "receipt_bill": receipt_bill,
        "cache": {},
    }

    for rule in sorted(matched_template.rules, key=lambda r: r.line_no):
        visible, rule_selected_records = _evaluate_rule_display_condition(
            rule.display_condition_expr,
            enriched,
            runtime_vars,
            rule_relation_base_ctx,
        )
        if not visible:
            continue
        rule_expression_context = _merge_selected_record_values(expression_context, rule_selected_records)
        summary = resolve_expr(rule.summary_expr, rule_expression_context)
        account_code = _normalize_literal_account_code(rule.account_code) or (rule.account_code or "").strip()
        amount_str = resolve_expr(rule.amount_expr, rule_expression_context)
        currency = resolve_expr(rule.currency_expr or "'CNY'", rule_expression_context)
        localrate = resolve_expr(rule.localrate_expr or "1", rule_expression_context)

        account_display_name = account_code
        if account_code:
            if account_code not in subject_names_cache:
                subj = db.query(models.AccountingSubject).filter(models.AccountingSubject.number == account_code).first()
                if subj:
                    subject_names_cache[account_code] = subj.fullname or subj.name
                    subject_type_cache[account_code] = subj.account_type_number or ""
                else:
                    subject_names_cache[account_code] = account_code
                    subject_type_cache[account_code] = ""

            fullname = subject_names_cache[account_code]
            if fullname != account_code:
                account_display_name = f"{account_code} {fullname}"

        amount_val = _try_parse_decimal(amount_str) or Decimal("0")
        localrate_val = _try_parse_decimal(localrate) or Decimal("1")

        assgrp = {}
        if rule.aux_items:
            try:
                aux_obj = json_mod.loads(rule.aux_items)
                for dim_key, dim_config in aux_obj.items():
                    assgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        assgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json_mod.JSONDecodeError, Exception):
                pass

        maincfassgrp = {}
        if rule.main_cf_assgrp:
            try:
                mcf_obj = json_mod.loads(rule.main_cf_assgrp)
                for dim_key, dim_config in mcf_obj.items():
                    maincfassgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        maincfassgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json_mod.JSONDecodeError, Exception):
                pass

        accounting_entries.append({
            "line_no": rule.line_no,
            "summary": summary,
            "account_code": account_code,
            "account_name": subject_names_cache.get(account_code, ""),
            "account_type_number": subject_type_cache.get(account_code, ""),
            "account_display": account_display_name,
            "dr_cr": rule.dr_cr,
            "debit": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "credit": _json_number(amount_val) if rule.dr_cr == "C" else 0.0,
            "debit_exact": _decimal_text(amount_val) if rule.dr_cr == "D" else "0",
            "credit_exact": _decimal_text(amount_val) if rule.dr_cr == "C" else "0",
            "currency": currency,
            "localrate": _json_number(localrate_val),
            "localrate_exact": _decimal_text(localrate_val),
            "assgrp": assgrp if assgrp else None,
            "maincfassgrp": maincfassgrp if maincfassgrp else None,
        })

        kd_entry = {
            "seq": rule.line_no,
            "edescription": summary,
            "account_number": account_code,
            "currency_number": currency,
            "localrate": _decimal_text(localrate_val),
            "debitori": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "creditori": "0.00" if rule.dr_cr == "D" else _money_text(amount_val),
            "debitlocal": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "creditlocal": "0.00" if rule.dr_cr == "D" else _money_text(amount_val),
        }
        if assgrp:
            kd_entry["assgrp"] = assgrp
        if maincfassgrp:
            kd_entry["maincfassgrp"] = maincfassgrp
        kingdee_entries.append(kd_entry)

    _normalize_voucher_money_fields(accounting_entries, kingdee_entries)
    total_debit = sum((_entry_decimal(e, "debit_exact", "debit")) for e in accounting_entries)
    total_credit = sum((_entry_decimal(e, "credit_exact", "credit")) for e in accounting_entries)

    kingdee_json = {
        "data": [
            {
                "book_number": book_number,
                "bizdate": biz_date,
                "bookeddate": booked_date,
                "period_number": period_number,
                "vouchertype_number": vouchertype_number,
                "description": (
                    matched_template.template_name
                    or matched_template.template_id
                    or "UnnamedTemplate"
                ),
                "attachment": _parse_attachment_count(attachment),
                "entries": kingdee_entries,
            }
        ]
    }

    return {
        "matched": True,
        "matched_root_source": "receipt_bills",
        "matched_via_receipt": False,
        "template_id": matched_template.template_id,
        "template_name": matched_template.template_name,
        "receipt_summary": {
            "id": receipt_bill.id,
            "community_id": receipt_bill.community_id,
            "receipt_id": receipt_bill.receipt_id,
            "deal_type": receipt_bill.deal_type,
            "deal_type_label": RECEIPT_BILL_DEAL_TYPE_LABELS.get(receipt_bill.deal_type, "placeholder"),
            "income_amount": _json_number(receipt_bill.income_amount),
            "amount": _json_number(receipt_bill.amount),
            "asset_name": receipt_bill.asset_name,
        },
        "receipt_summary": _build_receipt_summary_payload(receipt_bill),
        "matched_relation_sources": sorted(matched_selected_records.keys()),
        "accounting_view": {
            "entries": accounting_entries,
            "total_debit": _json_number(total_debit),
            "total_credit": _json_number(total_credit),
            "total_debit_exact": _decimal_text(total_debit),
            "total_credit_exact": _decimal_text(total_credit),
            "is_balanced": (total_debit - total_credit) == Decimal("0") and (total_credit - total_debit) == Decimal("0"),
        },
        "kingdee_json": kingdee_json,
        "selected_bills": source_bills,
        "selected_bill_push_summary": source_bill_push_summary,
        "source_bills": source_bills,
        "source_bill_push_summary": source_bill_push_summary,
        "push_blocked": push_blocked,
        "push_block_reason": push_block_reason,
    }


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
    if not payload.receipts:
        raise HTTPException(status_code=400, detail="No receipts selected")

    unique_refs = _normalize_receipt_refs(payload.receipts)
    if not allowed_community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    allowed_set = set(allowed_community_ids)
    unauthorized = [r for r in unique_refs if int(r["community_id"]) not in allowed_set]
    if unauthorized:
        bad = ", ".join([f"{r['community_id']}:{r['receipt_bill_id']}" for r in unauthorized[:10]])
        raise HTTPException(status_code=403, detail=f"Unauthorized receipt communities: {bad}")

    previews: List[Dict[str, Any]] = []
    skipped_bills: List[Dict[str, Any]] = []

    for ref in unique_refs:
        try:
            result = preview_voucher_for_receipt(
                receipt_bill_id=int(ref["receipt_bill_id"]),
                community_id=int(ref["community_id"]),
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
                allowed_community_ids=allowed_community_ids,
            )
            if not result.get("matched"):
                skipped_bills.append({
                    "bill_id": int(ref["receipt_bill_id"]),
                    "community_id": int(ref["community_id"]),
                    "reason": "template not matched",
                })
                continue
            previews.append(result)
        except HTTPException as exc:
            skipped_bills.append({
                "bill_id": int(ref["receipt_bill_id"]),
                "community_id": int(ref["community_id"]),
                "reason": exc.detail if isinstance(exc.detail, str) else str(exc.detail),
            })

    if not previews:
        details = "; ".join([f"{b['community_id']}:{b['bill_id']} -> {b['reason']}" for b in skipped_bills[:20]])
        raise HTTPException(
            status_code=400,
            detail=("No vouchers could be generated" + (f": {details}" if details else "")),
        )

    first_preview = previews[0]
    first_header = ((first_preview.get("kingdee_json") or {}).get("data") or [{}])[0]
    header_keys = ["book_number", "bookeddate", "period_number", "vouchertype_number"]
    merged_bizdates = [str(first_header.get("bizdate") or "").strip()]

    header_compatible_previews: List[Dict[str, Any]] = [first_preview]
    for preview in previews[1:]:
        header = ((preview.get("kingdee_json") or {}).get("data") or [{}])[0]
        incompatible_keys = [k for k in header_keys if first_header.get(k) != header.get(k)]
        if incompatible_keys:
            summary = preview.get("receipt_summary") or {}
            skipped_bills.append({
                "bill_id": int(summary.get("id") or 0),
                "community_id": int(summary.get("community_id") or 0),
                "reason": f"inconsistent voucher header ({', '.join(incompatible_keys)}); skipped from merge",
            })
            continue
        merged_bizdates.append(str(header.get("bizdate") or "").strip())
        header_compatible_previews.append(preview)

    previews = header_compatible_previews
    merged_bizdate = max([d for d in merged_bizdates if d], default=str(first_header.get("bizdate") or ""))

    source_bills: List[Dict[str, Any]] = []
    seen_source_keys = set()
    for preview in previews:
        for source_bill in preview.get("source_bills") or []:
            key = (int(source_bill.get("bill_id") or 0), int(source_bill.get("community_id") or 0))
            if key in seen_source_keys:
                continue
            seen_source_keys.add(key)
            source_bills.append(source_bill)

    source_bill_push_summary = _summarize_bill_push_statuses(source_bills)
    push_conflicts = _find_bill_push_conflicts(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})" for item in push_conflicts[:10]]
        )
        push_block_reason = f"Selected source bills already have pushed or pushing voucher records: {conflict_preview}"

    merged_entries: List[Dict[str, Any]] = []
    merged_accounting_entries: List[Dict[str, Any]] = []
    seq = 1
    for preview in previews:
        kd_header = ((preview.get("kingdee_json") or {}).get("data") or [{}])[0]
        for entry in kd_header.get("entries") or []:
            item = dict(entry)
            item["seq"] = seq
            merged_entries.append(item)
            seq += 1

        for entry in (preview.get("accounting_view") or {}).get("entries") or []:
            item = dict(entry)
            item["line_no"] = len(merged_accounting_entries) + 1
            merged_accounting_entries.append(item)

    total_debit = sum((_entry_decimal(e, "debit_exact", "debit")) for e in merged_accounting_entries)
    total_credit = sum((_entry_decimal(e, "credit_exact", "credit")) for e in merged_accounting_entries)
    merged_template_ids = sorted({str(p.get("template_id") or "") for p in previews if p.get("template_id")})
    template_name = first_preview.get("template_name") or first_preview.get("template_id") or "BatchMerged"
    merged_kingdee_json = {
        "data": [{
            "book_number": first_header.get("book_number"),
            "bizdate": merged_bizdate,
            "bookeddate": first_header.get("bookeddate"),
            "period_number": first_header.get("period_number"),
            "vouchertype_number": first_header.get("vouchertype_number"),
            "description": template_name,
            "attachment": first_header.get("attachment", 0),
            "entries": merged_entries,
        }]
    }

    return {
        "matched": True,
        "partial_matched": len(skipped_bills) > 0,
        "matched_bills": len(previews),
        "skipped_bills": skipped_bills,
        "template_id": first_preview.get("template_id"),
        "template_name": first_preview.get("template_name"),
        "template_ids": merged_template_ids,
        "source_bills": source_bills,
        "source_bill_push_summary": source_bill_push_summary,
        "push_blocked": push_blocked,
        "push_block_reason": push_block_reason,
        "accounting_view": {
            "entries": merged_accounting_entries,
            "total_debit": _json_number(total_debit),
            "total_credit": _json_number(total_credit),
            "total_debit_exact": _decimal_text(total_debit),
            "total_credit_exact": _decimal_text(total_credit),
            "is_balanced": (total_debit - total_credit) == Decimal("0") and (total_credit - total_debit) == Decimal("0"),
        },
        "kingdee_json": merged_kingdee_json,
    }


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
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    """
    濠电姷鏁告慨鐑藉极閸涘﹥鍙忛柣鎴濐潟閳ь剙鍊块幐濠冪珶閳哄绉€规洏鍔戝鍫曞箣閻欏懐骞㈤梻鍌欑閹测剝绗熷Δ鍛煑閹兼番鍔嶉崑鍕煕閳╁厾鑲╂崲閸℃ǜ浜滈柡宥冨妽閻ㄦ垶鎱ㄩ敐鍥т槐闁哄本绋撻埀顒婄秵閸嬪懐浜搁鐔翠簻妞ゆ劧绲跨粻鐐烘煙椤旂懓澧查柟顖涙閺佹劙宕熼澶嬫櫍闂傚倸鍊搁崐鐑芥倿閿旈敮鍋撶粭娑樻噺瀹曟煡鏌涢弴銊ョ仩缁惧墽绮换娑㈠箣濞嗗繒鍔撮梺杞扮濞差參寮诲☉婊呯杸闁哄洨鍋涙禒妯肩磽娴ｇ顣抽柛瀣洴閸╃偤骞嬮敂钘変汗闂佸湱绮敮鈺傚瀹€鍕拺閻炴稈鈧厖澹曢梻渚€娼ц墝闁哄懏绮撻崺娑㈠箳閹炽劌缍婇弫鎰板川椤斿吋娈橀梻浣筋嚃閸ㄤ即鎮ф繝鍕床婵炴垶鐭▽顏堟煕鐏炴崘澹樻い顒€顦靛娲焻濞戞埃鏁€闂佸憡姊归崹鍧楃嵁閸愵煈鐓ラ柛顐ゅ枎閻у嫭绻濋姀锝嗙【闁活剙銈稿鎼佸礃閵娿垺鏂€闂佺粯锕╅崰鏍倶闁秵鐓曢柍鍝勫€绘晶鐢碘偓瑙勬礃缁诲牆顕ｆ禒瀣垫晣闁绘劗顣槐閬嶆⒒娴ｇ懓顕滅紒璇插€胯棟濞寸厧鐡ㄩ崑鍌涚箾閸℃ɑ灏伴柣鎾寸☉椤法鎹勯悮鏉戝婵炲濮伴崹娲Φ閸曨垱鏅查柛鈩冦仜閺嬫瑩姊洪崫鍕効缂傚秳绶氬顐﹀箛閺夊潡鍞跺┑鐘诧工閸燁垶宕曢幘缁樷拻濞撴埃鍋撻柍褜鍓涢崑娑㈡嚐椤栨稒娅犻柛娆忣槶娴滄粍銇勯幇鈺佺労婵″弶鎮傞幃锟犲Χ閸℃瑧鐦堟繝鐢靛Т閸婄粯鏅堕弴鐘电＜闁归偊鍙庡▓婊堟煛瀹€鈧崰鏍嵁閹达箑绠涢梻鍫熺⊕椤斿嫰姊绘担铏瑰笡閻㈩垱甯￠幃妯侯潩鐠轰綍锕傛煕閺囥劌鐏犵紒鐘冲▕閺岀喓鈧稒顭囨俊鍥煕鐎ｎ偅宕勯柕鍥ㄥ姍楠炴帡骞嬮悙鍓佸彂闂傚倷绀侀崯鍧楀箹椤愶箑绠犻煫鍥ㄧ⊕閸嬪倹绻涢崱妯诲鞍闁绘搫缍侀悡顐﹀炊妞嬪骸鍩屾繛瀛樼矋缁诲牓骞冨Ο璺ㄧ杸闁哄洨鍠愬В鍫ユ⒑閸濆嫯顫﹂柛濠冩礈閸欏懎顪冮妶鍛闁哥噥鍨跺畷鏉款潩閼搁潧鈧灚绻涢崼婵堜虎闁哄绋掗妵鍕敇閵忊剝鏆犳繛锝呮搐閿曨亪骞冨鍫熷殟闁靛闄勯悵鏍⒒娓氣偓閳ь剛鍋涢懟顖涙櫠鐎电硶鍋撶憴鍕缂侇喗鎹囬妴浣割潨閳ь剚鎱ㄩ埀顒勬煃闁款垰浜鹃梺褰掓敱濡炶棄顫忓ú顏勫窛濠电姴瀚уΣ鍫ユ⒑閹稿孩纾搁柛搴㈠絻椤曘儵宕熼姘鳖槹濡炪倖鐗楃粙鎾诲储闁秵鐓熼幖鎼灣缁夐潧霉濠婂嫮鐭掓い銏℃閸╋繝宕ㄩ鎯у笚缂傚倸鍊烽悞锕佹懌閻庤娲栭惉濂稿焵椤掑喚娼愭繛鍙夌墵婵″墎绮欏▎鎯ф闂佸湱铏庨崰妤呭磻閸曨垱鐓ｆ慨姗嗗墮閳ь剙鎽滈埀顒€鐏氶悡鈥愁潖婵犳艾纾兼繛鍡樺灥婵′粙姊虹拠鈥虫灈闁绘牜鍘ч悾鐑藉即閻樼數锛滃┑鈽嗗灠濠€杈╃不濮樿埖鈷戦柟鑲╁仜閸旀挳鏌涙惔銏㈠弨鐎规洖鐖奸、妤佹媴鐟欏嫮褰囬梻鍌欒兌椤牓寮甸鍌滅煓闁规崘顕ч崒銊╂⒑椤掆偓缁夌敻鍩涢幋鐘电＝濞达絽顫栭鍛弿闁搞儯鍔嬬换鍡樸亜閺嶃劎鍟查棅顒夊墰閳ь剝顫夊ú婊堝极婵犳艾绠栭柕蹇嬪€曠粈鍌炴煠濞村娅呴悽顖涱殜閺岋綁鎮㈤崫銉х厑缂備緡鍠楅幐鎼佹偩瀹勯偊鐓ラ柛鎰剁稻椤秹姊洪棃娑氱濠殿喚鍏橀幃鍧楀炊椤掍讲鎷洪柣鐔哥懃鐎氼剟宕濋妶鍜佺唵鐟滃秶鈧稈鏅濈划娆愬緞婵犲骸鎮戦梺鎼炲劵缁茶姤绂嶉悙顒夋闁绘劘灏欐禒銏ゆ煕閺冣偓绾板秹濡甸崟顖涙櫆閻犲洩灏欐禒鎼佹⒑?

    闂傚倸鍊搁崐鎼佸磹閹间礁纾瑰瀣捣閻棗銆掑锝呬壕濡ょ姷鍋為悧鐘汇€侀弴姘辩Т闂佹悶鍎洪崜锕傚极閸愵喗鐓ラ柡鍥殔娴滈箖姊哄Ч鍥р偓妤呭磻閹捐埖宕叉繝闈涙川缁♀偓闂佺鏈划宀勩€傚ú顏呪拺闁芥ê顦弳鐔兼煕閻樺磭澧电€殿喖顭峰鎾偄閾忚鍟庨梻浣稿閻撳牓宕伴弽銊х彾闁告洦鍋€閺€浠嬫煟閹邦剙绾ч柍缁樻礀闇夋繝濠傚閻帞鈧娲橀敃銏ゅ春閳ь剚銇勯幒鍡椾壕濡炪値浜滈崯瀛樹繆閸洖骞㈡俊顖滃劋濞堫偊姊绘担渚劸妞ゆ垵娲畷浼村冀椤掍緡妫ㄩ梻鍌氬€风欢锟犲礈濞嗘垹鐭撻柣銏㈩焾閻撴洟鏌熸潏楣冩闁绘挻娲樼换娑㈠箣閻戝洤鍙曞┑顔硷攻椤ㄥ牏妲愰幒妤€閱囬柡鍥ㄧ閸犳岸姊洪崫鍕拱缂佸鍨奸悘鎺楁⒑闂堚晛鐦滈柛姗€绠栭幃锟犲箻缂佹ê鈧敻鎮峰▎蹇擃仾缂佲偓閸愨晝绠鹃柤纰卞墮閺嬫盯鏌ｅ☉鍗炴灈妞ゎ偅绮撻崺鈧い鎺嗗亾妞ゆ洩缍侀、妤呭礋椤愬鍔戦弻銊╁籍閸屾粌绗＄紓鍌氱Т閻倸顫忔繝姘＜婵炲棙鍔楅妶鏉款渻閵堝骸浜滄い锕傛涧閻ｇ兘寮撮姀鐘殿槰闂佽偐鈷堥崜姘枔閹屾富闁靛牆妫楃粭鎺楁倵濮樼厧澧撮柟顔惧仱瀹曞綊顢曢悩杈╃泿闂備胶鎳撻幖顐ょ矓閸洖绀夌€广儱娲犻崑鎾舵喆閸曨剛顦ラ梺娲诲幖閸婂灝顕ｆ繝姘櫖闁告洦浜濋崟鍐⒑缁嬪尅鍔熼柛鐘查叄椤㈡棃顢橀悜鍡樺瘜闂侀潧鐗嗘鎼佺嵁閹达附鐓曢柡鍐ｅ亾闁搞劎鏁婚幃楣冩煥鐎ｎ剟妾梺鍛婄☉閿曘倖绂嶅鍫熲拺闁告稑锕︾粻鎾绘倵濮樼厧澧柣鈥崇箻濮婂宕掑顑藉亾閹间礁纾瑰瀣椤愯姤鎱ㄥ鍡楀⒒闁绘帞鏅幉鎼佸籍閸繄浼嬮梺鎸庢礀閸婃悂鏌嬮崶顒佺厪濠㈣泛鐗嗛崝銈夋煥濞戞艾鏋涙慨濠勭帛缁楃喖宕惰椤晝绱撴担鍓叉Ш闁轰浇顕ч悾鐑藉箛閻楀牜妫冨┑鐐村灦鐢寮埀顒勬⒒娴ｈ櫣甯涢柨姘辩棯缂併垹寮柕鍡楀€垮濠氬Ψ閿旀儳骞?
    1. `accounting_view`: 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻鐔兼⒒鐎靛壊妲紒鐐劤缂嶅﹪寮婚悢鍏尖拻閻庣數顭堟俊浠嬫⒑缁嬫鍎忔い鎴濐樀瀵鈽夐姀鐘插祮闂侀潧顭堥崕铏閳哄懏鈷戦柟鑲╁仜婵″ジ鏌涙繝鍌ょ吋妤犵偛顦甸獮鏍ㄦ媴閻熼缃曢梻浣稿閸嬪懐鎹㈤崟顖涘仭闁靛ě鍛紳婵炶揪绲介幖顐ｇ墡闂備胶绮〃鍡涖€冮崼銉ョ闁靛繒濮弨浠嬫煕閵夈垺娅囬柣锕€鐗撳铏圭矙閹稿孩鎷遍梺鍛婂灱椤鈧潧銈搁崺鈧い鎺戝閳锋帒霉閿濆懏鎲搁柨娑樼Ф缁辨帡顢氶崨顓犱哗濡炪値鍋勭换妤呭Φ閹版澘绠抽柟瀵稿Х閺変粙姊绘笟鈧褔鈥﹂崼銉ョ？闁告繂瀚烽悞浠嬫煏婵炵偓娅嗛柣鎾存礋閺屽秹鍩℃担鍛婄亾濠电偛鐗婂褰掑Φ閸曨垼鏁冮柣鏂挎啞閻濇柨顪冮妶搴濈盎闁哥喎鐡ㄦ穱濠囨嚋閸偄鍔呴梺鐐藉劥鐏忔瑩骞愭径鎰拻濞达絼璀﹂悞楣冩煥閺囨ê鍔ら柟骞垮灲瀹曠厧鈹戦幇顒侇吙濠电姷鏁告慨鐢靛枈瀹ュ鐤炬い鎺戝閻撴洟鏌￠崶銉ュ缂併劌銈搁弻娑㈠即閻愬弶娈婚梺鍝勭焿缂嶄線骞冮埡鍛煑濠㈣泛顭Σ閬嶆煟閻斿摜鐭嬫繝銏★耿瀹曨垶顢涢悙鑼杽闂侀潧艌閺呮盯锝為崨瀛樼厽婵妫楁禍婵嬫煛閸屾浜鹃梻鍌氬€烽懗鍓佸垝椤栫偞鏅濋柍鍝勬噹绾惧鏌涢弴銊ュ妞も晜褰冭灃闁挎繂鎳庨弳鐐烘煃闁垮鐏撮柡灞剧☉閳藉顫滈崼婵嗩潬闂備礁鐤囧Λ鍕囬悽绋胯摕闁挎繂妫楃粻鐘绘⒑閸涘﹥鈷愰柛銊ф暬閹箖鎮滈挊澶婂祮闂佺粯鍨靛Λ妤呭箖閹寸偟绡€闁靛骏绲剧涵鐐亜閹存繃鍠樼€规洏鍨介弻鍡楊吋閸″繑瀚奸梻浣告啞缁诲倻鈧凹鍙冨畷鎺楀Ω閳哄倻鍘遍梺鍝勫€归娆撳磿閹达附鐓犳繛宸簷閹查箖鏌熼钘夊姢闁伙絾绻堥崺鈧い鎺戝€搁ˉ姘攽閸屾碍鍟為柣鎾寸懇濮婃椽顢橀妸褏鏆犻悗娈垮枤閸嬫挾鎹㈠☉銏犻唶闁绘棁娅ｉ悡澶愭⒑閸濆嫮鐏遍柛鐘崇墵閵嗕礁鈻庤箛濠冪€婚梺缁樺姦閸撴稓绮旈悜鑺モ拻濞达絿鍎ら崵鈧梺鎼炲灪閻擄繝鐛繝鍐╁劅闁靛濡囬悾鎶芥⒒娴ｇ瓔鍤欓柛鎴犳櫕缁辩偤宕卞Ο纰辨锤濠电偛妫欓崝妤呮偟閸洘鐓涢柛銉ｅ劚閻忣亪鏌嶉柨瀣伌闁哄本绋戦埥澶婎潨閸繀绱ｅ┑鐘愁問閸犳牕煤閵娾晛鐒垫い鎺嶇贰閸熷繘鏌涢悩宕囧⒌闁诡喓鍎茬缓浠嬪箹閻愨晛浜惧ù锝囩《閺嬪酣鐓崶椋庡埌闁诡喗鍨剁换婵嬪閿濆棛銆愬銈嗗灥濞差厼鐣烽姀锛勵浄閻庯綆鍋€閹疯櫣绱撻崒娆戝妽闁崇鍊濋、鏃堝礋闂堟稒顓块梻浣稿閸嬪懎煤濮椻偓瀹曟洖螖娴ｉ绠氶梺闈涚墕鐎氼垶宕楀畝鈧槐鎺楁偐閼姐倗鏆梺鍝勫閸撴繂顕ラ崟顓涘亾閿濆簼绨藉ù鐘虫倐濮婃椽宕妷銉愶綁鏌ㄩ弴妯虹伈鐎殿喖顭锋俊鎼佸Ψ閵忊剝鏉搁梻浣稿閸嬪懐鎹㈤埀顒勬煙鐎电鍘存慨濠冩そ閺屽懘鎮欓懠璺侯伃婵犫拃鍐ㄢ挃缂佽鲸鎸搁濂稿川椤曞懏锛嗛柣搴ゎ潐濞叉垿宕￠崘宸殨濞寸姴顑愰弫鍥煟閺傝法娈遍柛?
    2. `kingdee_json`: 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾剧懓顪冪€ｎ亝鎹ｉ柣顓炴閵嗘帒顫濋敐鍛婵°倗濮烽崑娑⑺囬悽绋挎瀬闁瑰墽绮崑鎰版煕閹邦垰绱﹂柣銏㈢帛娣囧﹪鎮欓鍕ㄥ亾閺嶎厽鍋嬫繝濠傜墕绾剧粯绻涢幋娆忕仼闁搞劌鍊块弻娑樼暆閳ь剟宕戦悙鐑樺亗闁哄洢鍨洪悡娆撴⒑椤撱劎鐣遍柣蹇氶哺閹便劍绻濋崘顭戝殝缂備胶绮粙鎴︻敊韫囨侗鏁婇柤濮愬€楀▔鍧楁⒒閸屾瑦绁伴柨鐔村劦瀹曟劙寮介妸褉鏀虫繝鐢靛Т濞层倕娲块梻浣侯焾閺堫剟鎯岄鐣岀彾闁哄洨濮风壕浠嬫煕鐏炲墽鎳嗛柛蹇撹嫰閳规垿顢欓懖鈺€绮电紓浣虹帛缁嬫帞鎹㈠┑瀣倞鐟滄垿骞楅弴銏♀拺闁告繂瀚婵嬫煕閻樿櫕宕岄柟顔炬暬閹虫粓鎮欓柅娑氱泿闂備礁鎼粔鏌ュ礉韫囨梻鐝舵俊顖涚湽娴滄粓鐓崶椋庡缂併劏鍋愮槐鎺旂磼濡吋鍒涢悗瑙勬礈閸樠囧煡婢跺á鐔兼⒒鐎靛憡锛忕紓?`voucherAdd` 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌ｉ幋锝呅撻柛濠傛健閺屻劑寮撮悙娴嬪亾瑜版帒纾块柟瀵稿У閸犳劙鏌ｅΔ鈧悧鍡欑箔閹烘梻纾奸柍褜鍓氬鍕沪缁嬪じ澹曢梺绋跨箰椤︻垱绂嶆ィ鍐┾拺闂侇偆鍋涢懟顖涙櫠閹绢喗鐓曢柍瑙勫劤娴滅偓淇婇悙顏勨偓鏍暜婵犲洦鍤勯柛顐ｆ礃閸嬪倹銇勯弽顐沪闁绘挻娲樻穱濠囧Χ閸屾矮澹曢梻浣告憸閸犲骸顭囬敓鐘茬畺濡わ絽鍟崐濠氭煠閹帒鍔滈柛搴簻椤啴濡堕崱妤€顫庢繝娈垮枛閻°劎绮嬪鍛斀閻庯綆鍋€閹锋椽姊洪崨濠勨槈闁挎洏鍊栭幈銊╁焵椤掑嫭鈷戦柛婵嗗閸ｈ櫣绱掗鑺ュ碍闁伙綁鏀辩€靛ジ寮堕幋鐙€鍞烘繝寰锋澘鈧挾鎷嬮弻銉ョ婵炲樊浜濋悡鐔兼煟濡搫绾х紒鈧畝鍕厓鐟滄粓宕滃▎鎴濐棜妞ゆ挾鍠撻々鐑芥煙闂傚鍔嶉柍閿嬪灩缁辨帞鈧綆鍋掗崕銉╂煕鎼达紕绠婚柡灞诲姂瀵挳濡搁妶鍥╂晨闂備椒绱徊鍧楀礂濮椻偓楠炲啯绂掔€ｎ亜绐涙繝鐢靛Т閸婄敻宕戦幘缁樺€婚柤鎭掑劚閳ь剙鐏氱换娑㈠箣閻戝棔鐥銈呯箰鐎氱兘宕?JSON
    """
    from services.voucher_engine import evaluate_expression
    from utils.variable_parser import build_variable_map, resolve_variables
    import json as json_mod
    from datetime import datetime

    # 1. Query bill
    bill_query = db.query(models.Bill).filter(models.Bill.id == bill_id)
    if community_id is not None:
        bill = bill_query.filter(models.Bill.community_id == community_id).first()
        if not bill:
            raise HTTPException(
                status_code=404,
                detail=f"Bill not found: id={bill_id}, community_id={community_id}"
            )
    else:
        candidates = bill_query.limit(2).all()
        if not candidates:
            raise HTTPException(status_code=404, detail=f"Bill not found: id={bill_id}")
        if len(candidates) > 1:
            raise HTTPException(
                status_code=400,
                detail="bill_id is not unique across communities; please pass community_id"
            )
        bill = candidates[0]

    if not allowed_community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    if int(bill.community_id) not in set(allowed_community_ids):
        raise HTTPException(
            status_code=403,
            detail=f"Unauthorized bill community: {bill.community_id}"
        )

    normalized_account_book_number = _decode_header_value(x_account_book_number) or None
    source_refs = [{
        "bill_id": int(bill.id),
        "community_id": int(bill.community_id),
    }]
    source_status_map = _get_bill_push_status_map(
        db,
        source_refs,
        account_book_number=normalized_account_book_number,
    )
    source_bills = [source_status_map[(int(bill.id), int(bill.community_id))]]
    source_bill_push_summary = _summarize_bill_push_statuses(source_bills)
    push_conflicts = _find_bill_push_conflicts(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [
                f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})"
                for item in push_conflicts[:10]
            ]
        )
        push_block_reason = f"Current bill already has voucher push records: {conflict_preview}"

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳婀遍埀顒傛嚀鐎氼參宕崇壕瀣ㄤ汗闁圭儤鍨归崐鐐差渻閵堝棗绗掓い锔垮嵆瀵煡顢旈崼鐔蜂画濠电姴锕ら崯鎵不缂佹﹩娈介柣鎰綑閻忔潙鈹戦鐟颁壕闂備線娼ч悧鍡涘箠閹扮増鍋柍褜鍓氭穱濠囨倷椤忓嫧鍋撻弽顬稒鎷呴懖婵囩洴瀹曠喖顢楁担绋垮Τ濠电姷鏁告慨鏉懨洪敃鍌氱厱闁瑰濮甸崰鎰版煟濡も偓閻楀棛绮幒鎳ㄧ懓顭ㄩ埀顒勫础閹惰棄钃熸繛鎴炵懄閸庣喖鏌曟繝蹇涙闂佹鍙冨铏光偓鍦У椤ュ銇勯敂璇茬仸闁挎繄鍋涢…銊╁醇濠靛棜鈧灝鈹戦悙鍙夘棞缂佺粯鍔曟晥闁哄被鍎查埛鎺楁煕鐏炲墽鎳呮い锔肩畵閺岀喎霉鐎Ｑ冧壕閻℃帊鐒﹀浠嬪极閸愵喖纾兼慨妯诲敾缁遍亶姊绘担铏广€婇柛鎾寸箞閹柉顦归柟顔挎珪缁绘繂顫濋鐘插箺闂備礁缍婇崑濠囧窗閺嶎厼绀夐悗锝庡枟閻撳啴姊洪崹顕呭剰闁诲繑鎸抽弻锛勪沪閸撗€妲堥梺瀹犳椤︻垶锝炲鍫濋唶闁绘柨鎲″В搴㈢節閻㈤潧袨闁搞劎鍘ч埢鏂库槈濮橈絽浜炬慨姗嗗亜瀹撳棛鈧鍠涢褔鍩ユ径鎰潊闁绘ɑ鐗戦弲鐘诲蓟閵娾晛鍗虫俊銈傚亾濞存粍鍎抽—鍐Χ閸愩劌濮庡銈嗗灥濡盯骞戦姀鐘闁靛繒濮烽娲⒑缂佹﹩鐒炬繛鍛礀琚欓柛顐犲劜閳锋垹绱掗娑欑闁哄缍婇弻?ORM 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳婀遍埀顒傛嚀鐎氼參宕崇壕瀣ㄤ汗闁圭儤鍨归崐鐐差渻閵堝棗绗掓い锔垮嵆瀵煡顢旈崼鐔蜂画濠电姴锕ら崯鐗堟櫏婵犵數濮崑鎾炽€掑锝呬壕濠殿喖锕ㄥ▍锝囨閹烘嚦鐔兼惞闁稓绀冨┑鐘殿暯濡插懘宕戦崟顓涘亾濮橆厽绶叉い鏇秮瀹曘劍绻濋崟顓熷殞闂備線鈧偛鑻晶瀵糕偓娈垮枛椤兘骞冮姀銈呯閻忓繑鐗楃€氫粙姊绘担鍛婅础闁惧繐閰ｅ畷鏉课旈崪浣规櫅闂佸搫绋侀崢浠嬪煕閹达附鈷戞い鎰╁€曟禒婊堟煠濞茶鐏︾€规洏鍨介獮鏍ㄦ媴閸︻厼骞楅梻浣侯攰濞咃綁宕戝☉顫偓鍛搭敆閸曨剛鍘靛Δ鐘靛仜閻忔繈鎮橀埡鍛厓閻熸瑥瀚悘鈺呮煃瑜滈崜銊х礊閸℃顩查柣鎰惈绾惧綊鏌ｉ幇顔煎妺闁抽攱鍨垮濠氬醇閻斿墎绻侀梺缁樺浮缁犳牠寮诲☉娆愬劅闁炽儴娅曞В鎰版煣娴兼瑧鍒伴柕鍡樺笒椤繈鏁愰崨顒€顥氬┑鐘愁問閸犳牠鏁冮敂鎯у灊妞ゆ牜鍋涚粻顖炴煕濞戞瑦缍戠€瑰憡绻傞埞鎴︽偐閹绘巻鍋撻悷鎵虫灁婵☆垵宕电弧鈧梺姹囧灲濞佳勭閳哄倶浜滈柡鍥╁枔閻瞼绱掗纰辩吋妤犵偞锚閻ｇ兘宕堕妸锔诲晭濠电姷鏁告慨鎾晝閵堝绐楁慨姗嗗墻閻掍粙鏌嶉崫鍕偓鑸电濠婂牊鐓欓柟瑙勫姈绾箖鏌＄€ｎ亝鎹ｉ柍褜鍓氶鏍窗閺嶎厽鍊舵繝闈涱儏閻撴﹢鏌熸潏楣冩闁稿鍔欓幃褰掑炊閸パ冩殨缂佹唻缍佸缁樻媴閾忕懓绗″銈冨妼閹虫﹢寮崘顕呮晜闁告洦鍘藉▓楣冩⒑绾懏褰ч梻鍕瀵煡骞撻幒婵堝數闁荤姾妗ㄧ拃锕傚磿閹达附鐓曟俊顖氬悑閺嗩剚鎱ㄦ繝鍕笡缂佹鍠栧畷鎯邦槻濞寸厧娴风槐鎾存媴閸濆嫅锝夋煕閵娿儳浠㈡い鏇悼缁瑦鎯旈幘鎼綌闂備線娼ф蹇曟閺囥垹鍌ㄩ柟闂寸劍閻撶喖骞栭幖顓炵仯缂佸鏁婚弻娑氣偓锝庝簼閸ゅ洭鎸婇悢鍏肩厱妞ゆ劗濮撮崝婊堟煟閹惧娲撮柟顔斤耿閹瑩宕归锝囧涧闂佸摜鍠撻…鍫モ€旈崘顔嘉﹂柛妤冨仜閳ь剝宕电划锝夊籍閸喓鍘搁柣蹇曞仜婢ц棄煤鐎涙﹩娈介柣鎰儗閻掍粙鏌嶈閸撴氨绮欓幒妞烩偓锕傚炊瑜夐弸鏃堟煏閸繃宸濈痪鎹愭閵嗘帒顫濋浣规倷闂佸搫顑囬崰鏍蓟閿濆鍋勯柛娆忣槹閻濇岸姊虹紒妯圭繁闁革綇缍侀悰顕€骞掑Δ鈧粻锝嗐亜閹捐泛鏋庨柣锔界矒濮婅櫣鎷犻懠顒傤唶缂備胶绮崹鍧楀箖瑜斿鎾偄娓氼垱閿ゅ┑掳鍊х徊浠嬪疮椤栫偞鍋傞柡鍥ュ灪閻撴瑥霉閻撳海鎽犳繛鎳峰洦鐓涢悗锝庡亞閵嗘帡鏌嶈閸撴盯骞婇幘璇茬疅闁挎稑瀚畷鏌ユ煕椤愮姴鍔氶柦鍐枛閺屽秹鍩℃担鍛婃缂備礁澧庨崑銈夊箖濡ゅ懏鏅查幖瀛樼箘閺佹牗绻涚€电甯舵繛宸弮楠炲啰鎹勬笟顖涘兊濡炪倖鎸炬慨鐑芥偪閸曨偀鏀?
    bill_data = {}
    for col in models.Bill.__table__.columns:
        val = getattr(bill, col.name, None)
        if val is not None:
            bill_data[col.name] = val if not hasattr(val, 'isoformat') else val.isoformat()
        else:
            bill_data[col.name] = None
    # Convert Decimal values to float for preview payload
    from decimal import Decimal as PyDecimal
    for k, v in bill_data.items():
        if isinstance(v, PyDecimal):
            bill_data[k] = float(v)

    # 2. 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻鐔兼⒒鐎靛壊妲紒鎯у⒔閹虫捇鈥旈崘顏佸亾閿濆簼绨奸柟鐧哥秮閺岋綁顢橀悙鎼闂傚洤顦甸弻銊モ攽閸♀晜效婵炲瓨鍤庨崐婵嬪蓟閵堝绾ч柟绋块娴犳潙鈹戦纭锋敾婵＄偘绮欓妴浣肝旈崨顓犲姦濡炪倖甯掗崐濠氭儗閹剧粯鐓涢悘鐐额嚙閸旀岸鏌ｉ幒鎴犱粵闁靛洤瀚伴獮鎺楀幢濡炴儳顥氶梻鍌欒兌椤牓鏁冮妶鍜冭€块弶鍫氭櫅閸ㄦ繈鎮楅敐搴℃灈缂佺姵濞婇弻锟犲磼濮橆厽鍎撴繛瀛樼矒缁犳牠寮婚妸銉㈡斀闁糕剝鐟ラ崵顒傜磽娴ｉ潧濡虹紒顔界懇瀵鏁嶉崟顏呭媰闂佺粯鏌ㄩ崲鏌ュ汲婵犲洦鐓涢柍褜鍓氱粋鎺斺偓锝庡亞閸樹粙姊鸿ぐ鎺戜喊闁告鏅槐鐐哄箣閿旂晫鍘介棅顐㈡处濞叉牠寮稿☉銏＄厸閻忕偛澧藉ú鏉戔攽閳╁啯鍊愰柡浣稿€归幈銊╁箛椤忓洦顎楅梻浣筋嚙濮橈箓锝炴径濞掑搫螣婵傝姤妞介幃銏ゆ嚃閳轰胶銈︽繝娈垮枟閵囨盯宕戦幘鍨涘亾鐟欏嫭绀堥柟铏崌閸┿儲寰勬繛銏㈠枛閹兘鏌囬敂绛嬫％濠电姴鐥夐弶搴撳亾濡や焦鍙忛柣鎴ｆ绾剧粯绻涢幋娆忕仾闁稿﹨鍩栫换婵嬫濞戝崬鍓扮紓浣插亾濠㈣泛顑嗛崣蹇旀叏濡も偓濡鐛幋婢濈懓顭ㄩ崟顓犵厜闂佸搫琚崝鎴濐嚕椤曗偓瀹曞墎鎹勯悜姗嗗敳闂傚倷娴囬褎顨ヨ箛鏂剧箚闁搞儺鍓欓悞鍨亜閹哄秶璐伴柛鐔风箻閺屾盯鎮╁畷鍥р拰濡ょ姷鍋涢ˇ鐢稿极瀹ュ绀嬫い鎾跺Х閺嗐儵姊绘担铏瑰笡婵﹤顭峰畷銏ｎ樄闁诡喚鍋ゅ畷褰掝敃閻樿京鐩庨梻浣告贡閸庛倝宕归悽鍓叉晜闁冲搫鎳忛悡銏ゆ煕閹板吀绨婚柡瀣洴閺岋紕浠︾拠鎻掝瀳闂佸疇顫夐崹鍨暦閸楃倣鐔兼倻濡崵褰鹃梻鍌氬€风粈渚€骞栭锕€鐤い鎰堕檮閸嬪鏌涢埄鍐姇闁稿﹤娼￠弻娑⑩€﹂幋婵呯按婵炲瓨绮嶇划鎾诲蓟閻斿吋鍊绘俊顖濇娴犳潙顪冮妶鍛濞存粠浜璇测槈濮橈絽浜鹃柨婵嗙凹閹查箖鏌涙惔銏☆棃闁哄矉绱曟禒锕傚礈瑜庨崚娑㈡⒑缁洘娅呴悗姘緲閻ｅ嘲顫滈埀顒勩€侀弮鍫濆窛妞ゆ挾鍠撹ぐ顖炴⒒閸屾艾鈧兘鎮為敃鈧—鍐锤濡も偓閸屻劌鈹戦崒姘暈闁稿顑夐弻鐔煎箲閹伴潧娈梺钘夊暟閸犳牠寮婚弴鐔虹闁绘劦鍓氶悵鏃堟⒑閸涘娈曞┑鐐诧躬瀵鏁愭径濠勭杸濡炪倖鎸炬慨浼村磻閻愮儤鈷戠紒瀣儥閸庢劙鏌熼柅娑氱獢濠碉紕鏁婚獮鍥级鐠侯煈鍞洪梻浣筋潐閸庢娊宕㈤弽顐ュС闁汇垹鎲￠埛鎺戙€掑锝呬壕濠电偘鍖犻崵韬插姂閸┾偓妞ゆ巻鍋撻柍瑙勫灴閸ㄩ箖鎼归銏㈢崺缂傚倷绶￠崰姘卞垝椤栫偛围闁挎繂顦粈鍐煃閸濆嫬鏆欐鐐茬Ч濮婅櫣鎷犻崣澶嬪闯闂佽桨鐒﹂幃鍌炲灳閿曞倸閱囬柕澶堝劤椤︻參姊绘笟鍥у缂佸鏁婚幃锟犳偄閸忚偐鍘甸梺缁樺灦钃辩紒鈧崘鈹夸簻闁靛鍎哄Σ褰掓煏閸パ冾伃妞ゃ垺锕㈤崹楣冨礃閹绘崼銉︿繆閻愵亙绱橀柛鎰╁妷閸嬫捇鎳￠妶鍡╂綗闂佸湱鍋撻崜姘跺触鐎ｎ喗鐓曢柡鍥ュ妼楠炴﹢鏌ｅ☉鏍х伈闁诡喗顨呴埢鎾诲垂椤旂晫浜俊鐐€ら崢楣冨礂濮椻偓閻涱噣宕橀纰辨綂闂侀潧鐗嗛幊鎰八囬銏♀拺闁告稑锕﹂幊鍐煥濞戞啸妞わ箑缍婇弻鈥崇暆閳ь剟宕伴弽顓炵疇闁哄稁鍘奸悡娑㈡煕閺囩儑宸ユい锔炬暬瀵顓奸崼顐ｎ€囬梻浣告啞閹稿鎮烽埡浣烘殾妞ゆ牗绮嶅畷澶愭煏婵炑冨€婚悷婵嬫⒒娴ｇ懓顕滅紒璇插€胯棟濞寸厧鐡ㄩ崑鍌炴煟閺冨倸甯剁紒鐘侯潐缁绘盯鏁愭惔鈥愁潻闂侀€炲苯鍘甸柛濠冩礋閳ワ箓宕堕鈧粻娑欍亜閹捐泛啸妞ゆ梹娲熷娲川婵犲嫭鍣у銈忕細缁瑩骞冮悙瀵割浄閻庯綆鍋嗛崢闈浳旈悩闈涗粶闁诲繑绻堥幃姗€鎳犻钘変壕閻熸瑥瀚粈鈧梺闈涚墕閹测剝绌辨繝鍥ㄥ€婚柦妯猴級閵娧勫枑闊洦绋戝Ч鍙夈亜閹板爼妾柍閿嬪笒闇夐柨婵嗘祩閻掗箖鏌￠崱娑楁喚闁哄瞼鍠撻崰濠囧础閻愭澘鏋堥柣搴ゎ潐濞诧箓宕归崼鏇樷偓浣糕槈濡攱鏁犻梺璇″瀻閸屾凹妫滃┑鐘愁問閸犳鏁冮埡鍛偍濠靛倻顭堟导鐘崇箾瀹割喕绨奸柣鎾寸洴閹﹢鎮欓幓鎺嗘寖闂佸疇妫勯ˇ鐢稿蓟瀹ュ洦瀚氶柛娆忣槸閺€顓犵磽娴ｅ搫校闁烩晩鍨跺顐﹀箛閺夊灝鑰块棅顐㈡处濮婄粯绂嶉弽褉鏀介柣妯虹仛閺嗏晠鏌涚€ｎ偆娲存い銏″哺椤㈡﹢鎮㈢粙鍨紟闂備焦鐪归崹濠氣€﹂崼銏笉濞寸厧鐡ㄩ悡鏇㈡煙闁箑澧柍缁樻礃缁绘盯宕楅懖鈺佲拡缂備浇椴搁幐濠氬箯閸涘瓨鍤冮柍鈺佸暙閻忥箓鏌涢幒鎴含妤犵偞锕㈤、娆撴寠婢跺鏁惧┑鐘垫暩閸嬫稑螞濡ゅ啯宕查柟浼村亰閺佸倿鏌嶉崫鍕櫤闁绘挾鍠愮换婵嬪垂椤愩垹顫堥梻渚囧弾閸ㄩ亶骞堥妸锔剧瘈闁稿被鍊楅崥瀣倵鐟欏嫭绀冮悽顖涘浮閿濈偛鈹戠€ｅ灚鏅㈡繝銏ｆ硾椤︿即宕戦崨瀛樷拻濞达絿鐡旈崵娆撴倵濞戞帗娅囬柛鐘诧工椤撳ジ宕担鍝勬暩婵犳鍠楅妵娑㈠磻閹剧粯鐓欐い鏇楀亾缂佺姵鐗犻獮鍐煥閸喎鐧勬繝銏ｆ硾閿曘倗绮婇鈧缁樻媴閾忕懓绗″┑顔硷功閹虫捇鈥旈崘顔藉癄濠㈣埖顭囬埀顒冨煐閵囧嫰寮村Δ鈧禍鎯р攽椤旂》鏀绘俊鐐舵閻ｇ兘濡搁敂鍓х槇闂佸憡娲忛崝宥夛綖瀹ュ鈷掑ù锝囨嚀椤曟粍绻涘ù瀣珖缂侇喖顭烽獮妯虹暦閸ャ劍顔曢梻渚€娼ф蹇曟閺囶潿鈧懘鎮滈懞銉モ偓鐢告煥濠靛棝顎楀ù婊勫姍閺岀喖鏌ㄧ€ｎ偁浠㈠┑顔硷工椤嘲鐣烽幒鎴僵闁告鍎愰弶鍝ョ磽閸屾瑧顦︽い鎴濇楠炴劙宕滆閸ㄦ繂鈹戦悩宕囶暡闁稿﹦鍏橀弻锝夊箣閻戝棛鍔烽梺鍛婏耿娴滆泛顫忓ú顏咁棃婵炴垼浜崝鎼佹⒑缁嬪潡顎楃痪缁㈠幘閸掓帡寮崼鐔稿劒闂佺绻愰ˇ顖涚閻愵剛绠鹃柛顐ｇ箘娴犮垽鏌＄€ｎ偆娲撮柡宀€鍠撻幏鐘侯槾缂佲檧鍋撴俊銈囧Х閸嬫稑螞濠靛棭鍤曟い鏇楀亾鐎规洜鍘ч…鍧楊敂娴ｈ鐝氶梺鍝勮閸旀垵顕ｉ鈧崺鈧い鎺戝绾惧潡鏌熼崜浣规珪鐎规挷鐒︽穱濠囧Χ閸涱喖娅ら梺鎶芥敱鐢帡婀侀梺鎸庣箓濞诧箓宕甸埀顒佺節閳封偓鐏炵晫浠搁梺鍝勬湰閻╊垱淇婇幖浣哥厸闁逞屽墴楠炲銈ｉ崘鈺冨幍濡炪倖姊归崕鎶藉储閺夋垟鏀介柨娑樺閸樻挳鏌涢埞鎯у⒉闁瑰嘲鎳橀幃婊兾熼悡搴⌒梻鍌氬€搁崐鐑芥嚄閸撲礁鍨濇い鏍亹閳ь剨绠撳畷濂稿Ψ椤旇姤娅旈柣鐔哥矋閺屻劑鎮惧畡鎵虫瀻闁圭偓娼欐禍鍦磽閸屾瑧鍔嶆い顓炴喘瀹曘垽骞栨担鍏夋嫼闂佸憡绋戦敃銈嗘叏閸岀偞鐓曢柡鍌濇硶閻掓悂鏌涢埞鎯т壕婵＄偑鍊栫敮鎺楀窗濮樿泛鍚规繛鍡樻尰閹虫岸鏌ｉ幇顓犳殬濞存粍绮撻弻鈥愁吋鎼粹€茬盎婵炲濯寸粻鎾诲蓟閿濆绠婚悗鐢电《濡插牆顪冮妶搴″绩婵炲娲熼獮鎴﹀礋椤掑倻鎳濆銈嗙墬閼圭偓绔熷鈧缁樻媴娓氼垳鍔哥紓浣靛妽濡炶棄鐣烽弴銏犵煑濠㈣泛澶囬弨铏節閻㈤潧孝婵炶绠撳畷鐢稿箣閿旂晫鍘鹃梺鍛婄☉椤剟宕箛鎾佸綊鎮╅锝嗙彇缂備浇椴搁幐濠氬箯閸涱喚顩烽悗锝庝簼閹虫瑩姊哄Ч鍥х労闁搞劎鏅幑銏犫攽閸℃瑦娈惧┑鐐叉▕娴滄粓鎮?
    enriched = mapping_enrich_source_data("bills", bill_data, db=db)

    # 3. Match candidate templates
    templates = db.query(models.VoucherTemplate).filter(
        models.VoucherTemplate.active == True,
        or_(
            models.VoucherTemplate.source_type == 'bills',
            models.VoucherTemplate.source_type.is_(None),
            models.VoucherTemplate.source_type == ''
        )
    ).order_by(
        models.VoucherTemplate.priority.asc(),
        models.VoucherTemplate.template_id.asc()
    ).all()

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氬┑掳鍊楁慨鐑藉磻閻愮儤鍋嬮柣妯荤湽閳ь兛绶氬鎾閳╁啯鐝曠紓鍌氬€烽悞锕佹懌濠电偛鎳庨悧鎾愁潖缂佹ɑ濯撮柛娑橈工閺嗗牊绻涢幘瀵割暡妞ゃ劌锕ら悾鐤亹閹烘繃鏅滈梺鍓插亝缁诲嫰鏁嶅☉銏♀拺闁革富鍘兼禍楣冩煕閹剧澹樻い顓炴喘閸╋繝宕ㄩ鎯у箺闂備礁婀遍崑鎾寸箾婵犲浂鏁佹俊銈勯檷娴滄粍銇勯幇闈涗簻濞存粎鍋熺槐鎺撴綇閵婏箑闉嶉梺鐟板槻閹冲繒绮嬮幒鏂哄亾閿濆簼绨奸柟铏懇濮婄粯鎷呴崨濠呯闂佺娅曢幐鍝ュ弲濡炪倖鎸堕崹娲磻濡眹浜滈柡鍌涘椤秹鏌ｉ弬鍨倯闁搞倕鍟撮弻宥夊传閸曨偅鐏曠紓浣稿船瀵墎鎹㈠┑鍫濇瀳婵☆垱妞垮鎴︽⒑閹肩偛濮傛繛浣冲洢鈧懏绺界粙璇锯晝鎲歌箛鏇烆棜濞寸姴顑嗛悡娆撴煕閹炬鎳庣粭锟犳⒑閹惰姤鏁遍悽顖涘浮婵℃挳骞掗幋顓熷兊闂佹寧绻傞幊宥嗙珶閺囩喓绡€闁汇垽娼ф禒鎺楁煕閺嶎偄鈻堢€规洖鐖奸弫鎰板磼濮橆偄顥氶梻浣瑰濞叉牠宕戦崱娑樻瀬闁搞儺鍓氶悡鐔镐繆椤栨碍鎯堥柟顔昏兌缁辨帒螖閳ь剟藝闂堟侗娼栭柧蹇氼潐閸忔粓鏌涘☉鍗炴灈婵炴嚪鍥ㄢ拻闁稿本鑹鹃鈺呮倵濮橆厽绶查柣锝囧厴楠炲鏁冮埀顒傚婵傚憡鐓熼柟閭﹀墻閸ょ喖鏌涘Ο缁樺€愭慨濠冩そ楠炴牠鎮欓幓鎺戭潙闂備胶顭堥柊锝嗙閸洖绠栨慨妞诲亾妞ゃ垺妫冨畷濂告偄閸濆嫬绠洪梻鍌欐祰椤宕曢幎鑺ュ仱闁靛ě鍛劶闂佸憡鍔︽禍鐐靛閼测晝纾藉ù锝堫嚃閻掕姤绻涢幓鎺濆殶闁逞屽墲椤煤閺嵮呮殾妞ゆ帒鍟版禍娆撴⒒娴ｅ憡鍟炲〒姘殜瀹曞綊寮跺▎鐐瘜闂佽姤锚椤︻偊寮ㄩ懞銉ｄ簻闁哄啫娲﹂ˉ澶娗庨崶璺虹仸闁哄本绋撻埀顒婄秵娴滄繈宕甸崶顒佺厵闁惧浚鍋嗙粻鏍煏閸剛绉€规洘锕㈤弫鎰板磼濮橆偄顥氶梻浣瑰濮婂宕戦幘宕囨殾濠㈣埖鍔栭悡蹇撯攽閻樿尙绠抽柣锝堜含閻ヮ亞绱掗姀鐘典桓濠殿喖锕ュ钘夌暦閵婏妇绡€闁告洦鍘鹃弳銈夋⒑鐠囨彃顒㈢€光偓閹间礁缁╅弶鍫氭櫇閻鏌熼悜姗嗘當缂佲偓閸垺鍠愰煫鍥ㄦ礀椤ユ岸鏌﹀Ο渚Ф闁逞屽墯鐢帡锝炲┑瀣櫜闁告侗鍓欓ˉ姘辩磽閸屾艾鈧悂宕愰幖浣哥９鐟滅増甯楅崵宀勬煙鐟欏嫬濮夊ù婊堜憾濮婄粯鎷呴崨濠傛殘濠电偠顕滅粻鎾崇暦瑜版帩鏁傞柛顐ｇ箘閸旓箑顪冮妶鍡楃瑐闁绘帪绠撹棢闁割偀鎳囬崑鎾舵喆閸曨剛顦ㄩ梺鎸庢磸閸ㄤ粙濡存担绯曟瀻闁圭偓娼欏▓鎰版⒑閸愬弶鎯堥柨鏇樺劥閸婃挳姊婚崒娆愮グ鐎规洜鏁诲畷浼村箛椤旂厧鐏婇悗鍏夊亾闁逞屽墴楠炴垿濮€閻欌偓濞笺劑鏌嶈閸撶喖骞冩导鎼晩闁芥ê顦辩粣鐐烘⒑鐟欏嫭绶查柣蹇斿哺瀵彃顭ㄩ崼鐔叉嫼闂佸憡绋戦敃锝囨闁秵鐓曢柕濠忕畱閳绘洘銇勯姀锛勬噰鐎殿喗鎸虫慨鈧柍閿亾闁归攱妞藉娲嚒閵堝懏鐏堥梺绋款儏閿曨亜鐣烽悽鍛婂亜闁稿繗鍋愰崢閬嶆煟鎼搭垳绉甸柛鎾寸懄缁傛帟顦查棁澶嬬節婵犲倸顏柛濠冨姈閵囧嫰濮€閳藉懓鈧潡鏌熼鍝勭伄缂佽鲸鐟╅幃鈺呮偨閸偅娅掗梻鍌氬€搁崐鐑芥嚄閸洖绠犻柟鍓х帛閸嬨倝鏌曟繛褍瀚惔濠囨⒑閸涘﹤濮﹂柛鐔哄枑瀵板嫰骞囬澶嬬秱闂備礁鍟块悘鍫ュ疾濞戞瑧顩?
    from urllib.parse import unquote
    org_name = current_user.organization.name if current_user.organization else "闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾剧懓顪冪€ｎ亝鎹ｉ柣顓炴閵嗘帒顫濋敐鍛婵°倗濮烽崑鐐烘偋閻樻眹鈧線寮撮姀鈩冩珕闂佽姤锚椤︻喚绱旈弴鐔虹瘈闁汇垽娼у瓭闂佹寧娲忛崐妤呭焵椤掍礁鍤柛锝忕秮婵℃挳宕ㄩ弶鎴犵厬婵犮垼娉涢惉濂告儊閸喓绡€闁汇垽娼у瓭闂佺锕︾划顖炲疾閸洖鍗抽柕蹇ョ磿閸橀亶姊洪棃娑辩劸闁稿酣浜堕崺鈧い鎺嗗亾婵炵》绻濋幃浼搭敋閳ь剙顕ｆ禒瀣р偓鏍Ψ閵夆晛寮板銈冨灪椤ㄥ﹪宕洪埀顒併亜閹哄秵顦风紒璇叉闇夐柣妯烘▕閸庢劙鏌ｉ幘璺烘灈闁哄瞼鍠撶槐鎺楀閻樺吀鍝楀┑?"
    user_context = {
        "current_user_id": str(current_user.id),
        "current_username": current_user.username,
        "current_user_realname": current_user.real_name or current_user.username,
        "current_org_id": str(current_user.org_id) if current_user.org_id else "",
        "current_org_name": org_name,
        "current_account_book_id": unquote(x_account_book_id) if x_account_book_id else "",
        "current_account_book_name": unquote(x_account_book_name) if x_account_book_name else "",
        "current_account_book_number": unquote(x_account_book_number) if x_account_book_number else "",
    }

    runtime_vars = build_variable_map(db, user_context=user_context)
    def resolve_expr(expr: Optional[str], ctx: Optional[Dict[str, Any]] = None) -> str:
        resolved_with_globals = resolve_variables(expr or '', db, preloaded_vars=runtime_vars)
        return evaluate_expression(resolved_with_globals, ctx or enriched)

    matched_template = None
    all_debug_logs = {}

    conditional_templates = [t for t in templates if t.trigger_condition]
    fallback_templates = [t for t in templates if not t.trigger_condition]

    for tmpl in conditional_templates:
        try:
            conditions = json_mod.loads(tmpl.trigger_condition)
            debug_logs = []
            if _check_trigger_conditions(conditions, enriched, debug_logs, runtime_vars):
                matched_template = tmpl
                break
            all_debug_logs[tmpl.template_name] = debug_logs
        except (json_mod.JSONDecodeError, Exception) as e:
            all_debug_logs[tmpl.template_name] = [f"JSON Parse Error: {e}"]
            continue

    if not matched_template and fallback_templates:
        matched_template = fallback_templates[0]

    if not matched_template:
        receipt_debug_logs = {}
        if allow_receipt_fallback and bill.deal_log_id:
            receipt_result = _preview_voucher_for_bill_via_receipt_templates(
                bill=bill,
                enriched_bill=enriched,
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
            )
            if receipt_result and receipt_result.get("matched"):
                return receipt_result
            if receipt_result:
                receipt_debug_logs = receipt_result.get("debug_logs") or {}

        combined_debug_logs = dict(all_debug_logs)
        for template_name, logs in receipt_debug_logs.items():
            combined_debug_logs[f"[receipt_bills] {template_name}"] = logs

        return {
            "matched": False,
            "message": "No applicable voucher template matched",
            "matched_root_source": "bills",
            "matched_via_receipt": False,
            "bill_summary": _build_bill_summary_payload(bill),
            "bill_data": enriched,
            "templates_checked": len(templates),
            "debug_logs": combined_debug_logs,
            "selected_bills": source_bills,
            "selected_bill_push_summary": source_bill_push_summary,
            "source_bills": source_bills,
            "source_bill_push_summary": source_bill_push_summary,
            "push_blocked": push_blocked,
            "push_block_reason": push_block_reason,
        }

    # 4. Resolve template header expressions
    now = datetime.now()
    book_number = resolve_expr(matched_template.book_number_expr or "'BU-35256'")
    vouchertype_number = resolve_expr(matched_template.vouchertype_number_expr or "'0001'")
    attachment = resolve_expr(matched_template.attachment_expr or "0")
    period_number = now.strftime("%Y%m")
    biz_date = resolve_expr(matched_template.bizdate_expr or "{CURRENT_DATE}") or now.strftime("%Y-%m-%d")
    booked_date = resolve_expr(matched_template.bookeddate_expr or "{CURRENT_DATE}") or now.strftime("%Y-%m-%d")

    def parse_attachment_count(value: str) -> int:
        try:
            return max(int(float(value)), 0)
        except (TypeError, ValueError):
            return 0

    # 5. 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鈧懓瀚崳纾嬨亹閹烘垹鍊為悷婊冪箻瀵娊鏁冮崒娑氬幗闂侀潧绻堥崺鍕倿閸撗呯＜闁归偊鍙庡▓婊堟煛瀹€鈧崰鏍嵁瀹ュ鏁婄痪鎷岄哺濮ｅ姊绘担渚劸妞ゆ垶鍨归幑銏犫攽閸♀晛娈ㄩ梺鍓插亝濞叉牠鏌嬮崶銊﹀弿婵妫楅獮妤呮煟濠靛洦鈷掔紒杈ㄦ尰閹峰懘鎮剧仦鐣屽闂備胶顭堥敃銉ッ哄┑瀣€堕柛鎰靛枟閳锋垿鏌熺粙鎸庢崳缂佺姵鎹囬弻鐔煎礃閺屻儱寮伴悗娈垮枟婵炲﹪骞冨▎鎾村€绘俊顖滃帶楠炲牆鈹戦悩鍨毄濠殿喖顕埀顒佸嚬閸欏啫顕ｉ幎绛嬫晢闁告洦鍓涢崢鎼佹倵閸忓浜鹃柣搴秵閸撴盯寮抽悩缁樼叄濞村吋鐟х粔顕€鏌″畝瀣埌妞ゎ偅绻堥、妤佸緞婵犲喚鍟€缂傚倸鍊风拋鏌ュ磻閹炬枼鏀介柣妯哄级婢跺嫰鏌嶉柨瀣伌闁哄本鐩弫鍌滄嫚閹绘帞顔愰梻浣告啞閺屻劑顢栭崱娆愬床婵炴垯鍨归惌妤€顭跨捄铏圭伇闁绘挸顦靛铏规嫚閳ヨ櫕鐏堝┑鐐点€嬬换婵嗙暦濞差亜鐒垫い鎺嶉檷娴滄粓鏌熼悜妯虹仴妞ゅ浚浜弻宥夋煥鐎ｎ亞浼岄梺鍝勬湰缁嬫垿鍩為幋锕€骞㈡俊銈咃梗閹綁姊绘担鍛婃儓婵☆偅顨婇弫鍐敂閸繆鎽曞┑鐐村灦閸╁啴宕戦幘缁樻櫜閹肩补鈧尙鍑归柣搴ゎ潐濞叉牠鎯岄崒鐐茶摕闁炽儱纾弳鍡涙倵閿濆骸澧伴柡鍡愬€濋幃妤冩喆閸曨剛顦ㄩ柣銏╁灡鐢繝鏁愰悙娴嬫斀閻庯綆鍋勬禍妤呮煙閼测晞藟闁逞屽墯閸撴艾螞閹达附鈷掗柛灞捐壘閳ь剟顥撶划鍫熺瑹閳ь剟鐛弽顓ф晝闁挎洍鍋撻悗姘槹閵囧嫰骞掗幋婵愪痪闂佺楠哥€涒晠濡甸崟顖氬唨妞ゆ劦婢€閹寸兘鎮楃憴鍕矮缂佽埖宀稿濠氭偄閸忕厧鈧粯鎱ㄥΔ鈧Λ娆撴偩閸撲胶纾藉ù锝呮惈椤庢挾绱撳鍕獢鐎殿喖顭烽弫鎰緞婵犲嫷鍚呮繝鐢靛█濞佳兠归崒姣兼盯鍨鹃幇浣瑰瘜闂侀潧鐗嗛崯顐﹀礉閻㈢數纾奸柤鑹板煐绾埖淇婇崣澶婂妤犵偞顭囬幏鐘绘嚑椤掑﹦搴婂┑鐘愁問閸犳鏁冮埡鍛婵せ鍋撶€规洘鍨块獮妯兼嫚闊厾鐐婇梻渚€娼ч敍蹇涘川椤栨艾鑴梻鍌欐祰椤曆囨煀閿濆拋鐒界憸鏃堢嵁濡も偓椤劑宕奸悢宄板闂備胶鎳撴晶鐣屽垝椤栫偞鍋傞柡鍥╁枂娴滄粓鏌熼弶鍨暢缂佸宕电槐鎺楀焵椤掑嫬鐒垫い鎺戝€荤壕钘壝归敐鍛儓閸熸悂姊洪崫銉バｉ柣妤冨Т椤曪絾绻濆顓炰簻闂佹儳绻愬﹢閬嶆晬濠婂牊鈷戦梻鍫熺〒缁犲啿鈹戦鐐毈闁诡喗锕㈠畷濂稿閵忣澁绱冲┑鐐舵彧缁叉崘銇愰崘鈺冾洸濡わ絽鍟悡鐔煎箹閹碱厼鐏ｇ紒澶屾暬閺屾稓鈧絺鏅濋崝宥囩磼閸屾氨孝妞ゎ厹鍔戝畷濂告偄閸濆嫬绠ラ梻鍌欑窔閳ь剚绋撶粊閿嬬箾閸涱喗绀€闁宠绉瑰畷銊р偓娑欘焽閸樹粙妫呴銏＄カ缂佽弓绮欏绋库槈閵忊槅姊?
    accounting_entries = []
    kingdee_entries = []

    # Prepare subject naming cache
    subject_names_cache = {}
    subject_type_cache = {}
    expression_context = dict(enriched)

    for rule in sorted(matched_template.rules, key=lambda r: r.line_no):
        visible, rule_selected_records = _evaluate_rule_display_condition(
            rule.display_condition_expr,
            enriched,
            runtime_vars,
        )
        if not visible:
            continue
        rule_expression_context = _merge_selected_record_values(expression_context, rule_selected_records)
        summary = resolve_expr(rule.summary_expr, rule_expression_context)
        account_code = _normalize_literal_account_code(rule.account_code) or (rule.account_code or "").strip()
        amount_str = resolve_expr(rule.amount_expr, rule_expression_context)
        currency = resolve_expr(rule.currency_expr or "'CNY'", rule_expression_context)
        localrate = resolve_expr(rule.localrate_expr or "1", rule_expression_context)

        # Fetch subject name for display
        account_display_name = account_code
        if account_code:
            if account_code not in subject_names_cache:
                subj = db.query(models.AccountingSubject).filter(models.AccountingSubject.number == account_code).first()
                if subj:
                    # Use fullname if available, else name
                    subject_names_cache[account_code] = subj.fullname or subj.name
                    subject_type_cache[account_code] = subj.account_type_number or ""
                else:
                    subject_names_cache[account_code] = account_code
                    subject_type_cache[account_code] = ""
            
            fullname = subject_names_cache[account_code]
            if fullname != account_code:
                account_display_name = f"{account_code} {fullname}"

        amount_val = _try_parse_decimal(amount_str) or Decimal("0")
        localrate_val = _try_parse_decimal(localrate) or Decimal("1")

        # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鈧懓瀚崳纾嬨亹閹烘垹鍊為悷婊冪箻瀵娊鏁冮崒娑氬幗闂侀潧绻堥崺鍕倿閸撗呯＜闁归偊鍙庡▓婊堟煛瀹€鈧崰鏍嵁瀹ュ鏁婄痪鎷岄哺濮ｅ姊绘担渚劸妞ゆ垶鍨归幑銏犫攽閸♀晛娈ㄩ梺鍓插亝濞叉牠鏌嬮崶銊﹀弿婵妫楅獮妤呮煟濠靛洦鈷掔紒杈ㄦ尰閹峰懘鎮剧仦鐣屽闂備胶顭堥敃銉ッ哄┑瀣€堕柛鎰靛枟閳锋垿鏌熺粙鎸庢崳缂佺姵鎹囬弻鐔煎礃閺屻儱寮伴悗娈垮枟婵炲﹪骞冨▎鎾村€绘俊顖滃帶楠炲牆鈹戦悩鍨毄濠殿喖顕埀顒佸嚬閸欏啫顕ｉ幎绛嬫晢闁告洦鍓涢崢鎼佹煟韫囨洖浠╂い鏇嗗洤鐒垫い鎺嶈兌缁犵偤鏌ｅ☉鍗炴灍缂佹鍠栭崺鈧い鎺戝瀹撲線鏌熼悜姗嗘當缂佺媴绲剧换婵嬫濞戞瑱绱炲┑鐐殿儠閸庡磭妲愰幘璇茬＜婵﹩鍏橀崑鎾诲箹娴ｅ摜锛欓梺鍛婄缚閸庢娊鎯岄幘缁樼厸濠㈣泛顑呭▓顔界箾瀹割喕鎲鹃柡浣告喘閺屾洝绠涢弴鐐愵剟鏌熼懞銉︾闁宠鍨块幃娆撳级閹寸姳妗撻梺鑹帮骏閸婃牗绌辨繝鍥х闁圭儤鏌ㄩ埅鐢告⒑閸濆嫭婀扮紒瀣灱閻忓啴姊洪崨濠傚闁告柨顑囬崚鎺楀礈娴ｈ櫣锛濇繛杈剧到閹碱偄鐡紓鍌氬€哥粔鎾晝椤忓牏宓侀柛鎰典簼瀹曞銆掑鐓庣仭闁稿秶鏁诲娲川婵犲孩鐣奸梺绋款儐閸旀瑥顕ｉ幖浣稿窛妞ゆ挻绋掗弬鈧俊鐐€栧濠氬Υ鐎ｎ喖缁╃紓浣姑肩换鍡涙煟閹邦垰鐓愭い銉ヮ樀閺岋綁鏁愰崶褍骞嬪銈冨灪濞茬喖寮崘顔肩劦妞ゆ帒鍊婚惌鍡涙倵閿濆骸浜栧ù婊勭矒閺岀喖鎮欓鈧晶顖涗繆閹绘帞鍩ｉ柡灞剧洴婵℃悂濡堕崶鈺冨幆闁诲孩顔栭崰鏍€﹂悜钘夌畺闁靛繈鍊曠粈鍌炴煟閹惧啿顒㈤柣銈呮嚇濮婄粯绗熼埀顒€顭囪閺佸秷绠涘☉妯虹獩濡炪倖鐗楅懝鐐綇閸涘瓨鈷?
        assgrp = {}
        if rule.aux_items:
            try:
                aux_obj = json_mod.loads(rule.aux_items)
                for dim_key, dim_config in aux_obj.items():
                    assgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        assgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json_mod.JSONDecodeError, Exception):
                pass

        # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鈧懓瀚崳纾嬨亹閹烘垹鍊為悷婊冪箻瀵娊鏁冮崒娑氬幗闂侀潧绻堥崺鍕倿閸撗呯＜闁归偊鍙庡▓婊堟煛瀹€鈧崰鏍嵁瀹ュ鏁婄痪鎷岄哺濮ｅ姊绘担渚劸妞ゆ垶鍨归幑銏犫攽閸♀晛娈ㄩ梺鍓插亝濞叉牠鏌嬮崶銊﹀弿婵妫楅獮妤呮煟濠靛洦鈷掔紒杈ㄦ尰閹峰懘鎮剧仦鐣屽闂備胶顭堥敃銉ッ哄┑瀣€堕柛鎰靛枟閳锋垿鏌熺粙鎸庢崳缂佺姵鎹囬弻鐔煎礃閺屻儱寮伴悗娈垮枟婵炲﹪骞冨▎鎾村€绘俊顖滃帶楠炲牆鈹戦悩鍨毄濠殿喖顕埀顒佸嚬閸欏啫顕ｉ幎绛嬫晢闁告洦鍓涢崢鎼佹煟韫囨洖浠╂い鏇嗗洤鐒垫い鎺嶈兌缁犵偤鏌ｅ☉鍗炴灍缂佹鍠栭崺鈧い鎺戝瀹撲線鏌熼悜姗嗘當缂佺媴绲剧换婵嬫濞戞瑧鍘愰梺纭呮彧闂勫嫰鍩涢幒鎳ㄥ綊鏁愰崨顔藉枑闂佹寧绋戠粔鍓佹閹烘鏁嬮柛娑卞幘娴犫晠鏌ч懡銈呬槐闁哄本鐩獮鍥濞戞瑧浜梻浣芥閸熶即宕伴弽顓炶摕闁靛ň鏅滈崑鍕煕濠靛棗鐝旈柕蹇嬪灮绾惧ジ鏌嶈閸撴盯鍩€椤掑﹦绉甸柛瀣╃劍缁傚秴顭ㄩ崼鐔哄幍闂侀€涚祷濞呮洖鈻嶉崨瀛樼厓鐟滄粓宕滃韬测偓鍐╃節閸パ呯暫闂佺鍕垫當缂佲偓鐎ｎ偁浜滈柡宥冨妿閳绘捇鏌熼柨瀣仢婵﹦绮粭鐔煎炊瑜岀花浠嬫⒑缁嬫鍎愰柣鈺婂灦楠炲﹪鎮㈢喊杈ㄦ櫖濠电偞鍨堕悷锕傛偟瀹勯偊娓婚柕鍫濇绾剧敻鏌涚€ｎ偅宕岄柡宀嬬磿娴狅箓宕滆閸掓盯姊虹拠鈥虫灁闁搞劏妫勯悾鐑藉Ω閿斿墽鐦堥梺鍛婃处閸樻悂寮歌箛鎾斀闁挎稑瀚禍濂告煕婵犲啰澧悡銈夋煃閸濆嫬鏆欑紒韬插€濋弻娑㈠箻濡も偓閹虫劙鎮為崸妤佲拺闁革富鍘奸崝瀣煛瀹€瀣瘈鐎规洘鍨块獮妯肩磼濡粯鐝抽梺鍦帶閻°劎鎹㈤崟顖涘殑闁惧繐婀辩壕钘壝归敐鍥剁劸闁肩缍婇弻锝夊冀椤愩垹浠樺銈庡亜缁绘劗鍙呭銈呯箰閹峰螞閸愩劉鏀介柣鎰綑閻忋儳鐥紒銏犲箻婵炴垹鏁婚崺鈧い鎺戝閳锋帒霉閿濆懏鍟為柛鐔哄仦缁绘稓鎷犺閻ｇ數鈧娲樼划宀勫煡婢跺á?
        maincfassgrp = {}
        if rule.main_cf_assgrp:
            try:
                mcf_obj = json_mod.loads(rule.main_cf_assgrp)
                for dim_key, dim_config in mcf_obj.items():
                    maincfassgrp[dim_key] = {}
                    for prop, expr in dim_config.items():
                        maincfassgrp[dim_key][prop] = resolve_expr(str(expr), rule_expression_context)
            except (json_mod.JSONDecodeError, Exception):
                pass

        # 婵犵數濮烽弫鍛婃叏閻戣棄鏋侀柛娑橈攻閸欏繘鏌ｉ幋锝嗩棄闁哄绶氶弻鐔兼⒒鐎靛壊妲紒鐐劤椤兘寮婚敐澶婃婵炲棛鍋撶粊鍙夌節閵忥絽鐓愰柛鏃€娲滅划璇测槈閵忥紕鍘告繝銏ｆ硾鐎涒晝娑甸崼鏇熺叆婵炴垶鐟уú瀛樻叏婵犲洨绱伴柕鍥ㄥ姍楠炴帡骞嬪鍐╃€抽梻鍌欑閹诧紕婀佺紓渚囧枟閻熴儵鎮鹃悜鑺ユ櫜闁割偁鍨婚弶鎼佹⒑閻熸壆浠㈤柛鐕佸灣閳ь剟娼ч惌鍌炲蓟閿濆棙鍎熸い鏍ㄧ矌鏍″┑鐐茬摠缁姵绂嶉鍫涒偓渚€骞樺鍕瀹曘劑顢欓幆褍绠為梻鍌欑窔濞佳団€﹂鐔剁箚闁搞儮鏅濇稉宥夋煟閹邦喖鍔嬮柣鎾寸〒閳ь剙鍘滈崑鎾绘倵閿濆骸澧扮悮锔戒繆閵堝洤啸闁稿鐩、鏍ㄥ緞閹邦剛鐣哄┑鈽嗗灠閵堜粙鎼圭憴鍕€涢梺瑙勫劶濡嫭绂掗幇顓濈箚闁绘劦浜滈埀顒佸灴瀹曞綊鎼归悷鐗堢€抽悗骞垮劚閹峰鎮炴禒瀣彄闁搞儯鍔庨埥澶愬箚閻斿吋鈷戦柟绋垮绾剧敻鏌涚€ｎ偅灏甸柍褜鍓氶鏍窗閺嶎厽鍊舵繝闈涱儏閻撴﹢鏌熸潏鍓х暠闁绘搫绻濋弻娑㈠焺閸愮偓鐣兼繛瀵稿У濞兼瑩鈥旈崘顔嘉ч柛鈩冾焾閸嬩線姊洪崨濠冨闁告挻宀搁幃闈涚暋闁附瀵岄梺闈涚墕濡稒鏅堕柆宥嗙厱閻庯綆鍓欐禒閬嶆煙椤曞棛绡€鐎殿喗鎸抽幃銏㈢矙閸喕绱熷┑鐘愁問閸犳銆冮崨瀛樺亱闊洦绋戠粈鍡涙煛婢跺绱╅柣鐔煎亰閻撱儵鏌涢鐘茬伄闁哄棭鍋勯埞鎴︻敊绾嘲浼愬銈庡幖閸㈡煡鎮鹃悜钘夌闁瑰瓨姊归悗濠氭⒑鐟欏嫭鍎楅柛妯衡偓鐔插徍闂傚倸鍊搁崐鐑芥嚄閸洏鈧焦绻濋崶鑸垫櫔濠电姴锕ら幊蹇撶暦閸欏绡€闂傚牊绋掗ˉ鐘绘煛閸☆參妾紒缁樼☉椤斿繘顢欓懡銈囨晨闂傚倷绀佸畷顒€煤椤撱垹钃熼柡鍥╁枎缁剁偤鏌涢锝囩畵濠殿喓鍨藉铏圭磼濡闉嶉梺鎼炲妼閻忔繈锝炶箛鎾佹椽顢旈崨顖氬Х闂備胶绮崝妯间焊椤忓棌鍋撳顓熺凡妞ゎ叀鍎婚ˇ鎶芥煙閸涘﹤鈻曠€殿喖顭烽崹鎯х暦閸ャ劍鐣烽梺璇插嚱缂嶅棝宕滃☉婧惧徍闂傚倸鍊峰ù鍥ь浖閵娧呯焼濞撴埃鍋撶€规洦鍨堕獮姗€顢欓崗鍏夹氶梻渚€鈧偛鑻晶顖炴煏閸パ冾伃妤犵偞甯￠獮瀣攽閹般劌浜炬慨妯垮煐閻撴洟鏌曟径妯烘灈濠⒀屽灡閵囧嫰濡烽妷褍顤€缂備胶绮惄顖炵嵁鐎ｎ亖鏀介柛鈩冭壘閻撴ɑ绻濋悽闈涗粶闁绘瀚板畷婵嗩吋閸ャ劌搴婂┑鐐村灦閸╁啴宕戦幘缁樺仺婵炲牐娉曢崝鎼佹倵濞戞瑧绠撴い顏勫暣婵″爼宕卞Δ鍐ф樊婵犵妲呴崑鍛崲閸岀偞鍋╅柣鎴ｆ缁狅綁鏌ㄩ弴妤€浜剧紒鐐劤閵堟悂寮婚敐澶婄疀妞ゆ牗姘ㄥВ銏狀渻閵堝棙鈷愰柛鏃€鐟╁濠氬焺閸愩劎绐炴繝鐢靛Т鐎氼剟銆傞悜妯肩瘈闁冲皝鍋撻柛灞剧矌閻撴挸螖閻橀潧浠滈柛鐔告綑閻ｅ嘲螖閸愵亞鐣堕柡澶屽仦婢瑰棝鎯?
        accounting_entries.append({
            "line_no": rule.line_no,
            "summary": summary,
            "account_code": account_code,
            "account_name": subject_names_cache.get(account_code, ""),
            "account_type_number": subject_type_cache.get(account_code, ""),
            "account_display": account_display_name,
            "dr_cr": rule.dr_cr,
            "debit": _json_number(amount_val) if rule.dr_cr == 'D' else 0.0,
            "credit": _json_number(amount_val) if rule.dr_cr == 'C' else 0.0,
            "debit_exact": _decimal_text(amount_val) if rule.dr_cr == "D" else "0",
            "credit_exact": _decimal_text(amount_val) if rule.dr_cr == "C" else "0",
            "currency": currency,
            "localrate": _json_number(localrate_val),
            "localrate_exact": _decimal_text(localrate_val),
            "assgrp": assgrp if assgrp else None,
            "maincfassgrp": maincfassgrp if maincfassgrp else None,
        })

        # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻鐔兼⒒鐎靛壊妲紒鐐劤缂嶅﹪寮婚悢鍏尖拻閻庨潧澹婂Σ顔剧磼閹冣挃闁硅櫕鎹囬垾鏃堝礃椤忎礁浜鹃柨婵嗙凹缁ㄥジ鏌熼惂鍝ョМ闁哄矉缍侀、姗€鎮欓幖顓燁棧闂傚倸娲らˇ鐢稿蓟閵娿儮鏀介柛鈩兠▍锝咁渻閵堝啫鍔氱紒缁橈耿瀵鈽夐姀鐘栄囨煕閳╁喚鐒芥い锔垮嵆濮婃椽宕崟顒佹嫳闂佺儵鏅╅崹鍫曞Υ娓氣偓瀵挳濮€閳╁啯鐝抽梻浣虹《閸撴繈鎮烽姣硷綁顢楅崟顑芥嫽婵炶揪缍€椤宕戦悩缁樼厱闁哄倽娉曢悞鍝モ偓瑙勬礃閸旀瑥鐣风粙璇炬棃鍩€椤掑嫭瀚呴柣鏂垮悑閻撱儲绻濋棃娑欙紞婵℃彃鎽滅槐鎺楁偐瀹割喚鍚嬮梺鍝勬湰閻╊垶鐛崶顒夋晬婵椴搁妤佺節瀵版灚鍊曠槐锕傛煕濡も偓閸熷潡锝?API JSON 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻鐔兼⒒鐎靛壊妲紒鎯у⒔閹虫捇鈥旈崘顏佸亾閿濆簼绨奸柟鐧哥秮閺岋綁顢橀悙鎼闂侀潧妫欑敮鎺楋綖濠靛鏅查柛娑卞墮椤ユ艾鈹戞幊閸婃鎱ㄩ悜钘夌；闁绘劗鍎ら崑瀣煟濡崵婀介柍褜鍏涚欢姘嚕閹绢喖顫呴柣妯荤垹閸ャ劎鍘遍梺闈涱槶閸ㄥ搫鈻嶉崶顒佺厱婵☆垳鍘ч埢鍫熸叏婵犲嫮甯涢柟宄版噽缁瑥鈻庨悙顒夋闂傚倷鑳堕…鍫ヮ敄閸涙潙绠犻幖杈剧到瀵煡姊绘担鍛婃儓缂佸绶氬畷銏ゆ嚃閳轰緡妫滄繝闈涘€搁幉锟犳偂韫囨挴鏀介柣鎰灥閸燁偊顢旈埡鍛厸閻庯綆鍋勯悘瀛樹繆椤愩垺鍤囨い銏℃礋婵偓闁炽儲鍓氬Σ閬嶆⒒?
        kd_entry = {
            "seq": rule.line_no,
            "edescription": summary,
            "account_number": account_code,
            "currency_number": currency,
            "localrate": _decimal_text(localrate_val),
            "debitori": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "creditori": "0.00" if rule.dr_cr == "D" else _money_text(amount_val),
            "debitlocal": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "creditlocal": "0.00" if rule.dr_cr == "D" else _money_text(amount_val),
        }
        if assgrp:
            kd_entry["assgrp"] = assgrp
        if maincfassgrp:
            kd_entry["maincfassgrp"] = maincfassgrp
        kingdee_entries.append(kd_entry)

    _normalize_voucher_money_fields(accounting_entries, kingdee_entries)
    total_debit = sum((_entry_decimal(e, "debit_exact", "debit")) for e in accounting_entries)
    total_credit = sum((_entry_decimal(e, "credit_exact", "credit")) for e in accounting_entries)

    # 6. 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻鐔兼⒒鐎靛壊妲紒鐐劤缂嶅﹪寮婚悢鍏尖拻閻庨潧澹婂Σ顔剧磼閻愵剙鍔ょ紓宥咃躬瀵鎮㈤崗灏栨嫽闁诲酣娼ф竟濠偽ｉ鍓х＜闁诡垎鍐ｆ寖闂佺娅曢幑鍥灳閺冨牆绀冩い蹇庣娴滈箖鏌ㄥ┑鍡欏嚬缂併劌銈搁弻鐔兼儌閸濄儳袦闂佸搫鐭夌紞渚€銆佸鈧幃娆撳箹椤撶噥妫ч梻鍌氬€稿ú銈壦囬悽绋胯摕闁靛鍎弨浠嬫煕閳╁啰鎳冩い锝呯埣濮婃椽鏌呴悙鑼跺闁告ɑ鎸抽弻娑氣偓锝庡亝瀹曞本鎱ㄦ繝鍛仩缂侇喗鐟ラ埢搴ㄥ箚瑜嶆竟澶愭⒒娴ｇ儤鍤€闁硅绻濋獮鍐磼閻愬瓨娅滈梺缁樺姈缁佹挳寮ㄦ禒瀣€甸柨婵嗛娴滄粌霉濠婂嫷娈滈柡宀€鍠栭幊婵嬫偋閸繃閿紓鍌欐祰鐏忔瑩宕㈣閵嗗懏绺界粙璺啋缂傚倷鐒﹁彜闁归绮换娑欐綇閸撗勫仹闂佺儵鍓濆Λ鍐ㄧ暦閻㈢绀冩い鏃傛櫕閸橀亶姊虹紒妯曟垿宕滃顑芥灁婵犲﹤鐗婇悡蹇涙煕閳╁喚娈旈柡鍡欏仧缁辨帞绱掑Ο鑲╃杽閻庤娲橀崕濂杆囬幘顔界厽闁挎繂妫欓妵婵囨叏婵犲懏顏犵紒杈ㄥ笒铻ｉ柧蹇涒偓娑氱婵犵數濮甸鏍窗閹捐纾规繝闈涙閺嗭箓鏌熺€电袥闁稿鎹囬弫鎰償閳╁啰浜堕梻浣虹帛閹歌煤濮椻偓婵＄敻宕熼姘辩杸闂佸疇妗ㄧ拋鏌ュ磻閹捐鍗抽柕蹇曞Т閸ゆ垿姊虹涵鍛涧闂傚嫬瀚板畷鎰板垂椤愶絽寮垮┑顔筋殔濡鐛弽顓熺厓缂備焦蓱缁€鍐煙娓氬灝濡界紒缁樼箞瀹曠喖顢橀悙娈垮仹闂傚倷鑳堕幊鎾诲床閺屻儱绠犳俊顖濇閺嗭箓鏌曟繝蹇擃洭缂佲檧鍋撻梻浣告啞閸旀垿宕濈仦鍓х彾鐎广儱鎳夐弨浠嬫煟濡椿鍟忛柡鍡╁灡娣囧﹪骞撻幒鎾虫畻閻庤娲橀崹鍨暦閻旂⒈鏁嶉柨婵嗘濞呮梹淇婇悙顏勨偓鏍箰閻愵剚鍙忛柣銏ゆ交缂嶆牠鏌￠崶銉ョ仾闁绘挾鍠栭弻鐔兼焽閿曗偓婢у鏌涢妶鍥ф灈闁哄矉缍侀幃銏☆槹鎼达及銊╂⒑閸濆嫯瀚扮紒澶屽厴绡撳〒姘ｅ亾闁哄本鐩獮妯兼崉閻戞浜梻浣筋嚃閸犳洟宕￠幎濮愨偓浣割潩鐠鸿櫣鍔﹀銈嗗坊閸嬫捇鏌ｉ敐鍥у幋鐎规洩绻濋幃娆撳煛閸屻倖缍屽┑鐘殿暯濡插懘宕归幎钘夌厱闁割偅鎯婇敐澶嬪亱闁割偅绮庣粻姘舵⒑缂佹ê濮﹀ù婊勭矒閸┾偓妞ゆ帊鑳舵晶顏堟懚閻愬眰鈧帒顫濋敐鍛闁诲氦顫夊ú姗€宕归崸妤冨祦闁搞儺鍓欑粈鍌涖亜閹扳晛鐏柛鐘愁焽閳ь剚顔栭崰娑樷枖濞戙垹鐓濋幖娣妽閸婇攱銇勯幒鍡椾壕闂佷紮闄勭划鎾愁潖缂佹鐟归柍褜鍓欓…鍥樄闁诡啫鍥у耿婵＄偑鍨虹粙鎴﹀煡婢跺ň鏋庨柟閭﹀幘瑜版寧淇婇悙顏勨偓鏍暜閹烘纾归悹鍥у棘濞戙埄鏁嶉柣鎰嚟閸樺憡绻濋姀锝嗙【閻庢稈鏅濆☉鐢稿醇閺囩偞鐎梺鍓插亝缁诲秴銆掓繝姘厪闁割偅绻傞弳娆撴煟韫囷絼绨煎ǎ鍥э躬椤㈡洟濮€閻樿櫕顔夊┑鐑囩到濞层倝鏁冮鍫濈畺婵犲﹤鐗婄€电姴顭跨捄楦垮妤犵偞鍔欏缁樻媴娓氼垳鍔搁梺鍝勭墱閸撶喖濡撮崘顔煎耿婵炲棗鑻禍鐐叏濮楀棗鍘甸柛瀣ㄥ灪閹便劍绻濋崟顓炵闂佺懓鍢查幊鎰垝閻㈢鍋撻敐搴′簽缂佸鍏樺缁樻媴娓氼垳鍔搁柣搴㈢▓閺呮粎鎹㈠☉娆戠瘈闁搞儮鏅涚粊锔界節閻㈤潧孝婵炲眰鍊楃划璇测槈閵忥紕鍘藉┑掳鍊愰崑鎾绘煟濡も偓濡瑧绮嬮幒鎾堕檮闁告稑艌閹锋椽鏌ｉ悢鍝ユ噧閻庢凹鍓熷畷婵堟崉鐞涒剝鏂€濡炪倖鏌ㄩ幖顐﹀焵椤掍胶绠炴鐐插暣閺佹捇鎮╅幓鎺戠ギ闂備線娼ф蹇曟閺囥垹鍌ㄩ柟鍓х帛閳锋垿鏌涘☉姗堝姛缂佺姵鎹囬幃妤€顫濋悡搴♀拫闂佺硶鏂侀崑鎾愁渻閵堝棗绗掗柛濠呭吹閺侇喖鈽夐姀锛勫幐闂佸憡渚楅崢楣冨春閿濆鐓涢悘鐐跺Г椤ユ粍銇勯幘鐐藉仮鐎规洖宕埢搴ょ疀閹鹃鍞舵繝鐢靛Х閺佹悂宕戦悙鍝勭濠电姵纰嶉崵宀勬煙椤栵絿浜规繛宸簻鍥存繝銏ｆ硾閿曘倝宕甸幋锔解拺闁圭瀛╃粈鈧梺绋匡工缂嶅﹪骞冮檱缁犳稑鈽夊▎鎴濆箞闂備線娼ч¨鈧紒鑼跺Г娣囧﹪鎮℃惔妯绘杸濡炪倖妫侀崑鎰墡婵＄偑鍊戦崝濠囧磿閻㈢绠栨繛鍡樻尭缁狙囨煙鐎电浠╅柣褌鐒︽穱濠囧Χ閸ヮ灝銉╂煕鐎ｎ剙浠遍柍銉畵瀹曞爼顢楅埀顒傜不閺屻儲鐓曢柡鍥ュ妼閻忕姴霉閼测晛鈻堥柡?JSON 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻鐔兼⒒鐎靛壊妲紒鐐劤缂嶅﹪寮婚悢鍏尖拻閻庨潧澹婂Σ顔剧磼閻愵剙鍔ょ紓宥咃躬瀵鎮㈤崗灏栨嫽闁诲酣娼ф竟濠偽ｉ鍓х＜闁绘劦鍓欓崝銈囩磽瀹ュ拑韬€殿喖顭烽弫鎰緞婵犲嫷鍚呴梻浣瑰缁诲倿骞夊☉銏犵缂備焦顭囬崢杈ㄧ節閻㈤潧孝闁稿﹤缍婂畷鎴﹀Ψ閳哄倻鍘搁柣蹇曞仩椤曆勬叏閸屾壕鍋撳▓鍨灍闁瑰憡濞婇獮鍐ㄢ枎瀵版繂婀遍埀顒婄秵娴滄瑦绔熼弴銏♀拺闁告稑锕︾紓姘舵煕鎼淬倖鐝紒瀣槸椤撳吋寰勭€ｎ剙骞愬┑鐘灱濞夋盯鏁冮敃鈧～婵嬪Ω閳哄倻鍘搁梺閫炲苯澧紒鍌涘笧閳ь剨缍嗛崑鍡涘储閽樺鏀介柍钘夋閻忋儲绻涢崪鍐М闁轰礁绉撮濂稿幢閹邦亞鐩庨梻浣瑰缁诲倸螞濞戙垹鐭楅柍褜鍓熷?
    kingdee_json = {
        "data": [
            {
                "book_number": book_number,
                "bizdate": biz_date,
                "bookeddate": booked_date,
                "period_number": period_number,
                "vouchertype_number": vouchertype_number,
                "description": (
                    matched_template.template_name
                    or matched_template.template_id
                    or "UnnamedTemplate"
                ),
                "attachment": _parse_attachment_count(attachment),
                "entries": kingdee_entries,
            }
        ]
    }
    return {
        "matched": True,
        "matched_root_source": "bills",
        "matched_via_receipt": False,
        "template_id": matched_template.template_id,
        "template_name": matched_template.template_name,
        "bill_summary": _build_bill_summary_payload(bill),
        "enriched_fields": {k: v for k, v in enriched.items() if k.startswith('kd_')},
        "accounting_view": {
            "entries": accounting_entries,
            "total_debit": _json_number(total_debit),
            "total_credit": _json_number(total_credit),
            "total_debit_exact": _decimal_text(total_debit),
            "total_credit_exact": _decimal_text(total_credit),
            "is_balanced": (total_debit - total_credit) == Decimal("0") and (total_credit - total_debit) == Decimal("0"),
        },
        "kingdee_json": kingdee_json,
        "selected_bills": source_bills,
        "selected_bill_push_summary": source_bill_push_summary,
        "source_bills": source_bills,
        "source_bill_push_summary": source_bill_push_summary,
        "push_blocked": push_blocked,
        "push_block_reason": push_block_reason,
    }


@router.post("/api/vouchers/preview-bills")
def preview_voucher_for_bills(
    payload: schemas.BatchVoucherPreviewRequest,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    if not payload.bills:
        raise HTTPException(status_code=400, detail="No bills selected")

    unique_refs = _normalize_bill_refs(payload.bills)

    if not allowed_community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    allowed_set = set(allowed_community_ids)
    unauthorized = [r for r in unique_refs if int(r["community_id"]) not in allowed_set]
    if unauthorized:
        bad = ", ".join([f"{r['community_id']}:{r['bill_id']}" for r in unauthorized[:10]])
        raise HTTPException(status_code=403, detail=f"Unauthorized bill communities: {bad}")

    normalized_account_book_number = _decode_header_value(x_account_book_number) or None
    selected_status_map = _get_bill_push_status_map(
        db,
        unique_refs,
        account_book_number=normalized_account_book_number,
    )
    selected_bills = [
        selected_status_map[(ref["bill_id"], ref["community_id"])]
        for ref in unique_refs
    ]

    previews: List[Dict[str, Any]] = []
    skipped_bills: List[Dict[str, Any]] = []

    for ref in unique_refs:
        try:
            result = preview_voucher_for_bill(
                bill_id=int(ref["bill_id"]),
                community_id=int(ref["community_id"]),
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
                allowed_community_ids=allowed_community_ids,
            )
            if not result.get("matched"):
                skipped_bills.append({
                    "bill_id": int(ref["bill_id"]),
                    "community_id": int(ref["community_id"]),
                    "reason": "template not matched",
                })
                continue
            previews.append(result)
        except HTTPException as exc:
            skipped_bills.append({
                "bill_id": int(ref["bill_id"]),
                "community_id": int(ref["community_id"]),
                "reason": exc.detail if isinstance(exc.detail, str) else str(exc.detail),
            })

    if not previews:
        details = "; ".join([f"{b['community_id']}:{b['bill_id']} -> {b['reason']}" for b in skipped_bills[:20]])
        raise HTTPException(
            status_code=400,
            detail=("No vouchers could be generated" + (f": {details}" if details else ""))
        )

    # Merge across different templates; keep business date flexible so receipts from
    # different transaction days in the same batch can still produce a single voucher.
    first_preview = previews[0]
    first_header = ((first_preview.get("kingdee_json") or {}).get("data") or [{}])[0]
    header_keys = ["book_number", "bookeddate", "period_number", "vouchertype_number"]
    merged_bizdates = [
        str(first_header.get("bizdate") or "").strip()
    ]

    header_compatible_previews: List[Dict[str, Any]] = [first_preview]
    for p in previews[1:]:
        header = ((p.get("kingdee_json") or {}).get("data") or [{}])[0]
        incompatible_keys = [k for k in header_keys if first_header.get(k) != header.get(k)]
        if incompatible_keys:
            summary = p.get("bill_summary") or {}
            skipped_bills.append({
                "bill_id": int(summary.get("id") or 0),
                "community_id": int(summary.get("community_id") or 0),
                "reason": f"inconsistent voucher header ({', '.join(incompatible_keys)}); skipped from merge",
            })
            continue
        merged_bizdates.append(str(header.get("bizdate") or "").strip())
        header_compatible_previews.append(p)

    previews = header_compatible_previews
    merged_bizdate = max([d for d in merged_bizdates if d], default=str(first_header.get("bizdate") or ""))

    source_bills: List[Dict[str, Any]] = []
    seen_source_keys = set()
    for preview in previews:
        for source_bill in preview.get("source_bills") or []:
            key = (
                int(source_bill.get("bill_id") or 0),
                int(source_bill.get("community_id") or 0),
            )
            if key in seen_source_keys:
                continue
            seen_source_keys.add(key)
            source_bills.append(source_bill)

    selected_bill_push_summary = _summarize_bill_push_statuses(selected_bills)
    source_bill_push_summary = _summarize_bill_push_statuses(source_bills)
    push_conflicts = _find_bill_push_conflicts(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [
                f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})"
                for item in push_conflicts[:10]
            ]
        )
        push_block_reason = f"Selected bills already have pushed or pushing voucher records: {conflict_preview}"

    merged_entries: List[Dict[str, Any]] = []
    merged_accounting_entries: List[Dict[str, Any]] = []
    seq = 1

    for p in previews:
        kd_header = ((p.get("kingdee_json") or {}).get("data") or [{}])[0]
        kd_entries = kd_header.get("entries") or []
        for entry in kd_entries:
            e = dict(entry)
            e["seq"] = seq
            merged_entries.append(e)
            seq += 1

        acct_view = p.get("accounting_view") or {}
        acct_entries = acct_view.get("entries") or []
        for ae in acct_entries:
            entry = dict(ae)
            entry["line_no"] = len(merged_accounting_entries) + 1
            merged_accounting_entries.append(entry)

    total_debit = sum((_entry_decimal(e, "debit_exact", "debit")) for e in merged_accounting_entries)
    total_credit = sum((_entry_decimal(e, "credit_exact", "credit")) for e in merged_accounting_entries)

    merged_template_ids = sorted({str(p.get("template_id") or "") for p in previews if p.get("template_id")})
    template_name = first_preview.get("template_name") or first_preview.get("template_id") or "BatchMerged"
    merged_kingdee_json = {
        "data": [{
            "book_number": first_header.get("book_number"),
            "bizdate": merged_bizdate,
            "bookeddate": first_header.get("bookeddate"),
            "period_number": first_header.get("period_number"),
            "vouchertype_number": first_header.get("vouchertype_number"),
            "description": template_name,
            "attachment": first_header.get("attachment", 0),
            "entries": merged_entries,
        }]
    }

    return {
        "matched": True,
        "partial_matched": len(skipped_bills) > 0,
        "matched_bills": len(previews),
        "skipped_bills": skipped_bills,
        "template_id": first_preview.get("template_id"),
        "template_name": first_preview.get("template_name"),
        "template_ids": merged_template_ids,
        "selected_bills": selected_bills,
        "selected_bill_push_summary": selected_bill_push_summary,
        "source_bills": source_bills,
        "source_bill_push_summary": source_bill_push_summary,
        "push_blocked": push_blocked,
        "push_block_reason": push_block_reason,
        "accounting_view": {
            "entries": merged_accounting_entries,
            "total_debit": _json_number(total_debit),
            "total_credit": _json_number(total_credit),
            "total_debit_exact": _decimal_text(total_debit),
            "total_credit_exact": _decimal_text(total_credit),
            "is_balanced": (total_debit - total_credit) == Decimal("0") and (total_credit - total_debit) == Decimal("0"),
        },
        "kingdee_json": merged_kingdee_json,
    }


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
    """
    Push previewed Kingdee voucher JSON to configured external API.
    Default target: active ExternalApi matching voucherAdd endpoint.
    """
    import requests
    import time
    from urllib.parse import unquote
    from services.external_auth import ExternalAuthService

    if not isinstance(payload.kingdee_json, dict) or not payload.kingdee_json:
        raise HTTPException(status_code=400, detail="kingdee_json is required")
    _validate_voucher_json_amounts(payload.kingdee_json)

    api_record: Optional[models.ExternalApi] = None
    if payload.api_id is not None:
        api_record = db.query(models.ExternalApi).filter(
            models.ExternalApi.id == payload.api_id,
            models.ExternalApi.is_active == True
        ).first()
        if not api_record:
            raise HTTPException(status_code=404, detail=f"External API not found or inactive: id={payload.api_id}")
    else:
        api_record = db.query(models.ExternalApi).filter(
            models.ExternalApi.is_active == True,
            or_(
                models.ExternalApi.name == "闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鈧懓瀚崳纾嬨亹閹烘垹鍊炲銈嗗笒閿曪妇绮欒箛鏃傜瘈闁靛骏绲剧涵鐐亜閹存繃鍠橀柕鍡楁嚇楠炴捇骞戝Δ鈧紞濠囧箖閳轰緡鍟呮い鏃傚帶婢瑰牏绱撻崒娆掝唹闁稿鎸搁…鍧楁嚋闂堟稑顫嶉梺缁樻尰閻熲晛顕ｉ崼鏇為唶婵犻潧妫岄幐鍐⒑娴兼瑧绉ù婊冪埣瀵鏁愭径濠勵唺闂佺懓顕慨宕囧垝閻㈠憡鈷戦梻鍫熺⊕閹兼劙鎮楀顐㈠祮闁绘侗鍠氶埀顒婄秵閸犳寮查弻銉︾厱闁斥晛鍟伴幊鈧梺閫炲苯澧柨鏇濡叉劙骞樼€涙ê顎撻梺鑽ゅ枑濠㈡﹢锝炲鍛斀闁宠棄妫楁禍婊堟煕閻斿憡缍戞い鏇悼閹风姴霉鐎ｎ偒娼旈梻渚€娼х换鍡涘礈濠靛棌鏋嶆繝濠傚缁♀偓闂侀潧楠忕徊鍓ф兜妤ｅ啯鐓熸い鎺嗗亾闁靛牏顭堥锝夊箮缁涘鏅濋梺鎸庢琚欓柟鐤缁辨帞绱掗姀鐘茬闂佺懓鍟垮锕傘€傜捄銊х＝闁稿本鐟︾粊鐗堛亜閺囧棗娲﹂崑瀣煙閸撗呭笡闁稿蓱閵囧嫰寮村Δ鈧禍楣冩倵鐟欏嫭绀€鐎规洦鍓熼崺銉﹀緞婵炵偓鐎婚梺鐟扮摠缁诲倹顨欓梻鍌氬€搁崐鐑芥倿閿旈敮鍋撶粭娑樺幘閸濆嫷鍚嬪璺猴功閿涙盯姊洪悷鏉库挃缂侇噮鍨堕幃鈥斥槈濡繐缍婇弫鎰板川椤旇棄鏋戠紓鍌欑椤︽澘顪冩禒瀣摕闁炽儱纾弳鍡涙倵閿濆骸澧扮悮锕傛⒒娴ｇ瓔鍤冮柛鐘冲浮瀵煡鎮╅懠顒佹濠殿喗銇涢崑鎾搭殽閻愬瓨宕屾鐐村灴椤㈡洟妾遍柛蹇撶灱缁辨帗娼忛妸锕€闉嶉梺鐟板槻閹虫ê鐣烽锕€绀嬮柟鎼灣缁夘噣鏌＄仦鍓ф创濠碘剝鎮傛俊鐤槺闁惧繐閰ｅ鐑樺濞嗘垶鍋ч梺绋跨箲閿曘垽鐛崘鈺冾浄閻庯綆浜滅粣娑欑節閻㈤潧孝闁稿﹪顥撻懞杈ㄧ節濮橆厸鎷绘繛杈剧秬濞咃絿鏁☉銏＄厱闁靛鍎抽崺锝夋煛娴ｅ壊鍎旀慨?",
                models.ExternalApi.url_path.ilike("%/gl/gl_voucher/voucherAdd%"),
                models.ExternalApi.url_path.ilike("%gl_voucher/voucherAdd%"),
            )
        ).order_by(models.ExternalApi.id.asc()).first()
        if not api_record:
            raise HTTPException(
                status_code=404,
                detail="No active voucher push external API found. Please configure it in 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鈧懓瀚崳纾嬨亹閹烘垹鍊炲銈嗗笒閿曪妇绮欒箛鏃傜瘈闁靛骏绲剧涵鐐亜閹存繃鍠橀柕鍡楁嚇楠炴捇骞戝Δ鈧紞濠囧箖閳轰緡鍟呮い鏃傚帶婢瑰牏绱撴担鍝勪壕闁稿骸鍟块…鍥灳閹颁礁娈ㄩ梺鍓插亝濞叉牠鏌嬮崶銊ｄ簻闁规澘鐏氱欢姘辩磼閵娿儳鎽犵紒缁樼〒閳ь剚绋掗…鍥儗鎼搭潿浜滄い鎰╁焺濡叉椽鏌熼獮鍨仼闁宠棄顦垫慨鈧柣妯活問閸氬懘姊绘担铏瑰笡闁告梹娲熼、姘额敇閻愨晜鐏侀梺闈涚墕濡稓绮绘ィ鍐╃厵閻庣數顭堟禒锕傛倶韫囷絽骞樼紒杈ㄥ笚瀵板嫭绻濋崟顓夈劑鎮楀▓鍨灈妞ゎ參鏀辨穱濠囧箹娴ｈ倽銊╂煏韫囧﹥顫婃繛鍏兼⒐缁绘繄鍠婂Ο娲绘綉闂佸壊鐓堥崹鎶藉箯閸愵喗鏅滈柣锝呯焾濞村嫭淇婇妶蹇曞埌闁哥噥鍨堕崺?"
            )

    service = db.query(models.ExternalService).filter(models.ExternalService.id == api_record.service_id).first()
    if not service or not service.is_active:
        raise HTTPException(status_code=404, detail=f"External service not found or inactive: id={api_record.service_id}")

    account_book_id = _decode_header_value(x_account_book_id) or None
    account_book_name = _decode_header_value(x_account_book_name) or None
    account_book_number = _decode_header_value(x_account_book_number) or None
    tracked_refs = _normalize_bill_refs(payload.bills)
    push_batch_no = (
        f"VP{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:8].upper()}"
        if tracked_refs else None
    )
    request_payload_text = json.dumps(payload.kingdee_json, ensure_ascii=False)

    if tracked_refs:
        if not allowed_community_ids:
            raise HTTPException(status_code=403, detail="No authorized communities for this account book")

        allowed_set = set(allowed_community_ids)
        unauthorized = [
            ref for ref in tracked_refs
            if int(ref["community_id"]) not in allowed_set
        ]
        if unauthorized:
            preview = ", ".join([f"{ref['community_id']}:{ref['bill_id']}" for ref in unauthorized[:10]])
            raise HTTPException(status_code=403, detail=f"Unauthorized bill communities: {preview}")

        bill_conditions = [
            and_(
                models.Bill.id == ref["bill_id"],
                models.Bill.community_id == ref["community_id"],
            )
            for ref in tracked_refs
        ]
        locked_bills = db.query(models.Bill).filter(or_(*bill_conditions)).with_for_update().all()
        locked_keys = {(int(b.id), int(b.community_id)) for b in locked_bills}
        missing_refs = [
            ref for ref in tracked_refs
            if (int(ref["bill_id"]), int(ref["community_id"])) not in locked_keys
        ]
        if missing_refs:
            preview = ", ".join([f"{ref['community_id']}:{ref['bill_id']}" for ref in missing_refs[:10]])
            raise HTTPException(status_code=404, detail=f"Bills not found: {preview}")

        tracked_status_map = _get_bill_push_status_map(
            db,
            tracked_refs,
            account_book_number=account_book_number,
        )
        tracked_statuses = [
            tracked_status_map[(ref["bill_id"], ref["community_id"])]
            for ref in tracked_refs
        ]
        conflicts = _find_bill_push_conflicts(tracked_statuses)
        if conflicts and not payload.force_push:
            preview = ", ".join(
                [
                    f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})"
                    for item in conflicts[:10]
                ]
            )
            raise HTTPException(status_code=409, detail=f"Selected bills already pushed or pushing: {preview}")

        for ref in tracked_refs:
            db.add(models.BillVoucherPushRecord(
                bill_id=ref["bill_id"],
                community_id=ref["community_id"],
                push_batch_no=push_batch_no,
                push_status="pushing",
                account_book_id=account_book_id,
                account_book_name=account_book_name,
                account_book_number=account_book_number,
                api_id=api_record.id,
                api_name=api_record.name,
                pushed_by=current_user.id,
                message="Push request submitted",
                request_payload=request_payload_text,
            ))
        db.commit()

    org_name = current_user.organization.name if current_user.organization else "闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾剧懓顪冪€ｎ亝鎹ｉ柣顓炴閵嗘帒顫濋敐鍛婵°倗濮烽崑鐐烘偋閻樻眹鈧線寮撮姀鈩冩珕闂佽姤锚椤︻喚绱旈弴鐔虹瘈闁汇垽娼у瓭闂佹寧娲忛崐妤呭焵椤掍礁鍤柛锝忕秮婵℃挳宕ㄩ弶鎴犵厬婵犮垼娉涢惉濂告儊閸喓绡€闁汇垽娼у瓭闂佺锕︾划顖炲疾閸洖鍗抽柕蹇ョ磿閸橀亶姊洪棃娑辩劸闁稿酣浜堕崺鈧い鎺嗗亾婵炵》绻濋幃浼搭敋閳ь剙顕ｆ禒瀣р偓鏍Ψ閵夆晛寮板銈冨灪椤ㄥ﹪宕洪埀顒併亜閹哄秵顦风紒璇叉闇夐柣妯烘▕閸庢劙鏌ｉ幘璺烘灈闁哄瞼鍠撶槐鎺楀閻樺吀鍝楀┑?"
    user_context = {
        "current_user_id": str(current_user.id),
        "current_username": current_user.username,
        "current_user_realname": current_user.real_name or current_user.username,
        "current_org_id": str(current_user.org_id) if current_user.org_id else "",
        "current_org_name": org_name,
        "current_account_book_id": account_book_id or "",
        "current_account_book_name": account_book_name or "",
        "current_account_book_number": account_book_number or "",
    }

    auth = ExternalAuthService(db=db, service_record=service, user_context=user_context)
    token = auth.get_token()
    base_headers = auth.get_auth_headers()

    custom_headers: Dict[str, Any] = {}
    if api_record.request_headers:
        try:
            parsed_headers = json.loads(api_record.request_headers) if isinstance(api_record.request_headers, str) else api_record.request_headers
            if isinstance(parsed_headers, dict):
                custom_headers = resolve_dict_variables(parsed_headers, db, user_context=user_context)
        except Exception:
            custom_headers = {}

    def _merge_headers(token_value: str) -> Dict[str, str]:
        merged = {k: str(v) for k, v in (base_headers or {}).items()}
        for k, v in (custom_headers or {}).items():
            val = str(v)
            if "{access_token}" in val:
                val = val.replace("{access_token}", token_value)
            merged[k] = val
        return merged

    headers = _merge_headers(token)
    method = (api_record.method or "POST").upper()
    raw_path = (api_record.url_path or "").strip()
    if not raw_path:
        raise HTTPException(status_code=400, detail=f"External API {api_record.id} url_path is empty")

    if raw_path.startswith("http://") or raw_path.startswith("https://"):
        full_url = raw_path
    else:
        base = (service.base_url or "").strip()
        if base and raw_path and not base.endswith("/") and not raw_path.startswith("/"):
            full_url = f"{base}/{raw_path}"
        else:
            full_url = f"{base}{raw_path}"

    if not full_url:
        raise HTTPException(status_code=400, detail="External API url is empty")

    request_started = time.time()

    for attempt in range(2):
        try:
            if attempt > 0:
                auth.invalidate_token()
                db.commit()
                token = auth.get_token()
                headers = _merge_headers(token)

            req_kwargs: Dict[str, Any] = {"headers": headers, "timeout": 30}
            content_type = next((v for k, v in headers.items() if k.lower() == "content-type"), "").lower()
            if method == "GET":
                req_kwargs["params"] = payload.kingdee_json
            elif "application/x-www-form-urlencoded" in content_type:
                req_kwargs["data"] = payload.kingdee_json
            else:
                req_kwargs["json"] = payload.kingdee_json

            resp = requests.request(method, full_url, **req_kwargs)
            try:
                resp_data = resp.json()
            except Exception:
                resp_data = {"raw": resp.text}

            auth_failed = resp.status_code in [401, 602]
            if not auth_failed and isinstance(resp_data, dict):
                err_code = str(resp_data.get("errorCode") or resp_data.get("code") or "").strip()
                if err_code in ["401", "602"]:
                    auth_failed = True

            if auth_failed and attempt == 0:
                continue

            success = bool(resp.ok)
            message = "Push successful" if success else "Push failed"
            if isinstance(resp_data, dict):
                status_flag = resp_data.get("status")
                if status_flag is False:
                    success = False
                error_code = str(resp_data.get("errorCode") or "").strip()
                if error_code not in ("", "0", "None", "null"):
                    success = False
                data_obj = resp_data.get("data")
                if isinstance(data_obj, dict):
                    fail_count = str(data_obj.get("failCount") or "").strip()
                    if fail_count not in ("", "0", "None", "null"):
                        success = False

            binding = _extract_kingdee_voucher_result(resp_data)
            if binding.get("bill_status") is False:
                success = False

            message = _extract_kingdee_push_message(resp_data, message)
            response_payload_text = json.dumps(resp_data, ensure_ascii=False) if isinstance(resp_data, (dict, list)) else str(resp_data)

            if tracked_refs and push_batch_no:
                _finalize_bill_push_records(
                    db=db,
                    push_batch_no=push_batch_no,
                    push_status="success" if success else "failed",
                    message=message,
                    response_payload=response_payload_text,
                    voucher_number=binding.get("voucher_number"),
                    voucher_id=binding.get("voucher_id"),
                )
                tracked_status_map = _get_bill_push_status_map(
                    db,
                    tracked_refs,
                    account_book_number=account_book_number,
                )
                tracked_statuses = [
                    tracked_status_map[(ref["bill_id"], ref["community_id"])]
                    for ref in tracked_refs
                ]
            else:
                tracked_statuses = []

            duration_ms = round((time.time() - request_started) * 1000, 2)
            return {
                "success": success,
                "message": message,
                "status_code": resp.status_code,
                "duration_ms": duration_ms,
                "api_id": api_record.id,
                "api_name": api_record.name,
                "api_url": full_url,
                "push_batch_no": push_batch_no,
                "voucher_number": binding.get("voucher_number"),
                "voucher_id": binding.get("voucher_id"),
                "tracked_bills": tracked_statuses,
                "response": resp_data,
            }
        except Exception as exc:
            if attempt == 1:
                if tracked_refs and push_batch_no:
                    _finalize_bill_push_records(
                        db=db,
                        push_batch_no=push_batch_no,
                        push_status="failed",
                        message=str(exc),
                        response_payload=str(exc),
                    )
                raise HTTPException(status_code=502, detail=f"Push voucher request failed: {str(exc)}")

    raise HTTPException(status_code=502, detail="Push voucher request failed")


def _check_trigger_conditions(
    node: dict,
    data: dict,
    debug_logs: list = None,
    global_context: Optional[dict] = None,
    relation_context: Optional[dict] = None,
) -> bool:
    """闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻鐔兼⒒鐎靛壊妲紒鐐劤缂嶅﹪寮婚悢鍏尖拻閻庨潧澹婂Σ顔剧磼閹冣挃闁硅櫕鎹囬垾鏃堝礃椤忎礁浜鹃柨婵嗙凹缁ㄧ粯銇勯幒瀣仾闁靛洤瀚伴獮鍥敂閸℃瑧鍘梻浣告惈鐞氼偊宕濋幋锕€绠栭柕蹇嬪€曟导鐘绘煕閺囩喎鐏熼柛銊ヮ煼閹偓妞ゅ繐鐗嗙粻姘辨喐濠婂牊鍋傚┑鍌氭啞閻撴盯鎮橀悙鎻掆挃闁宠棄顦甸弻宥夋寠婢舵ɑ鈻堥悗瑙勬穿缁绘繈骞冨▎鎾崇闁告縿鍎查弳浼存⒒閸屾艾鈧绮堟笟鈧獮鏍敃閳惰姤绋戦埢搴ㄥ箣濠靛棛浜伴梻浣筋潐瀹曟﹢顢氳閹锋垿鎮㈤崗鑲╁幗闂佸搫鍊搁悘婵嬪箖閹达附鐓曞┑鐘插鐢稑菐閸パ嶈含妞ゃ垺绋戦…銊╁礃閵娿儛鏇㈡⒒娴ｅ憡璐￠柡灞筋槸閻ｇ兘鎮介崹顐綗闂佽鍎抽悺銊﹀垔閹绢喗鐓欑紓浣姑粭鍌氼熆鐟欏嫭绀€闁宠鍨块幃鈺呭垂椤愶絾鐦庡┑鐘愁問閸犳骞愰崘鑼殾闁规壆澧楅崑銊х磼鐎ｎ厽纭堕柛婵囶殕缁绘稓鈧數顭堝瓭濡炪倖鍨靛Λ婵嬪箖閿熺姵鍋勯梻鈧幇顔剧暰婵＄偑鍊栭悧妤€顫濋妸锔绢浄闁靛繈鍨荤壕濂告煙閹绘帩鍎戦柣锝囨暩閳ь剚顔栭崰鏍偉婵傜鏄ラ柨鐔哄Т缁€瀣煕椤垵浜炵紒澶愵棑缁辨捇宕掑▎鎺戝帯缂備緡鍣崹鍫曞箚閸曨垼鏁嶉柣鎰版涧缁侊箓鏌ｆ惔顖滅У濞存粍鐗犲鍛婃償閵娧冨絼闂佹悶鍎崝宥囧婵犳碍鐓欐い鏇炴缁夘喗鎱ㄦ繝鍕笡闁瑰嘲鎳橀幖褰掓偡閹殿噮鍋ч梻浣圭湽閸╁嫰宕规潏鈺傛殰闁跨喓濮寸粻鏍ㄤ繆椤栨瑨顒熸繛灏栨櫊閹娼幍顔拘梺鎸庢礀閸婂綊鎮￠弴銏＄厸闁搞儯鍎辨俊濂告煟韫囨洖啸缂佽鲸甯￠幃鈺呭礃濞村鐏嗛梻浣告惈閻ジ宕伴幘璇茬劦妞ゆ帊鑳堕埊鏇熴亜椤撶偞宸濈紒顔碱煼婵＄兘濡烽崘銊хШ闁诡喒鏅濇禒锕傚磼濮橆偄鍨濆┑鐘愁問閸犳牠鏁冮妷銉富濞寸姴顑呯粻鏉库攽閻樻彃鏆欑紒鍓佸仜閳规垿鎮╅幓鎺撴濡炪倖鏌ㄧ粔鐟邦潖缂佹ɑ濯撮柛娑橈攻閸犳劖绻濆▓鍨灓闁轰礁顭烽獮鍐樄闁诡喒鏅涢蹇涱敊閸忕⒈鍚欐繝鐢靛Х閺佸憡鎱ㄩ悽鍛婂殞濡わ絽鍟涵鈧梺瑙勫劶婵倝鎮￠悢鍏肩厸闁告劑鍔庢晶杈ㄤ繆椤愶絽鐏╃紒杈ㄥ浮楠炲洭顢欓梻瀛橆潟婵犳鍠栭敃銉ヮ渻閽樺鏆︽繝濠傜墕缁犵敻鏌熼崫鍕ら柛鎴濇贡缁辨捇宕掑▎鎰偘濡炪倖娉﹂崶褏锛欓梺鍏煎墯閸ㄩ亶鏁嶉崒鐐粹拺闁荤喐婢橀幃鎴︽煟閿濆簼閭€规洘绻傞鍏煎緞婵犲嫬骞堥梻浣侯攰閹活亪姊介崟顖涘亗闁哄洨鍠撶弧鈧梻鍌氱墛缁嬫帡藟濠婂嫨浜滈煫鍥风导闁垶鏌＄仦鍓с€掑ù鐙呭閹风娀骞撻幒婵囩秵闂傚倷绀侀幖顐﹀嫉椤掑嫭鍎庢い鏍ㄧ◥缁诲棝鏌熼梻瀵割槮閸烆垶鎮峰鍐妞ゃ劍鐟╁濠氬磼濞嗘劗銈板銈嗘礃閻楃姴鐣风憴鍕嚤閻庢稒锚濞堬絽顪冮妶鍡欏缂佽瀚粋宥呪枎閹剧补鎷绘繛杈剧到閹诧繝宕悙灞傗偓鎺戭潩椤撗勭杹閻庤娲樺姗€锝炲┑鍫熷磯闁告繂瀚禍鍫曟⒒娴ｅ湱婀介柛銊ョ秺楠炲鏁撻悩鍐蹭簵闂佺粯姊婚崢褏绮昏ぐ鎺戠骇闁割偅绻傞埛鏃傜磼鐎ｎ亶鐓奸柡宀嬬節瀹曡精绠涢弮鈧悵鏇㈡⒑闂堟稒鎼愰悗姘嵆閵嗕礁顫滈埀顒勫箖濞嗘挻顥堟繛鎴ｉ哺濠㈡垹绱撻崒姘偓椋庣矆娓氣偓瀹曟劙宕烽鐔告闂佽法鍠撴慨鎾几娓氣偓閺屾盯骞囬棃娑欑亪缂佺偓鍎抽崥瀣崲濞戙垹绠ｉ柣鎰硾椤ユ繈鎮楀▓鍨珮闁革綇绲介～蹇曠磼濡顎撴繛鎾村嚬閸ㄦ娊宕濋幖浣光拺闁告繂瀚晶閬嶆煕閹捐泛鏋庨柣锝呭槻閳诲酣骞掔€ｂ晜鐫忛梻浣告贡閸庛倝宕硅ぐ鎺撴櫖闁绘柨鍚嬮ˉ濠冦亜閹扳晛鐏璺哄缁辨帞鈧綆鍋勫ù顕€鎸婇悢鍏肩厱闁斥晛鍠氬▓銏ゆ煟閺傛寧顥㈤柡灞诲€濋獮鏍ㄦ媴鐟欏嫰鏁┑鐘愁問閸犳牠鎮ч幘璇茶摕闁挎繂顦粻濠氭偣閾忕懓鍔嬮柣搴ｅ亾缁绘盯鏁愰崨顔芥倷闂佹寧娲︽禍顏堝Υ娴ｇ硶妲堟俊顖炴敱閺傗偓闂備礁缍婇崑濠囧礈濮橀鏁婇柛鏇ㄥ幘绾句粙鏌涚仦鍓ф噭缂佷椒鍗抽幃妯跨疀閺冨倹鍣梺鍛婂笚鐢繝銆佸☉銏″€烽柛娆忓亰缁犳捇寮诲☉銏犲嵆闁靛鍎虫禒顓㈡⒑閸濆嫭鍣洪柣鎿勭節瀵鈽夊Ο閿嬵潔闂佸憡顨堥崑鐔稿閸垻纾藉ù锝嗗灊閸氼偊鏌涚€ｎ剙鏋旀俊鍙夊姍楠炴鈧稒锚椤庢挻绻涚€电孝妞ゆ垵鎳橀獮妤呮偨閸涘﹦鍘介梺闈涚箚閺呮盯鎮橀懠顒傜＜缂備焦顭囩粻鐐测攽閳ュ磭鎽犻柟宄版噽閸栨牠寮撮悢琛″亾婵犳碍鈷戦悷娆忓閸斻倝鏌涘Ο鑽ゅ⒈婵″弶鍔欏畷濂稿即閻斿弶瀚奸梻浣告啞缁嬫垿鏁冮妷锕€绶為柛鏇ㄥ灡閻撴洟鎮楅敐搴′簻缂佹甯楅妵鍕敇閻愭潙顦╅梺璇″灡濡啯淇婇幖浣规櫆闁诡垎鍐啈?"""
    if debug_logs is None:
        debug_logs = []

    def resolve_actual_candidates(field_name: str, actual_value: Any, ctx: dict) -> List[str]:
        candidates: List[str] = []
        primary_value = "" if actual_value is None else str(actual_value)
        candidates.append(primary_value)

        if isinstance(field_name, str) and field_name and not field_name.endswith("_label"):
            label_field = f"{field_name}_label"
            if label_field in ctx:
                label_value = "" if ctx.get(label_field) is None else str(ctx.get(label_field))
                if label_value not in candidates:
                    candidates.append(label_value)

        return candidates

    def resolve_value(val_str: str, ctx: dict) -> str:
        if not isinstance(val_str, str):
            return str(val_str)
        # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾剧懓顪冪€ｎ亝鎹ｉ柣顓炴閵嗘帒顫濋敐鍛婵°倗濮烽崑娑⑺囬悽绋挎瀬闁瑰墽绮崑鎰版煕閹邦剙绾ч柣銈呭閳规垶骞婇柛濞у懎绶ゅù鐘差儏閻ゎ喗銇勯弽顐粶缁炬儳顭烽幃妤呮晲鎼粹剝鐏嶉梺缁樻尰濞茬喖寮诲澶婄厸濞达絽鎲″▓鍫曟⒑閹颁礁鐏℃繛鑼枛瀵鈽夐姀鐘栄囨煕閳╁喚娈旀い顐ｅ浮濮婃椽宕崟顓犲姽缂傚倸绉崇粈渚€顢氶敐澶樻晪闁逞屽墮閻ｇ兘骞掗幋顓熷兊濡炪倖鍨煎▔鏇犳暜濞戙垺鈷掗柛灞剧懅椤︼箓鏌熺喊鍗炰喊妞ゃ垺鐗犲畷鐔碱敇閻曚礁浠烘繝娈垮枟椤牆鈻斿☉銏″珔闁绘柨鍚嬮悡銉︾節闂堟稒顥為柛鐔稿浮閺岋繝宕橀妸褍顣洪梺缁樺笒閻忔岸濡甸崟顖氱闁瑰瓨绻嶆禒楣冩⒑缁嬫鍎忔い鎴濐樀瀵鈽夐姀鐘靛姶闂佸憡鍔︽禍婵嬪闯椤曗偓濮婂搫煤鐠囨彃绠归梺缁橆殕閹瑰洤顕ｆ繝姘╅柍杞扮瀹撳棗鈹戞幊閸婃劙宕戦幘瓒佺懓顭ㄩ崼銏㈡毇濠殿喖锕ㄥ▍锝夊焵椤掆偓濠€杈ㄦ叏閻㈡潌澶嬪緞鐎ｃ劋绨婚梺鎸庢⒒閸嬫捇寮抽鍕厽婵炴垵宕弸娑㈡懚閿濆鐓犳繛鏉戭儐濞呭﹪鏌嶉悷鎵闁哄矉缍侀幃銏ゅ传閵夛箑娅戦梺璇插濮樸劑宕楀Ο渚殨闁瑰墎鐡旈弫鍥煟濡吋鏆╅柨娑欑箞濮婅櫣绮欓幐搴㈡嫳闂佽崵鍟欓崶褏顦悗骞垮劚濡孩绂嶅鍫熺厪濠电偛鐏濋崝婊堟煕閵堝懏澶勬い銊ｅ劦閹瑧鎷犺娴兼劙鏌ф导娆戝埌闁靛棙甯掗～婵嬫偂鎼达絼鍝楁繝鐢靛仜閻楀嫰宕濆畝鍕ㄢ偓鏃堝礃椤斿槈褔鏌涢埄鍐炬畼闁荤喆鍔戦弻锝嗘償閵忕姴姣堥梺鍛婃尵閸犳牠鐛崘銊㈡瀻闁归偊鍠氶惁鍫ユ⒑閸涘﹥澶勯柛鎾寸懇瀵悂骞嬮敂鐣屽幍闂佺厧婀辨晶妤勩亹瑜忕槐鎺楀矗婢跺浠㈤悗瑙勬礃閸ㄥ潡鐛鈧獮鍥ㄦ媴閻熸澘鍘炲┑锛勫亼閸婃牠骞愰悙顒佸弿闁哄鍩堥崵鏇㈡煏婵炵偓娅嗛柍閿嬪灴閺屾稑鈹戦崱妤婁痪濠电姭鍋撻柟娈垮枤绾惧ジ鎮楅敐搴′簽濠⒀嗕含缁辨帡顢欏▎鎯ф闂佸疇妫勯ˇ顖烇綖濠靛鏅查柛娑卞墮椤ユ岸姊绘担鐟邦嚋缂佽鍊婚懞閬嶆嚃閳哄嫬小缂傚倷鐒﹁彠濞存粍绮嶉妵鍕箳閸℃ぞ澹曢梻浣虹帛椤ㄥ棝骞愰幖渚婄稏闊洦绋戞导鐘绘煕閺傚簱鍋撻悢鍓蹭哗闂佺懓鍢查澶愬箹瑜版帩鏁冮柕蹇ョ磿娴滈箖姊婚崒姘偓椋庣矆娓氣偓楠炲鏁撻悩鑼唶闁荤姴娲ゅΟ濠傤焽閳哄懏鐓忓┑鐐戝啫鏆欓柣蹇擄工閳规垿鎮欓崣澶樻！闂佸憡姊瑰ú婊冣枎閵忋倖鍊烽柣鎴灻埀顒傛暬閺屻劌鈹戦崱娑扁偓妤€顭胯閸楁娊寮婚敐澶嬫櫇闁逞屽墴閹勭節閸曨剙搴婂┑鐐村灦閻燂妇绱為崶顒佺厱闁圭偓顨呴幉娑橆潩椤戣姤鏂€闂佺粯鍔曞鍓佲偓姘噽缁辨挸顓奸崟顓犵崲闂佺粯渚楅崰妤€顕ラ崟顖氱疀闁割煈鍋呭▍鏃堟⒒娓氣偓閳ь剛鍋涢懟顖涙櫠椤斿浜滄い鎰╁灮缁犱即鎮￠妶鍡愪簻闊洦鎸搁褏绱掗崡鐐靛煟婵﹥妞藉畷銊︾節閸愵煈妲遍梻浣侯焾椤戝倿宕戦幘缁樷拺缂佸顑欓崕鎰版煙缁嬪灝鈷旈柛鎺撳笒閳诲骸顕ラ锝囨创婵☆偄鍟埥澶婎潩濮ｆ瑣鍔戦幃妤冩喆閸曨剛顦梺鍝ュУ閻楃娀濡存担鍓叉建闁逞屽墴楠炲啴濡堕崱妯侯€撻梺闈╁瘜閸橀箖鍩ｉ妷锔剧瘈闁汇垽娼у暩闂佽桨绀侀幉锛勬崲濞戙垹鐒垫い鎺嶈兌缁犳儳霉閿濆懎鏆辨繛鏉戝€垮顐﹀礋椤栨稓鍘卞┑鐐村灦閻燂妇绱為幋锔界厱闁瑰濮村瓭闂侀潧妫欑敮鎺楋綖濠靛鏅查柛娑卞墮椤ユ岸姊婚崒娆戭槮闁圭⒈鍋婇幆灞惧緞鐏炵晫绛忛梺绋匡功閸犳挻绂嶅▎鎾粹拻濞撴埃鍋撻柍褜鍓涢崑娑㈡嚐椤栨稒娅犻柟缁㈠枟閻撴瑦銇勯弬璇插婵炲眰鍊濋悰顕€濮€閿涘嫮顔曢梺绯曞墲閿氶柣蹇婃櫊閺岋綁骞掑Δ鍐毇濠殿喖锕ュ浠嬬嵁閹邦厽鍎熼柕蹇嬪焺濡差剟姊绘担渚劸闁挎洏鍊楃槐鐐寸節閸屾粍娈鹃梺鍦濠㈡﹢宕归崒娑栦簻闁规壋鏅涢埀顒侇殜椤㈡瑩寮撮姀鈾€鎷洪梺鑽ゅ枛閸嬪﹪宕甸悢鍏肩厱閻庯綆鍓欓弸娑欘殽閻愯韬鐐叉喘椤㈡﹢鎮╁▓鍨櫗闂傚倷绀佹竟濠囧磻閸℃稑绐楅柟閭﹀枤閻棝鏌涢幇闈涙灍闁绘挸绻橀弻娑㈠Ψ閹存繂鏋ゅù鐓庡暣閹鈻撻崹顔界彯闂佸憡鎸鹃崰鏍偘椤曗偓楠炲洭顢橀悢宄板Τ闂備線娼х换鍡涘焵椤掍礁澧ù鐓庢濮婂宕掑▎鎰偘濡炪倖娉﹂崗鐘茬秺閹粓鎸婃径瀣偓顒勬⒑瑜版帗锛熺紒鈧笟鈧畷鎺楀Ω瑜庨崰鎰版煙鏉堥箖妾柛濠傜埣閺岀喖鎮滃鍡樼暦闂佹娊鏀遍崹鍧楀蓟濞戞ǚ妲堟慨妤€鐗嗘慨娑氱磽娴ｅ搫校闁圭懓娲獮鍐ㄎ旈埀顒勶綖濠靛鍋傞幖绮规濞肩粯淇?
        merged_ctx = dict(global_context or {})
        merged_ctx.update(ctx)
        # 濠电姷鏁告慨鐑藉极閸涘﹥鍙忛柣鎴ｆ閺嬩線鏌熼梻瀵割槮缁炬儳顭烽弻锝呂熷▎鎯ф缂備胶濮撮悘姘跺Φ閸曨喚鐤€闁圭偓鎯屽Λ銈囩磽娴ｆ彃浜炬繝鐢靛Т濞诧箓鎮￠崘顏呭枑婵犲﹤鐗嗙粈鍫熸叏濡潡鍝虹€规洖寮剁换娑㈠箣閻愬灚鍣х紓浣稿閸嬨倝骞冨Δ鍛櫜閹肩补鈧尙鏁栨俊鐐€х紓姘跺础閹惰棄绠?evaluate_expression 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻鐔兼⒒鐎靛壊妲紒鐐劤缂嶅﹪寮婚敐澶婄闁挎繂鎲涢幘缁樼厱闁靛牆鎳庨顓㈡煛鐏炲墽娲存い銏℃礋閺佹劙宕卞▎妯恍氱紓鍌氬€烽悞锕傚礉閺嶎厹鈧啴宕奸妷銉у姦濡炪倖宸婚崑鎾剁磼閻樿尙效鐎规洘娲熷畷锟犳倶缂佹ɑ銇濆┑鈩冩倐閸┾剝鎷呮笟顖涙暏濠电姵顔栭崰妤呪€﹂崼銉ユ槬闁哄稁鍘肩壕褰掓煕椤垵浜炵紒鐘荤畺閺岀喓鈧數顭堥崜鍗灻归悩闈涗壕闁靛洤瀚粻娑㈠箻閹碱厽锛佹俊鐐€栭崝鎺斿垝濞嗘挸钃熼柨婵嗩槸缁犳稒銇勯弮鍌氬付濠碘剝妞藉铏圭磼濡闉嶅┑鐐跺皺閸犳牕顕ｆ繝姘櫢闁绘灏欓崐鐐烘⒑闂堟侗妲堕柛搴ㄤ憾閳ユ牠宕煎顏呮閹晠妫冨☉妤佸媰婵＄偑鍊戦崝宀勬晝椤忓嫷鍤曢悹鍥ㄧゴ濡插牊绻涢崱妯虹仼闁伙箑鐗撳娲川婵犲啫顦╅梺绋款儏閸婄宓勯梺瑙勫婢ф鍩涢幋锔界厱婵犻潧妫楅顏呯節閳ь剟骞嶉鍓э紲闁诲函缍嗛崑鍕倶闁秵鐓熼煫鍥ㄦ尭閺嗭絾鎱ㄦ繝鍐┿仢鐎规洏鍔嶇换婵嬪磼濮樺吋缍嗛梻鍌欑閹诧繝骞戦崶銊ь洸闁割偅娲栭弸浣衡偓骞垮劚椤︻垳绮堢€ｎ偁浜滈柟鎹愭硾椤庢挾绱掓潏銊モ枙闁哄矉绲鹃幆鏃堟晬閸曨厽娈梻浣侯焾闁帮絾绂嶉崼鏇犲祦濠电姴鎳愰悿鈧┑鐐村灦閿曘垹螞閻戣姤鈷戝ù鍏肩懅閸掍即鏌ｉ鍐ㄤ汗濠㈣娲熷畷妤冪箔鏉炴壆鐩庨梻浣告惈濞层劑宕戦悙鍝勯棷闁革富鍘搁崑鎾斥枔閸喗鐝梺闈╃秶缂嶄礁顕ｆ繝姘労闁告劏鏅涢鎾剁磽娴ｅ壊鍎忕紒銊╀憾瀹曟垿骞樼拠鏌ユ暅濠德板€愰崑鎾剁磼閻橀潧鈻堥柡宀嬬秮楠炲洭顢楁担鐟板壍闂佸摜鍎愰崹鍫曞箖瀹勯偊鐓ラ柛鏇ㄥ幘閻撯偓闂佺粯鎸堕崐婵嬪蓟閿濆绠ｉ柨婵嗘啗閹剧粯鐓冮梺鍨儏缁楁帡妫佹径鎰叆婵犻潧妫欓幖鎰版煕閺傛鍎旈柡灞剧洴婵″爼宕掑顐㈩棜缂傚倸鍊搁崐鎼佸磹閻戣姤鍤勯柤绋跨仛閸欏繘鎮楅棃娑欐喐闁活厽鎹囬弻锝夋偄缁嬫妫嗛梺鍝勬４婵″洭鎯€椤忓牜鏁囬柣鎰綑濞呪剝绻涚€涙鐭婄紓宥咃躬瀵鎮㈤崗鐓庘偓缁樹繆椤栨繃顏犲ù鐘靛帶椤啴濡堕崱妯煎弳濠碘槅鍋呴悷褔骞戦姀鐘闁靛繒濮锋鍥⒑閻熸壆鎽犵紒璇插暣瀹曟劙顢涘锝嗘杸闂佺粯鍔栬ぐ鍐箖閹达附鐓熼柣鏂垮级濞呭懘鏌ｉ敐鍛Щ妞ゎ偅绮撻崺鈧い鎺戝閺勩儵鏌嶈閸撴岸濡甸崟顖氱闁瑰瓨绻嶆禒楣冩⒑閹惰姤鏁遍柛銊ユ贡濡叉劙骞樼€涙ê顎撻梺鍏肩ゴ閸撴繈宕圭憴鍕洸闁归棿绶￠弫鍥煟閺冨洦顏犳い鏃€娲熷娲捶椤撯剝顎楅梺闈╃秵閸犳牞鐏嬮梺鐟邦嚟閸嬬喓绮绘ィ鍐╃厵閻庣數顭堟牎闂佸搫妫崜鐔煎蓟濞戙垹鐓橀柟顖嗗倸顥氭繝纰夌磿閸嬫垿宕愰弽顐ｆ殰闁圭儤鏌￠崑鎾愁潩閻撳骸绫嶉梺绯曟櫆閻╊垶鐛€ｎ喗鏅滈柣锝呰嫰楠炲牊绻濋悽闈涗沪闁搞劌鐖奸弫鍐敃閿曗偓缁€澶愭煟閺傛娈犻柣鏂挎閹茬顭ㄩ崼婵堫槶闂佺粯姊婚崢褔鎷戦悢鍏肩叆婵犻潧妫欓崯鎺楁煛閸愶絽浜鹃梺鐟板槻閹虫ê鐣烽悜绛嬫晣鐟滃孩鏅ラ梻鍌欐祰椤曆冾潩閿曞偊缍栧璺衡姇閸濆嫷鐓ラ柛顐ｇ箖閻庮剚绻濋悽闈浶ｉ柤褰掔畺閹锋垿鎮㈤崗鑲╁幈闂佹枼鏅涢崰姘枔閵忋倖鐓曟慨姗嗗墻閸庢棃鏌＄仦鍓р槈闁宠棄顦靛畷锟犳倷鐎甸晲澹曢梻?
        from utils.expression_functions import evaluate_expression as _eval_expr
        return _eval_expr(val_str, merged_ctx)

    try:
        node_type = node.get("type", "group")
        
        if node_type in {"group", "relation"}:
            if node_type == "relation":
                resolver = str(node.get("resolver", "")).strip()
                quantifier = str(node.get("quantifier", "EXISTS")).upper()
                relation_meta = RELATION_REGISTRY.get(resolver)
                root_record = (relation_context or {}).get("root_record") or (relation_context or {}).get("receipt_bill")
                db = (relation_context or {}).get("db")
                relation_cache = (relation_context or {}).setdefault("cache", {})
                selected_records = (relation_context or {}).setdefault("selected_records", {})
                relation_group = _normalize_relation_group(node)
                logic = relation_group["logic"]
                children = relation_group["children"]

                if not relation_meta or not relation_meta.loader:
                    debug_logs.append(f"Relation resolver '{resolver}' is not registered")
                    return False

                if not db or root_record is None:
                    debug_logs.append(f"Relation resolver '{resolver}' is unavailable in current context")
                    return False

                scoped_records_map = (relation_context or {}).get("scoped_records") or {}
                if relation_meta.target_source in scoped_records_map:
                    records = list(scoped_records_map.get(relation_meta.target_source) or [])
                else:
                    cache_key = (
                        resolver,
                        int(getattr(root_record, "id", 0) or 0),
                        int(getattr(root_record, "community_id", 0) or 0),
                    )
                    if cache_key not in relation_cache:
                        relation_cache[cache_key] = relation_meta.loader(db, root_record)
                    records = relation_cache.get(cache_key, [])
                matched_record = None

                for idx, record in enumerate(records):
                    nested_logs: List[str] = []
                    record_globals = dict(global_context or {})
                    record_globals.update(data)
                    candidate = _check_trigger_conditions(
                        {
                            "type": "group",
                            "logic": logic,
                            "children": children,
                        },
                        record,
                        nested_logs,
                        record_globals,
                        relation_context,
                    )
                    debug_logs.append(
                        f"Relation resolver={resolver}, quantifier={quantifier}, candidate={idx + 1}/{len(records)}, match={candidate}"
                    )
                    debug_logs.extend([f"  {line}" for line in nested_logs])
                    if candidate:
                        matched_record = record
                        break

                matched = matched_record is not None
                if matched:
                    selected_records[relation_meta.target_source] = matched_record

                if quantifier == "NOT_EXISTS":
                    return not matched
                return matched

            logic = str(node.get("logic", "AND")).upper()
            children = node.get("children", [])
            if not children:
                return True

            results = [
                _check_trigger_conditions(c, data, debug_logs, global_context, relation_context)
                for c in children
            ]
            return all(results) if logic == "AND" else any(results)

        elif node_type == "rule":
            field = node.get("field", "")
            raw_operator = node.get("operator", "==")
            operator = _canonicalize_trigger_operator(raw_operator)
            if not operator:
                debug_logs.append(f"Unsupported operator '{raw_operator}' for field '{field}', treated as False")
                return False
            raw_value = str(node.get("value", ""))
            
            # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳婀遍埀顒傛嚀鐎氼參宕崇壕瀣ㄤ汗闁圭儤鍨归崐鐐烘偡濠婂啰绠荤€殿喗濞婇弫鍐磼濞戞艾骞楅梻渚€娼х换鍫ュ春閸曨垱鍊块柛鎾楀懐锛滈梺褰掑亰閸欏骸鈻撳鍫熺厸鐎光偓閳ь剟宕伴弽顓犲祦鐎广儱顦介弫濠勭棯閹峰矂鍝烘慨锝咁樀濮婄粯鎷呮笟顖滃姼濡炪倖鍨堕崹褰掑箲閵忕姭鏀介柛鈾€鏅涘▓銊╂⒑閸撴彃浜濇繛鍙夌墵閺屽宕堕妸锕€寮垮┑顔筋殔濡鐛Δ鍛厽婵犻潧娲﹂埛鎺旂磼鏉堛劍灏伴柟宄版嚇閹墽浠﹂悾灞筋潽闂傚倷鑳堕…鍫ユ晝閵夈儍鍝勨攽鐎ｎ偄鈧爼鏌涢幇闈涙灍闁抽攱鍨块弻鐔虹矙閹稿孩宕崇紓浣哄У閹瑰洭寮婚悢鐓庣闁哄被鍎卞浼存倵濞堝灝鏋熷┑鐐诧躬楠炲啫鈻庨幘鏉戔偓缁樹繆椤栨粌甯舵鐐茬墕閳规垿鎮╅崹顐ｆ瘎婵犳鍠楀娆戝弲闂佹寧娲嶉崑鎾绘煃閽樺妲兼い锕侇潐娣囧﹪顢曢敍鍕閻庡灚婢樼€氫即鐛崶顒夋晣闁绘ɑ褰冪粻鍝勨攽閻樻鏆俊鎻掓嚇瀹曞綊骞庨挊澶岋紵闂侀潧鐗嗛ˇ顖炴偂濠靛鐓涢柛銉ｅ劚閻忣亪鏌ｉ幘瀛樼妤犵偞鐗楀蹇涘礈瑜忛敍鐔虹磽娴ｅ搫孝妞ゎ厾鍏樺濠氬灳瀹曞洦娈曢柣搴秵閸撴稖鈪靛┑锛勫亼閸婃垿宕瑰ú顏呮櫇闁靛繈鍊曠粻鏍煏閸繃顥炲┑顖涙尦閺屾盯鏁傜拠鎻掔濠电偛鐗撶粻鏍ь潖婵犳艾纾兼繛鍡樺灩閻涖垽姊洪柅鐐茶嫰婢ь垳绱掔€ｎ偅宕岄柛鈺冨仱楠炴帒螖娴ｅ搫骞堥梻浣告惈閸婅棄鈻旈弴銏″€块柟闂寸劍閻撳啴鏌曟径娑㈡妞ゃ儱鐗忛埀顒冾潐濞叉﹢鏁冮姀銈冣偓渚€寮崼婵嗙獩濡炪倖鎸炬慨鎾煀闁秵鈷掗柛灞捐壘閳ь剚鎮傞垾锕傤敆閸曨偆锛涢梺鍦劋椤ㄥ懘宕掗妸銉㈡斀闁稿本绋掔紞鍕熆鐠轰警鐓繛灏栨櫊閺屻倝宕妷顔芥瘜闂?
            value = resolve_value(raw_value, data)
            actual_raw = data.get(field, "")
            raw_candidates = resolve_actual_candidates(field, actual_raw, data)
            actual = raw_candidates[0] if raw_candidates else ""

            # 闂傚倸鍊搁崐鎼佸磹閹间礁纾圭€瑰嫭鍣磋ぐ鎺戠倞妞ゆ帒顦伴弲顏堟偡濠婂啰绠婚柛鈹惧亾濡炪倖甯婇懗鍫曞煝閹剧粯鐓涢柛娑卞枤缁犳﹢鏌涢幒鎾崇瑨闁宠閰ｉ獮妯虹暦閸ヨ泛鏁藉┑鐘殿暜缁辨洟宕戝☉銏″剭闁绘垼妫勭壕濠氭煙閹规劦鍤欑紒鈧崘鈹夸簻闁哄啫鍊哥敮鍫曟煠閺夎法浠㈤柍瑙勫灴閹瑩寮堕幋鐘辩礃濠碉紕鍋涢悺銊╁箖閸屾凹鍤曢柡灞诲劚閻撴盯鏌涘☉鍗炴灓闁告ü绮欏Λ鍛搭敃閵忊€愁槱濠电偛寮堕悧妤冪矉閹烘埈鐓ラ柛鎰⒔閸炵敻鎮峰鍐伇闁告帗甯掗埢搴ㄥ箻瀹曞洨鏆梻浣虹帛閸ㄧ厧螞閸曨垰绠查柤鍝ュ仯娴滄粓鏌熼幑鎰【闁哄閰ｉ弻锛勨偓锝庡亜閻忔挳鏌″畝鈧崰鏍х暦濡ゅ懏鍤冮柍鍝勫€归鍐⒒娴ｅ憡鍟為柡灞诲妿缁棃鎮烽幍顔芥闂佺懓顕慨顓㈠磻閹剧粯鏅查幖绮光偓鎰佹骄缂備胶鍋撻崕鎶藉Χ閹间礁绠栨俊銈呮噺閺呮煡骞栫划鍏夊亾閼艰泛鐒婚梻鍌欒兌椤牏鑺卞ú顏勭？闁汇垻顭堥拑鐔哥箾閹存瑥鐏╅崶鎾⒑缁洖澧叉い顓炴喘钘濆ù鐓庣摠閳锋垹绱撴担鑲℃垿骞嗛崟顖涚厱閻庯綆浜峰銉╂煟閿濆洤鍘寸€规洖銈稿鎾倷瀹ヤ焦娅婇柡灞诲姂瀵潙螖閳ь剚绂嶉崜褏纾奸柣鎰靛墯缁惰尙绱掓径瀣唉闁炽儲妫冨畷姗€顢欓崲澹洦鐓曢柍鈺佸枤閻掗箖鏌涚€ｂ晝绐旀慨濠冩そ楠炴牠鎮欓幓鎺戭潙闂備礁鎲￠弻銊╂煀閿濆懐鏆﹂柨婵嗩槸楠炪垺淇婇悙鐢靛笡闁哄倵鍋撻梻鍌欒兌缁垶宕濋弽褜鐒芥繛鍡樻尰閸婂爼鏌ㄩ弴鐐测偓褰掓偂閻斿吋鐓熼柡鍐ｅ亾婵炲吋鐟︽穱濠囨偩瀹€鈧壕濂告煛閸愩劌鈧悂寮告惔銊︽嚉闁挎繂妫欓崣蹇涙煟閻斿搫顣奸柣顓燁殘缁辨帡濡搁妷顔惧悑濠殿喖锕ㄥ▍锝夊焵椤掆偓濠€杈ㄥ垔椤撱垹鍚归柛鎰靛枟閻撴洘淇婇婊冨付閻㈩垰鐖奸幃锟犲Χ婢跺鍘梺鍓插亝缁诲秴危閸濄儳纾奸柍褜鍓熷畷鐔碱敍濞戞艾骞楁繝纰樻閸ㄩ潧鈻嶉敐澶嬫櫖鐎广儱娲ㄧ壕濂告煃闁款垰浜鹃梺绋款儐閹告悂鍩為幋锔藉亹闁割煈鍋呭В鍕節濞堝灝鏋熼柟绋垮暱閻ｇ兘骞囬鍓э紲濠碘槅鍨崇划顖炴偟閻戣姤鈷戦柛婵嗗閸屻劑鏌涢妸锔姐仢鐎规洘鍨挎俊鎼佹晜鏉炴壆鐩庨梻浣告惈缁夋煡宕濆澶嬪剭闁硅揪闄勯悡銉︾箾閹寸儐鐒介懖鏍磽娴ｄ粙鍝洪悽顖涱殔椤洩绠涘☉妯溾晠鏌ㄩ弴妤€浜剧紒鍓у亾婵炲﹪骞冨Δ鍐╁枂闁告洦鍓涢ˇ銊╂⒑閹稿孩纾搁柛銊ょ矙閹即顢氶埀顒€鐣烽崡鐑嗘僵闁稿繐銇欓鍫熲拻濞达絿鐡旈崵娆戠磼缂佹ê濮囬摶鐐寸箾閹寸偟顣叉い?field_format 婵犵數濮烽弫鍛婃叏閻戣棄鏋侀柛娑橈攻閸欏繘鏌ｉ幋婵愭綗闁逞屽墮閸婂潡骞愭繝鍐彾闁冲搫顑囩粔顔锯偓瑙勬磸閸旀垵顕ｉ崼鏇炵婵犻潧鐗冮崑鎾活敇閻戝棙瀵岄梺闈涚墕閹虫劗绮绘导瀛樼厽婵°倐鍋撴俊顐ｇ〒閸掓帡宕奸妷銉╁敹闂佺粯妫佸▍锝夋儊閸儲鈷掗柛灞炬皑婢ф稓绱掔€ｎ偄娴柡浣哥Т椤撳吋寰勭€ｎ剙骞楅梻浣告惈閸婂湱鈧瑳鍥佸鎮欓悜妯煎幍婵炴挻鑹鹃悘婵囦繆閸忕浜滄い蹇撳閺嗙喖鏌熸搴♀枅闁绘搩鍋婇、姗€鎮滈崱姗嗘％闂傚倸鍊搁崐鎼佸磹瀹勬噴褰掑炊瑜滃ù鏍煏婵炑冩噽閻掑吋绻涙潏鍓хК婵炲拑缍侀幆宀勫箻缂佹鍘鹃梺鍛婄缚閸庢煡寮抽埡鍌樹簻闁冲搫鍟崢鎾煛鐏炶濮傜€殿噮鍓熷畷褰掝敊鐟欏嫬鐦辨繝纰夌磿閸嬬娀锝炴径瀣濠电姴娲ょ粻鏍ㄤ繆閵堝懏鍣洪柡鍛叀楠炴牜鈧稒顭囩粻姗€鏌涢幋鐘冲殌妞ゎ亜鍟存俊鍫曞幢濡厧寮崇紓鍌欒兌缁垶鏁嬪銈庡亜缁绘﹢骞栭崷顓熷枂闁告洦鍋嗚ぐ鎾⒒娴ｅ憡璐＄紒顕呭灣閺侇噣鍩￠崨顓狀槷婵犮垼娉涢惉鑲╁閽樺褰掓晲閸涱収妫岄梺绋垮閸旀瑩骞冩禒瀣垫晬闁靛牆娲ㄩ惁鍫熺節绾版ê澧查柟绋垮暱閻ｅ嘲顫滈埀顒勩€佸▎鎾村亗閹兼番鍊愰崑鎾诲垂椤旇鏂€闂佺粯鍔栧娆撴倶閸楃儐娓婚悗娑櫳戦崐鎰偓娈垮枟閻擄繝宕洪敓鐘插窛妞ゆ柨鐨烽崑鎾绘偨閸涘﹦鍘遍棅顐㈡处閹尖晛危缁嬫５鐟邦煥閸垻鏆┑顔硷攻濡炶棄鐣烽妸锔剧瘈闁告劦鐓堝Σ閬嶆⒒娴ｄ警鐒炬い鎴濇噽閳ь剚纰嶅姗€鎮鹃悜钘夌婵犲灚鍔曢幃鎴炵節閵忥絾纭炬い鎴濆€块獮蹇涙惞閸︻厾锛濋梺绋挎湰閻熝囧礉瀹ュ瀚呴梺顒€绉甸悡鐔兼煙閹冭埞闁告梹绮撻弻鏇㈠炊瑜嶉顓炩攽閳╁啯鍊愬┑锛勫厴閸┾剝绻涢幆褌澹曞┑掳鍊撻梽宥嗙濠婂嫨浜滈柟鎹愭硾娴狅箓鏌熼崗鐓庡闁硅棄鐖煎浠嬵敇閻斿搫骞堟繝鐢靛仦閸ㄩ潧鐣烽鍕嚑闁靛牆顦伴悡鏇熶繆椤栨碍鍋ラ柛婵婃缁辨帞绱掑Ο鑲╃杽闂佽鍠曠划娆徫涢崘顭嬪綊鐓幓鎺斾紙闂佽鍠楅〃鍛达綖濠靛鍋傞幖绮规濞奸箖姊绘担鑺ャ€冪紒璁圭節瀹曟澘顫濈捄铏诡唹闂佸憡娲﹂崜娑㈡⒔閸曨垱鐓㈡俊顖欒濡茶櫣绱掓径瀣仢闁哄瞼鍠栭、娆戞喆閸曨剛褰呴梻浣风串缁插潡宕楀Ο铏规殾闁割偅娲栨儫闂侀潧顦崕铏椤撱垺鈷掑ù锝囩摂閸ゆ瑦鎱ㄥ鍫㈢暤闁挎繄鍋炲鍕箾閹烘挻銇濋柛鈺嬬節瀹曟﹢濡歌閻℃﹢姊绘担渚劸闁挎洩绠撳畷浼村箻鐎靛壊娲告繛瀵稿Т椤戝棝鍩涢幋锔界厾濠殿喗鍔曢埀顒佹礀閻☆厽绻濋悽闈涗哗妞ゆ洘绮庣划濠氬箳濡も偓妗呴梺鍦濠㈡绮堥崘鈹夸簻闊洦鎸婚幖鎰焊?
            field_format = node.get("field_format", "")
            if field_format and "__VALUE__" in field_format:
                from utils.expression_functions import evaluate_expression as _eval_expr
                # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌ｉ幋锝呅撻柛濠傛健閺屻劑寮村Δ鈧禍鎯ь渻閵堝簼绨婚柛鐔告綑閻ｇ柉銇愰幒婵囨櫔闂佸憡渚楅崹鐗堟叏濞差亝鈷掑ù锝勮濞兼帡鏌涢弴鐐典粵闁伙絽澧庣槐鎾存媴閹绘帊澹曞┑鐘灱濞夋稒寰勯崶顒€纾婚柟鎹愵嚙缁€鍌氼熆鐠虹尨姊楀瑙勬礋濮婄粯鎷呴崨濠傛殘濠电偠顕滅粻鎾崇暦濠婂啠鏋庨柟閭﹀櫘濞村嫰姊绘笟鍥у缂佸鏁诲畷鎴﹀焺閸愵亞鐦堟繝鐢靛Т閸婃悂寮抽悢鐓庢瀬闁割偁鍎查埛鎴犵磼椤栨稒绀冮柡澶嬫そ閺屾盯濡搁妶鍥╃厯闂佺粯渚楅崰鏍亽闂佸吋绁撮弲婵嬫晬濠婂啠鏀介柍钘夋閻忋儲淇婂鐓庡缂佽鲸鎹囬獮妯肩磼濡攱瀚藉┑鐐舵彧缁插潡鈥﹂崼銉嬪绠涘☉娆戝幗闂佽鍎崇壕顓熸櫠閿旈敮鍋撶憴鍕闁搞劌鐏濋悾鐑藉础閻愨晜顫嶅┑鈽嗗灡濡叉帞娆㈤姀銈嗏拻濞达綀娅ｇ敮娑欍亜椤撶偞宸濋柛鎺撳浮椤㈡盯鎮欓弶鎴滅暗闂備胶绮濠氬储瑜忕划濠氬捶椤撶姷锛滃銈嗘⒒閸嬫捇宕甸埀顒佺節閳封偓閸曨厼寮ㄩ梺璇″枛缂嶅﹪鐛崶顒€绀堝ù锝堟濡插洭姊绘担鍛婃儓闁哄牜鍓涚划娆撳箻鐠囪尙鍔﹀銈嗗笒閿曪妇绮婇悜鑺ョ厱闁哄倽娉曢悞鎼佹煕閳规儳浜炬俊鐐€栫敮濠囨倿閿斿墽鐭嗛悗锝庡枟閻撳啴鏌曟径娑橆洭濠⒀屽墰缁辨帞绱掑Ο鑲╃杽閻庤娲栭悥濂稿箠濠婂牆鍨傛い鎰剁悼瑜般劑姊婚崒姘偓鐑芥嚄閸撲礁鍨濇い鏍仜缁€澶嬩繆椤栫偞锛熼柣鎺戯攻缁绘盯宕卞Ο璇查瀺闂佸搫鎷嬮崜姘辨崲濞戙垹骞㈡俊顖濐嚙闂夊秹姊洪幎鑺ユ暠闁搞劌婀卞Σ鎰板箳閺傚搫浜鹃柨婵嗛楠炴鐥鐐差暢缂佽鲸甯￠幃鈺呭础閻愯尙銈梻浣告惈閺堫剛绮欓幋锔肩稏婵犻潧顑愰弫鍡楊熆鐠虹尨宸ュ鐟邦儑缁辨捇宕掑▎鎴ｇ獥闂佸摜濮甸悧鐘汇€侀弽顓炲窛閻庢稒锚娴犵厧鈹戦悩缁樻锭妞ゆ垵鎳愮划鍫⑩偓锝庡亖娴滄粓鏌熼悜妯虹仴闁逞屽墯閹倿鎮?__VALUE__ 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾剧懓顪冪€ｎ亝鎹ｉ柣顓炴閵嗘帒顫濋敐鍛婵°倗濮烽崑娑⑺囬悽绋挎瀬闁瑰墽绮崑鎰版煕閹邦剙绾ч柣銈呭濮婄粯鎷呴崨濠傛殘闂佸湱顭堝Λ婵嬪春閳ь剚銇勯幒鎴濐仼缁炬儳顭烽弻鐔兼倷椤掍胶浼囧┑鈩冨絻閻楁捇寮婚悢鍏煎€绘俊顖濐嚙闂夊秹姊虹紒妯肩畺鐎光偓缁嬫娼栭柧蹇氼潐鐎氭岸鏌嶉妷銉э紞濞寸姭鏅涢埞鎴︽倷妫版繂娈濇繝纰樷偓铏枠闁糕斁鍋撳銈嗗笒鐎氼厼鈽夎閺屾洟宕奸姀鈺冨姼濡炪倖娲╃紞浣哥暦婵傜鍗抽柣鎰問閸氬懘姊绘担铏瑰笡闁荤喆鍎甸獮濠囧箛椤撶姭鏋?
                expr = field_format.replace("__VALUE__", actual)
                try:
                    actual = _eval_expr(expr, {})
                    debug_logs.append(f"Field format applied: {field_format} => {expr} => {actual}")
                except Exception as fmt_err:
                    debug_logs.append(f"Field format error for '{field}': {fmt_err}, falling back to raw value")

            actual_candidates = [actual]
            for candidate in raw_candidates[1:]:
                if candidate not in actual_candidates:
                    actual_candidates.append(candidate)

            compare_mode = "string"
            
            if operator == "==":
                res = any(candidate == value for candidate in actual_candidates)
            elif operator == "!=":
                res = all(candidate != value for candidate in actual_candidates)
            elif operator == ">":
                res, compare_mode = _compare_ordered_values(actual_raw, value, operator)
            elif operator == ">=":
                res, compare_mode = _compare_ordered_values(actual_raw, value, operator)
            elif operator == "<":
                res, compare_mode = _compare_ordered_values(actual_raw, value, operator)
            elif operator == "<=":
                res, compare_mode = _compare_ordered_values(actual_raw, value, operator)
            elif operator == "contains":
                res = any(value in candidate for candidate in actual_candidates)
            elif operator == "not_contains":
                res = all(value not in candidate for candidate in actual_candidates)
            elif operator == "startswith":
                res = any(candidate.startswith(value) for candidate in actual_candidates)
            elif operator == "endswith":
                res = any(candidate.endswith(value) for candidate in actual_candidates)
            else:
                res = False
                
            debug_logs.append(
                f"Field: {field}, OP: {operator} (raw={raw_operator}), CompareAs: {compare_mode}, "
                f"Expected: {value}, Actual: {actual}, ActualCandidates: {actual_candidates}, Match: {res}"
            )
            return res
            
        return True
    except Exception as e:
        debug_logs.append(f"Error checking condition: {e}")
        return False




