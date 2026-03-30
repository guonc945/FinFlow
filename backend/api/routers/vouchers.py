import json
import json as json_mod
import re
import uuid
from datetime import datetime
from decimal import Decimal
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

    tolerance = Decimal("0.000001")
    if abs(debit_ori - credit_ori) > tolerance:
        raise HTTPException(
            status_code=400,
            detail=f"Voucher JSON debit/credit not balanced: debitori={debit_ori} creditori={credit_ori}",
        )
    if abs(debit_local - credit_local) > tolerance:
        raise HTTPException(
            status_code=400,
            detail=f"Voucher JSON local debit/credit not balanced: debitlocal={debit_local} creditlocal={credit_local}",
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
    if normalized in {"false", "0", "0.0", "none", "null", "no", "off", "n", "f", "闂?, "闂?}:
        return False
    if normalized in {"true", "1", "1.0", "yes", "on", "y", "t", "闂?, "闂?}:
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
    "id": "placeholder",
    "community_id": "闂傚倷鐒﹂幃鍫曞磿濞差亜鍌ㄩ柤娴嬫櫃閻掑﹪鏌ｉ妶鍡愬亱",
    "charge_item_id": "placeholder",
    "ci_snapshot_id": "闂傚倷娴囬妴鈧柛瀣崌閺岀喖顢涘☉娆戝嚒闂佽瀵掗崢濂糕€旈崘顔嘉ч柛娑卞枦绾偓婵犵數鍋涘鑸靛垔閻ｅ瞼涓嶆繛鎴欏焺閺佸啴鏌曢崼婵囧櫣閻庢侗鏋廌",
    "charge_item_name": "placeholder",
    "charge_item_type": "placeholder",
    "category_name": "placeholder",
    "asset_id": "placeholder",
    "asset_name": "placeholder",
    "asset_type": "placeholder",
    "asset_type_str": "闂備浇宕垫慨宥夊礃椤垳杩樺┑掳鍊楁慨鏉懨洪妸鈺傚仏闁告挆鍕彴闂佽偐鈷堥崗姗€宕?闂傚倷绀侀幖顐﹀磹缁嬫５娲晝閳ь剟鏁?",
    "house_id": "闂傚倷娴囬～澶婎焽濞嗘挻鍊舵繝闈涱儐閸庢垹绱掗崫鍕疅",
    "full_house_name": "placeholder",
    "bind_house_id": "缂傚倸鍊搁崐鐑芥倿閿曞倸鍨傞柣銏犳啞閸嬧晛螖閿濆懎鏆欑紒鐙呯秮閺屾盯鈥﹂幋婵囩亾濠电姍鍐╃D",
    "bind_house_name": "placeholder",
    "park_id": "placeholder",
    "park_name": "placeholder",
    "bill_month": "placeholder",
    "in_month": "闂傚倷绀佸﹢閬嶃€傛禒瀣；闁瑰墽绮崐鍫曟煟閹扮増娑уù鐙呴檮缁绘繈鍩€椤掑嫬鐒垫い鎺嗗亾闂?",
    "start_time": "闂備浇宕垫慨宕囩尵瀹ュ围闁告侗鍨崑鎾活敇閻橆偄浜炬繛鍫濈仢閺嬫捇鏌涚€ｎ偅灏柍瑙勫灴椤㈡瑧鍠婇崡鐐插婵犵绱曢崑娑㈠磹閸噮娼?",
    "end_time": "placeholder",
    "amount": "placeholder",
    "bill_amount": "placeholder",
    "discount_amount": "placeholder",
    "late_money_amount": "濠电姷鏁告慨鎾晝閿曞倸纾婚柣鏃傚帶缁犺鈹戦崒姘暈闁?",
    "deposit_amount": "placeholder",
    "second_pay_amount": "placeholder",
    "pay_status": "闂傚倷娴囬妴鈧柛瀣尰閵囧嫰寮介妸褎鍣柣銏╁灡閻╊垶寮婚敐鍛傜喖宕归鎯у缚闂備線鈧偛鑻崢鎼佹煠閸愭彃顣虫俊鍙夊姍椤㈡棃宕奸悢宄板?",
    "pay_status_str": "闂傚倷娴囬妴鈧柛瀣尰閵囧嫰寮介妸褎鍣柣銏╁灡閻╊垶寮婚敐鍛傜喖宕归鎯у缚闂?",
    "pay_type": "placeholder",
    "pay_type_str": "placeholder",
    "pay_time": "闂傚倷娴囬妴鈧柛瀣尰閵囧嫰寮介妸褎鍣柣銏╁灡閻╊垶寮婚敓鐘茬闁靛ě鍐幗婵犵妲呴崑鍕焽閿熺姷宓?",
    "second_pay_channel": "placeholder",
    "bill_type": "placeholder",
    "bill_type_str": "placeholder",
    "deal_log_id": "婵犵數鍋涢悺銊╁吹鎼淬劌纾归柡宥庣仜閿濆憘鏃堝川椤撶姷娼夐梻渚€娼чˇ顓㈠磿瀹曞洨涓嶇紒鍌涘潖",
    "receipt_id": "闂傚倷娴囬妴鈧柛瀣崌閺屻倝骞栨担瑙勯敪缂備椒绶ょ粻鎾诲蓟?",
    "sub_mch_id": "placeholder",
    "sub_mch_name": "闂備浇顕х€涒晝绮欓幒妤€绀夐柡鍥ュ灩閻ら箖鏌曡箛瀣偓鏇犵不閿濆洠鍋撻悷鏉款伀濠⒀勵殜瀹曟劕鐣烽崶鈺冿紲?",
    "bad_bill_state": "闂傚倷鑳堕～瀣礃椤忓棭鍟堝┑鐘灱濞夋稓鈧矮鍗抽獮鍐煛閸涱喖娈濈紒鍓у閿氬ù?",
    "is_bad_bill": "闂傚倷绀侀幖顐も偓姘卞厴瀹曡瀵奸弶鎴犵暰婵炴挻鍩冮崑鎾垛偓瑙勬礃閻℃洜绮诲☉姗嗘僵妞ゆ帒顦拌ⅸ",
    "has_split": "placeholder",
    "split_desc": "placeholder",
    "visible_type": "placeholder",
    "visible_desc_str": "placeholder",
    "can_revoke": "placeholder",
    "version": "placeholder",
    "meter_type": "placeholder",
    "snapshot_size": "placeholder",
    "now_size": "placeholder",
    "remark": "placeholder",
    "bind_toll": "闂傚倷娴囬妴鈧柛瀣崌閺岀喖顢涘☉娆戝嚒闂佽瀵掗崢濂糕€旈崘顔嘉ч柛娑卞枦绾偓婵犵數鍋涘鑸靛垔閻ｅ瞼涓嶆繛鎴欏焺閺佸啴鏌曢崼婵囧櫣閻?JSON)",
    "user_list": "闂備浇顕ф鍝ョ不瀹ュ鍨傛繛宸簻閺勩儲绻涢幋娆忕仼缂佲偓閸℃ü绻嗛柕鍫濇噹椤忋儵鏌?JSON)",
    "create_time": "placeholder",
    "last_op_time": "闂傚倷绀侀幖顐︽偋閸愵喖纾婚柟鍓х帛閻撴洘鎱ㄥ鍡楀妞ゆ帇鍨介弻鐔哥附閸涘﹥鏆犻梺鐟板槻閹冲酣鈥﹂妸鈺佺妞ゆ帒顦拌ⅶ闂?",
    "created_at": "闂傚倷绀侀幉锛勬暜濡ゅ啰鐭欓柟瀵稿Х绾句粙鏌熼幑鎰靛殭缂侇偄绉归弻娑㈩敃閿濆棛顦ㄩ梺?缂傚倸鍊风欢锟犲垂闂堟稓鏆﹂柣銏ゆ涧閸?",
    "updated_at": "闂傚倷绀侀幖顐⒚洪妶澶嬪仱闁靛ň鏅涢拑鐔封攽閻樺弶鎼愮紒顐㈢Ч閺屾盯顢曢敐鍡欘槰闂?缂傚倸鍊风欢锟犲垂闂堟稓鏆﹂柣銏ゆ涧閸?",
    "kd_house_number": "placeholder",
    "kd_house_name": "placeholder",
    "kd_park_house_number": "placeholder",
    "kd_park_house_name": "placeholder",
    "kd_customer_number": "placeholder",
    "kd_customer_name": "placeholder",
    "kd_project_number": "placeholder",
    "kd_project_name": "placeholder",
    "kd_receive_bank_number": "placeholder",
    "kd_receive_bank_name": "闂傚倷娴囬妴鈧柛瀣崌閺屻倝宕ㄦ繝搴℃櫛闂佺绨洪崕闈涱潖濞差亝鍊锋い鎺嗗亾闁告棁鍩栭妵鍕閳ュ磭浠奸梺鐟板槻缂嶅﹪寮崒鐐村癄濠㈣泛鑻獮鍫ユ⒒娴ｅ憡鍟為柤鐟板⒔缁棃骞嗛幍鍐插緮",
    "kd_pay_bank_number": "placeholder",
    "kd_pay_bank_name": "placeholder",
    "customer_name": "placeholder",
    "customer_id": "闂備浇宕垫慨鐢稿礉瑜忕划濠氬箣濠靛牊娈鹃梺缁樻⒒閸樠呯矆閸岀偞鍊甸柛锔诲幖灏忕紓渚囧枟閹瑰洭骞冨Δ鍛祦闁割煈鍠氭禒楣冩⒑缁洘娅嗛柡澶屾瀺",
    "receive_date": "placeholder",
}


_RECEIPT_BILL_RUNTIME_EXTRA_FIELDS: Set[str] = {"community_name", "payer_name", "deal_type_label"}
_RECEIPT_BILL_FIELD_LABELS: Dict[str, str] = {
    "id": "placeholder",
    "community_id": "闂傚倷鐒﹂幃鍫曞磿濞差亜鍌ㄩ柤娴嬫櫃閻掑﹪鏌ｉ妶鍡愬亱",
    "community_name": "placeholder",
    "payer_name": "婵犵數鍋涢顓熸叏娴ｅ啯鎳屾俊鐐€栭崹鍫曟偡閿斿墽鐭?",
    "deal_time": "placeholder",
    "deal_date": "placeholder",
    "income_amount": "placeholder",
    "amount": "placeholder",
    "bill_amount": "placeholder",
    "discount_amount": "placeholder",
    "late_money_amount": "濠电姷鏁告慨鎾晝閿曞倸纾婚柣鏃傚帶缁犺鈹戦崒姘暈闁?",
    "deposit_amount": "placeholder",
    "pay_channel_str": "placeholder",
    "pay_channel": "placeholder",
    "pay_channel_list": "闂傚倷娴囬妴鈧柛瀣尰閵囧嫰寮介妸褎鍣柣銏╁灡閻╊垶寮婚敓鐘茬＜婵犲﹤瀚▓宀€绱撴担鐟版暰缂佺粯绻傞悾宄邦潩椤戔晜妫冨畷鐔煎煘閹傚?JSON)",
    "payee": "闂傚倷娴囬妴鈧柛瀣崌閺屻倝宕ㄦ繝搴℃櫛闂佺绨洪崕閬嶆箒?",
    "receipt_id": "闂傚倷娴囬妴鈧柛瀣崌閺屻倝骞栨担瑙勯敪缂備椒绶ょ粻鎾诲蓟?",
    "receipt_record_id": "闂傚倷娴囬妴鈧柛瀣崌閺屻倝骞栨担瑙勯敪缂備椒绶ょ粻鎾诲箖鐟欏嫭濯撮悷娆忓閸戯紕绱撻崒姘毙㈠☉鎾宠唺",
    "receipt_version": "placeholder",
    "invoice_number": "闂傚倷绀侀幉锟犳偡閿曞倸鍨傜憸鐗堝笒鍥撮梺闈浥堥弲娑㈡儗?",
    "invoice_urls": "闂傚倷绀侀幉锟犳偡閿曞倸鍨傜憸鐗堝笒鍥撮梺闈浥堥弲婊堟偂閿濆鐓熼柟浼存涧婢ь喖顭?JSON)",
    "invoice_status": "闂傚倷绀侀幉锟犳偡閿曞倸鍨傜憸鐗堝笒鍥撮梺闈浥堥弲婊堝磻閹扮増鐓犻柛婵勫労閺嗩垶鏌?",
    "open_invoice": "闂傚倷绀侀幖顐も偓姘卞厴瀹曡瀵奸弶鎴犵暰婵炶揪绲芥竟濠囧吹閺囥垺鐓冪憸婊堝礈濞戞艾鍨?",
    "asset_name": "placeholder",
    "asset_id": "placeholder",
    "asset_type": "placeholder",
    "deal_type": "placeholder",
    "remark": "placeholder",
    "fk_id": "FK_ID",
    "bind_users_raw": "闂傚倷鑳堕…鍫㈡崲閹扮増鍋柛銉ｅ妿椤╃兘鏌涢銈呮瀾濠殿垱鎸抽悡顐﹀炊閵婏妇鍙嗛梺缁樻尰濞叉牠鈥︾捄銊﹀磯闁绘垶顭囬悡鎴︽⒑?JSON)",
    "created_at": "闂傚倷绀侀幉锛勬暜濡ゅ啰鐭欓柟瀵稿Х绾句粙鏌熼幑鎰靛殭缂侇偄绉归弻娑㈩敃閿濆棛顦ㄩ梺?缂傚倸鍊风欢锟犲垂闂堟稓鏆﹂柣銏ゆ涧閸?",
    "updated_at": "闂傚倷绀侀幖顐⒚洪妶澶嬪仱闁靛ň鏅涢拑鐔封攽閻樺弶鎼愮紒顐㈢Ч閺屾盯顢曢敐鍡欘槰闂?缂傚倸鍊风欢锟犲垂闂堟稓鏆﹂柣銏ゆ涧閸?",
    "kd_house_number": "placeholder",
    "kd_house_name": "placeholder",
    "kd_park_house_number": "placeholder",
    "kd_park_house_name": "placeholder",
    "kd_customer_number": "placeholder",
    "kd_customer_name": "placeholder",
    "kd_project_number": "placeholder",
    "kd_project_name": "placeholder",
    "kd_receive_bank_number": "placeholder",
    "kd_receive_bank_name": "闂傚倷娴囬妴鈧柛瀣崌閺屻倝宕ㄦ繝搴℃櫛闂佺绨洪崕闈涱潖濞差亝鍊锋い鎺嗗亾闁告棁鍩栭妵鍕閳ュ磭浠奸梺鐟板槻缂嶅﹪寮崒鐐村癄濠㈣泛鑻獮鍫ユ⒒娴ｅ憡鍟為柤鐟板⒔缁棃骞嗛幍鍐插緮",
    "kd_pay_bank_number": "placeholder",
    "kd_pay_bank_name": "placeholder",
}


_DEPOSIT_RECORD_RUNTIME_EXTRA_FIELDS: Set[str] = {"operate_type_label"}
_DEPOSIT_RECORD_FIELD_LABELS: Dict[str, str] = {
    "id": "placeholder",
    "community_id": "闂傚倷鐒﹂幃鍫曞磿濞差亜鍌ㄩ柤娴嬫櫃閻掑﹪鏌ｉ妶鍡愬亱",
    "community_name": "placeholder",
    "house_id": "闂傚倷娴囬～澶婎焽濞嗘挻鍊舵繝闈涱儐閸庢垹绱掗崫鍕疅",
    "house_name": "placeholder",
    "amount": "placeholder",
    "operate_type": "placeholder",
    "operate_type_label": "placeholder",
    "operator": "placeholder",
    "operator_name": "闂傚倷鑳堕幊鎾绘倶濠靛牏鐭撶€规洖娲ㄧ粈濠囨煛閸愶絽浜剧紓?",
    "operate_time": "闂傚倷鑳堕幊鎾绘倶濠靛牏鐭撶€规洖娲ㄧ粈濠囨煛閸愩劎澧曠紒顐㈢Ч閺屾盯顢曢敐鍡欘槰闂佹寧绋撻崰鏍蓟?",
    "operate_date": "placeholder",
    "cash_pledge_name": "placeholder",
    "remark": "placeholder",
    "pay_time": "闂傚倷娴囬妴鈧柛瀣尰閵囧嫰寮介妸褎鍣柣銏╁灡閻╊垶寮婚敓鐘茬闁靛ě鍐幗婵犵妲呴崑鍕焽閿熺姷宓?",
    "pay_date": "placeholder",
    "payment_id": "闂傚倷鑳堕…鍫㈡崲閹扮増鍋柛銉ｅ妿椤╃兘鏌涢鐘插姎缂備讲鏅犻弻銈夊川婵犲骸鏅遍梺绋跨昂閸庣敻寮诲☉妯滄梹鎷呯拠鈩冃―",
    "has_refund_receipt": "闂傚倷绀侀幖顐も偓姘卞厴瀹曡瀵奸弶鎴犵暰婵炴挻鍩冮崑鎾垛偓瑙勬礃椤ㄥ牓鍩€椤掆偓濠€杈ㄦ叏瀹曞洤鍨濋柟鍓х帛閳锋垶銇勯幇顔兼瀻濞存粍鍎抽埞鎴︽倷閸欏妫ら梺绋款儐閹瑰洤顕ｉ銉ｄ汗闁圭儤鍨归?",
    "refund_receipt_id": "placeholder",
    "pay_channel_str": "placeholder",
    "raw_data": "闂傚倷绀侀幉锟犫€﹂崶顒€绐楅幖鎼厜缂嶆牠鏌熼悧鍫熺凡闁哄绶氶弻锝呂旈埀顒勬偋閸℃瑧鐭?JSON)",
    "created_at": "闂傚倷绀侀幉锛勬暜濡ゅ啰鐭欓柟瀵稿Х绾句粙鏌熼幑鎰靛殭缂侇偄绉归弻娑㈩敃閿濆棛顦ㄩ梺?缂傚倸鍊风欢锟犲垂闂堟稓鏆﹂柣銏ゆ涧閸?",
    "updated_at": "闂傚倷绀侀幖顐⒚洪妶澶嬪仱闁靛ň鏅涢拑鐔封攽閻樺弶鎼愮紒顐㈢Ч閺屾盯顢曢敐鍡欘槰闂?缂傚倸鍊风欢锟犲垂闂堟稓鏆﹂柣銏ゆ涧閸?",
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
    "oa": VoucherSourceModuleMeta(id="oa", label="OA缂傚倸鍊风欢锟犲垂闂堟稓鏆﹂柣銏ゆ涧閸?, note="婵犵數鍋涢顓熸叏閹绢喖绠犳慨妞诲亾闁靛棗鍊块崺锟犲川椤旈棿鎮ｆ繝鐢靛仜濡瑩宕濋弴銏犵厱闁哄稁鍘介崐鍫曟煟閹邦剛鎽犵紒鈧崘顔界厽闊洢鍎抽悾鐢碘偓娈垮枟閹倸鐣烽悢鐓庣濞达絼璀﹀Σ鐑芥⒒娴ｅ憡鎯堟俊顐ｎ殜瀹曟劙鎮风敮顔库偓鎸庛亜閹惧崬鐏╃紒鐘冲▕閺屾洘寰勯崼婵嗗閻庢鍠楁繛濠囧箖濡ゅ懎鎹舵い鎾跺仒缁呯磽閸屾氨孝妞ゆ垵顦锝夘敃閿旇棄浜遍梺鍓插亝缁诲嫰濡?),
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
        label="婵犵妲呴崑鍛熆濡皷鍋撳鐓庡⒋闁诡喗妞芥俊鎼佸Ψ閵忥紕鈧姊虹涵鍜佹綈闁告棑绠撳畷顒勫礈娴ｇ懓鏋?",
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
        note="婵犵數鍋涢顓熸叏閹绢喖绠犳慨妞诲亾闁靛棗鍊块崺锟犲川椤旈棿鎮ｆ繝鐢靛仜濡瑩宕濋弴銏犵厱闁哄稁鍘介崐鍫曟煟閹邦剛鎽犵紒鈧崘顔界厽闊洢鍎抽悾鐢碘偓娈垮枟閹倸鐣烽悢鐓庣濞达絼璀﹀Σ鐑芥⒒娴ｅ憡鎯堟俊顐ｎ殜瀹曟劙鎮风敮顔库偓鎸庛亜閹惧崬鐏╃紒鐘冲▕閺屾洘寰勯崼婵嗗閻庢鍠楁繛濠囧箖濡ゅ懎鎹舵い鎾跺仒缁呯磽閸屾氨孝妞ゆ垵娲崺鈧い鎺嶈兌閳洘銇勯妸銉︻棦妞ゃ垺鐟ф禒锕傚礈瑜夐弸鏍煙閼测晞藟闁逞屽墮閸熻法绱炲鈧娲嚌娴兼潙鎽甸梺鍝勮閸旀垵鐣烽敐鍫Ь闂?",
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
        label="闂傚倷鑳堕…鍫㈡崲閹扮増鍋柛銉ｅ妿椤╃兘鏌涢鐘插姎缂佺媭鍠栭埞鎴﹀磼濠婂海鍔稿┑鐐叉噷閸旀垵顫忓ú顏嶆晝闁绘棁娓规竟鏇熺節?",
        root_source="receipt_bills",
        target_source="deposit_records",
        loader=_load_receipt_to_deposit_refund_relation,
    ),
    "receipt_to_prepayment_recharge": VoucherRelationMeta(
        resolver="receipt_to_prepayment_recharge",
        label="闂傚倷鑳堕…鍫㈡崲閹扮増鍋柛銉ｅ妿椤╃兘鏌涢銈呮珢缂佹唻缍侀弻娑㈠即閵娿儰绨介梺鍦櫕婵數鎹㈠☉銏犲耿婵炲棗绻嬫竟鏇㈡⒑鏉炴壆顦︾紓宥咃工閻?",
        root_source="receipt_bills",
        target_source="prepayment_records",
        loader=_load_receipt_to_prepayment_recharge_relation,
    ),
    "receipt_to_prepayment_refund": VoucherRelationMeta(
        resolver="receipt_to_prepayment_refund",
        label="闂傚倷鑳堕…鍫㈡崲閹扮増鍋柛銉ｅ妿椤╃兘鏌涢銈呮珢缂佹唻缍侀弻娑㈠即閵娿儰绨介梺鍦櫕婵數鎹㈠☉銏犲耿婵炴垶姘ㄩ弳顐︽⒑闁偛鑻崢鎾煕鐎ｎ偅灏扮紒?",
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

    # 闂傚倸鍊烽悞锕€顪冮崹顕呯劷闁秆勵殔缁€澶屸偓骞垮劚椤︻垶寮伴妷锔剧闁瑰鍋熼幊鍛存煃缂佹ɑ鐓ラ柍瑙勫灴閹晜娼忛銏犲腐闂備浇宕甸崰鎰板箲閸ヮ剙钃熼柨鐔哄Т绾惧吋鎱ㄥ鍡楀箺闁诡喗鐟╅幃宄邦煥閸曨剛鍑″銈忓閺佽顕ｇ拠鑼殕闁告劦浜為弶鎼佹⒑闂堟冻绱￠柛婊€鐒﹁ⅸ闂傚倷绀侀幖顐λ囬锕€鐤炬繝濠傜墕缁€澶嬫叏濡炶浜鹃梺闈涙缁舵岸鐛€ｎ喗鏅濋柍褜鍓涢悮鎯ь吋婢跺鍘遍梺瑙勫礃鐏忔瑩宕濆澶嬬厸闁糕剝鐟ュ暩闂佺懓寮堕幃鍌炲箖瑜斿畷濂告偄妞嬪寒鏆℃繝鐢靛Л閹峰啴宕ㄩ娑欑€伴梻浣筋嚃閸ㄤ即宕愭繝姘闁绘绮崵鎴炪亜閹烘垵鈧劙顢橀姀鈾€鎷哄┑鐐跺皺缁垱绻涢崶顒佺厱闁哄倽娉曟晥閻庤娲栭幖顐﹀煡婢舵劕顫呴柣妯活問濡茬兘姊绘笟鈧褔鈥﹂銏″殘鐟滅増甯掔壕濠氭煙閹呮憼濠殿垱鎸抽幃宄扳枎韫囨搩浠惧┑鐐茬墛閿曘垹顫忛搹鍦煓閻犳亽鍔庨悿鍕⒑缁嬫寧鍞夌€规洟娼у嵄闁归偊鍏橀弨浠嬫倵閿濆簼绨婚柛鐐姂濮婅櫣鎹勯妸銉︾亖婵犳鍠栭崲鏌ユ偩閻戣姤鏅插璺侯儌閹峰姊洪崨濠傚Е闁告ê銈搁幃锟犲箛椤撴粈绨婚梺鍝勫€藉▔鏇㈡倿閸涘﹣绻嗘い鎰剁磿缁愭棃鏌涢埞鎯т壕婵＄偑鍊栫敮濠勬閵堝洦顫曢柨婵嗩槹閻撴洟鏌￠崶銉ュ濞存粍绮撻幃浠嬵敍濡搫濮﹂梺鍝勭焿缁绘繈宕洪埀顒併亜閹烘垵顏╃紒鐘崇叀閺岀喖寮剁捄銊ょ驳闂佸搫鍊甸崑鎾绘⒒?
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
    闂備浇宕甸崰鎰版偡鏉堚晛绶ゅΔ锝呭暞閸婇潧霉閻樺樊鍎戠€规挷绶氶弻鐔碱敍濮橆剙绲兼繝銏ｅ煐閸旀洜绮堥崒鐐寸厪濠电偛鐏濇俊鍏笺亜閿旇娅婇柡灞剧☉铻栧ù锝呮贡椤︺劑姊洪崫鍕棤闁哥姵鐗犻悰顕€骞嬮敂缁樻櫓闂佸搫瀚换鎰扮嵁閳ь剟姊绘担鍦菇闁告柨鐬奸埀顒佺煯閸楀啿鐣烽搹顐犲亝闁告劏鏅滈崰鎺楁⒑鐠団€崇仸闁稿锕顐﹀焵椤掑嫭鈷戞慨鐟版搐閻忣噣鏌涢悩鏌ュ弰闁诡噣缂氶ˇ鏌ユ煃缂佹ɑ宕岄柟顔界懇瀹曟寰勬繝浣割棜?

    闂備礁鎼ˇ顖炴偋婵犲洤绠伴柟闂寸閻鏌涢埄鍐噮闁活厼妫濆鍫曟倷閺夋埈妫嗘繛?
    { "bill_data": { "house_id": "123", "park_id": "456" } }

    闂備礁鎼ˇ顐﹀疾濠婂牆钃熼柕濞垮剭濞差亜鍐€妞ゆ劑鍊楅悞濂告煛婢跺苯浠﹀鐟版鐎?
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

    org_name = current_user.organization.name if current_user.organization else "闂傚倷绀侀幖顐︽偋濠婂嫮顩叉繝濠傜墕閸ㄥ倿骞栧ǎ顒€濡介柣鎾亾?"
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
            "currency": currency,
            "localrate": _json_number(localrate_val),
            "assgrp": assgrp if assgrp else None,
            "maincfassgrp": maincfassgrp if maincfassgrp else None,
        })

        kd_entry = {
            "seq": rule.line_no,
            "edescription": summary,
            "account_number": account_code,
            "currency_number": currency,
            "localrate": _json_number(localrate_val),
            "debitori": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "creditori": 0.0 if rule.dr_cr == "D" else _json_number(amount_val),
            "debitlocal": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "creditlocal": 0.0 if rule.dr_cr == "D" else _json_number(amount_val),
        }
        if assgrp:
            kd_entry["assgrp"] = assgrp
        if maincfassgrp:
            kd_entry["maincfassgrp"] = maincfassgrp
        kingdee_entries.append(kd_entry)

    total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in accounting_entries)
    total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in accounting_entries)

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
            "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
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

            total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in merged_accounting_entries)
            total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in merged_accounting_entries)

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
                    "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
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
            "currency": currency,
            "localrate": _json_number(localrate_val),
            "assgrp": assgrp if assgrp else None,
            "maincfassgrp": maincfassgrp if maincfassgrp else None,
        })

        kd_entry = {
            "seq": rule.line_no,
            "edescription": summary,
            "account_number": account_code,
            "currency_number": currency,
            "localrate": _json_number(localrate_val),
            "debitori": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "creditori": 0.0 if rule.dr_cr == "D" else _json_number(amount_val),
            "debitlocal": _json_number(amount_val) if rule.dr_cr == "D" else 0.0,
            "creditlocal": 0.0 if rule.dr_cr == "D" else _json_number(amount_val),
        }
        if assgrp:
            kd_entry["assgrp"] = assgrp
        if maincfassgrp:
            kd_entry["maincfassgrp"] = maincfassgrp
        kingdee_entries.append(kd_entry)

    total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in accounting_entries)
    total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in accounting_entries)

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
            "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
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

    total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in merged_accounting_entries)
    total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in merged_accounting_entries)
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
            "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
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
    婵犵妲呴崑鍛熆濡皷鍋撳鐓庣仸闁挎繄鍋涢鍏煎緞婵犲嫷鏀ㄩ梻浣告惈濞层劍鎱ㄩ悜钘夌疇濠㈣埖鍔栭崑锝夋煙閺夊灝顣抽柣锝堜含閳ь剝顫夊ú婊堝窗閹捐鐒垫い鎺戝€归弳鈺呮煙閾忣偅灏甸柤娲憾瀵濡烽敃鈧崜顓㈡⒑閸涘﹥澶勯柛瀣閳绘捇宕奸弴鐔蜂化闂佹悶鍎绘俊鍥囬敃鍌涚厱闁绘劕妯婇崵鐔兼煃瑜滈崜娑㈠箠閹惧墎涓嶇€广儱顦悞?

    闂備礁鎼ˇ顐﹀疾濠婂牆钃熼柕濞垮剭濞差亜鍐€妞ゆ挾鍋熼ˇ閬嶆⒑閸涘﹣绶遍柛妯圭矙瀹曟洟骞橀弬銉︻潔闂佽鍎冲畷顒勫焵椤掍焦宕岄柡灞借嫰椤劑宕奸悢鍝勫Ъ闂備線娼ч…鍫ュ磿闁秵鍊靛┑鐘崇閻?
    1. `accounting_view`: 闂傚倸鍊搁崐鎼佹偋閸曨垰鍨傜憸鐗堝笒缁犳牗绻濇繝鍌氼伀闁崇粯姊归幈銊ヮ潨閸℃顫╁銈庡亖閸婃繈寮婚敍鍕勃閻犲洦褰冩慨銏犫攽閻橆偄浜鹃梺鍛婄☉閻°劑宕曞澶嬬厱闁哄洢鍔嬬花鐣岀磼閹邦収娈滈柟顔款潐閹峰懎霉鐎ｎ亙澹曢梺鍛婂姇瀵爼顢欏澶嬬厽闁绘ê寮堕惌妤佺箾閺夋垶鍠橀柨婵堝仧閳ь剨缍嗛崰鏍倷?
    2. `kingdee_json`: 闂傚倷绀侀幉锟犳偡椤栫偛鍨傞柛鎰梿濞差亜閿ゆ俊銈傚亾缂佺姵濞婇弻鏇熺珶椤栨艾顏繛鍫涘灪缁?`voucherAdd` 闂傚倷鐒﹂惇褰掑礉瀹€鈧埀顒佺煯閸楀啿鐣烽搹顐犲亝闁告劏鏅滈崰鎺楁⒑鐠団€崇仸闁稿锕畷鐢告倷鐎涙ê寮?JSON
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

    # 闂傚倸鍊峰ù鍥敋瑜忛幑銏ゅ箛椤旇棄搴婇梺褰掑亰閸庨潧鈽夊鍡欏弳闂佸憡娲﹂崑鍡欑玻閻愮儤鈷戦柛娑橈攻婢跺嫰鏌涢妸锕€顥嬫繛鎴犳暬閸┾偓?ORM 闂傚倸鍊峰ù鍥敋瑜嶉湁闁绘垼妫勯弸渚€鏌熼梻鎾闁逞屽厸閻掞妇鎹㈠┑瀣倞闁靛鍎冲Ο渚€姊绘担鍛婂暈婵炶绠撳畷婊冾潩椤撶喎寮垮┑鐘诧工閻楀﹪宕戦敐澶嬬厵闂侇叏绠戦弸娑㈡煛鐎ｎ偅顥堥柡灞界Х椤т線鏌涢幘瀛樼殤缂侇喗鐟╅獮鎺戭渻鐏忔牕浜鹃柛鎰靛枛瀹告繈鏌℃径瀣伇鐞氭繈姊?
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

    # 2. 闂傚倸鍊搁崐椋庣矆娴ｉ潻鑰块梺顒€绉查埀顒€鍊圭粋鎺斺偓锝庝簽閺屟冣攽閻愭潙鐏熼柛銊︽そ瀹曟洟骞橀崹娑樹壕妤犵偛鐏濋崝姘亜閿旇鐏︾€殿喖鎲＄粭鐔煎焵椤掑嫬钃熼柨婵嗘啒閺冨牆鐒垫い鎺戝閸嬪鏌涢埄鍐噮闁活厼鐗撻弻銊╁即閻愭祴鍋撹ぐ鎺撳亗闁靛濡囩粻楣冩煙鐎电鍓遍柣鎺曟椤儻顧傜紓宥勭窔瀵鎮㈤崜鍙壭ч柟鑲╄ˉ閳ь剙鍟跨粻锝夋煟鎼淬値娼愭繛鍙夘焽閹广垽宕奸妷銉х暫闂佸啿鎼崯顖溾偓姘哺閺屾稑鈻庤箛锝嗏枔婵炲濮甸幐鍐差潖缂佹ɑ濯撮悷娆忓閻濐亪姊婚崒姘仼閻庢碍婢橀悾鐑藉即閻旀椽妾梺鍛婄☉閿曪箓宕㈡禒瀣拺闂傚牊绋撶粻鐐烘煕閵娿儳绉虹€殿喖鎲￠幆鏃堝Ω閿旀儳骞楅梻浣瑰墯閸ㄩ亶鎮烽妷鈺佺柈闁绘顕ч崙鐘绘煛閸愩劌鈧螞椤栫偞鐓涘璺侯儛閸庛儲淇婇懠鑸殿仩缂佽鲸甯￠崺鈧?
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

    # 闂傚倸鍊风粈渚€骞栭锔绘晞闁告侗鍨崑鎾愁潩閻撳骸顫紓浣介哺閹瑰洭鐛Ο鑲╃＜婵☆垵妗ㄧ紓鎾绘⒒娓氣偓濞佳団€﹂鐘典笉闁硅揪闄勯崑鍌炴煛閸ャ儱鐏柣鎾存礃缁绘盯宕卞Δ鍛椽婵帩鍋呴懝楣冣€︾捄銊﹀磯闁告繂瀚锋禒鍓х磽娓氣偓缂傛艾螞閸曨垪鈧棃宕橀鑲╊槶閻熸粌绻橀獮澶愵敂閸涱垳顔曢梺鐟邦嚟閸嬬偟浜搁鐔翠簻闁瑰瓨绻冮ˉ銏⑩偓?
    from urllib.parse import unquote
    org_name = current_user.organization.name if current_user.organization else "闂傚倷绀侀幖顐︽偋濠婂嫮顩叉繝濠傜墕閸ㄥ倿骞栧ǎ顒€濡介柣鎾亾?"
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

    # 5. 闂傚倷娴囧畷鐢稿窗閹扮増鍋￠弶鍫氭櫅缁躲倕螖閿濆懎鏆為柛濠勬暬閺岋箑螣閻氬绀婇梺鍦劋濞叉粓鎮块埀顒勬⒑閸濆嫬鏆婇柛瀣崌閺岋箓宕掑鐓庡壎闂佸搫鏈惄顖炵嵁閹烘绠ｉ柣鎴濇椤ワ絿绱撻崒姘偓鍝ョ矓鐎靛摜鐭撻柛顐ｆ礀缁犵喖鏌熺紒銏犳灈缂佲偓鐎ｎ偁浜滈柟鎵虫櫅閻忣喖霉?
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

        # 闂傚倷娴囧畷鐢稿窗閹扮増鍋￠弶鍫氭櫅缁躲倕螖閿濆懎鏆為柛濠勬暬閺屻倝骞侀幒鎴濆缂傚倸绉村ú顓㈠蓟濞戞ǚ妲堥柛妤冨仜缁犲綊鎮跺鍗炩枅婵﹥妞藉畷顐﹀礋椤撳鍨介弻鐔兼嚍閵壯呯厜闂?
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

        # 闂傚倷娴囧畷鐢稿窗閹扮増鍋￠弶鍫氭櫅缁躲倕螖閿濆懎鏆為柛濠勬暬閺屻倝骞侀幒鎴濆Б闂佸磭绮Λ鍐蓟閿熺姴绀嬫い鎰╁€楅悿鍕⒑闁偛鑻晶顕€鏌涢悢鍛婄稇妞ゆ洩绲剧粋鎺斺偓锝庝簷缁卞爼姊洪棃娑崇础闁告洦鍓涘Σ?
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

        # 濠电姷鏁搁崑鐔妓夐幇鏉跨；闁归偊鍘介崣蹇擃渻鐎ｎ亪顎楅柛銊︾箞閺岋綁骞嬮悘娲讳邯椤㈡碍娼忛妸锕€寮垮┑鈽嗗灥椤曆勬櫠椤栫偞鐓曟慨姗堢到娴滈箖姊婚崒娆戭槮闁硅绻濆畷娲醇閵夈儳鐓戦梺閫炲苯澧撮柡宀嬬秮閺佸啴鍩€椤掑媻鍥濞戝崬娈ㄦ繛瀵稿Т椤戝棝宕戦崒鐐茬閺夊牆澧介幃鍏笺亜?
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
            "currency": currency,
            "localrate": _json_number(localrate_val),
            "assgrp": assgrp if assgrp else None,
            "maincfassgrp": maincfassgrp if maincfassgrp else None,
        })

        # 闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗ù锝呭閸ゆ洟鏌涢锝嗙闁搞劌鍊块弻锝夊箻閸愯尙妲伴梺?API JSON 闂傚倸鍊搁崐椋庣矆娓氣偓楠炴牠顢曢妶鍥╃厠闂佸壊鍋呭ú鏍磼閵娧勫枑闊洦鎷?
        kd_entry = {
            "seq": rule.line_no,
            "edescription": summary,
            "account_number": account_code,
            "currency_number": currency,
            "localrate": _json_number(localrate_val),
            "debitori": _json_number(amount_val) if rule.dr_cr == 'D' else 0.0,
            "creditori": 0.0 if rule.dr_cr == 'D' else _json_number(amount_val),
            "debitlocal": _json_number(amount_val) if rule.dr_cr == 'D' else 0.0,
            "creditlocal": 0.0 if rule.dr_cr == 'D' else _json_number(amount_val),
        }
        if assgrp:
            kd_entry["assgrp"] = assgrp
        if maincfassgrp:
            kd_entry["maincfassgrp"] = maincfassgrp
        kingdee_entries.append(kd_entry)

    total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in accounting_entries)
    total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in accounting_entries)

    # 6. 缂傚倸鍊搁崐鎼佸磹閹间礁纾瑰瀣捣閻棗霉閿濆牄鈧偓闁稿鎸搁～婵嬫偂鎼达絼妗撻梻浣筋嚃閸犳銆冩繝鍥х畺闁绘垼濮ら崑瀣煕椤愩倕鏋戦悗姘▕濮婄粯鎷呴崨濠冨創濠碘槅鍋勯顓犳閻愬鐟归柍褜鍓熼悰顕€宕卞☉妯碱槰濡炪倖妫侀崑鎰八囬弶娆炬富闁靛牆妫涙晶顒傜棯閺夎法效妞ゃ垺妫冩俊鐑藉煛閸屾瀚奸梻浣筋潐閸庢娊骞婇幇鐗堝仼闁规儼濮ら悡鏇㈡煃閸濆嫬鈧粯鏅堕柆宥嗙厸濞撴艾娲ら弸锕傛煏閸ャ劌濮嶆鐐村浮楠炴鎹勯崫鍕?JSON 缂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸婂潡鏌ㄩ弴鐐测偓鍝ョ不閺夊簱鏀介柣妯虹－椤ｆ煡鏌?
    kingdee_json = {
        "data": [{
            "book_number": book_number,
            "bizdate": biz_date,
            "bookeddate": booked_date,
            "period_number": period_number,
            "vouchertype_number": vouchertype_number,
            "description": (matched_template.template_name or matched_template.template_id or "闂傚倷绀侀幖顐︽偋濠婂嫮顩叉繝濠傜墕缁犵娀鏌ㄩ悢鍝勑㈤悷娆欏閳ь剙绠嶉崕閬嶅几閻戞鐟归柍褜鍓欓?)",
            "attachment": _parse_attachment_count(attachment),
            "entries": kingdee_entries
        }]
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
            "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
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

    total_debit = sum((_try_parse_decimal(e.get("debit")) or Decimal("0")) for e in merged_accounting_entries)
    total_credit = sum((_try_parse_decimal(e.get("credit")) or Decimal("0")) for e in merged_accounting_entries)

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
            "is_balanced": abs(total_debit - total_credit) < Decimal("0.01"),
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
                models.ExternalApi.name == "闂傚倷娴囬～澶嬬娴犲绀夐柟杈剧畱閻掑灚銇勯幋锝嗙《缂佹劖妫冮弻娑㈡晲婢跺瞼鏆梺纭呭Г缁诲牓鐛崱娑欑劷闁挎洍鍋撻柡浣哥秺閺岋綁鎮╅崘鎻捫佺紓浣哄У閸ㄥ灝鐣烽悽鍛婂亜闁绘垶锚閻?",
                models.ExternalApi.url_path.ilike("%/gl/gl_voucher/voucherAdd%"),
                models.ExternalApi.url_path.ilike("%gl_voucher/voucherAdd%"),
            )
        ).order_by(models.ExternalApi.id.asc()).first()
        if not api_record:
            raise HTTPException(
                status_code=404,
                detail="No active voucher push external API found. Please configure it in 闂傚倷娴囬～澶嬬娴犲纾块弶鍫亖娴滆绻涢幋鐐寸殤濞戞挸绉归弻銊モ槈濡警浠鹃梺?"
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

    org_name = current_user.organization.name if current_user.organization else "闂傚倷绀侀幖顐︽偋濠婂嫮顩叉繝濠傜墕閸ㄥ倿骞栧ǎ顒€濡介柣鎾亾?"
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
    """闂傚倸鍊搁崐鎼佸磹妞嬪孩顐介柨鐔哄Т绾惧鏌涘☉鍗炵仭闁哄棙绮撻弻鐔兼倻濡儵鎷荤紓浣插亾濠电姴鍊甸弨浠嬫煟濡灝绱﹂弶鈺勵潐缁绘盯骞嬪▎蹇曚患闂佺粯甯掗悘姘跺Φ閸曨垰绠抽柟瀛樼箥娴犲ジ姊洪挊澶婃殶闁哥姵鐗犲濠氬即閻旈绐為梺鍓插亝缁诲倿藟鎼淬劍鈷戦柛婵嗗閸屻劑鏌涢妸銉хШ闁诡垰鑻灃闁告侗鍠氶崢鐢告煟鎼达絾鏆╂い顓炵墛缁傛帡鏁冮崒娑氬弮闂佺鏈崙褰掑磿韫囨稒顥嗗鑸靛姈閻撶喐淇婇婵愬殭缂佽尙绮妵鍕晲閸涱垽绱炵紓浣介哺鐢繝銆佸▎鎾充紶闁告洦鍋掑搴ㄦ煟?"""
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
        # 闂傚倷绀侀幉锟犳嚌妤ｅ啯鍋嬮柛鈩冪⊕閸ゆ劙鏌ｉ弮鍌氬付闁哄绶氶弻锝呂旈埀顒勬偋閸℃瑧鐭堥柨鏇楀亾闁宠棄顦甸獮妯肩驳鐟欏嫷鏆紓鍌欑劍椤ㄥ懘骞婂Ο渚殨妞ゆ帒瀚悙濠勬喐鎼达絺鍋撳顓犲弨闁哄本绋栫粻娑氣偓锝庝簻椤牓姊哄ú璁虫喚闁告挻姘ㄧ划娆愬緞鐎ｎ剛鐦堟繛杈剧秬濞咃綁骞夐鈧娲传閸曨偀鍋撶粙妫靛綊宕熼鐕佹綗濠电娀娼ч鍛村几娓氣偓閺岋絽螖閳ь剟鎮ч崱娆戠焾闁挎洖鍊归崐鍨箾閹寸偟鎳愭繛鍫熸礃閵囧嫰寮幐搴㈡嫳闁诲海鏁搁崢褑鐏掗梺鍛婄箓鐎氼剙顕?
        merged_ctx = dict(global_context or {})
        merged_ctx.update(ctx)
        # 婵犵數鍋犻幓顏嗙礊閳ь剚绻涙径瀣鐎?evaluate_expression 缂傚倸鍊搁崐鐑芥嚄閸洖绐楃€广儱娲ㄩ崡姘舵倵濞戞顏嗘閻愮儤鐓曢柡鍥ュ妼楠炴鏌涙繝鍕槐闁哄本绋戣灃闁告劑鍓遍敍鍕＝鐎广儱鎳忛崳鐣岀磼椤曞懎寮柡浣稿€块幊鐐哄Ψ瑜滃ú鐑芥⒒娴ｇ儤鍤€缂佺姴绉瑰畷纭呫亹閹烘垹顔嗘繛鏉戝悑濞兼瑩宕橀埀顒勬⒑缂佹ɑ灏繛鎾棑濞戠敻鍩€椤掑嫭鈷戦柛娑橈功婢ь剟鏌ｅΔ浣圭鐎规洜鏁婚弻鍡楊吋閸涱喚鈧?
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
            
            # 闂傚倸鍊峰ù鍥х暦閻㈢绐楅柟鎵閸嬶繝寮堕崼姘珔缂佽翰鍊曡灃闁挎繂鎳庨弳鐐烘煕婵犲洦娑ч棁澶愭煟濡儤鈻曢柛搴＄箻閺岋綁顢橀悤浣圭暦闂侀€涚┒閸斿矂鈥旈崘顏呭珰闂婎偒鍘奸ˉ姘舵⒒?
            value = resolve_value(raw_value, data)
            actual_raw = data.get(field, "")
            raw_candidates = resolve_actual_candidates(field, actual_raw, data)
            actual = raw_candidates[0] if raw_candidates else ""

            # 闂備浇顕х€涒晝绮欓幒妞尖偓鍐幢濞戣鲸鏅╅柣蹇曞仜婢т粙鎮烽崷顓熷枑閹兼番鍔嶉崑瀣磼鐎ｎ偒鍎ラ柛銈嗘礋閻擃偊宕堕妸锕€纰嶅銈嗘肠閸ャ劎鍙嗗┑鐐村灦閻熴劍绔熷Ο姹囦簻闁宠鍘煎ú銈夊礄閻樺磭绡€濠电姴鍊绘晶鏇㈡偨椤栨氨锛嶇紒杈ㄦ崌瀹曟﹢骞嗚閻忓棛绱?field_format 濠电姷顣藉Σ鍛村垂椤忓牆绀堟繝闈涙－閻斿棙鎱ㄥ璇蹭壕闂佽桨绀佺粔鑸电閿斿墽椹抽悗锝庝簼椤斿繘姊虹拠鎻掝劉缂佸甯￠妴鍐幢濞戣鲸鏅╅悗鍏夊亾闁告洦鍋勯崑宥夋⒑缂佹ɑ鐓ユい銊ラ椤洤螖閸涱喚鍘辨繝鐢靛Т鐎氼剛鏁捄濂藉綊鎮╁畷鍥ㄥ垱閻庤娲╃徊鍊熺亽闁荤姴娲﹁ぐ鍐姳?
            field_format = node.get("field_format", "")
            if field_format and "__VALUE__" in field_format:
                from utils.expression_functions import evaluate_expression as _eval_expr
                # 闂傚倷鐒﹀鍨焽閸ф绀夌€光偓閸曨剙浠遍悷婊冪Ч閸┿垽骞樼紒妯轰缓闂佸壊鐓堥崑鍡涘汲閻樼粯鐓熼幖绮瑰墲鐠愶繝鏌涚€ｃ劌鈧洟鈥栨繝鍥舵晬闁绘劘灏欓敍娆撴⒑缁嬭法绠伴柛姘儑缁鐣濋崟顒傚幍?__VALUE__ 闂傚倷绀侀幉锟犳嚌閸撗€鍋撳☉鎺撴珚闁诡垰娲︾€靛ジ寮堕幋鐙呯串?
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


