# -*- coding: utf-8 -*-
import json
from importlib import import_module
from typing import Any, Dict, List, Optional, Set

from fastapi import HTTPException
from sqlalchemy.orm import Session

import models
import schemas
from voucher_field_mapping import enrich_source_data as mapping_enrich_source_data


def _main_attr(name: str):
    return getattr(import_module("main"), name)


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


def _normalize_template_for_response(template: models.VoucherTemplate) -> None:
    if template.priority is not None:
        try:
            template.priority = max(int(template.priority), 0)
        except (TypeError, ValueError):
            template.priority = 100
    if template.rules:
        for idx, rule in enumerate(template.rules, start=1):
            _main_attr("_normalize_rule_for_response")(rule, idx)


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
    enforce_field_check = _main_attr("_get_source_meta")(normalized_source) is not None

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
            operator = _main_attr("_canonicalize_trigger_operator")(raw_operator)
            if operator is None:
                errors.append(f"{path}.operator is not supported: {raw_operator}")

            _main_attr("_validate_unknown_placeholders")(node.get("value", ""), f"{path}.value", allowed_placeholders, errors)
            _main_attr("_validate_unknown_functions")(node.get("value", ""), f"{path}.value", errors)
            return

        if node_type == "relation":
            resolver = str(node.get("resolver", "")).strip()
            target_source = str(node.get("target_source", "")).strip().lower()
            quantifier = str(node.get("quantifier", "EXISTS")).upper()
            relation_meta = _main_attr("RELATION_REGISTRY").get(resolver)

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

            relation_group = _main_attr("_normalize_relation_group")(node)
            if relation_group["logic"] not in {"AND", "OR"}:
                errors.append(f"{path}.logic must be AND or OR")
            if not isinstance(relation_group["children"], list):
                errors.append(f"{path}.children must be an array")
                return

            relation_fields = _main_attr("_build_allowed_source_fields_for_type")(expected_target or target_source)
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
    allowed_placeholders = _main_attr("_build_allowed_placeholders")(source_type, source_module, db)
    normalized_source = (source_type or "").strip().lower()
    allowed_source_fields = _main_attr("_build_allowed_source_fields_for_type")(normalized_source or "bills")

    _main_attr("_validate_unknown_placeholders")(payload.get("book_number_expr"), "book_number_expr", allowed_placeholders, errors)
    _main_attr("_validate_unknown_placeholders")(payload.get("vouchertype_number_expr"), "vouchertype_number_expr", allowed_placeholders, errors)
    _main_attr("_validate_unknown_placeholders")(payload.get("attachment_expr"), "attachment_expr", allowed_placeholders, errors)
    _main_attr("_validate_unknown_placeholders")(payload.get("bizdate_expr"), "bizdate_expr", allowed_placeholders, errors)
    _main_attr("_validate_unknown_placeholders")(payload.get("bookeddate_expr"), "bookeddate_expr", allowed_placeholders, errors)
    _main_attr("_validate_unknown_functions")(payload.get("book_number_expr"), "book_number_expr", errors)
    _main_attr("_validate_unknown_functions")(payload.get("vouchertype_number_expr"), "vouchertype_number_expr", errors)
    _main_attr("_validate_unknown_functions")(payload.get("attachment_expr"), "attachment_expr", errors)
    _main_attr("_validate_unknown_functions")(payload.get("bizdate_expr"), "bizdate_expr", errors)
    _main_attr("_validate_unknown_functions")(payload.get("bookeddate_expr"), "bookeddate_expr", errors)

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

        _main_attr("_validate_unknown_placeholders")(rule.get("amount_expr"), f"rules[{idx}].amount_expr", allowed_placeholders, errors)
        _main_attr("_validate_unknown_placeholders")(rule.get("summary_expr"), f"rules[{idx}].summary_expr", allowed_placeholders, errors)
        _main_attr("_validate_unknown_placeholders")(rule.get("currency_expr"), f"rules[{idx}].currency_expr", allowed_placeholders, errors)
        _main_attr("_validate_unknown_placeholders")(rule.get("localrate_expr"), f"rules[{idx}].localrate_expr", allowed_placeholders, errors)
        _main_attr("_validate_unknown_functions")(rule.get("amount_expr"), f"rules[{idx}].amount_expr", errors)
        _main_attr("_validate_unknown_functions")(rule.get("summary_expr"), f"rules[{idx}].summary_expr", errors)
        _main_attr("_validate_unknown_functions")(rule.get("currency_expr"), f"rules[{idx}].currency_expr", errors)
        _main_attr("_validate_unknown_functions")(rule.get("localrate_expr"), f"rules[{idx}].localrate_expr", errors)

        _validate_trigger_condition(
            rule.get("display_condition_expr"),
            source_type,
            allowed_placeholders,
            allowed_source_fields,
            errors,
            f"rules[{idx}].display_condition_expr",
        )

        aux_mapping = _main_attr("_validate_dimension_mapping_json")(
            rule.get("aux_items"),
            f"rules[{idx}].aux_items",
            allowed_placeholders,
            errors,
        )
        _main_attr("_validate_dimension_mapping_json")(
            rule.get("main_cf_assgrp"),
            f"rules[{idx}].main_cf_assgrp",
            allowed_placeholders,
            errors,
        )

        account_code = _main_attr("_normalize_literal_account_code")(rule.get("account_code"))
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

        required_dims = _main_attr("_extract_required_check_dimensions")(subject)
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


def get_voucher_templates(
    db: Session,
    current_user: models.User,
):
    """Get all voucher templates"""
    _main_attr("_require_api_permission")(db, current_user, "voucher_template.manage")
    categories = db.query(models.VoucherTemplateCategory).all()
    category_path_map = _main_attr("build_template_category_path_map")(categories)
    templates = db.query(models.VoucherTemplate).order_by(
        models.VoucherTemplate.priority.asc(),
        models.VoucherTemplate.template_id.asc(),
    ).all()
    for template in templates:
        template.category_path = category_path_map.get(getattr(template, "category_id", None))
        _normalize_template_for_response(template)
    return templates


def get_voucher_template(
    template_id: str,
    db: Session,
    current_user: models.User,
):
    """Get a specific voucher template"""
    _main_attr("_require_api_permission")(db, current_user, "voucher_template.manage")
    template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template_id).first()
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    if getattr(template, "category_id", None) is not None:
        categories = db.query(models.VoucherTemplateCategory).all()
        category_path_map = _main_attr("build_template_category_path_map")(categories)
        template.category_path = category_path_map.get(getattr(template, "category_id", None))
    _normalize_template_for_response(template)
    return template


def create_voucher_template(
    template: schemas.VoucherTemplateCreate,
    db: Session,
    current_user: models.User,
):
    """Create a new voucher template with rules"""
    _main_attr("_require_api_permission")(db, current_user, "voucher_template.manage")
    existing = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template.template_id).first()
    if existing:
        raise HTTPException(status_code=400, detail="Template ID already exists")

    template_data = template.dict()
    rules_data = template_data.pop("rules", [])
    _main_attr("_validate_voucher_template_payload")({**template_data, "rules": rules_data}, db)

    new_template = models.VoucherTemplate(**template_data)
    db.add(new_template)

    for rule in rules_data:
        new_rule = models.VoucherEntryRule(**rule, template_id=new_template.template_id)
        db.add(new_rule)

    db.commit()
    db.refresh(new_template)
    return new_template


def update_voucher_template(
    template_id: str,
    template: schemas.VoucherTemplateUpdate,
    db: Session,
    current_user: models.User,
):
    """Update a voucher template and its rules"""
    _main_attr("_require_api_permission")(db, current_user, "voucher_template.manage")
    db_template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template_id).first()
    if not db_template:
        raise HTTPException(status_code=404, detail="Template not found")

    update_data = template.dict(exclude_unset=True)
    rules_data = update_data.pop("rules", None)

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
    _main_attr("_validate_voucher_template_payload")(full_payload, db)

    for key, value in update_data.items():
        setattr(db_template, key, value)

    if rules_data is not None:
        db.query(models.VoucherEntryRule).filter(models.VoucherEntryRule.template_id == template_id).delete()
        for rule in rules_data:
            new_rule = models.VoucherEntryRule(**rule, template_id=template_id)
            db.add(new_rule)

    db.commit()
    db.refresh(db_template)
    return db_template


def delete_voucher_template(
    template_id: str,
    db: Session,
    current_user: models.User,
):
    """Delete a voucher template"""
    _main_attr("_require_api_permission")(db, current_user, "voucher_template.manage")
    db_template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.template_id == template_id).first()
    if not db_template:
        raise HTTPException(status_code=404, detail="Template not found")

    db.query(models.VoucherEntryRule).filter(models.VoucherEntryRule.template_id == template_id).delete()
    db.delete(db_template)
    db.commit()
    return {"message": "Template deleted"}


def resolve_voucher_fields(
    payload: dict,
    db: Session,
):
    """
    解析并补全账单数据中的金蝶衍生字段。

    输入示例:
    { "bill_data": { "house_id": "123", "park_id": "456" } }

    返回示例:
    { "enriched_data": { "kd_house_number": "H001" } }
    """
    bill_data = payload.get("bill_data", {})
    enriched = mapping_enrich_source_data("bills", bill_data, db=db)
    return {"enriched_data": enriched}
