# -*- coding: utf-8 -*-
import json
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session, joinedload

import models
import schemas


@lru_cache(maxsize=256)
def _main_attr(name: str):
    return getattr(import_module("main"), name)


def _decimal_text(value: Any) -> str:
    parsed = _main_attr("_try_parse_decimal")(value)
    if parsed is None:
        return "0"
    text = format(parsed, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


MONEY_QUANTIZER = Decimal("0.01")


def _money_text(value: Any) -> str:
    parsed = _main_attr("_try_parse_decimal")(value) or Decimal("0")
    rounded = parsed.quantize(MONEY_QUANTIZER, rounding=ROUND_HALF_UP)
    return format(rounded, ".2f")


def _is_money_field(field_name: Optional[str]) -> bool:
    normalized = str(field_name or "").strip().lower()
    return normalized == "amount" or normalized.endswith("_amount") or normalized == "income_amount" or normalized == "balance_after_change"


def _serialize_source_scalar(field_name: Optional[str], value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return _money_text(value) if _is_money_field(field_name) else _decimal_text(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _entry_decimal(entry: Dict[str, Any], *field_names: str) -> Decimal:
    for field_name in field_names:
        parsed = _main_attr("_try_parse_decimal")(entry.get(field_name))
        if parsed is not None:
            return parsed
    return Decimal("0")


def _is_tax_accounting_entry(entry: Dict[str, Any]) -> bool:
    account_code = str(entry.get("account_code") or "").strip()
    summary = str(entry.get("summary") or "").strip()
    return account_code.startswith("2221") or "税" in summary


def _allocate_money_amounts(values: List[Decimal], entries: Optional[List[Dict[str, Any]]] = None) -> List[Decimal]:
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
        def _build_candidate_order(positive_only: bool) -> List[int]:
            candidates: List[Tuple[int, Decimal]] = []
            for idx, remainder in remainders:
                if positive_only and remainder <= Decimal("0"):
                    continue
                candidates.append((idx, remainder))

            if not candidates:
                return []

            def _sort_key(item: Tuple[int, Decimal]) -> Tuple[int, Decimal, Decimal, int]:
                idx, remainder = item
                entry = entries[idx] if entries and idx < len(entries) else {}
                exact_value = values[idx].copy_abs()
                is_tax = _is_tax_accounting_entry(entry)
                return (1 if is_tax else 0, -exact_value, -remainder, idx)

            ordered = sorted(candidates, key=_sort_key)
            return [idx for idx, _ in ordered]

        candidate_order = _build_candidate_order(positive_only=True)
        if not candidate_order:
            candidate_order = _build_candidate_order(positive_only=False)

        for i in range(delta):
            base_cents[candidate_order[i % len(candidate_order)]] += 1

    return [Decimal(cents) / scale_factor for cents in base_cents]


def _normalize_voucher_money_fields(accounting_entries: List[Dict[str, Any]], kingdee_entries: List[Dict[str, Any]]) -> None:
    debit_values = [
        _entry_decimal(entry, "debit_formula_exact", "debit_exact", "debit")
        for entry in accounting_entries
    ]
    credit_values = [
        _entry_decimal(entry, "credit_formula_exact", "credit_exact", "credit")
        for entry in accounting_entries
    ]
    rounded_debits = _allocate_money_amounts(debit_values, accounting_entries)
    rounded_credits = _allocate_money_amounts(credit_values, accounting_entries)

    for idx, accounting_entry in enumerate(accounting_entries):
        debit_value = rounded_debits[idx]
        credit_value = rounded_credits[idx]
        debit_text = format(debit_value, ".2f")
        credit_text = format(credit_value, ".2f")

        accounting_entry["debit_exact"] = debit_text
        accounting_entry["credit_exact"] = credit_text
        accounting_entry["debit"] = debit_text
        accounting_entry["credit"] = credit_text

        if idx >= len(kingdee_entries):
            continue

        kingdee_entry = kingdee_entries[idx]
        kingdee_entry["debitori"] = debit_text
        kingdee_entry["creditori"] = credit_text
        kingdee_entry["debitlocal"] = debit_text
        kingdee_entry["creditlocal"] = credit_text


def _check_trigger_conditions(
    node: dict,
    data: dict,
    debug_logs: list = None,
    global_context: Optional[dict] = None,
    relation_context: Optional[dict] = None,
) -> bool:
    """闂侇偅甯掔紞濠偽涢埀顒勫蓟閵夈剱鏇㈠矗閹寸偞钂嬪ù鐘插缁劑寮?"""
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
        # 鍚堝苟鏁版嵁涓婁笅鏂囧拰鍏ㄥ眬涓婁笅鏂囷紝鏁版嵁瀛楁浼樺厛
        merged_ctx = dict(global_context or {})
        merged_ctx.update(ctx)
        from utils.expression_functions import evaluate_expression as _eval_expr
        return _eval_expr(val_str, merged_ctx)

    try:
        node_type = node.get("type", "group")

        if node_type in {"group", "relation"}:
            if node_type == "relation":
                resolver = str(node.get("resolver", "")).strip()
                quantifier = str(node.get("quantifier", "EXISTS")).upper()
                relation_meta = _main_attr("RELATION_REGISTRY").get(resolver)
                root_record = (relation_context or {}).get("root_record") or (relation_context or {}).get("receipt_bill")
                db = (relation_context or {}).get("db")
                relation_cache = (relation_context or {}).setdefault("cache", {})
                selected_records = (relation_context or {}).setdefault("selected_records", {})
                relation_group = _main_attr("_normalize_relation_group")(node)
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
            operator = _main_attr("_canonicalize_trigger_operator")(raw_operator)
            if not operator:
                debug_logs.append(f"Unsupported operator '{raw_operator}' for field '{field}', treated as False")
                return False
            raw_value = str(node.get("value", ""))

            # 閻熸瑱绲鹃悗浠嬪矗濮椻偓閸?
            value = resolve_value(raw_value, data)
            actual_raw = data.get(field, "")
            raw_candidates = resolve_actual_candidates(field, actual_raw, data)
            actual = raw_candidates[0] if raw_candidates else ""

            # 瀛楁渚ф牸寮忓寲锛氬鏋滈厤缃簡 field_format 妯℃澘锛屽瀛楁鍘熷鍊煎仛鍙樻崲
            field_format = node.get("field_format", "")
            if field_format and "__VALUE__" in field_format:
                from utils.expression_functions import evaluate_expression as _eval_expr
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
                res, compare_mode = _main_attr("_compare_ordered_values")(actual_raw, value, operator)
            elif operator == ">=":
                res, compare_mode = _main_attr("_compare_ordered_values")(actual_raw, value, operator)
            elif operator == "<":
                res, compare_mode = _main_attr("_compare_ordered_values")(actual_raw, value, operator)
            elif operator == "<=":
                res, compare_mode = _main_attr("_compare_ordered_values")(actual_raw, value, operator)
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


def _preview_voucher_for_bill_via_receipt_templates(
    bill: models.Bill,
    enriched_bill: Dict[str, Any],
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
    current_user: models.User,
    db: Session,
    receipt_bill_override: Optional[models.ReceiptBill] = None,
    receipt_enriched_override: Optional[Dict[str, Any]] = None,
    runtime_vars_override: Optional[Dict[str, str]] = None,
    receipt_templates_override: Optional[List[models.VoucherTemplate]] = None,
    source_bills_override: Optional[List[Dict[str, Any]]] = None,
    relation_cache_override: Optional[Dict[Any, Any]] = None,
    trigger_condition_cache_override: Optional[Dict[str, Any]] = None,
    subject_names_cache_override: Optional[Dict[str, str]] = None,
    subject_type_cache_override: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    from services.voucher_engine import evaluate_expression
    from utils.variable_parser import build_variable_map, resolve_variables

    if not bill.deal_log_id:
        return None

    receipt_bill = receipt_bill_override or (
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

    # 浣跨敤澶栭儴浼犲叆鐨勬帹閫佺姸鎬侊紝閬垮厤姣忎釜 bill 鍗曠嫭鏌?DB
    if source_bills_override is not None:
        source_bills = source_bills_override
    else:
        normalized_account_book_number = _main_attr("_decode_header_value")(x_account_book_number) or None
        bill_ref = {"bill_id": int(bill.id), "community_id": int(bill.community_id)}
        source_status_map = _main_attr("_get_bill_push_status_map")(
            db,
            [bill_ref],
            account_book_number=normalized_account_book_number,
        )
        source_bills = [source_status_map[(int(bill.id), int(bill.community_id))]]
    source_bill_push_summary = _main_attr("_summarize_bill_push_statuses")(source_bills)
    push_conflicts = _main_attr("_find_bill_push_conflicts")(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})" for item in push_conflicts[:10]]
        )
        push_block_reason = f"Current bill already has voucher push records: {conflict_preview}"

    enriched_receipt = receipt_enriched_override
    if enriched_receipt is None:
        receipt_data = _main_attr("_serialize_receipt_bill_model")(receipt_bill, db)
        enriched_receipt = _main_attr("_enrich_receipt_bill_data")(receipt_data, receipt_bill=receipt_bill, db=db)

    runtime_vars = runtime_vars_override
    if runtime_vars is None:
        user_context = _main_attr("_build_preview_user_context")(
            current_user,
            x_account_book_id=x_account_book_id,
            x_account_book_name=x_account_book_name,
            x_account_book_number=x_account_book_number,
        )
        runtime_vars = build_variable_map(db, user_context=user_context)
    scoped_relation_records = {"bills": [enriched_bill]}

    match_result = _main_attr("_match_receipt_templates")(
        receipt_bill=receipt_bill,
        enriched=enriched_receipt,
        runtime_vars=runtime_vars,
        db=db,
        scoped_relation_records=scoped_relation_records,
        templates_override=receipt_templates_override,
        relation_cache_override=relation_cache_override,
        trigger_condition_cache_override=trigger_condition_cache_override,
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
            "receipt_summary": _main_attr("_build_receipt_summary_payload")(receipt_bill),
            "bill_summary": _main_attr("_build_bill_summary_payload")(bill),
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
    subject_names_cache = subject_names_cache_override if subject_names_cache_override is not None else {}
    subject_type_cache = subject_type_cache_override if subject_type_cache_override is not None else {}
    rule_relation_base_ctx = {
        "db": db,
        "root_record": receipt_bill,
        "receipt_bill": receipt_bill,
        "cache": relation_cache_override if relation_cache_override is not None else {},
    }
    if scoped_relation_records is not None:
        rule_relation_base_ctx["scoped_records"] = scoped_relation_records

    for rule in sorted(matched_template.rules, key=lambda r: r.line_no):
        visible, rule_selected_records = _main_attr("_evaluate_rule_display_condition")(
            rule.display_condition_expr,
            enriched_receipt,
            runtime_vars,
            rule_relation_base_ctx,
        )
        if not visible:
            continue
        rule_expression_context = _main_attr("_merge_selected_record_values")(expression_context, rule_selected_records)
        summary = resolve_expr(rule.summary_expr, rule_expression_context)
        account_code = _main_attr("_normalize_literal_account_code")(rule.account_code) or (rule.account_code or "").strip()
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

        amount_val = _main_attr("_try_parse_decimal")(amount_str) or Decimal("0")
        localrate_val = _main_attr("_try_parse_decimal")(localrate) or Decimal("1")

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
            "debit_formula_exact": _decimal_text(amount_val) if rule.dr_cr == "D" else "0",
            "credit_formula_exact": _decimal_text(amount_val) if rule.dr_cr == "C" else "0",
            "debit": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "credit": _money_text(amount_val) if rule.dr_cr == "C" else "0.00",
            "debit_exact": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "credit_exact": _money_text(amount_val) if rule.dr_cr == "C" else "0.00",
            "currency": currency,
            "localrate": _decimal_text(localrate_val),
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
            "attachment": _main_attr("_parse_attachment_count")(attachment),
            "entries": kingdee_entries,
        }]
    }

    return {
        "matched": True,
        "matched_root_source": "receipt_bills",
        "matched_via_receipt": True,
        "template_id": matched_template.template_id,
        "template_name": matched_template.template_name,
        "receipt_summary": _main_attr("_build_receipt_summary_payload")(receipt_bill),
        "bill_summary": _main_attr("_build_bill_summary_payload")(bill),
        "matched_relation_sources": sorted(matched_selected_records.keys()),
        "accounting_view": {
            "entries": accounting_entries,
            "total_debit": _money_text(total_debit),
            "total_credit": _money_text(total_credit),
            "total_debit_exact": _money_text(total_debit),
            "total_credit_exact": _money_text(total_credit),
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


def preview_voucher_for_bill(
    bill_id: int,
    community_id: Optional[int],
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
    allow_receipt_fallback: bool,
    current_user: models.User,
    db: Session,
    allowed_community_ids: List[int],
    _bill_override: Optional[models.Bill] = None,
    _enriched_bill_override: Optional[Dict[str, Any]] = None,
    _source_bills_override: Optional[List[Dict[str, Any]]] = None,
):
    """
    棰勮鍗曠瑪璐﹀崟瀵瑰簲鐨勫嚟璇佸唴瀹广€?
    杩斿洖鍖呭惈涓や釜閮ㄥ垎锛?    1. `accounting_view`: 闈㈠悜涓氬姟鏌ョ湅鐨勪細璁″嚟璇佽鍥?    2. `kingdee_json`: 鍙洿鎺ョ敤浜?`voucherAdd` 鐨勯噾铦惰姹?JSON
    """
    from services.voucher_engine import evaluate_expression
    from utils.variable_parser import build_variable_map, resolve_variables
    import json as json_mod
    from datetime import datetime

    # 1. Query bill
    if _bill_override is not None:
        bill = _bill_override
    else:
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

    normalized_account_book_number = _main_attr("_decode_header_value")(x_account_book_number) or None
    if _source_bills_override is not None:
        source_bills = _source_bills_override
    else:
        source_refs = [{
            "bill_id": int(bill.id),
            "community_id": int(bill.community_id),
        }]
        source_status_map = _main_attr("_get_bill_push_status_map")(
            db,
            source_refs,
            account_book_number=normalized_account_book_number,
        )
        source_bills = [source_status_map[(int(bill.id), int(bill.community_id))]]
    source_bill_push_summary = _main_attr("_summarize_bill_push_statuses")(source_bills)
    push_conflicts = _main_attr("_find_bill_push_conflicts")(source_bills)
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

    if _enriched_bill_override is not None:
        enriched = _enriched_bill_override
    else:
        bill_data = {}
        for col in models.Bill.__table__.columns:
            bill_data[col.name] = _serialize_source_scalar(col.name, getattr(bill, col.name, None))

        enriched = _main_attr("mapping_enrich_source_data")("bills", bill_data, db=db)

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

    from urllib.parse import unquote
    org_name = current_user.organization.name if current_user.organization else "未分配组织"
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
            if _main_attr("_check_trigger_conditions")(conditions, enriched, debug_logs, runtime_vars):
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
            receipt_result = _main_attr("_preview_voucher_for_bill_via_receipt_templates")(
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
            "bill_summary": _main_attr("_build_bill_summary_payload")(bill),
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

    # 5. 鐟欙絾鐎藉В蹇旀蒋閸掑棗缍嶇憴鍕灟
    accounting_entries = []
    kingdee_entries = []

    # Prepare subject naming cache
    subject_names_cache = {}
    subject_type_cache = {}
    expression_context = dict(enriched)

    for rule in sorted(matched_template.rules, key=lambda r: r.line_no):
        visible, rule_selected_records = _main_attr("_evaluate_rule_display_condition")(
            rule.display_condition_expr,
            enriched,
            runtime_vars,
        )
        if not visible:
            continue
        rule_expression_context = _main_attr("_merge_selected_record_values")(expression_context, rule_selected_records)
        summary = resolve_expr(rule.summary_expr, rule_expression_context)
        account_code = _main_attr("_normalize_literal_account_code")(rule.account_code) or (rule.account_code or "").strip()
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

        amount_val = _main_attr("_try_parse_decimal")(amount_str) or Decimal("0")
        localrate_val = _main_attr("_try_parse_decimal")(localrate) or Decimal("1")

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

        # 鐟欙絾鐎芥稉鏄忋€冮弽鍝ョ暬
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

        # 娴肩姷绮烘导姘愁吀閸戭叀鐦夐弽鐓庣础
        accounting_entries.append({
            "line_no": rule.line_no,
            "summary": summary,
            "account_code": account_code,
            "account_name": subject_names_cache.get(account_code, ""),
            "account_type_number": subject_type_cache.get(account_code, ""),
            "account_display": account_display_name,
            "dr_cr": rule.dr_cr,
            "debit_formula_exact": _decimal_text(amount_val) if rule.dr_cr == 'D' else "0",
            "credit_formula_exact": _decimal_text(amount_val) if rule.dr_cr == 'C' else "0",
            "debit": _money_text(amount_val) if rule.dr_cr == 'D' else "0.00",
            "credit": _money_text(amount_val) if rule.dr_cr == 'C' else "0.00",
            "debit_exact": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "credit_exact": _money_text(amount_val) if rule.dr_cr == "C" else "0.00",
            "currency": currency,
            "localrate": _decimal_text(localrate_val),
            "localrate_exact": _decimal_text(localrate_val),
            "assgrp": assgrp if assgrp else None,
            "maincfassgrp": maincfassgrp if maincfassgrp else None,
        })

        # 闂佸弶鍨煎?API JSON 闁哄秶鍘х槐?
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

    # 6. 缂備礁瀚悗鐟版湰閺嗭綁鎯冮崟顖氭闁撅箒鍩栫敮褰掓焻?JSON 缂備焦鎸婚悗?
    kingdee_json = {
        "data": [{
            "book_number": book_number,
            "bizdate": biz_date,
            "bookeddate": booked_date,
            "period_number": period_number,
            "vouchertype_number": vouchertype_number,
            "description": (matched_template.template_name or matched_template.template_id or "未命名模板"),
            "attachment": _main_attr("_parse_attachment_count")(attachment),
            "entries": kingdee_entries
        }]
    }

    return {
        "matched": True,
        "matched_root_source": "bills",
        "matched_via_receipt": False,
        "template_id": matched_template.template_id,
        "template_name": matched_template.template_name,
        "bill_summary": _main_attr("_build_bill_summary_payload")(bill),
        "enriched_fields": {k: v for k, v in enriched.items() if k.startswith('kd_')},
        "accounting_view": {
            "entries": accounting_entries,
            "total_debit": _money_text(total_debit),
            "total_credit": _money_text(total_credit),
            "total_debit_exact": _money_text(total_debit),
            "total_credit_exact": _money_text(total_credit),
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


def preview_voucher_for_receipt(
    receipt_bill_id: int,
    community_id: int,
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
    allow_bill_fallback: bool,
    current_user: models.User,
    db: Session,
    allowed_community_ids: List[int],
    _templates_cache: Optional[List[models.VoucherTemplate]] = None,
    _runtime_vars_cache: Optional[Dict[str, str]] = None,
    _user_context_cache: Optional[Dict[str, str]] = None,
    _trigger_condition_cache: Optional[Dict[str, Any]] = None,
):
    from services.voucher_engine import evaluate_expression
    from utils.variable_parser import build_variable_map, resolve_variables
    import json as json_mod

    preloaded_scoped_relation_records: Optional[Dict[str, List[Dict[str, Any]]]] = None

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

    normalized_account_book_number = _main_attr("_decode_header_value")(x_account_book_number) or None
    source_bills = _main_attr("_collect_receipt_source_bills")(
        db,
        receipt_bill_id=int(receipt_bill.id),
        community_id=int(receipt_bill.community_id),
        account_book_number=normalized_account_book_number,
    )
    source_bill_push_summary = _main_attr("_summarize_bill_push_statuses")(source_bills)
    push_conflicts = _main_attr("_find_bill_push_conflicts")(source_bills)
    push_blocked = len(push_conflicts) > 0
    push_block_reason = None
    if push_blocked:
        conflict_preview = ", ".join(
            [f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})" for item in push_conflicts[:10]]
        )
        push_block_reason = f"Related bills already have voucher push records: {conflict_preview}"

    if source_bills:
        receipt_data_cached = _main_attr("_serialize_receipt_bill_model")(receipt_bill, db)
        enriched_receipt_cached = _main_attr("_enrich_receipt_bill_data")(receipt_data_cached, receipt_bill=receipt_bill, db=db)
        if _user_context_cache is not None:
            user_context_cached = _user_context_cache
        else:
            user_context_cached = _main_attr("_build_preview_user_context")(
                current_user,
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
            )
        runtime_vars_cached = _runtime_vars_cache if _runtime_vars_cache is not None else build_variable_map(db, user_context=user_context_cached)
        if _templates_cache is not None:
            receipt_templates_cached = _templates_cache
        else:
            receipt_templates_cached = db.query(models.VoucherTemplate).filter(
                models.VoucherTemplate.active == True,
                models.VoucherTemplate.source_type == "receipt_bills",
            ).order_by(
                models.VoucherTemplate.priority.asc(),
                models.VoucherTemplate.template_id.asc(),
            ).all()

        related_bills = (
            db.query(models.Bill)
            .filter(
                models.Bill.deal_log_id == int(receipt_bill.id),
                models.Bill.community_id == int(receipt_bill.community_id),
            )
            .order_by(models.Bill.id.asc())
            .all()
        )

        all_bill_data_list: List[Dict[str, Any]] = []
        for related_bill in related_bills:
            bill_data = {}
            for col in models.Bill.__table__.columns:
                bill_data[col.name] = _serialize_source_scalar(col.name, getattr(related_bill, col.name, None))
            all_bill_data_list.append(bill_data)

        from services.voucher_engine import batch_preload_kd_cache, enrich_bill_data_cached
        kd_cache = batch_preload_kd_cache(all_bill_data_list, db)
        all_enriched_related_bills = [enrich_bill_data_cached(bill_data, kd_cache) for bill_data in all_bill_data_list]
        preloaded_scoped_relation_records = {"bills": all_enriched_related_bills}

        # 鎵归噺棰勫姞杞芥墍鏈?bill 鐨勬帹閫佺姸鎬侊紙涓€娆?DB 鏌ヨ鏇夸唬 N 娆★級
        all_bill_refs = [
            {"bill_id": int(b.id), "community_id": int(b.community_id)}
            for b in related_bills
        ]
        all_push_status_map = _main_attr("_get_bill_push_status_map")(
            db,
            all_bill_refs,
            account_book_number=normalized_account_book_number,
        )

        previews: List[Dict[str, Any]] = []
        skipped_bills: List[Dict[str, Any]] = []
        shared_relation_cache: Dict[Any, Any] = {}
        shared_condition_cache = _trigger_condition_cache if _trigger_condition_cache is not None else {}
        shared_subject_names: Dict[str, str] = {}
        shared_subject_types: Dict[str, str] = {}

        for idx, related_bill in enumerate(related_bills):
            enriched_bill = all_enriched_related_bills[idx]

            bill_key = (int(related_bill.id), int(related_bill.community_id))
            bill_push_entry = all_push_status_map.get(bill_key)
            bill_source_bills = [bill_push_entry] if bill_push_entry else []

            result = _main_attr("_preview_voucher_for_bill_via_receipt_templates")(
                bill=related_bill,
                enriched_bill=enriched_bill,
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                current_user=current_user,
                db=db,
                receipt_bill_override=receipt_bill,
                receipt_enriched_override=enriched_receipt_cached,
                runtime_vars_override=runtime_vars_cached,
                receipt_templates_override=receipt_templates_cached,
                source_bills_override=bill_source_bills,
                relation_cache_override=shared_relation_cache,
                trigger_condition_cache_override=shared_condition_cache,
                subject_names_cache_override=shared_subject_names,
                subject_type_cache_override=shared_subject_types,
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

            matched_source_bill_push_summary = _main_attr("_summarize_bill_push_statuses")(matched_source_bills)
            matched_push_conflicts = _main_attr("_find_bill_push_conflicts")(matched_source_bills)
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
                "receipt_summary": _main_attr("_build_receipt_summary_payload")(receipt_bill),
                "selected_bills": source_bills,
                "selected_bill_push_summary": source_bill_push_summary,
                "source_bills": matched_source_bills,
                "source_bill_push_summary": matched_source_bill_push_summary,
                "skipped_bills": skipped_bills,
                "push_blocked": merged_push_blocked,
                "push_block_reason": merged_push_block_reason,
                "accounting_view": {
                    "entries": merged_accounting_entries,
                    "total_debit": _money_text(total_debit),
                    "total_credit": _money_text(total_credit),
                    "total_debit_exact": _money_text(total_debit),
                    "total_credit_exact": _money_text(total_credit),
                    "is_balanced": (total_debit - total_credit) == Decimal("0") and (total_credit - total_debit) == Decimal("0"),
                },
                "kingdee_json": merged_kingdee_json,
            }

    receipt_data = _main_attr("_serialize_receipt_bill_model")(receipt_bill, db)
    enriched = _main_attr("_enrich_receipt_bill_data")(receipt_data, receipt_bill=receipt_bill, db=db)

    if _user_context_cache is not None:
        user_context = _user_context_cache
    else:
        user_context = _main_attr("_build_preview_user_context")(
            current_user,
            x_account_book_id=x_account_book_id,
            x_account_book_name=x_account_book_name,
            x_account_book_number=x_account_book_number,
        )
    runtime_vars = _runtime_vars_cache if _runtime_vars_cache is not None else build_variable_map(db, user_context=user_context)

    match_result = _main_attr("_match_receipt_templates")(
        receipt_bill=receipt_bill,
        enriched=enriched,
        runtime_vars=runtime_vars,
        db=db,
        scoped_relation_records=preloaded_scoped_relation_records,
        templates_override=_templates_cache,
        trigger_condition_cache_override=_trigger_condition_cache,
    )
    templates = match_result["templates"]
    matched_template = match_result["matched_template"]
    matched_selected_records = match_result["matched_selected_records"]
    all_debug_logs = match_result["debug_logs"]

    if not matched_template:
        if allow_bill_fallback and source_bills:
            return _main_attr("preview_voucher_for_bills")(
                payload=schemas.BatchVoucherPreviewRequest(
                    bills=[schemas.BillPreviewRef(bill_id=int(item["bill_id"]), community_id=int(item["community_id"])) for item in source_bills]
                ),
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                allow_receipt_fallback=False,
                current_user=current_user,
                db=db,
                allowed_community_ids=allowed_community_ids,
            )

        return {
            "matched": False,
            "message": "No applicable voucher template matched",
            "matched_root_source": "receipt_bills",
            "matched_via_receipt": False,
            "receipt_summary": _main_attr("_build_receipt_summary_payload")(receipt_bill),
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
        visible, rule_selected_records = _main_attr("_evaluate_rule_display_condition")(
            rule.display_condition_expr,
            enriched,
            runtime_vars,
            rule_relation_base_ctx,
        )
        if not visible:
            continue
        rule_expression_context = _main_attr("_merge_selected_record_values")(expression_context, rule_selected_records)
        summary = resolve_expr(rule.summary_expr, rule_expression_context)
        account_code = _main_attr("_normalize_literal_account_code")(rule.account_code) or (rule.account_code or "").strip()
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

        amount_val = _main_attr("_try_parse_decimal")(amount_str) or Decimal("0")
        localrate_val = _main_attr("_try_parse_decimal")(localrate) or Decimal("1")

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
            "debit_formula_exact": _decimal_text(amount_val) if rule.dr_cr == "D" else "0",
            "credit_formula_exact": _decimal_text(amount_val) if rule.dr_cr == "C" else "0",
            "debit": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "credit": _money_text(amount_val) if rule.dr_cr == "C" else "0.00",
            "debit_exact": _money_text(amount_val) if rule.dr_cr == "D" else "0.00",
            "credit_exact": _money_text(amount_val) if rule.dr_cr == "C" else "0.00",
            "currency": currency,
            "localrate": _decimal_text(localrate_val),
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
            "attachment": _main_attr("_parse_attachment_count")(attachment),
            "entries": kingdee_entries,
        }]
    }

    return {
        "matched": True,
        "matched_root_source": "receipt_bills",
        "matched_via_receipt": False,
        "template_id": matched_template.template_id,
        "template_name": matched_template.template_name,
        "receipt_summary": _main_attr("_build_receipt_summary_payload")(receipt_bill),
        "matched_relation_sources": sorted(matched_selected_records.keys()),
        "accounting_view": {
            "entries": accounting_entries,
            "total_debit": _money_text(total_debit),
            "total_credit": _money_text(total_credit),
            "total_debit_exact": _money_text(total_debit),
            "total_credit_exact": _money_text(total_credit),
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


def preview_voucher_for_receipts(
    payload: schemas.BatchReceiptVoucherPreviewRequest,
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
    current_user: models.User,
    db: Session,
    allowed_community_ids: List[int],
):
    if not payload.receipts:
        raise HTTPException(status_code=400, detail="No receipts selected")

    unique_refs = _main_attr("_normalize_receipt_refs")(payload.receipts)
    if not allowed_community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    allowed_set = set(allowed_community_ids)
    unauthorized = [r for r in unique_refs if int(r["community_id"]) not in allowed_set]
    if unauthorized:
        bad = ", ".join([f"{r['community_id']}:{r['receipt_bill_id']}" for r in unauthorized[:10]])
        raise HTTPException(status_code=403, detail=f"Unauthorized receipt communities: {bad}")

    previews: List[Dict[str, Any]] = []
    skipped_bills: List[Dict[str, Any]] = []

    # 鎵归噺棰勫姞杞藉叡浜暟鎹細妯℃澘銆佺敤鎴蜂笂涓嬫枃銆佸叏灞€鍙橀噺锛堜竴娆℃浛浠?N 娆￠噸澶嶆煡璇級
    from utils.variable_parser import build_variable_map
    batch_user_context = _main_attr("_build_preview_user_context")(
        current_user,
        x_account_book_id=x_account_book_id,
        x_account_book_name=x_account_book_name,
        x_account_book_number=x_account_book_number,
    )
    batch_runtime_vars = build_variable_map(db, user_context=batch_user_context)
    batch_templates = db.query(models.VoucherTemplate).filter(
        models.VoucherTemplate.active == True,
        models.VoucherTemplate.source_type == "receipt_bills",
    ).order_by(
        models.VoucherTemplate.priority.asc(),
        models.VoucherTemplate.template_id.asc(),
    ).all()
    batch_trigger_condition_cache: Dict[str, Any] = {}

    for ref in unique_refs:
        try:
            result = _main_attr("preview_voucher_for_receipt")(
                receipt_bill_id=int(ref["receipt_bill_id"]),
                community_id=int(ref["community_id"]),
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                allow_bill_fallback=False,
                current_user=current_user,
                db=db,
                allowed_community_ids=allowed_community_ids,
                _templates_cache=batch_templates,
                _runtime_vars_cache=batch_runtime_vars,
                _user_context_cache=batch_user_context,
                _trigger_condition_cache=batch_trigger_condition_cache,
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

    source_bill_push_summary = _main_attr("_summarize_bill_push_statuses")(source_bills)
    push_conflicts = _main_attr("_find_bill_push_conflicts")(source_bills)
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
            "total_debit": _money_text(total_debit),
            "total_credit": _money_text(total_credit),
            "total_debit_exact": _money_text(total_debit),
            "total_credit_exact": _money_text(total_credit),
            "is_balanced": (total_debit - total_credit) == Decimal("0") and (total_credit - total_debit) == Decimal("0"),
        },
        "kingdee_json": merged_kingdee_json,
    }


def preview_voucher_for_bills(
    payload: schemas.BatchVoucherPreviewRequest,
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
    current_user: models.User,
    db: Session,
    allowed_community_ids: List[int],
    allow_receipt_fallback: bool = True,
):
    if not payload.bills:
        raise HTTPException(status_code=400, detail="No bills selected")

    unique_refs = _main_attr("_normalize_bill_refs")(payload.bills)

    if not allowed_community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    allowed_set = set(allowed_community_ids)
    unauthorized = [r for r in unique_refs if int(r["community_id"]) not in allowed_set]
    if unauthorized:
        bad = ", ".join([f"{r['community_id']}:{r['bill_id']}" for r in unauthorized[:10]])
        raise HTTPException(status_code=403, detail=f"Unauthorized bill communities: {bad}")

    normalized_account_book_number = _main_attr("_decode_header_value")(x_account_book_number) or None
    selected_status_map = _main_attr("_get_bill_push_status_map")(
        db,
        unique_refs,
        account_book_number=normalized_account_book_number,
    )
    selected_bills = [
        selected_status_map[(ref["bill_id"], ref["community_id"])]
        for ref in unique_refs
    ]

    from services.voucher_engine import batch_preload_kd_cache, enrich_bill_data_cached

    bill_filters = [
        and_(
            models.Bill.id == int(ref["bill_id"]),
            models.Bill.community_id == int(ref["community_id"]),
        )
        for ref in unique_refs
    ]
    bill_rows = db.query(models.Bill).filter(or_(*bill_filters)).all() if bill_filters else []

    bill_row_map: Dict[Tuple[int, int], models.Bill] = {}
    bill_data_map: Dict[Tuple[int, int], Dict[str, Any]] = {}
    all_bill_data_list: List[Dict[str, Any]] = []
    for bill in bill_rows:
        key = (int(bill.id), int(bill.community_id))
        bill_row_map[key] = bill

        bill_data: Dict[str, Any] = {}
        for col in models.Bill.__table__.columns:
            bill_data[col.name] = _serialize_source_scalar(col.name, getattr(bill, col.name, None))

        bill_data_map[key] = bill_data
        all_bill_data_list.append(bill_data)

    kd_cache = batch_preload_kd_cache(all_bill_data_list, db) if all_bill_data_list else None

    previews: List[Dict[str, Any]] = []
    skipped_bills: List[Dict[str, Any]] = []

    for ref in unique_refs:
        try:
            key = (int(ref["bill_id"]), int(ref["community_id"]))
            bill_row = bill_row_map.get(key)
            if bill_row is None:
                skipped_bills.append({
                    "bill_id": int(ref["bill_id"]),
                    "community_id": int(ref["community_id"]),
                    "reason": "bill not found",
                })
                continue
            bill_data = bill_data_map.get(key) or {}
            enriched_bill = (
                enrich_bill_data_cached(bill_data, kd_cache)
                if (bill_data and kd_cache is not None)
                else None
            )
            bill_status = selected_status_map.get(key)
            result = _main_attr("preview_voucher_for_bill")(
                bill_id=int(ref["bill_id"]),
                community_id=int(ref["community_id"]),
                x_account_book_id=x_account_book_id,
                x_account_book_name=x_account_book_name,
                x_account_book_number=x_account_book_number,
                allow_receipt_fallback=allow_receipt_fallback,
                current_user=current_user,
                db=db,
                allowed_community_ids=allowed_community_ids,
                _bill_override=bill_row,
                _enriched_bill_override=enriched_bill,
                _source_bills_override=[bill_status] if bill_status else None,
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

    selected_bill_push_summary = _main_attr("_summarize_bill_push_statuses")(selected_bills)
    source_bill_push_summary = _main_attr("_summarize_bill_push_statuses")(source_bills)
    push_conflicts = _main_attr("_find_bill_push_conflicts")(source_bills)
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
            "total_debit": _money_text(total_debit),
            "total_credit": _money_text(total_credit),
            "total_debit_exact": _money_text(total_debit),
            "total_credit_exact": _money_text(total_credit),
            "is_balanced": (total_debit - total_credit) == Decimal("0") and (total_credit - total_debit) == Decimal("0"),
        },
        "kingdee_json": merged_kingdee_json,
    }
