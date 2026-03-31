import json
from importlib import import_module
from typing import Any, Dict, List, Optional, Set

from sqlalchemy.orm import Session

import models


def _main_attr(name: str):
    return getattr(import_module("main"), name)


def _build_allowed_placeholders(source_type: Optional[str], source_module: Optional[str], db: Session) -> Set[str]:
    from utils.variable_parser import build_variable_map

    allowed = set()
    try:
        allowed.update(build_variable_map(db).keys())
    except Exception:
        allowed.update(v.key for v in db.query(models.GlobalVariable).all())

    # 鐢ㄦ埛涓婁笅鏂囧彉閲忥紙杩愯鏃剁敱褰撳墠鐧诲綍鐢ㄦ埛鍔ㄦ€佹敞鍏ワ紝鏍￠獙闃舵闇€棰勫厛鏀捐锛?
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

    source_meta = _main_attr("_get_source_meta")(normalized_source) if normalized_source else None
    module_prefix = normalized_module or (source_meta.module_id if source_meta else "marki")

    source_types: Set[str] = set()
    if module_prefix:
        source_types.update(_main_attr("_get_module_source_types")(module_prefix))

    if normalized_source:
        source_types.add(normalized_source)
    else:
        source_types.add("bills")

    for current_source in sorted(source_types):
        source_fields = _main_attr("_build_source_fields")(current_source)
        if not source_fields:
            continue

        allowed.update(source_fields)
        allowed.update({f"{current_source}.{name}" for name in source_fields})
        if module_prefix:
            allowed.update({f"{module_prefix}.{current_source}.{name}" for name in source_fields})

        registered_meta = _main_attr("_get_source_meta")(current_source)
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
    unknown = sorted(_main_attr("_extract_placeholders")(expr) - allowed_placeholders)
    if unknown:
        errors.append(f"{field_path} contains unknown placeholders: {_main_attr('_format_placeholders')(unknown)}")


def _validate_unknown_functions(expr: Any, field_path: str, errors: List[str]) -> None:
    allowed_functions = set(_main_attr("get_public_expression_function_names")())
    unknown = sorted({
        name for name in _main_attr("extract_expression_function_names")(expr)
        if name not in allowed_functions
    })
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

    base_fields = _main_attr("_build_source_fields")(normalized_source)
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
