import random
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from sqlalchemy.orm import Session

from models import GlobalVariable


_PLACEHOLDER_RE = re.compile(r"\{(.*?)\}")
_USER_CONTEXT_VARIABLE_KEYS = {
    "CURRENT_ACCOUNT_BOOK_NUMBER",
    "CURRENT_ACCOUNT_BOOK_NAME",
    "CURRENT_USER_REALNAME",
    "CURRENT_USERNAME",
    "CURRENT_USER_ID",
    "CURRENT_ORG_ID",
    "CURRENT_ORG_NAME",
}


def _build_builtin_variable_map(user_context: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    now = datetime.now()
    var_map = {
        "CURRENT_DATE": now.strftime("%Y-%m-%d"),
        "CURRENT_DATETIME": now.strftime("%Y-%m-%d %H:%M:%S"),
        "CURRENT_TIME": now.strftime("%H:%M:%S"),
        "YESTERDAY": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
        "TOMORROW": (now + timedelta(days=1)).strftime("%Y-%m-%d"),
        "TIMESTAMP": str(int(now.timestamp())),
        "YEAR": now.strftime("%Y"),
        "MONTH": now.strftime("%m"),
        "DAY": now.strftime("%d"),
        "YEAR_MONTH": now.strftime("%Y-%m"),
        "DATE": now.strftime("%Y-%m-%d"),
        "WEEKDAY": str(now.weekday()),
        "SYSTEM_VERSION": "1.0.4-stable",
        "APP_ENV": "development",
        "BASE_PATH": "/api/v1",
        "UUID": str(uuid.uuid4()),
        "RANDOM_6": "".join(random.choices("0123456789", k=6)),
        "NONCE": "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=16)),
        "CURRENCY": "CNY",
        "DEFAULT_TAX": "0.13",
    }

    if user_context:
        ctx_mapping = {
            "current_account_book_number": "CURRENT_ACCOUNT_BOOK_NUMBER",
            "current_account_book_name": "CURRENT_ACCOUNT_BOOK_NAME",
            "current_user_realname": "CURRENT_USER_REALNAME",
            "current_username": "CURRENT_USERNAME",
            "current_user_id": "CURRENT_USER_ID",
            "current_org_id": "CURRENT_ORG_ID",
            "current_org_name": "CURRENT_ORG_NAME",
        }
        for ctx_key, var_key in ctx_mapping.items():
            val = user_context.get(ctx_key)
            if val is not None:
                var_map[var_key] = str(val)

    return var_map


def get_builtin_variable_keys(user_context: Optional[Dict[str, str]] = None) -> Set[str]:
    keys = set(_build_builtin_variable_map(user_context).keys())
    keys.update(_USER_CONTEXT_VARIABLE_KEYS)
    return keys


def extract_placeholder_keys(text: Any) -> List[str]:
    if text is None:
        return []
    content = str(text)
    keys = []
    for match in _PLACEHOLDER_RE.finditer(content):
        key = match.group(1).strip()
        if key:
            keys.append(key)
    return keys


def _query_global_variables(
    db: Session,
    required_keys: Optional[Set[str]] = None,
) -> Dict[str, str]:
    if required_keys is not None and not required_keys:
        return {}

    query = db.query(GlobalVariable)
    if required_keys:
        query = query.filter(GlobalVariable.key.in_(required_keys))
    return {item.key: item.value for item in query.all()}


def build_variable_map(
    db: Session,
    required_keys: Optional[List[str]] = None,
    user_context: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """
    Build runtime variable map from DB variables, built-ins, and user context.
    """
    builtin_map = _build_builtin_variable_map(user_context)
    normalized_required = {
        str(key).strip()
        for key in (required_keys or [])
        if str(key).strip()
    }

    if normalized_required:
        variable_map = _query_global_variables(
            db,
            required_keys={key for key in normalized_required if key not in builtin_map},
        )
    else:
        variable_map = _query_global_variables(db, required_keys=None)

    result = dict(variable_map)
    result.update(builtin_map)
    return result


def resolve_variables(
    text: str,
    db: Session,
    preloaded_vars: Optional[Dict[str, str]] = None,
    user_context: Optional[Dict[str, str]] = None,
) -> str:
    """
    Find all occurrences of {variable_key} and replace them with their actual values.
    """
    if not text or not isinstance(text, str):
        return text

    matches = re.findall(r"\{(.*?)\}", text)
    if not matches:
        return text

    unique_keys = list(set(matches))
    var_map = (
        preloaded_vars
        if preloaded_vars is not None
        else build_variable_map(db, unique_keys, user_context=user_context)
    )

    resolved_text = text
    for key in unique_keys:
        if key in var_map:
            resolved_text = resolved_text.replace(f"{{{key}}}", var_map[key])

    return resolved_text


def resolve_dict_variables(
    data: Dict[str, Any],
    db: Session,
    preloaded_vars: Optional[Dict[str, str]] = None,
    user_context: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Recursively resolve variables in a dictionary.
    """
    if not data:
        return data

    resolved_data = {}
    for k, v in data.items():
        if isinstance(v, str):
            resolved_data[k] = resolve_variables(v, db, preloaded_vars, user_context=user_context)
        elif isinstance(v, dict):
            resolved_data[k] = resolve_dict_variables(v, db, preloaded_vars, user_context=user_context)
        elif isinstance(v, list):
            resolved_data[k] = [
                resolve_variables(item, db, preloaded_vars, user_context=user_context) if isinstance(item, str) else item
                for item in v
            ]
        else:
            resolved_data[k] = v

    return resolved_data
