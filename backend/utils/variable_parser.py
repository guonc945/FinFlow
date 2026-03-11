import re
import uuid
import random
from sqlalchemy.orm import Session

from models import GlobalVariable
from typing import Dict, Any, Optional, List


def build_variable_map(
    db: Session,
    required_keys: Optional[List[str]] = None,
    user_context: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """
    Build runtime variable map from DB and built-ins.
    If required_keys is provided, DB fetch is limited to those keys.
    user_context 可包含: current_user_id, current_username, current_user_realname,
                         current_org_id, current_org_name,
                         current_account_book_id, current_account_book_name
    """
    db_query = db.query(GlobalVariable)
    if required_keys:
        db_query = db_query.filter(GlobalVariable.key.in_(required_keys))
    variables = db_query.all()
    var_map = {v.key: v.value for v in variables}

    from datetime import datetime, timedelta
    now = datetime.now()
    var_map.update({
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
    })

    # 注入用户上下文变量
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


def resolve_variables(
    text: str,
    db: Session,
    preloaded_vars: Optional[Dict[str, str]] = None,
    user_context: Optional[Dict[str, str]] = None
) -> str:
    """
    Find all occurrences of {variable_key} and replace them with their actual values from the database.
    """
    if not text or not isinstance(text, str):
        return text

    # Find all matches for {...}
    matches = re.findall(r'\{(.*?)\}', text)
    if not matches:
        return text

    unique_keys = list(set(matches))
    var_map = preloaded_vars if preloaded_vars is not None else build_variable_map(db, unique_keys, user_context=user_context)

    # Perform replacement
    resolved_text = text
    for key in unique_keys:
        if key in var_map:
            resolved_text = resolved_text.replace(f'{{{key}}}', var_map[key])


    return resolved_text


def resolve_dict_variables(
    data: Dict[str, Any],
    db: Session,
    preloaded_vars: Optional[Dict[str, str]] = None,
    user_context: Optional[Dict[str, str]] = None
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
