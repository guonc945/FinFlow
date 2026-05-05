# -*- coding: utf-8 -*-
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import models
import schemas
from api.dependencies import get_current_user, get_db, _require_api_permission
from services.reporting_database import ReportingDatabaseService, ReportingDatabaseError, UnsafeQueryError
from utils.expression_functions import (
    get_public_expression_function_names,
    get_public_expression_functions,
)
from utils.variable_parser import build_variable_map, get_builtin_variable_keys

router = APIRouter()


def _build_settings_user_context(
    current_user: models.User,
    account_book_id: Optional[str] = None,
    account_book_name: Optional[str] = None,
) -> Dict[str, str]:
    org_name = current_user.organization.name if current_user.organization else "未分配"
    return {
        "current_user_id": str(current_user.id),
        "current_username": current_user.username,
        "current_user_realname": current_user.real_name or current_user.username,
        "current_org_id": str(current_user.org_id) if current_user.org_id else "",
        "current_org_name": org_name,
        "current_account_book_id": account_book_id or "",
        "current_account_book_name": account_book_name or "",
    }


def _validate_global_resource_key_conflicts(
    db: Session,
    key: str,
    *,
    exclude_variable_id: Optional[int] = None,
    exclude_dictionary_id: Optional[int] = None,
) -> List[str]:
    normalized_key = str(key or "").strip()
    if not normalized_key:
        return []

    errors: List[str] = []
    builtin_keys = get_builtin_variable_keys()
    function_names = set(get_public_expression_function_names())

    if normalized_key in builtin_keys:
        errors.append(f"Key conflicts with built-in variable: {normalized_key}")
    if normalized_key in function_names:
        errors.append(f"Key conflicts with built-in function: {normalized_key}")

    existing_variable = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == normalized_key).first()
    if existing_variable and (exclude_variable_id is None or int(existing_variable.id) != int(exclude_variable_id)):
        errors.append(f"Key already exists in global variables: {normalized_key}")

    existing_dictionary = db.query(models.DataDictionary).filter(models.DataDictionary.key == normalized_key).first()
    if existing_dictionary and (exclude_dictionary_id is None or int(existing_dictionary.id) != int(exclude_dictionary_id)):
        errors.append(f"Key already exists in data dictionaries: {normalized_key}")

    return errors


def _parse_dictionary_config(raw: str) -> Dict[str, Any]:
    try:
        config = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Dictionary config_json is invalid JSON: {exc}") from exc
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="Dictionary config_json must be a JSON object")
    return config


def _quote_identifier(db_type: str, identifier: str) -> str:
    value = str(identifier or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="Identifier cannot be empty")
    if db_type in {"mysql", "mariadb"}:
        return f"`{value.replace('`', '``')}`"
    if db_type in {"sqlserver", "mssql"}:
        return f"[{value.replace(']', ']]')}]"
    return f"\"{value.replace('\"', '\"\"')}\""


def _build_table_preview_sql(connection: models.ReportingDbConnection, schema_name: Optional[str], table_name: str, limit: int) -> str:
    db_type = str(connection.db_type or "").strip().lower()
    quoted_table = _quote_identifier(db_type, table_name)
    table_ref = quoted_table
    if schema_name:
        table_ref = f"{_quote_identifier(db_type, schema_name)}.{quoted_table}"
    if db_type in {"sqlserver", "mssql"}:
        return f"SELECT TOP {int(limit)} * FROM {table_ref}"
    return f"SELECT * FROM {table_ref} LIMIT {int(limit)}"


def _map_dictionary_items(
    rows: List[Dict[str, Any]],
    key_field: str,
    label_field: str,
    value_field: Optional[str] = None,
    parent_id_field: Optional[str] = None,
) -> List[schemas.DataDictionaryItem]:
    if not key_field or not label_field:
        return []
    items: List[schemas.DataDictionaryItem] = []
    for row in rows:
        if key_field not in row or label_field not in row:
            available = ", ".join(row.keys())
            raise HTTPException(
                status_code=400,
                detail=f"Dictionary field mapping failed. Available fields: {available}",
            )
        key_value = row.get(key_field)
        label_value = row.get(label_field)
        if key_value is None or label_value is None:
            continue
        items.append(
            schemas.DataDictionaryItem(
                key=str(key_value),
                label=str(label_value),
                value=None if not value_field or row.get(value_field) is None else str(row.get(value_field)),
                parent_id=None if not parent_id_field or row.get(parent_id_field) is None else str(row.get(parent_id_field)),
                raw=row,
            )
        )
    item_map = {item.key: item for item in items}

    def build_item_path(item_key: str) -> str:
        parts: List[str] = []
        cursor = item_map.get(item_key)
        visited = set()
        while cursor and cursor.key not in visited:
            visited.add(cursor.key)
            parts.append(cursor.label)
            cursor = item_map.get(cursor.parent_id) if cursor.parent_id else None
        return " / ".join(reversed(parts))

    for item in items:
        item.path = build_item_path(item.key)

    return items


def _resolve_dictionary_preview(
    *,
    db: Session,
    source_type: str,
    config: Dict[str, Any],
    limit: int,
    current_user: models.User,
) -> schemas.DataDictionaryPreviewResponse:
    preview_limit = max(1, min(int(limit or 50), 500))
    user_context = _build_settings_user_context(current_user)

    if source_type == "static":
        items_raw = config.get("items") or []
        if not isinstance(items_raw, list):
            raise HTTPException(status_code=400, detail="Static dictionary items must be a JSON array")
        items = [
            schemas.DataDictionaryItem(
                key=str(item.get("key", "")).strip(),
                label=str(item.get("label", "")).strip(),
                value=None if item.get("value") is None else str(item.get("value")),
                parent_id=None if item.get("parent_id") is None else str(item.get("parent_id")),
                raw=item if isinstance(item, dict) else None,
            )
            for item in items_raw
            if isinstance(item, dict) and str(item.get("key", "")).strip() and str(item.get("label", "")).strip()
        ][:preview_limit]
        return schemas.DataDictionaryPreviewResponse(items=items, total=len(items), columns=[], rows=[item.raw or {} for item in items])

    if source_type == "dataset":
        dataset_id = config.get("dataset_id")
        dataset = db.query(models.ReportingDataset).filter(models.ReportingDataset.id == dataset_id).first()
        if not dataset:
            raise HTTPException(status_code=404, detail="Dictionary dataset not found")
        connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == dataset.connection_id).first()
        if not connection:
            raise HTTPException(status_code=404, detail="Dictionary dataset connection not found")
        result = ReportingDatabaseService.execute_dataset(
            connection=connection,
            dataset=dataset,
            limit=preview_limit,
            db_session=db,
            user_context=user_context,
        )
        items = _map_dictionary_items(
            result["rows"],
            str(config.get("key_field") or ""),
            str(config.get("label_field") or ""),
            None if config.get("value_field") is None else str(config.get("value_field")),
            None if config.get("parent_id_field") is None else str(config.get("parent_id_field")),
        )
        return schemas.DataDictionaryPreviewResponse(items=items, total=len(items), columns=result["columns"], rows=result["rows"])

    if source_type == "table":
        connection_id = config.get("connection_id")
        table_name = str(config.get("table_name") or "").strip()
        schema_name = str(config.get("schema_name") or "").strip() or None
        connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
        if not connection:
            raise HTTPException(status_code=404, detail="Dictionary connection not found")
        if not table_name:
            raise HTTPException(status_code=400, detail="Dictionary table_name is required")
        sql_text = _build_table_preview_sql(connection, schema_name, table_name, preview_limit)
        result = ReportingDatabaseService.execute_query(
            connection=connection,
            sql_text=sql_text,
            limit=preview_limit,
            default_limit=preview_limit,
        )
        items = _map_dictionary_items(
            result["rows"],
            str(config.get("key_field") or ""),
            str(config.get("label_field") or ""),
            None if config.get("value_field") is None else str(config.get("value_field")),
            None if config.get("parent_id_field") is None else str(config.get("parent_id_field")),
        )
        return schemas.DataDictionaryPreviewResponse(items=items, total=len(items), columns=result["columns"], rows=result["rows"])

    if source_type == "sql":
        connection_id = config.get("connection_id")
        sql_text = str(config.get("sql_text") or "").strip()
        params_json = None if config.get("params_json") is None else json.dumps(config.get("params_json"), ensure_ascii=False)
        connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
        if not connection:
            raise HTTPException(status_code=404, detail="Dictionary connection not found")
        if not sql_text:
            raise HTTPException(status_code=400, detail="Dictionary sql_text is required")
        result = ReportingDatabaseService.execute_query(
            connection=connection,
            sql_text=sql_text,
            params_json=params_json,
            limit=preview_limit,
            default_limit=preview_limit,
            db_session=db,
            user_context=user_context,
        )
        items = _map_dictionary_items(
            result["rows"],
            str(config.get("key_field") or ""),
            str(config.get("label_field") or ""),
            None if config.get("value_field") is None else str(config.get("value_field")),
            None if config.get("parent_id_field") is None else str(config.get("parent_id_field")),
        )
        return schemas.DataDictionaryPreviewResponse(items=items, total=len(items), columns=result["columns"], rows=result["rows"])

    raise HTTPException(status_code=400, detail=f"Unsupported dictionary source_type: {source_type}")


@router.get("/api/settings/variables", response_model=List[schemas.GlobalVariableResponse])
def get_global_variables(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return db.query(models.GlobalVariable).all()


@router.get("/api/settings/variables/runtime")
def get_runtime_variables(
    account_book_id: Optional[str] = None,
    account_book_name: Optional[str] = None,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return the full runtime variable map, including user context variables."""
    user_context = _build_settings_user_context(
        current_user,
        account_book_id=account_book_id,
        account_book_name=account_book_name,
    )
    var_map = build_variable_map(db, user_context=user_context)
    return var_map


@router.get("/api/settings/functions", response_model=List[schemas.ExpressionFunctionResponse])
def get_global_expression_functions(current_user: models.User = Depends(get_current_user)):
    return get_public_expression_functions()


@router.post("/api/settings/variables", response_model=schemas.GlobalVariableResponse)
def create_global_variable(
    variable: schemas.GlobalVariableCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    key_errors = _validate_global_resource_key_conflicts(db, variable.key)
    if key_errors:
        raise HTTPException(status_code=400, detail={"message": "Variable key is invalid", "errors": key_errors})

    new_variable = models.GlobalVariable(**variable.dict())
    db.add(new_variable)
    db.commit()
    db.refresh(new_variable)
    return new_variable


@router.put("/api/settings/variables/{variable_id}", response_model=schemas.GlobalVariableResponse)
def update_global_variable(
    variable_id: int,
    variable: schemas.GlobalVariableUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    db_variable = db.query(models.GlobalVariable).filter(models.GlobalVariable.id == variable_id).first()
    if not db_variable:
        raise HTTPException(status_code=404, detail="Variable not found")

    update_data = variable.dict(exclude_unset=True)
    if "key" in update_data and update_data["key"] is not None:
        key_errors = _validate_global_resource_key_conflicts(
            db,
            update_data["key"],
            exclude_variable_id=variable_id,
        )
        if key_errors:
            raise HTTPException(status_code=400, detail={"message": "Variable key is invalid", "errors": key_errors})

    for key, value in update_data.items():
        setattr(db_variable, key, value)

    db.commit()
    db.refresh(db_variable)
    return db_variable


@router.delete("/api/settings/variables/{variable_id}")
def delete_global_variable(
    variable_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    db_variable = db.query(models.GlobalVariable).filter(models.GlobalVariable.id == variable_id).first()
    if not db_variable:
        raise HTTPException(status_code=404, detail="Variable not found")

    db.delete(db_variable)
    db.commit()
    return {"message": "Variable deleted"}


@router.get("/api/settings/dictionaries", response_model=List[schemas.DataDictionaryResponse])
def get_data_dictionaries(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return db.query(models.DataDictionary).order_by(models.DataDictionary.id.desc()).all()


@router.post("/api/settings/dictionaries", response_model=schemas.DataDictionaryResponse)
def create_data_dictionary(
    payload: schemas.DataDictionaryCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    key_errors = _validate_global_resource_key_conflicts(db, payload.key)
    if key_errors:
        raise HTTPException(status_code=400, detail={"message": "Dictionary key is invalid", "errors": key_errors})
    _parse_dictionary_config(payload.config_json)
    item = models.DataDictionary(**payload.dict())
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.put("/api/settings/dictionaries/{dictionary_id}", response_model=schemas.DataDictionaryResponse)
def update_data_dictionary(
    dictionary_id: int,
    payload: schemas.DataDictionaryUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    item = db.query(models.DataDictionary).filter(models.DataDictionary.id == dictionary_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Dictionary not found")

    update_data = payload.dict(exclude_unset=True)
    if "key" in update_data and update_data["key"] is not None:
        key_errors = _validate_global_resource_key_conflicts(
            db,
            update_data["key"],
            exclude_dictionary_id=dictionary_id,
        )
        if key_errors:
            raise HTTPException(status_code=400, detail={"message": "Dictionary key is invalid", "errors": key_errors})
    if "config_json" in update_data and update_data["config_json"] is not None:
        _parse_dictionary_config(update_data["config_json"])

    for key, value in update_data.items():
        setattr(item, key, value)

    db.commit()
    db.refresh(item)
    return item


@router.delete("/api/settings/dictionaries/{dictionary_id}")
def delete_data_dictionary(
    dictionary_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    item = db.query(models.DataDictionary).filter(models.DataDictionary.id == dictionary_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Dictionary not found")
    db.delete(item)
    db.commit()
    return {"message": "Dictionary deleted"}


@router.post("/api/settings/dictionaries/preview-draft", response_model=schemas.DataDictionaryPreviewResponse)
def preview_data_dictionary_draft(
    payload: schemas.DataDictionaryPreviewRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    config = _parse_dictionary_config(payload.config_json)
    try:
        return _resolve_dictionary_preview(
            db=db,
            source_type=payload.source_type,
            config=config,
            limit=payload.limit or 50,
            current_user=current_user,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/api/settings/dictionaries/{dictionary_id}/items", response_model=schemas.DataDictionaryPreviewResponse)
def get_data_dictionary_items(
    dictionary_id: int,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    item = db.query(models.DataDictionary).filter(models.DataDictionary.id == dictionary_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Dictionary not found")
    try:
        return _resolve_dictionary_preview(
            db=db,
            source_type=item.source_type,
            config=_parse_dictionary_config(item.config_json),
            limit=limit,
            current_user=current_user,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
