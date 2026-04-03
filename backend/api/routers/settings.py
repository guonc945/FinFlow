# -*- coding: utf-8 -*-
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import models
import schemas
from api.dependencies import get_current_user, get_db, _require_api_permission
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

    return errors


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
