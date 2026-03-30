import hashlib
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import models
import schemas
import utils.auth as auth_utils
from api.dependencies import (
    API_PERMISSION_DEFINITIONS,
    MENU_PERMISSION_DEFINITIONS,
    MENU_PERMISSION_ROLE_DEFINITIONS,
    MENU_PERMISSION_ROLE_MAP,
    _build_menu_permission_role_state,
    _deserialize_column_preference,
    _get_allowed_permission_keys,
    _get_required_menu_keys,
    _get_role_api_keys,
    _get_role_menu_keys,
    _normalize_column_preference_items,
    _require_api_permission,
    get_current_user,
    get_db,
    require_admin,
)

router = APIRouter()

def hash_password(password: str) -> str:
    """Simple password hashing using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

from pydantic import BaseModel

class LoginRequest(BaseModel):
    username: str
    password: str

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)




@router.post("/api/auth/login")
def login(login_req: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.username == login_req.username).first()
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
        
    hashed_pwd = hash_password(login_req.password)
    if user.password_hash != hashed_pwd:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
        
    if user.status != 1:
        raise HTTPException(status_code=403, detail="Account is disabled")
        
    # Update last login
    user.last_login = datetime.now()
    db.commit()
    
    import utils.auth as auth_utils
    access_token = auth_utils.create_access_token({"sub": user.id})
    
    org_name = user.organization.name if user.organization else "未分配"
    menu_keys = _get_role_menu_keys(db, user.role)
    api_keys = _get_role_api_keys(db, user.role)
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "username": user.username,
            "real_name": user.real_name or user.username,
            "org_name": org_name,
            "avatar": user.avatar,
            "role": user.role,
            "menu_keys": menu_keys,
            "api_keys": api_keys,
        }
    }


@router.get("/api/users/me")
def get_me(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get current active user"""
    user = current_user
    org_name = user.organization.name if user.organization else "未分配"
    
    # get user authorized account books
    account_books = [{"id": b.id, "name": b.name, "number": b.number} for b in user.account_books]
    menu_keys = _get_role_menu_keys(db, user.role)
    api_keys = _get_role_api_keys(db, user.role)
    
    return {
        "id": user.id,
        "username": user.username,
        "real_name": user.real_name or user.username,
        "org_id": user.org_id,
        "org_name": org_name,
        "avatar": user.avatar,
        "role": user.role,
        "account_books": account_books,
        "menu_keys": menu_keys,
        "api_keys": api_keys,
    }


@router.get(
    "/api/users/me/table-column-preferences/{table_id}",
    response_model=schemas.UserTableColumnPreferenceResponse,
)
def get_my_table_column_preference(
    table_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    normalized_table_id = table_id.strip()
    if not normalized_table_id:
        raise HTTPException(status_code=400, detail="table_id is required")

    preference = (
        db.query(models.UserTableColumnPreference)
        .filter(
            models.UserTableColumnPreference.user_id == current_user.id,
            models.UserTableColumnPreference.table_id == normalized_table_id,
        )
        .first()
    )

    if not preference:
        return schemas.UserTableColumnPreferenceResponse(
            table_id=normalized_table_id,
            hidden=[],
            order=[],
            updated_at=None,
        )

    return schemas.UserTableColumnPreferenceResponse(
        table_id=normalized_table_id,
        hidden=_deserialize_column_preference(preference.hidden_columns),
        order=_deserialize_column_preference(preference.column_order),
        updated_at=preference.updated_at,
    )


@router.put(
    "/api/users/me/table-column-preferences/{table_id}",
    response_model=schemas.UserTableColumnPreferenceResponse,
)
def save_my_table_column_preference(
    table_id: str,
    payload: schemas.UserTableColumnPreferenceUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    normalized_table_id = table_id.strip()
    if not normalized_table_id:
        raise HTTPException(status_code=400, detail="table_id is required")

    hidden = _normalize_column_preference_items(payload.hidden)
    order = _normalize_column_preference_items(payload.order)

    preference = (
        db.query(models.UserTableColumnPreference)
        .filter(
            models.UserTableColumnPreference.user_id == current_user.id,
            models.UserTableColumnPreference.table_id == normalized_table_id,
        )
        .first()
    )

    if not preference:
        preference = models.UserTableColumnPreference(
            user_id=current_user.id,
            table_id=normalized_table_id,
        )
        db.add(preference)

    preference.hidden_columns = _serialize_column_preference(hidden)
    preference.column_order = _serialize_column_preference(order)
    db.commit()
    db.refresh(preference)

    return schemas.UserTableColumnPreferenceResponse(
        table_id=normalized_table_id,
        hidden=hidden,
        order=order,
        updated_at=preference.updated_at,
    )


@router.get("/api/users")
def get_users(
    skip: int = 0,
    limit: int = 100,
    org_id: int = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get all users with optional org filter"""
    _require_api_permission(db, current_user, "user.manage")
    query = db.query(models.User)
    if org_id:
        query = query.filter(models.User.org_id == org_id)
    users = query.order_by(models.User.created_at.desc()).offset(skip).limit(limit).all()
    
    result = []
    for user in users:
        org_name = None
        if user.organization:
            org_name = user.organization.name
        result.append({
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "phone": user.phone,
            "real_name": user.real_name,
            "org_id": user.org_id,
            "org_name": org_name,
            "status": user.status,
            "role": user.role,
            "avatar": user.avatar,
            "last_login": user.last_login,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
            "account_book_ids": [ab.id for ab in user.account_books] if user.account_books else []
        })
    return result


@router.get("/api/users/{user_id}")
def get_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "user.manage")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    org_name = None
    if user.organization:
        org_name = user.organization.name
    
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "phone": user.phone,
        "real_name": user.real_name,
        "org_id": user.org_id,
        "org_name": org_name,
        "status": user.status,
        "role": user.role,
        "avatar": user.avatar,
        "last_login": user.last_login,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
        "account_book_ids": [ab.id for ab in user.account_books] if user.account_books else []
    }


def _normalize_optional_user_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _normalize_user_role(value: Optional[str]) -> str:
    normalized_role = str(value or "user").strip() or "user"
    if normalized_role not in MENU_PERMISSION_ROLE_MAP:
        raise HTTPException(status_code=400, detail="Invalid user role")
    return normalized_role


def _normalize_user_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)

    if "username" in normalized and normalized["username"] is not None:
        normalized["username"] = str(normalized["username"]).strip()

    for field in ("email", "phone", "real_name"):
        if field in normalized:
            normalized[field] = _normalize_optional_user_text(normalized[field])

    if "role" in normalized and normalized["role"] is not None:
        normalized["role"] = _normalize_user_role(normalized["role"])

    if "org_id" in normalized and normalized["org_id"] in ("", "0", 0):
        normalized["org_id"] = None

    return normalized


def _build_user_integrity_error(exc: IntegrityError) -> HTTPException:
    error_text = str(getattr(exc, "orig", exc))
    lowered = error_text.lower()

    if "users_username_key" in lowered or "(username)" in lowered:
        return HTTPException(status_code=400, detail="Username already exists")

    if "users_email_key" in lowered or "(email)" in lowered:
        return HTTPException(status_code=400, detail="Email already exists")

    return HTTPException(status_code=400, detail="User save failed")


@router.post("/api/users")
def create_user(
    user_data: schemas.UserCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "user.manage")
    create_data = _normalize_user_payload(user_data.dict())

    if not create_data["username"]:
        raise HTTPException(status_code=400, detail="Username is required")

    # Check if username exists
    existing = db.query(models.User).filter(models.User.username == create_data["username"]).first()
    if existing:
        raise HTTPException(status_code=400, detail="Username already exists")

    # Check if email exists
    if create_data["email"]:
        existing_email = db.query(models.User).filter(models.User.email == create_data["email"]).first()
        if existing_email:
            raise HTTPException(status_code=400, detail="Email already exists")

    account_book_ids = create_data.pop("account_book_ids", None)
    user = models.User(
        username=create_data["username"],
        email=create_data["email"],
        phone=create_data["phone"],
        real_name=create_data["real_name"],
        password_hash=hash_password(user_data.password),
        org_id=create_data["org_id"],
        status=create_data["status"],
        role=create_data["role"],
    )
    db.add(user)

    if account_book_ids is not None:
        user.account_books = db.query(models.KingdeeAccountBook).filter(
            models.KingdeeAccountBook.id.in_(account_book_ids)
        ).all()

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise _build_user_integrity_error(exc) from exc

    db.refresh(user)
    return {"id": user.id, "message": "User created successfully"}


@router.put("/api/users/{user_id}")
def update_user(
    user_id: int,
    user_data: schemas.UserUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "user.manage")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    update_data = _normalize_user_payload(user_data.dict(exclude_unset=True))

    if "username" in update_data:
        if not update_data["username"]:
            raise HTTPException(status_code=400, detail="Username is required")
        existing = db.query(models.User).filter(
            models.User.username == update_data["username"],
            models.User.id != user_id
        ).first()
        if existing:
            raise HTTPException(status_code=400, detail="Username already exists")
    
    if "email" in update_data and update_data["email"]:
        existing = db.query(models.User).filter(
            models.User.email == update_data["email"],
            models.User.id != user_id
        ).first()
        if existing:
            raise HTTPException(status_code=400, detail="Email already exists")

    if "role" in update_data and update_data["role"] is not None:
        update_data["role"] = str(update_data["role"]).strip() or "user"

    if "password" in update_data and update_data["password"]:
         update_data["password_hash"] = hash_password(update_data.pop("password"))
    
    if "account_book_ids" in update_data:
        account_book_ids = update_data.pop("account_book_ids")
        if account_book_ids is not None:
            user.account_books = db.query(models.KingdeeAccountBook).filter(
                models.KingdeeAccountBook.id.in_(account_book_ids)
            ).all()
        else:
            user.account_books = []

    for key, value in update_data.items():
        setattr(user, key, value)

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise _build_user_integrity_error(exc) from exc

    return {"message": "User updated successfully"}


@router.delete("/api/users/{user_id}")
def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "user.manage")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.id == current_user.id:
        raise HTTPException(status_code=400, detail="Current user cannot be deleted")
    
    db.delete(user)
    db.commit()
    return {"message": "User deleted successfully"}


@router.get(
    "/api/menu-permissions",
    response_model=schemas.MenuPermissionOverviewResponse,
)
def get_menu_permissions(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_admin),
):
    roles = [_build_menu_permission_role_state(db, item["role"]) for item in MENU_PERMISSION_ROLE_DEFINITIONS]
    return {
        "menus": MENU_PERMISSION_DEFINITIONS,
        "apis": API_PERMISSION_DEFINITIONS,
        "roles": roles,
    }


@router.put(
    "/api/menu-permissions/{role}",
    response_model=schemas.MenuPermissionRoleState,
)
def update_menu_permissions(
    role: str,
    payload: schemas.MenuPermissionRoleUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_admin),
):
    normalized_role = str(role or "").strip()
    if not normalized_role:
        raise HTTPException(status_code=400, detail="Role is required")

    role_meta = MENU_PERMISSION_ROLE_MAP.get(normalized_role)
    if not role_meta:
        raise HTTPException(status_code=404, detail="Role not found")
    if not role_meta.get("editable", True):
        raise HTTPException(status_code=400, detail="This role uses fixed permissions")

    allowed_menu_keys = _get_allowed_permission_keys(MENU_PERMISSION_DEFINITIONS, normalized_role, "key")
    allowed_api_keys = _get_allowed_permission_keys(API_PERMISSION_DEFINITIONS, normalized_role, "key")
    required_menu_keys = _get_required_menu_keys(normalized_role)
    submitted_menu_keys = {
        key
        for key in payload.menu_keys
        if isinstance(key, str) and key in allowed_menu_keys
    }
    submitted_api_keys = {
        key
        for key in payload.api_keys
        if isinstance(key, str) and key in allowed_api_keys
    }
    final_menu_keys = submitted_menu_keys | required_menu_keys

    db.query(models.RoleMenuPermission).filter(
        models.RoleMenuPermission.role == normalized_role
    ).delete(synchronize_session=False)

    for menu_key in final_menu_keys:
        db.add(models.RoleMenuPermission(role=normalized_role, menu_key=menu_key))

    db.query(models.RoleApiPermission).filter(
        models.RoleApiPermission.role == normalized_role
    ).delete(synchronize_session=False)

    for api_key in submitted_api_keys:
        db.add(models.RoleApiPermission(role=normalized_role, api_key=api_key))

    db.commit()
    return _build_menu_permission_role_state(db, normalized_role)


# ===================== External Service Management =====================

