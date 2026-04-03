# -*- coding: utf-8 -*-
import json
from typing import Any, Dict, List, Optional, Set

from fastapi import Depends, Header, HTTPException, Request
from sqlalchemy import String, cast
from sqlalchemy.orm import Session

import database
import models
import utils.auth as auth_utils

def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_current_user(request: Request, db: Session = Depends(get_db)):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = auth_header.split(" ")[1]
    import utils.auth as auth_utils
    payload = auth_utils.verify_access_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
        
    user_id = payload.get("sub")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.status != 1:
        raise HTTPException(status_code=403, detail="User account is disabled")
        
    return user


def require_admin(current_user: models.User = Depends(get_current_user)):
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


MENU_PERMISSION_ROLE_DEFINITIONS = [
    {
        "role": "admin",
        "label": "管理员",
        "description": "拥有全部菜单和接口访问权限，当前角色固定显示全部能力。",
        "editable": False,
    },
    {
        "role": "user",
        "label": "普通用户",
        "description": "可按角色配置菜单可见范围，并进一步控制后台管理接口访问权限。",
        "editable": True,
    },
]

MENU_PERMISSION_DEFINITIONS = [
    {"key": "/", "label": "首页仪表盘", "section": "工作台", "group": "首页", "required": True, "admin_only": False, "default_enabled": True},
    {"key": "/receipt-bills", "label": "收款单据", "section": "马克业务", "group": "业务单据", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/deposit-records", "label": "押金管理", "section": "马克业务", "group": "业务单据", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/prepayment-records", "label": "预存款管理", "section": "马克业务", "group": "业务单据", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/bills", "label": "运营账单", "section": "马克业务", "group": "业务单据", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/projects", "label": "园区管理", "section": "马克业务", "group": "基础资料", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/charge-items", "label": "收费项目", "section": "马克业务", "group": "基础资料", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/houses", "label": "房屋管理", "section": "马克业务", "group": "基础资料", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/residents", "label": "住户管理", "section": "马克业务", "group": "基础资料", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/parks", "label": "车位管理", "section": "马克业务", "group": "基础资料", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/account-books", "label": "账簿管理", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/accounting-subjects", "label": "会计科目", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/auxiliary-data-categories", "label": "辅助资料分类", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/auxiliary-data", "label": "辅助资料", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/customers", "label": "客户管理", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/suppliers", "label": "供应商管理", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/tax-rates", "label": "税率档案", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/kd-houses", "label": "金蝶房号", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/bank-accounts", "label": "银行账户", "section": "金蝶财务", "group": "财务档案", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/vouchers/templates", "label": "凭证模板", "section": "集成中心", "group": "财务凭证", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/vouchers/categories", "label": "模板分类", "section": "集成中心", "group": "财务凭证", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/oa-center", "label": "泛微协同", "section": "泛微协同", "group": "协同入口", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/integrations/reporting", "label": "报表设计", "section": "集成中心", "group": "集成能力", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/integrations/sync-schedules", "label": "同步计划", "section": "集成中心", "group": "集成能力", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/integrations/credentials", "label": "接口认证", "section": "集成中心", "group": "接口接入", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/integrations/apis", "label": "接口管理", "section": "集成中心", "group": "接口接入", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/report-center", "label": "报表中心", "section": "报表中心", "group": "数据展示", "required": False, "admin_only": False, "default_enabled": True},
    {"key": "/organizations", "label": "组织管理", "section": "系统管理", "group": "权限与系统", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/users", "label": "用户管理", "section": "系统管理", "group": "权限与系统", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/menu-permissions", "label": "菜单权限", "section": "系统管理", "group": "权限与系统", "required": False, "admin_only": True, "default_enabled": False},
    {"key": "/settings", "label": "系统设置", "section": "系统管理", "group": "权限与系统", "required": False, "admin_only": False, "default_enabled": False},
    {"key": "/account", "label": "个人设置", "section": "系统管理", "group": "权限与系统", "required": True, "admin_only": False, "default_enabled": True},
]

API_PERMISSION_DEFINITIONS = [
    {"key": "project.manage", "label": "园区管理接口", "section": "马克业务", "group": "基础资料", "description": "允许访问园区管理页涉及的更新与同步接口。", "admin_only": False, "default_enabled": False},
    {"key": "charge_item.manage", "label": "收费项目接口", "section": "马克业务", "group": "基础资料", "description": "允许维护收费项目映射与同步任务。", "admin_only": False, "default_enabled": False},
    {"key": "credential.manage", "label": "接口认证接口", "section": "集成中心", "group": "接口接入", "description": "允许维护外部系统认证、令牌和连接测试。", "admin_only": False, "default_enabled": False},
    {"key": "api_registry.manage", "label": "接口管理接口", "section": "集成中心", "group": "接口接入", "description": "允许维护外部服务接口定义和调试配置。", "admin_only": False, "default_enabled": False},
    {"key": "sync_schedule.manage", "label": "同步计划接口", "section": "集成中心", "group": "集成能力", "description": "允许维护多进程同步计划、手动执行和查看执行记录。", "admin_only": False, "default_enabled": False},
    {"key": "reporting.manage", "label": "报表设计接口", "section": "集成中心", "group": "集成能力", "description": "允许维护报表连接、数据集和报表定义。", "admin_only": False, "default_enabled": False},
    {"key": "voucher_template.manage", "label": "财务凭证模板接口", "section": "集成中心", "group": "财务凭证", "description": "允许维护财务凭证模板和模板分类。", "admin_only": False, "default_enabled": False},
    {"key": "organization.manage", "label": "组织管理接口", "section": "系统管理", "group": "权限与系统", "description": "允许维护组织架构信息。", "admin_only": False, "default_enabled": False},
    {"key": "user.manage", "label": "用户管理接口", "section": "系统管理", "group": "权限与系统", "description": "允许查看、创建、更新和删除用户。", "admin_only": False, "default_enabled": False},
    {"key": "setting.manage", "label": "系统设置接口", "section": "系统管理", "group": "权限与系统", "description": "允许维护全局变量与系统设置。", "admin_only": False, "default_enabled": False},
    {"key": "menu_permission.manage", "label": "菜单权限接口", "section": "系统管理", "group": "权限与系统", "description": "允许维护角色的菜单与接口权限。", "admin_only": True, "default_enabled": False},
]

MENU_PERMISSION_ROLE_MAP = {item["role"]: item for item in MENU_PERMISSION_ROLE_DEFINITIONS}
MENU_PERMISSION_DEFINITION_MAP = {item["key"]: item for item in MENU_PERMISSION_DEFINITIONS}
API_PERMISSION_DEFINITION_MAP = {item["key"]: item for item in API_PERMISSION_DEFINITIONS}


def _ordered_permission_keys(definitions: List[Dict[str, Any]], keys: Set[str], key_field: str) -> List[str]:
    ordered: List[str] = []
    for item in definitions:
        key = item[key_field]
        if key in keys:
            ordered.append(key)
    return ordered


def _get_allowed_permission_keys(definitions: List[Dict[str, Any]], role: str, key_field: str) -> Set[str]:
    normalized_role = str(role or "user").strip() or "user"
    return {
        item[key_field]
        for item in definitions
        if normalized_role == "admin" or not item.get("admin_only")
    }


def _get_required_menu_keys(role: str) -> Set[str]:
    normalized_role = str(role or "user").strip() or "user"
    return {
        item["key"]
        for item in MENU_PERMISSION_DEFINITIONS
        if item["required"] and (normalized_role == "admin" or not item["admin_only"])
    }


def _get_default_menu_keys(role: str) -> List[str]:
    normalized_role = str(role or "user").strip() or "user"
    if normalized_role == "admin":
        return [item["key"] for item in MENU_PERMISSION_DEFINITIONS]

    default_keys = {
        item["key"]
        for item in MENU_PERMISSION_DEFINITIONS
        if item.get("default_enabled") and not item.get("admin_only")
    }
    return _ordered_permission_keys(MENU_PERMISSION_DEFINITIONS, default_keys, "key")


def _get_default_api_keys(role: str) -> List[str]:
    normalized_role = str(role or "user").strip() or "user"
    if normalized_role == "admin":
        return [item["key"] for item in API_PERMISSION_DEFINITIONS]

    default_keys = {
        item["key"]
        for item in API_PERMISSION_DEFINITIONS
        if item.get("default_enabled") and not item.get("admin_only")
    }
    return _ordered_permission_keys(API_PERMISSION_DEFINITIONS, default_keys, "key")


def _get_role_menu_keys(db: Session, role: str) -> List[str]:
    normalized_role = str(role or "user").strip() or "user"
    allowed_definition_keys = _get_allowed_permission_keys(MENU_PERMISSION_DEFINITIONS, normalized_role, "key")
    required_keys = _get_required_menu_keys(normalized_role)

    if normalized_role == "admin":
        return _ordered_permission_keys(MENU_PERMISSION_DEFINITIONS, allowed_definition_keys, "key")

    rows = (
        db.query(models.RoleMenuPermission.menu_key)
        .filter(models.RoleMenuPermission.role == normalized_role)
        .all()
    )
    if rows:
        persisted_keys = {row[0] for row in rows if row[0] in allowed_definition_keys}
        return _ordered_permission_keys(MENU_PERMISSION_DEFINITIONS, persisted_keys | required_keys, "key")

    return _ordered_permission_keys(
        MENU_PERMISSION_DEFINITIONS,
        set(_get_default_menu_keys(normalized_role)) | required_keys,
        "key",
    )


def _get_role_api_keys(db: Session, role: str) -> List[str]:
    normalized_role = str(role or "user").strip() or "user"
    allowed_definition_keys = _get_allowed_permission_keys(API_PERMISSION_DEFINITIONS, normalized_role, "key")

    if normalized_role == "admin":
        return _ordered_permission_keys(API_PERMISSION_DEFINITIONS, allowed_definition_keys, "key")

    rows = (
        db.query(models.RoleApiPermission.api_key)
        .filter(models.RoleApiPermission.role == normalized_role)
        .all()
    )
    if rows:
        persisted_keys = {row[0] for row in rows if row[0] in allowed_definition_keys}
        return _ordered_permission_keys(API_PERMISSION_DEFINITIONS, persisted_keys, "key")

    return _ordered_permission_keys(
        API_PERMISSION_DEFINITIONS,
        set(_get_default_api_keys(normalized_role)),
        "key",
    )


def _has_api_permission(db: Session, user: models.User, permission_key: str) -> bool:
    if not user:
        return False
    if user.role == "admin":
        return True
    return permission_key in set(_get_role_api_keys(db, user.role))


def _require_api_permission(db: Session, user: models.User, permission_key: str) -> None:
    if not _has_api_permission(db, user, permission_key):
        raise HTTPException(status_code=403, detail="Permission denied")


def _require_any_api_permission(db: Session, user: models.User, permission_keys: List[str]) -> None:
    if any(_has_api_permission(db, user, permission_key) for permission_key in permission_keys):
        return
    raise HTTPException(status_code=403, detail="Permission denied")


def _build_menu_permission_role_state(db: Session, role: str) -> Dict[str, Any]:
    role_meta = MENU_PERMISSION_ROLE_MAP.get(role, {
        "role": role,
        "label": role,
        "description": "",
        "editable": role != "admin",
    })
    normalized_role = role_meta["role"]
    return {
        "role": normalized_role,
        "label": role_meta["label"],
        "description": role_meta.get("description"),
        "editable": bool(role_meta.get("editable", True)),
        "menu_keys": _get_role_menu_keys(db, normalized_role),
        "api_keys": _get_role_api_keys(db, normalized_role),
    }


def _normalize_column_preference_items(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    normalized: List[str] = []
    seen: Set[str] = set()
    for value in values:
        if value is None:
            continue
        key = str(value).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return normalized


def _deserialize_column_preference(raw_value: Optional[str]) -> List[str]:
    if not raw_value:
        return []

    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError):
        return []

    return _normalize_column_preference_items(parsed)


def _serialize_column_preference(values: Any) -> str:
    return json.dumps(_normalize_column_preference_items(values), ensure_ascii=False)


def get_user_context(
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    current_user: models.User = Depends(get_current_user)
) -> Dict[str, str]:
    """Helper to extract user context from request for variable resolution."""
    from urllib.parse import unquote
    org_name = current_user.organization.name if current_user.organization else "未分配"
    return {
        "current_user_id": str(current_user.id),
        "current_username": current_user.username,
        "current_user_realname": current_user.real_name or current_user.username,
        "current_org_id": str(current_user.org_id) if current_user.org_id else "",
        "current_org_name": org_name,
        "current_account_book_number": unquote(x_account_book_number) if x_account_book_number else "",
        "current_account_book_name": unquote(x_account_book_name) if x_account_book_name else "",
    }


def get_allowed_community_ids(
    request: Request,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
) -> List[int]:
    """Get the community IDs the current user can access."""
    # 澧炲姞绠＄悊鍛樿鑹插垽鏂細绠＄悊鍛樺彲浠ユ搷浣滄墍鏈夊洯鍖?
    if current_user and current_user.role == 'admin':
        all_ids = db.query(models.ProjectList.proj_id).all()
        return [r[0] for r in all_ids]

    account_book_number = request.headers.get('X-Account-Book-Number')
    if not account_book_number:
        # 濡傛灉娌℃湁璐︾翱鍙凤紝杩斿洖绌哄垪琛ㄥ疄鐜板畬鍏ㄩ殧绂伙紝闄ら潪鏄壒娈婄鐞嗗憳鏉冮檺锛堟殏涓嶅鐞嗭級
        return []
        
    from sqlalchemy import cast, String
    from urllib.parse import unquote
    book_num = unquote(account_book_number)
    
    # 鏌ユ壘鍏宠仈鍒版璐︾翱鐨勬墍鏈夊洯鍖篒D
    allowed_ids = db.query(models.ProjectList.proj_id).join(
        models.KingdeeAccountBook, 
        cast(models.ProjectList.kingdee_account_book_id, String) == cast(models.KingdeeAccountBook.id, String)
    ).filter(
        models.KingdeeAccountBook.number == book_num
    ).all()
    
    return [r[0] for r in allowed_ids]


