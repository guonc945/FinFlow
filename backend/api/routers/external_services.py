from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import models
import schemas
from api.dependencies import get_current_user, get_db
from services.kingdee_auth import KingdeeAuthService
from utils.crypto import encrypt_value

router = APIRouter()


def _main_attr(name: str):
    from importlib import import_module

    return getattr(import_module("main"), name)


def _has_api_permission(*args, **kwargs):
    return _main_attr("_has_api_permission")(*args, **kwargs)


def _require_api_permission(*args, **kwargs):
    return _main_attr("_require_api_permission")(*args, **kwargs)


@router.get("/api/external/services", response_model=List[schemas.ExternalServiceWithApis])
def get_external_services(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get all external services"""
    can_manage_credentials = _has_api_permission(db, current_user, "credential.manage")
    can_manage_apis = _has_api_permission(db, current_user, "api_registry.manage")
    if not (can_manage_credentials or can_manage_apis):
        raise HTTPException(status_code=403, detail="Permission denied")

    from utils.crypto import decrypt_value

    services = db.query(models.ExternalService).all()
    for s in services:
        if can_manage_credentials and s.app_secret:
            s.app_secret = decrypt_value(s.app_secret)
        elif not can_manage_credentials:
            s.app_secret = None
    return services


@router.post("/api/external/services", response_model=schemas.ExternalServiceResponse)
def create_external_service(
    service: schemas.ExternalServiceCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Create a new external service credential config"""
    _require_api_permission(db, current_user, "credential.manage")
    existing = db.query(models.ExternalService).filter(models.ExternalService.service_name == service.service_name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Service name already exists")

    service_data = service.dict()
    if service_data.get("app_secret"):
        service_data["app_secret"] = encrypt_value(service_data["app_secret"])

    new_service = models.ExternalService(**service_data)
    db.add(new_service)
    db.commit()
    db.refresh(new_service)

    from utils.crypto import decrypt_value

    if new_service.app_secret:
        new_service.app_secret = decrypt_value(new_service.app_secret)

    return new_service


@router.post("/api/external/services/test-connection")
def test_external_service_connection(
    service: schemas.ExternalServiceCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Test connection for a service configuration without saving it"""
    _require_api_permission(db, current_user, "credential.manage")
    import requests
    from services.external_auth import ExternalAuthService
    from utils.crypto import encrypt_value

    service_data = service.dict()
    if service_data.get("app_secret"):
        service_data["app_secret"] = encrypt_value(service_data["app_secret"])

    temp_service = models.ExternalService(**service_data)

    try:
        auth = ExternalAuthService(db=db, service_record=temp_service)
        token = auth.get_token()

        connectivity_status = "Skipped (No Base URL)"
        if temp_service.base_url:
            try:
                headers = auth.get_auth_headers()
                resp = requests.get(temp_service.base_url, headers=headers, timeout=10)
                connectivity_status = f"Success (HTTP {resp.status_code})"
            except Exception as e:
                connectivity_status = f"Failed: {str(e)}"

        return {
            "success": True,
            "message": "Authentication successful",
            "token_preview": token[:10] + "..." if token else "N/A",
            "connectivity": connectivity_status,
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Connection failed: {str(e)}",
        }


@router.put("/api/external/services/{service_id}", response_model=schemas.ExternalServiceResponse)
def update_external_service(
    service_id: int,
    service: schemas.ExternalServiceUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Update external service config"""
    _require_api_permission(db, current_user, "credential.manage")
    db_service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
    if not db_service:
        raise HTTPException(status_code=404, detail="Service not found")

    update_data = service.dict(exclude_unset=True)
    for key, value in update_data.items():
        if key == "app_secret" and value:
            if value != db_service.app_secret:
                value = encrypt_value(value)
        setattr(db_service, key, value)

    db.commit()
    db.refresh(db_service)

    from utils.crypto import decrypt_value

    if db_service.app_secret:
        db_service.app_secret = decrypt_value(db_service.app_secret)

    return db_service


@router.delete("/api/external/services/{service_id}")
def delete_external_service(
    service_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Delete external service"""
    _require_api_permission(db, current_user, "credential.manage")
    db_service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
    if not db_service:
        raise HTTPException(status_code=404, detail="Service not found")

    db.delete(db_service)
    db.commit()
    return {"message": "Service deleted"}


@router.post("/api/external/services/{service_id}/apis", response_model=schemas.ExternalApiResponse)
def create_external_api(
    service_id: int,
    api: schemas.ExternalApiCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Add an API definition to a service"""
    _require_api_permission(db, current_user, "api_registry.manage")
    service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")

    existing = db.query(models.ExternalApi).filter(
        models.ExternalApi.service_id == service_id,
        models.ExternalApi.name == api.name,
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="API name already exists for this service")

    new_api = models.ExternalApi(**api.dict())
    new_api.service_id = service_id
    db.add(new_api)
    db.commit()
    db.refresh(new_api)
    return new_api


@router.put("/api/external/apis/{api_id}", response_model=schemas.ExternalApiResponse)
def update_external_api(
    api_id: int,
    api: schemas.ExternalApiUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Update an external API"""
    _require_api_permission(db, current_user, "api_registry.manage")
    db_api = db.query(models.ExternalApi).filter(models.ExternalApi.id == api_id).first()
    if not db_api:
        raise HTTPException(status_code=404, detail="API not found")

    update_data = api.dict(exclude_unset=True)
    if "name" in update_data and update_data["name"] != db_api.name:
        existing = db.query(models.ExternalApi).filter(
            models.ExternalApi.service_id == db_api.service_id,
            models.ExternalApi.name == update_data["name"],
        ).first()
        if existing:
            raise HTTPException(status_code=400, detail="API name already exists for this service")

    for key, value in update_data.items():
        setattr(db_api, key, value)

    db.commit()
    db.refresh(db_api)
    return db_api


@router.delete("/api/external/apis/{api_id}")
def delete_external_api(
    api_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Delete an external API"""
    _require_api_permission(db, current_user, "api_registry.manage")
    api = db.query(models.ExternalApi).filter(models.ExternalApi.id == api_id).first()
    if not api:
        raise HTTPException(status_code=404, detail="API not found")

    db.delete(api)
    db.commit()
    return {"message": "API deleted"}


@router.get("/api/external/kingdee/status")
def get_kingdee_status(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get Kingdee token status (Adapted to use ExternalService)"""
    _require_api_permission(db, current_user, "credential.manage")
    service = db.query(models.ExternalService).filter(models.ExternalService.service_name == "kingdee_oauth").first()

    if not service or not service.access_token:
        return {
            "status": "not_connected",
            "message": "尚未获取凭证",
            "expires_at": None,
            "has_refresh_token": False,
        }

    now = datetime.now()
    is_expired = service.expires_at and service.expires_at < now

    return {
        "status": "expired" if is_expired else "connected",
        "message": "Token expired" if is_expired else "Connected",
        "expires_at": service.expires_at,
        "has_refresh_token": bool(service.refresh_token),
        "last_updated": service.updated_at,
    }


@router.post("/api/external/kingdee/refresh")
def refresh_kingdee_token(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "credential.manage")
    try:
        service = KingdeeAuthService(db)
        service._login_and_save()
        return {"success": True, "message": "刷新成功"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/external/marki/status")
def get_marki_status(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "credential.manage")
    service = db.query(models.ExternalService).filter(models.ExternalService.service_name == "marki").first()
    if not service or not service.app_id:
        return {
            "status": "not_connected",
            "message": "未配置 Marki 系统账户",
            "expires_at": None,
            "has_refresh_token": False,
        }

    has_cookie = bool(service.extra_info)
    return {
        "status": "connected" if has_cookie else "expired",
        "message": "已连接" if has_cookie else "凭证已过期",
        "expires_at": None,
        "has_refresh_token": False,
        "last_updated": service.updated_at,
    }


@router.post("/api/external/marki/config")
def update_marki_config(
    req: schemas.MarkiConfigRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "credential.manage")
    service = db.query(models.ExternalService).filter(models.ExternalService.service_name == "marki").first()
    if not service:
        service = models.ExternalService(
            service_name="marki",
            display_name="Marki 物业系统",
            auth_url="https://sttc-os-lgn.markiapp.com/lgn/login/authorize.do",
            base_url="https://charge-api.markiapp.com",
        )
        db.add(service)
    service.app_id = req.app_id
    if req.app_secret and req.app_secret != "********":
        service.app_secret = req.app_secret

    db.commit()
    return {"success": True, "message": "配置已保存"}


@router.post("/api/external/marki/refresh")
def refresh_marki_token(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "credential.manage")
    from utils.marki_client import marki_client

    service = db.query(models.ExternalService).filter(models.ExternalService.service_name == "marki").first()
    if service:
        service.extra_info = None
        db.commit()

    success = marki_client.login()
    if success:
        return {"success": True, "message": "刷新成功"}
    raise HTTPException(status_code=400, detail="登录失败，请检查账号密码")


@router.post("/api/external/services/{service_id}/token")
def refresh_service_token(
    service_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Force refresh or acquire token for a specific service"""
    _require_api_permission(db, current_user, "credential.manage")
    from services.external_auth import ExternalAuthService

    db_service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
    if not db_service:
        raise HTTPException(status_code=404, detail="Service not found")

    try:
        auth = ExternalAuthService(db_service.service_name, db)
        token = auth._login_and_save()

        return {
            "success": True,
            "message": "Token acquired successfully",
            "access_token_preview": token[:10] + "..." if token else None,
            "expires_at": auth.service_record.expires_at,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Authentication failed: {str(e)}")
