from typing import Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

import models
from api.dependencies import get_db, get_user_context
from utils.variable_parser import resolve_dict_variables, resolve_variables

router = APIRouter()


@router.get("/api/archives/types")
def get_archive_types(db: Session = Depends(get_db)):
    """Get the list of registered archive types."""
    var = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == "ARCHIVE_TYPE_REGISTRY").first()
    if not var:
        return [{"key": "accounting-subjects", "label": "会计科目", "icon": "FileText"}]
    import json

    try:
        return json.loads(var.value)
    except Exception:
        return []


@router.post("/api/archives/types")
def save_archive_types(types: List[dict], db: Session = Depends(get_db)):
    """Update the list of registered archive types."""
    import json

    val = json.dumps(types)
    var = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == "ARCHIVE_TYPE_REGISTRY").first()
    if var:
        var.value = val
    else:
        var = models.GlobalVariable(
            key="ARCHIVE_TYPE_REGISTRY",
            value=val,
            description="归档类型注册表，用于维护归档接口管理中的归档类型清单",
            category="system",
        )
        db.add(var)
    db.commit()
    return {"message": "Archive types updated"}


@router.get("/api/archives/config/{archive_key}")
def get_archive_config(archive_key: str, db: Session = Depends(get_db)):
    """Get configuration for a specific archive type."""
    storage_key = f"ARCHIVE_CONFIG_{archive_key.upper().replace('-', '_')}"
    if archive_key == "accounting-subjects":
        storage_key = "ACCOUNTING_SUBJECT_CONFIG"

    config = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == storage_key).first()
    if not config:
        return {}
    import json

    try:
        return json.loads(config.value)
    except Exception:
        return {}


@router.post("/api/archives/config/{archive_key}")
def save_archive_config(archive_key: str, config: dict, db: Session = Depends(get_db)):
    """Save configuration for a specific archive type."""
    storage_key = f"ARCHIVE_CONFIG_{archive_key.upper().replace('-', '_')}"
    if archive_key == "accounting-subjects":
        storage_key = "ACCOUNTING_SUBJECT_CONFIG"

    import json

    val = json.dumps(config)
    var = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == storage_key).first()
    if var:
        var.value = val
    else:
        config_description = (
            "会计科目同步配置，用于维护会计科目归档与同步所需的接口参数"
            if archive_key == "accounting-subjects"
            else "归档数据拉取配置"
        )
        var = models.GlobalVariable(
            key=storage_key,
            value=val,
            description=config_description,
            category="api_config",
        )
        db.add(var)
    db.commit()
    return {"message": "Config saved"}


@router.post("/api/archives/test")
def test_archive_config(
    config_data: dict,
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db),
):
    """Test a given archive configuration without saving it."""
    try:
        service_id = config_data.get("service_id")
        if not service_id:
            return {"success": False, "error": "未选择外部集成服务"}

        service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
        if not service:
            return {"success": False, "error": "所选服务不存在"}

        import json
        import requests
        from services.external_auth import ExternalAuthService

        auth = None
        if service.service_name == "marki":
            from utils.marki_client import MarkiClient

            marki = MarkiClient()
            marki._load_config()
            headers = marki.headers.copy()
            token = marki.cookie or ""
            if not token:
                if marki.login():
                    headers = marki.headers.copy()
                    token = marki.cookie or ""
                else:
                    return {"success": False, "error": "Marki 系统自动登录失败，请检查集成配置"}
        else:
            auth = ExternalAuthService(db=db, service_record=service, user_context=user_ctx)
            try:
                token = auth.get_token()
                headers = auth.get_auth_headers()
            except Exception as e:
                return {"success": False, "error": f"认证失败: {str(e)}"}

        user_headers = config_data.get("request_headers", {})
        if isinstance(user_headers, str):
            try:
                user_headers = json.loads(user_headers)
            except Exception:
                user_headers = {}

        user_headers = resolve_dict_variables(user_headers, db, user_context=user_ctx)
        for k, v in user_headers.items():
            if isinstance(v, str) and "{access_token}" in v:
                v = v.replace("{access_token}", token)
            if service.service_name == "marki" and k.lower() == "cookie":
                pass
            else:
                headers[k] = str(v)

        url = config_data.get("url")
        if not url:
            path = config_data.get("url_path") or ""
            if path.startswith("http://") or path.startswith("https://"):
                url = path
            else:
                base = service.base_url or ""
                if base and path and not base.endswith("/") and not path.startswith("/"):
                    url = f"{base}/{path}"
                else:
                    url = base + path

        url = resolve_variables(url or "", db, user_context=user_ctx)
        if not url:
            return {"success": False, "error": "请求地址不能为空"}

        method = config_data.get("method", "POST").upper()
        body_template = config_data.get("request_body", "")
        body = None
        if body_template:
            if isinstance(body_template, str):
                try:
                    body = json.loads(body_template)
                except Exception:
                    return {"success": False, "error": "请求体 JSON 格式错误"}
            else:
                body = body_template
            body = resolve_dict_variables(body, db, user_context=user_ctx)

        start_time = __import__("time").time()
        for attempt in range(2):
            try:
                if attempt > 0:
                    if service.service_name == "marki":
                        from utils.marki_client import MarkiClient

                        marki = MarkiClient()
                        marki.login()
                        headers = marki.headers.copy()
                        token = marki.cookie or ""
                    elif auth:
                        auth.invalidate_token()
                        db.commit()
                        service = db.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                        auth.service_record = service
                        token = auth.get_token()
                        headers = auth.get_auth_headers()

                    for k, v in user_headers.items():
                        if isinstance(v, str) and "{access_token}" in v:
                            v = v.replace("{access_token}", token)
                        if service.service_name == "marki" and k.lower() == "cookie":
                            pass
                        else:
                            headers[k] = str(v)

                is_get = method.upper() == "GET"
                req_kwargs = {"timeout": 15}
                if body is not None:
                    if is_get:
                        req_kwargs["params"] = body
                    else:
                        content_type = next((v for k, v in headers.items() if k.lower() == "content-type"), "").lower()
                        if "application/x-www-form-urlencoded" in content_type:
                            req_kwargs["data"] = body
                        else:
                            req_kwargs["json"] = body

                resp = requests.request(method, url, headers=headers, **req_kwargs)

                auth_failed = False
                if resp.status_code in [401, 602]:
                    auth_failed = True
                else:
                    try:
                        resp_json = resp.json()
                        err_code = str(resp_json.get("errorCode") or resp_json.get("code", ""))
                        if err_code in ["401", "602"]:
                            auth_failed = True
                    except Exception:
                        pass

                if auth_failed and attempt == 0:
                    continue

                duration = round((__import__("time").time() - start_time) * 1000, 2)
                try:
                    response_json = resp.json()
                except Exception:
                    response_json = {"raw": resp.text}

                return {
                    "success": resp.ok and not auth_failed,
                    "status_code": resp.status_code,
                    "duration_ms": duration,
                    "data": response_json,
                    "headers": dict(resp.headers),
                }
            except Exception as e:
                if attempt == 1:
                    return {"success": False, "error": f"请求失败，请稍后重试: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"执行异常: {str(e)}"}
