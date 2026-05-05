import logging
import time
from datetime import datetime, timedelta
from importlib import import_module
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.orm import Session

import models
import schemas
from api.bootstrap import _upsert_rows
from api.dependencies import get_db, get_user_context
from utils.api_config import get_api_id
from utils.variable_parser import resolve_dict_variables, resolve_variables

router = APIRouter()
logger = logging.getLogger("project_sync")
TAX_RATE_SYNC_STATUS_KEY = "TAX_RATE_SYNC_STATUS"
KINGDEE_INCREMENTAL_OVERLAP_MINUTES = 5
KINGDEE_FULL_SYNC_BASELINE = "1900-01-01 00:00:00"
ACCOUNTING_SUBJECT_LAST_SYNC_AT_KEY = "ACCOUNTING_SUBJECT_LAST_MODIFYTIME_SYNC_AT"
CUSTOMER_LAST_SYNC_AT_KEY = "CUSTOMER_LAST_MODIFYTIME_SYNC_AT"
SUPPLIER_LAST_SYNC_AT_KEY = "SUPPLIER_LAST_MODIFYTIME_SYNC_AT"
TAX_RATE_LAST_SYNC_AT_KEY = "TAX_RATE_LAST_MODIFYTIME_SYNC_AT"
KINGDEE_HOUSE_LAST_SYNC_AT_KEY = "KINGDEE_HOUSE_LAST_MODIFYTIME_SYNC_AT"
KINGDEE_ACCOUNT_BOOK_LAST_SYNC_AT_KEY = "KINGDEE_ACCOUNT_BOOK_LAST_MODIFYTIME_SYNC_AT"
AUXILIARY_DATA_LAST_SYNC_AT_KEY = "AUXILIARY_DATA_LAST_MODIFYTIME_SYNC_AT"
AUXILIARY_DATA_CATEGORY_LAST_SYNC_AT_KEY = "AUXILIARY_DATA_CATEGORY_LAST_MODIFYTIME_SYNC_AT"
KINGDEE_BANK_ACCOUNT_LAST_SYNC_AT_KEY = "KINGDEE_BANK_ACCOUNT_LAST_MODIFYTIME_SYNC_AT"

KINGDEE_SYNC_MODULES: dict[str, dict[str, str]] = {
    "accounting-subjects": {
        "module_key": ACCOUNTING_SUBJECT_LAST_SYNC_AT_KEY,
        "label": "会计科目",
        "description": "会计科目同步状态",
    },
    "customers": {
        "module_key": CUSTOMER_LAST_SYNC_AT_KEY,
        "label": "客户档案",
        "description": "客户档案同步状态",
    },
    "suppliers": {
        "module_key": SUPPLIER_LAST_SYNC_AT_KEY,
        "label": "供应商档案",
        "description": "供应商档案同步状态",
    },
    "tax-rates": {
        "module_key": TAX_RATE_LAST_SYNC_AT_KEY,
        "label": "税率档案",
        "description": "税率档案同步状态",
    },
    "kd-houses": {
        "module_key": KINGDEE_HOUSE_LAST_SYNC_AT_KEY,
        "label": "房号档案",
        "description": "房号档案同步状态",
    },
    "kd-account-books": {
        "module_key": KINGDEE_ACCOUNT_BOOK_LAST_SYNC_AT_KEY,
        "label": "账簿档案",
        "description": "账簿档案同步状态",
    },
    "auxiliary-data": {
        "module_key": AUXILIARY_DATA_LAST_SYNC_AT_KEY,
        "label": "辅助资料",
        "description": "辅助资料同步状态",
    },
    "auxiliary-data-categories": {
        "module_key": AUXILIARY_DATA_CATEGORY_LAST_SYNC_AT_KEY,
        "label": "辅助资料分类",
        "description": "辅助资料分类同步状态",
    },
    "kd-bank-accounts": {
        "module_key": KINGDEE_BANK_ACCOUNT_LAST_SYNC_AT_KEY,
        "label": "银行账户",
        "description": "银行账户同步状态",
    },
}


def _main_attr(name: str):
    return getattr(import_module("main"), name)


def get_archive_config(*args, **kwargs):
    return _main_attr("get_archive_config")(*args, **kwargs)


def save_archive_config(*args, **kwargs):
    return _main_attr("save_archive_config")(*args, **kwargs)


def _save_tax_rate_sync_status(db_session: Session, payload: Dict[str, object]) -> None:
    import json

    value = json.dumps(payload, ensure_ascii=False)
    var = db_session.query(models.GlobalVariable).filter(models.GlobalVariable.key == TAX_RATE_SYNC_STATUS_KEY).first()
    if var:
        var.value = value
        var.category = "sync_status"
        var.description = "Tax rate sync latest status"
    else:
        var = models.GlobalVariable(
            key=TAX_RATE_SYNC_STATUS_KEY,
            value=value,
            description="Tax rate sync latest status",
            category="sync_status",
        )
        db_session.add(var)
    db_session.commit()


def _load_datetime_global_variable(db_session: Session, key: str) -> Optional[datetime]:
    row = db_session.query(models.GlobalVariable).filter(models.GlobalVariable.key == key).first()
    if not row or not row.value:
        return None
    try:
        return datetime.fromisoformat(str(row.value).strip())
    except Exception:
        return None


def _load_sync_module_watermark(db_session: Session, module_key: str) -> Optional[datetime]:
    row = (
        db_session.query(models.SyncModuleStatus)
        .filter(models.SyncModuleStatus.module_key == module_key)
        .first()
    )
    if row and row.last_modifytime_sync_at:
        return row.last_modifytime_sync_at

    legacy_value = _load_datetime_global_variable(db_session, module_key)
    if legacy_value:
        db_session.add(
            models.SyncModuleStatus(
                module_key=module_key,
                source_system="kingdee",
                last_modifytime_sync_at=legacy_value,
                last_success_at=legacy_value,
                description=f"Migrated legacy watermark for {module_key}",
            )
        )
        db_session.commit()
        logger.info(
            "Migrated legacy Kingdee sync watermark from global_variables to sync_module_status: module_key=%s watermark=%s",
            module_key,
            legacy_value.isoformat(),
        )
    return legacy_value


def _save_datetime_global_variable(
    db_session: Session,
    *,
    key: str,
    value: datetime,
    description: str,
) -> None:
    value_text = value.replace(microsecond=0).isoformat()
    row = db_session.query(models.GlobalVariable).filter(models.GlobalVariable.key == key).first()
    if row:
        row.value = value_text
        row.description = description
        row.category = "sync_status"
        row.is_secret = False
    else:
        db_session.add(
            models.GlobalVariable(
                key=key,
                value=value_text,
                description=description,
                category="sync_status",
                is_secret=False,
            )
        )
    db_session.commit()


def _save_sync_module_watermark(
    db_session: Session,
    *,
    module_key: str,
    value: datetime,
    description: str,
    full_sync: bool,
) -> None:
    watermark_time = value.replace(microsecond=0)
    row = (
        db_session.query(models.SyncModuleStatus)
        .filter(models.SyncModuleStatus.module_key == module_key)
        .first()
    )
    if row:
        row.source_system = "kingdee"
        row.last_modifytime_sync_at = watermark_time
        row.last_success_at = datetime.now().replace(microsecond=0)
        if full_sync:
            row.last_full_sync_at = watermark_time
        row.description = description
    else:
        db_session.add(
            models.SyncModuleStatus(
                module_key=module_key,
                source_system="kingdee",
                status="idle",
                last_modifytime_sync_at=watermark_time,
                last_success_at=datetime.now().replace(microsecond=0),
                last_full_sync_at=watermark_time if full_sync else None,
                description=description,
            )
        )
    db_session.commit()


def _save_sync_module_run_status(
    db_session: Session,
    *,
    module_key: str,
    description: str,
    status: str,
    message: Optional[str] = None,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
) -> None:
    normalized_started_at = started_at.replace(microsecond=0) if started_at else None
    normalized_finished_at = finished_at.replace(microsecond=0) if finished_at else None
    row = (
        db_session.query(models.SyncModuleStatus)
        .filter(models.SyncModuleStatus.module_key == module_key)
        .first()
    )
    if row:
        row.source_system = "kingdee"
        row.status = status
        row.message = message
        row.description = description
        if normalized_started_at is not None:
            row.started_at = normalized_started_at
        if status == "running":
            row.finished_at = None
        elif normalized_finished_at is not None:
            row.finished_at = normalized_finished_at
    else:
        db_session.add(
            models.SyncModuleStatus(
                module_key=module_key,
                source_system="kingdee",
                status=status,
                message=message,
                started_at=normalized_started_at,
                finished_at=None if status == "running" else normalized_finished_at,
                description=description,
            )
        )
    db_session.commit()


def _parse_kingdee_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _format_kingdee_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _update_max_seen_modifytime(current: Optional[datetime], row: Dict[str, Any]) -> Optional[datetime]:
    row_modifytime = _parse_kingdee_datetime(row.get("modifytime"))
    if row_modifytime and (current is None or row_modifytime > current):
        return row_modifytime
    return current


def _prepare_incremental_sync_context(
    db_session: Session,
    *,
    body: Dict[str, Any],
    sync_name: str,
    module_key: str,
    force_full_sync: bool,
    page_size_default: int = 1000,
) -> tuple[datetime, Optional[datetime], bool]:
    if not body:
        body = {"data": {}}
    if "data" not in body or not isinstance(body["data"], dict):
        body["data"] = {}

    page_size = body.get("pageSize")
    if not page_size or not isinstance(page_size, int) or page_size <= 0:
        body["pageSize"] = page_size_default
    else:
        body["pageSize"] = page_size

    query_end_time = datetime.now().replace(microsecond=0)
    configured_modifytime = body["data"].get("modifytime")
    manual_time_filter = bool(
        body["data"].get("start_createtime")
        or body["data"].get("end_createtime")
    )
    last_sync_at = None if (force_full_sync or manual_time_filter) else _load_sync_module_watermark(db_session, module_key)

    if manual_time_filter:
        logger.info(
            "%s sync will use manual createtime filter from API config: start=%s, end=%s",
            sync_name,
            body["data"].get("start_createtime"),
            body["data"].get("end_createtime"),
        )
    elif force_full_sync:
        body["data"]["modifytime"] = KINGDEE_FULL_SYNC_BASELINE
        logger.info(
            "%s sync running in full sync mode because request.full_sync=true. modifytime=%s",
            sync_name,
            body["data"]["modifytime"],
        )
    elif last_sync_at:
        incremental_start_time = last_sync_at - timedelta(minutes=KINGDEE_INCREMENTAL_OVERLAP_MINUTES)
        body["data"]["modifytime"] = _format_kingdee_datetime(incremental_start_time)
        logger.info(
            "%s sync running in incremental mode by modifytime: modifytime>=%s",
            sync_name,
            body["data"]["modifytime"],
        )
    elif configured_modifytime:
        body["data"]["modifytime"] = configured_modifytime
        logger.info(
            "%s sync bootstrapping incremental mode from configured modifytime=%s because no saved watermark was found.",
            sync_name,
            body["data"]["modifytime"],
        )
    else:
        body["data"]["modifytime"] = KINGDEE_FULL_SYNC_BASELINE
        logger.info(
            "%s sync running initial full sync because no previous modifytime watermark was found. modifytime=%s",
            sync_name,
            body["data"]["modifytime"],
        )

    return query_end_time, last_sync_at, manual_time_filter


def _save_incremental_sync_watermark(
    db_session: Session,
    *,
    sync_name: str,
    module_key: str,
    description: str,
    manual_time_filter: bool,
    max_seen_modifytime: Optional[datetime],
    query_end_time: datetime,
    full_sync: bool,
) -> None:
    if manual_time_filter:
        return
    watermark_time = max_seen_modifytime or query_end_time
    _save_sync_module_watermark(
        db_session,
        module_key=module_key,
        value=watermark_time,
        description=description,
        full_sync=full_sync,
    )
    logger.info("%s sync modifytime watermark saved at %s", sync_name, watermark_time.isoformat())


def _log_page_write_summary(
    sync_name: str,
    *,
    page_no: int,
    fetched_rows: int,
    unique_rows: int,
    upserted_rows: int,
) -> None:
    logger.info(
        "%s page %s write summary: fetched_rows=%s unique_rows=%s upserted_rows=%s",
        sync_name,
        page_no,
        fetched_rows,
        unique_rows,
        upserted_rows,
    )


def _serialize_sync_module_status(
    module_code: str,
    definition: Dict[str, str],
    row: Optional[models.SyncModuleStatus],
) -> Dict[str, Any]:
    return {
        "module_code": module_code,
        "module_key": definition["module_key"],
        "label": definition["label"],
        "description": (row.description if row and row.description else definition["description"]),
        "status": row.status if row and row.status else "idle",
        "message": row.message if row else None,
        "started_at": row.started_at if row else None,
        "finished_at": row.finished_at if row else None,
        "last_modifytime_sync_at": row.last_modifytime_sync_at if row else None,
        "last_success_at": row.last_success_at if row else None,
        "last_full_sync_at": row.last_full_sync_at if row else None,
        "has_status": bool(
            row
            and (
                row.started_at
                or row.finished_at
                or row.last_modifytime_sync_at
                or row.last_success_at
                or row.last_full_sync_at
            )
        ),
    }


def _resolve_external_api_id(request_api_id: Optional[int], env_key: str) -> Optional[int]:
    if request_api_id is not None:
        return int(request_api_id)
    try:
        return get_api_id(env_key)
    except ValueError:
        logger.error("Invalid %s. Expected integer external_apis.id.", env_key)
        return None


def _load_external_api_by_id(
    db_session: Session,
    *,
    request_api_id: Optional[int],
    env_key: str,
    sync_label: str,
) -> tuple[Optional[Any], Optional[int]]:
    api_id = _resolve_external_api_id(request_api_id, env_key)
    if api_id is None:
        logger.error(
            "No configuration found for %s: api_id is required. Pass request.api_id or configure %s.",
            sync_label,
            env_key,
        )
        return None, None

    api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.id == api_id).first()
    if not api_record:
        logger.error("No configuration found for %s: external_apis.id=%s not found.", sync_label, api_id)
        return None, api_id

    return api_record, api_id

# ===================== Accounting Subject Management (Legacy/Specific) =====================

@router.get("/api/finance/accounting-subjects", response_model=schemas.PaginatedAccountingSubjectResponse)
def get_accounting_subjects(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    account_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get accounting subjects with pagination and search"""
    query = db.query(models.AccountingSubject)
    
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.AccountingSubject.number.ilike(search_filter)) |
            (models.AccountingSubject.name.ilike(search_filter)) |
            (models.AccountingSubject.fullname.ilike(search_filter))
        )
    
    if account_type:
        query = query.filter(models.AccountingSubject.account_type_number == account_type)
    
    total = query.count()
    subjects = query.order_by(models.AccountingSubject.number).offset(skip).limit(limit).all()
    
    return {"items": subjects, "total": total}

@router.get("/api/finance/accounting-subjects/config")
def get_accounting_subject_config(db: Session = Depends(get_db)):
    return get_archive_config("accounting-subjects", db)

@router.post("/api/finance/accounting-subjects/config")
def save_accounting_subject_config(config: dict, db: Session = Depends(get_db)):
    return save_archive_config("accounting-subjects", config, db)

@router.post("/api/finance/accounting-subjects/sync")
def sync_accounting_subjects(
    request: schemas.AccountingSubjectSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync accounting subjects using configured API"""
    
    from database import SessionLocal
    request_api_id = request.api_id
    force_full_sync = bool(request.full_sync)
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        started_at = datetime.now().replace(microsecond=0)
        _save_sync_module_run_status(
            db_session,
            module_key=CUSTOMER_LAST_SYNC_AT_KEY,
            description="客户档案同步状态",
            status="running",
            message="客户档案同步执行中",
            started_at=started_at,
        )

        def mark_failed(message: str) -> None:
            _save_sync_module_run_status(
                db_session,
                module_key=CUSTOMER_LAST_SYNC_AT_KEY,
                description="客户档案同步状态",
                status="failed",
                message=message,
                started_at=started_at,
                finished_at=datetime.now(),
            )
        try:
            # 1. Get Config (Try ExternalApi first, then fallback to GlobalVariable)
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record, _ = _load_external_api_by_id(
                db_session,
                request_api_id=request_api_id,
                env_key="KINGDEE_ACCOUNTING_SUBJECT_API_ID",
                sync_label="accounting_subjects",
            )
            if not api_record:
                mark_failed("未找到会计科目同步接口配置")
                return

            config = {
                "method": api_record.method,
                "url": api_record.url_path,
                "request_headers": api_record.request_headers,
                "request_body": api_record.request_body,
                "service_id": api_record.service_id,
            }
            service_id = api_record.service_id
                 
            if not service_id:
                 logger.error("Configuration missing service_id")
                 mark_failed("会计科目同步缺少服务配置")
                 return
                 
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                 logger.error(f"Service with ID {service_id} not found")
                 mark_failed(f"会计科目同步服务不存在: {service_id}")
                 return
                 
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            # Initial token fetch attempt
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 mark_failed(f"会计科目同步认证失败: {e}")
                 return
                 
            # 3. Prepare Request Parts
            full_url = config.get("url")
            if not full_url:
                full_url = (service.base_url or "") + config.get("url_path", "")
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            method = config.get("method", "POST")
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = config.get("request_headers", {})
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = config.get("request_body", "")
            body = {}
            try:
                if isinstance(body_template, str):
                    try:
                        body = json.loads(body_template) if body_template else {}
                    except:
                        body = {} 
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)

            query_end_time, last_sync_at, manual_time_filter = _prepare_incremental_sync_context(
                db_session,
                body=body,
                sync_name="Accounting subject",
                module_key=ACCOUNTING_SUBJECT_LAST_SYNC_AT_KEY,
                force_full_sync=force_full_sync,
                page_size_default=1000,
            )

            # Incremental sync without truncating existing data
            page_no = 1
            total_synced = 0
            prev_page_ids = []
            max_seen_modifytime = last_sync_at
            request_timeout = 60
            transient_retry_limit = 3
            
            while True:
                body["pageNo"] = page_no
                logger.info(
                    "Syncing accounting subjects page %s with pageSize=%s, modifytime=%s, start_createtime=%s, end_createtime=%s...",
                    page_no,
                    body.get("pageSize"),
                    body["data"].get("modifytime"),
                    body["data"].get("start_createtime"),
                    body["data"].get("end_createtime"),
                )
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(method, url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                if not success:
                    break
                    
                # Process Data
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                
                if isinstance(rows, list) and rows:
                    count = 0
                    current_page_ids = []
                    unique_rows = {} # Python 娓氀勭壌閹?number 閸樺鍣?
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                        max_seen_modifytime = _update_max_seen_modifytime(max_seen_modifytime, row)
                             
                        number = str(row.get("number", ""))
                        if not number: continue
                        
                        subject_data = {
                            "id": number,
                            "number": number,
                            "name": row.get("name", ""),
                            "fullname": row.get("fullname", ""),
                            "long_number": row.get("longnumber", ""),
                            "level": row.get("level"),
                            "is_leaf": row.get("isleaf"),
                            "direction": str(row.get("dc", "")),
                            "is_active": (str(row.get("enable")) == "1"),
                            "is_cash": row.get("iscash", False),
                            "is_bank": row.get("isbank", False),
                            "is_cash_equivalent": row.get("iscashequivalent", False),
                            "acct_currency": row.get("acctcurrency", ""),
                            "account_type_number": row.get("accounttype_accounttype", ""),
                            "ac_check": row.get("accheck", False),
                            "is_qty": row.get("isqty", False),
                            "currency_entry": json.dumps(row["currencyentry"]) if "currencyentry" in row else None,
                            "raw_data": json.dumps(row),
                            "check_items": json.dumps(row["checkitementry"]) if "checkitementry" in row else None
                        }
                        unique_rows[number] = subject_data
                    
                    # 閹靛綊鍣洪崚鍡?Upsert
                    unique_list = list(unique_rows.values())
                    unique_count = len(unique_list)
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.AccountingSubject,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
                    )
                    _log_page_write_summary(
                        "Accounting subjects",
                        page_no=page_no,
                        fetched_rows=len(rows),
                        unique_rows=unique_count,
                        upserted_rows=count,
                    )

                    total_synced += count
                    
                    # 瀵板箚閻旀梹鏌?
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Accounting subjects pagination repeated, breaking.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            _save_incremental_sync_watermark(
                db_session,
                sync_name="Accounting subject",
                module_key=ACCOUNTING_SUBJECT_LAST_SYNC_AT_KEY,
                description="Last successful accounting subject sync watermark by modifytime",
                manual_time_filter=manual_time_filter,
                max_seen_modifytime=max_seen_modifytime,
                query_end_time=query_end_time,
                full_sync=force_full_sync,
            )
            logger.info(f"Finished. Total synced accounting subjects: {total_synced}")
            _save_sync_module_run_status(
                db_session,
                module_key=ACCOUNTING_SUBJECT_LAST_SYNC_AT_KEY,
                description="会计科目同步状态",
                status="success",
                message=f"会计科目同步完成，共写入 {total_synced} 条记录",
                started_at=started_at,
                finished_at=datetime.now(),
            )
                    
        except Exception as e:
             logger.error(f"Sync failed outer: {e}")
             mark_failed(f"会计科目同步失败: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Sync started"}

# ===================== Customer Management =====================

@router.get("/api/finance/customers", response_model=schemas.PaginatedCustomerResponse)
def get_customers(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.Customer)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.Customer.number.ilike(search_filter)) |
            (models.Customer.name.ilike(search_filter))
        )
    total = query.count()
    customers = query.order_by(models.Customer.number).offset(skip).limit(limit).all()
    return {"items": customers, "total": total}

@router.post("/api/finance/customers/sync")
def sync_customers(
    request: schemas.CustomerSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync customers using configured API"""
    from database import SessionLocal
    request_api_id = request.api_id
    force_full_sync = bool(request.full_sync)
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        started_at = datetime.now().replace(microsecond=0)
        _save_sync_module_run_status(
            db_session,
            module_key=ACCOUNTING_SUBJECT_LAST_SYNC_AT_KEY,
            description="会计科目同步状态",
            status="running",
            message="会计科目同步执行中",
            started_at=started_at,
        )

        def mark_failed(message: str) -> None:
            _save_sync_module_run_status(
                db_session,
                module_key=ACCOUNTING_SUBJECT_LAST_SYNC_AT_KEY,
                description="会计科目同步状态",
                status="failed",
                message=message,
                started_at=started_at,
                finished_at=datetime.now(),
            )
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record, _ = _load_external_api_by_id(
                db_session,
                request_api_id=request_api_id,
                env_key="KINGDEE_CUSTOMER_API_ID",
                sync_label="customers",
            )
            if not api_record:
                mark_failed("未找到客户档案同步接口配置")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                mark_failed(f"客户档案同步服务不存在: {service_id}")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 mark_failed(f"客户档案同步认证失败: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            query_end_time, last_sync_at, manual_time_filter = _prepare_incremental_sync_context(
                db_session,
                body=body,
                sync_name="Customer",
                module_key=CUSTOMER_LAST_SYNC_AT_KEY,
                force_full_sync=force_full_sync,
                page_size_default=1000,
            )
            
            page_no = 1
            total_synced = 0
            prev_page_ids = []
            max_seen_modifytime = last_sync_at
            request_timeout = 60
            transient_retry_limit = 3
            
            while True:
                body["pageNo"] = page_no
                logger.info(
                    "Syncing customers page %s with pageSize=%s, modifytime=%s, start_createtime=%s, end_createtime=%s...",
                    page_no,
                    body.get("pageSize"),
                    body["data"].get("modifytime"),
                    body["data"].get("start_createtime"),
                    body["data"].get("end_createtime"),
                )
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        transient_attempt = 0
                        while True:
                            try:
                                resp = requests.request(
                                    api_record.method or "POST",
                                    url,
                                    headers=headers,
                                    json=body,
                                    timeout=request_timeout,
                                )
                                break
                            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                                transient_attempt += 1
                                if transient_attempt > transient_retry_limit:
                                    raise exc
                                wait_seconds = min(transient_attempt * 2, 10)
                                logger.warning(
                                    "Customer sync transient error on page %s (pageSize=%s, retry=%s/%s): %s",
                                    page_no,
                                    body.get("pageSize"),
                                    transient_attempt,
                                    transient_retry_limit,
                                    exc,
                                )
                                time.sleep(wait_seconds)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                # Process Data
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        # Convert to boolean handling strict typing
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                row_count = len(rows) if isinstance(rows, list) else 0
                logger.info(
                    "Customers page %s completed with %s rows (pageSize=%s, lastPage=%s)",
                    page_no,
                    row_count,
                    body.get("pageSize"),
                    last_page,
                )
                
                if isinstance(rows, list) and rows:
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {} # 閸?Python 娓氀勭壌閹?number 閸樺鍣?
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)

                        max_seen_modifytime = _update_max_seen_modifytime(max_seen_modifytime, row)
                            
                        number = str(row.get("number", ""))
                        if not number: continue
                        
                        # Build upsert payload
                        customer_data = {
                            "id": number,
                            "number": number,
                            "name": row.get("name", ""),
                            "status": str(row.get("status", "")),
                            "enable": str(row.get("enable", "")),
                            "type": str(row.get("type", "")),
                            "linkman": str(row.get("linkman", "")),
                            "bizpartner_phone": str(row.get("bizpartner_phone", "")),
                            "bizpartner_address": str(row.get("bizpartner_address", "")),
                            "societycreditcode": str(row.get("societycreditcode", "")),
                            "org_name": str(row.get("org_name", "")),
                            "createorg_name": str(row.get("createorg_name", "")),
                            "entry_bank": json.dumps(row["entry_bank"]) if "entry_bank" in row else None,
                            "entry_linkman": json.dumps(row["entry_linkman"]) if "entry_linkman" in row else None,
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[number] = customer_data
                    
                    # 閸戝棗閸掑棙澹?Upsert 閸忋儱绨?
                    unique_list = list(unique_rows.values())
                    unique_count = len(unique_list)
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.Customer,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
                    )
                    _log_page_write_summary(
                        "Customers",
                        page_no=page_no,
                        fetched_rows=row_count,
                        unique_rows=unique_count,
                        upserted_rows=count,
                    )
                            
                    total_synced += count
                    
                    # Stop if API starts repeating the same page payload.
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            _save_incremental_sync_watermark(
                db_session,
                sync_name="Customer",
                module_key=CUSTOMER_LAST_SYNC_AT_KEY,
                description="Last successful customer sync watermark by modifytime",
                manual_time_filter=manual_time_filter,
                max_seen_modifytime=max_seen_modifytime,
                query_end_time=query_end_time,
                full_sync=force_full_sync,
            )

            logger.info(f"Finished. Total synced customers: {total_synced}")
            _save_sync_module_run_status(
                db_session,
                module_key=CUSTOMER_LAST_SYNC_AT_KEY,
                description="客户档案同步状态",
                status="success",
                message=f"客户档案同步完成，共写入 {total_synced} 条记录",
                started_at=started_at,
                finished_at=datetime.now(),
            )
                    
        except Exception as e:
             logger.error(f"Sync customers failed: {e}")
             mark_failed(f"客户档案同步失败: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Customer sync started"}

# ===================== Supplier Management =====================

@router.get("/api/finance/suppliers", response_model=schemas.PaginatedSupplierResponse)
def get_suppliers(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.Supplier)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.Supplier.number.ilike(search_filter)) |
            (models.Supplier.name.ilike(search_filter))
        )
    total = query.count()
    suppliers = query.order_by(models.Supplier.number).offset(skip).limit(limit).all()
    return {"items": suppliers, "total": total}

@router.post("/api/finance/suppliers/sync")
def sync_suppliers(
    request: schemas.SupplierSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync suppliers using configured API"""
    from database import SessionLocal
    request_api_id = request.api_id
    force_full_sync = bool(request.full_sync)
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        started_at = datetime.now().replace(microsecond=0)
        _save_sync_module_run_status(
            db_session,
            module_key=SUPPLIER_LAST_SYNC_AT_KEY,
            description="供应商档案同步状态",
            status="running",
            message="供应商档案同步执行中",
            started_at=started_at,
        )

        def mark_failed(message: str) -> None:
            _save_sync_module_run_status(
                db_session,
                module_key=SUPPLIER_LAST_SYNC_AT_KEY,
                description="供应商档案同步状态",
                status="failed",
                message=message,
                started_at=started_at,
                finished_at=datetime.now(),
            )
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record, _ = _load_external_api_by_id(
                db_session,
                request_api_id=request_api_id,
                env_key="KINGDEE_SUPPLIER_API_ID",
                sync_label="suppliers",
            )
            if not api_record:
                mark_failed("未找到供应商档案同步接口配置")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                mark_failed(f"供应商档案同步服务不存在: {service_id}")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 mark_failed(f"供应商档案同步认证失败: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            query_end_time, last_sync_at, manual_time_filter = _prepare_incremental_sync_context(
                db_session,
                body=body,
                sync_name="Supplier",
                module_key=SUPPLIER_LAST_SYNC_AT_KEY,
                force_full_sync=force_full_sync,
                page_size_default=1000,
            )
             
            page_no = 1
            total_synced = 0
            prev_page_ids = []
            max_seen_modifytime = last_sync_at
             
            while True:
                body["pageNo"] = page_no
                logger.info(
                    "Syncing suppliers page %s with pageSize=%s, modifytime=%s, start_createtime=%s, end_createtime=%s...",
                    page_no,
                    body.get("pageSize"),
                    body["data"].get("modifytime"),
                    body["data"].get("start_createtime"),
                    body["data"].get("end_createtime"),
                )
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                # Process Data
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {} # 閸?Python 娓氀勭壌閹?number 閸樺鍣?
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                        max_seen_modifytime = _update_max_seen_modifytime(max_seen_modifytime, row)
                             
                        number = str(row.get("number", ""))
                        if not number: continue
                        
                        supplier_data = {
                            "id": number,
                            "number": number,
                            "name": row.get("name", ""),
                            "status": str(row.get("status", "")),
                            "enable": str(row.get("enable", "")),
                            "type": str(row.get("type", "")),
                            "createorg_number": str(row.get("createorg_number", "")),
                            "supplier_status_name": str(row.get("supplier_status_name", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[number] = supplier_data
                    
                    unique_list = list(unique_rows.values())
                    unique_count = len(unique_list)
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.Supplier,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
                    )
                    _log_page_write_summary(
                        "Suppliers",
                        page_no=page_no,
                        fetched_rows=len(rows),
                        unique_rows=unique_count,
                        upserted_rows=count,
                    )
                             
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            _save_incremental_sync_watermark(
                db_session,
                sync_name="Supplier",
                module_key=SUPPLIER_LAST_SYNC_AT_KEY,
                description="Last successful supplier sync watermark by modifytime",
                manual_time_filter=manual_time_filter,
                max_seen_modifytime=max_seen_modifytime,
                query_end_time=query_end_time,
                full_sync=force_full_sync,
            )
            logger.info(f"Finished. Total synced suppliers: {total_synced}")
            _save_sync_module_run_status(
                db_session,
                module_key=SUPPLIER_LAST_SYNC_AT_KEY,
                description="供应商档案同步状态",
                status="success",
                message=f"供应商档案同步完成，共写入 {total_synced} 条记录",
                started_at=started_at,
                finished_at=datetime.now(),
            )
                    
        except Exception as e:
             logger.error(f"Sync suppliers failed: {e}")
             mark_failed(f"供应商档案同步失败: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Supplier sync started"}

# ===================== Tax Rate Management =====================

@router.get("/api/finance/tax-rates", response_model=schemas.PaginatedTaxRateResponse)
def get_tax_rates(
    skip: int = 0,
    limit: int = 100,
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.TaxRate)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.TaxRate.number.ilike(search_filter)) |
            (models.TaxRate.name.ilike(search_filter))
        )
    total = query.count()
    items = query.order_by(models.TaxRate.number).offset(skip).limit(limit).all()
    return {"items": items, "total": total}


@router.get("/api/finance/tax-rates/sync-status")
def get_tax_rate_sync_status(db: Session = Depends(get_db)):
    import json
    default_api_id = _resolve_external_api_id(None, "KINGDEE_TAX_RATE_API_ID")

    var = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == TAX_RATE_SYNC_STATUS_KEY).first()
    if not var or not var.value:
        return {
            "status": "idle",
            "message": "尚未执行税率同步",
            "started_at": None,
            "finished_at": None,
            "total_synced": 0,
            "api_id": default_api_id,
        }

    try:
        payload = json.loads(var.value)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass

    return {
        "status": "unknown",
        "message": "税率同步状态记录已损坏",
        "started_at": None,
        "finished_at": None,
        "total_synced": 0,
        "api_id": default_api_id,
    }


@router.get("/api/finance/sync-modules/status")
def get_finance_sync_module_statuses(db: Session = Depends(get_db)):
    rows = db.query(models.SyncModuleStatus).filter(models.SyncModuleStatus.source_system == "kingdee").all()
    row_map = {row.module_key: row for row in rows}
    return {
        "items": [
            _serialize_sync_module_status(module_code, definition, row_map.get(definition["module_key"]))
            for module_code, definition in KINGDEE_SYNC_MODULES.items()
        ]
    }


@router.get("/api/finance/sync-modules/{module_code}/status")
def get_finance_sync_module_status(module_code: str, db: Session = Depends(get_db)):
    definition = KINGDEE_SYNC_MODULES.get(module_code)
    if not definition:
        return {"error": "Unknown sync module", "module_code": module_code}

    row = (
        db.query(models.SyncModuleStatus)
        .filter(
            models.SyncModuleStatus.source_system == "kingdee",
            models.SyncModuleStatus.module_key == definition["module_key"],
        )
        .first()
    )
    return _serialize_sync_module_status(module_code, definition, row)

@router.post("/api/finance/tax-rates/sync")
def sync_tax_rates(
    request: schemas.TaxRateSyncRequest,
    background_tasks: BackgroundTasks,
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync tax rates using the configured Kingdee API (ExternalApi ID 32)."""
    from database import SessionLocal
    request_api_id = request.api_id
    force_full_sync = bool(request.full_sync)

    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        from datetime import datetime

        started_at = datetime.utcnow().replace(microsecond=0).isoformat()
        resolved_api_id = _resolve_external_api_id(request_api_id, "KINGDEE_TAX_RATE_API_ID")
        try:
            import json
            import requests
            from services.external_auth import ExternalAuthService

            _save_tax_rate_sync_status(
                db_session,
                {
                    "status": "running",
                    "message": "税率同步任务执行中",
                    "started_at": started_at,
                    "finished_at": None,
                    "total_synced": 0,
                    "api_id": resolved_api_id,
                },
            )
            _save_sync_module_run_status(
                db_session,
                module_key=TAX_RATE_LAST_SYNC_AT_KEY,
                description="税率档案同步状态",
                status="running",
                message="税率档案同步执行中",
                started_at=datetime.fromisoformat(started_at),
            )

            api_record, resolved_api_id = _load_external_api_by_id(
                db_session,
                request_api_id=request_api_id,
                env_key="KINGDEE_TAX_RATE_API_ID",
                sync_label="tax_rates",
            )
            if not api_record:
                _save_tax_rate_sync_status(
                    db_session,
                    {
                        "status": "failed",
                        "message": "未找到税率同步接口配置，请检查 api_id 或 KINGDEE_TAX_RATE_API_ID",
                        "started_at": started_at,
                        "finished_at": datetime.utcnow().replace(microsecond=0).isoformat(),
                        "total_synced": 0,
                        "api_id": resolved_api_id,
                    },
                )
                _save_sync_module_run_status(
                    db_session,
                    module_key=TAX_RATE_LAST_SYNC_AT_KEY,
                    description="税率档案同步状态",
                    status="failed",
                    message="未找到税率同步接口配置，请检查 api_id 或 KINGDEE_TAX_RATE_API_ID",
                    started_at=datetime.fromisoformat(started_at),
                    finished_at=datetime.now(),
                )
                return

            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                _save_tax_rate_sync_status(
                    db_session,
                    {
                        "status": "failed",
                        "message": f"未找到服务 ID={service_id}",
                        "started_at": started_at,
                        "finished_at": datetime.utcnow().replace(microsecond=0).isoformat(),
                        "total_synced": 0,
                        "api_id": resolved_api_id,
                    },
                )
                _save_sync_module_run_status(
                    db_session,
                    module_key=TAX_RATE_LAST_SYNC_AT_KEY,
                    description="税率档案同步状态",
                    status="failed",
                    message=f"未找到服务 ID={service_id}",
                    started_at=datetime.fromisoformat(started_at),
                    finished_at=datetime.now(),
                )
                return

            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                auth.get_token()
            except Exception as e:
                logger.error(f"Auth failed: {e}")
                _save_tax_rate_sync_status(
                    db_session,
                    {
                        "status": "failed",
                        "message": f"认证失败: {str(e)}",
                        "started_at": started_at,
                        "finished_at": datetime.utcnow().replace(microsecond=0).isoformat(),
                        "total_synced": 0,
                        "api_id": resolved_api_id,
                    },
                )
                _save_sync_module_run_status(
                    db_session,
                    module_key=TAX_RATE_LAST_SYNC_AT_KEY,
                    description="税率档案同步状态",
                    status="failed",
                    message=f"认证失败: {str(e)}",
                    started_at=datetime.fromisoformat(started_at),
                    finished_at=datetime.now(),
                )
                return

            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            url = resolve_variables(full_url or "", db_session, user_context=user_context)

            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try:
                    user_headers = json.loads(user_headers)
                except Exception:
                    user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)

            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except Exception:
                body = {}
            body = resolve_dict_variables(body, db_session, user_context=user_context)

            query_end_time, last_sync_at, manual_time_filter = _prepare_incremental_sync_context(
                db_session,
                body=body,
                sync_name="Tax rate",
                module_key=TAX_RATE_LAST_SYNC_AT_KEY,
                force_full_sync=force_full_sync,
                page_size_default=1000,
            )

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            max_seen_modifytime = last_sync_at

            while True:
                body["pageNo"] = page_no
                logger.info(
                    "Syncing tax rates page %s with pageSize=%s, modifytime=%s, start_createtime=%s, end_createtime=%s...",
                    page_no,
                    body.get("pageSize"),
                    body["data"].get("modifytime"),
                    body["data"].get("start_createtime"),
                    body["data"].get("end_createtime"),
                )

                success = False
                data = {}

                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    auth_failed = False
                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except Exception:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            if resp.status_code != 200:
                                resp.raise_for_status()
                            raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try:
                            data = resp.json()
                        except Exception:
                            data = {}

                        success = True
                        break

                    except Exception as e:
                        if attempt == 1:
                            raise e
                        if not auth_failed:
                            raise e

                if not success:
                    break

                rows = []
                last_page = True

                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == "true"
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == "true"
                elif isinstance(data, list):
                    rows = data

                if isinstance(rows, list) and rows:
                    count = 0
                    current_page_ids = []
                    unique_rows = {}

                    for row in rows:
                        if not isinstance(row, dict):
                            continue

                        api_native_id = str(row.get("id", "")).strip()
                        number = str(row.get("number", "")).strip()
                        if not api_native_id and not number:
                            continue
                        max_seen_modifytime = _update_max_seen_modifytime(max_seen_modifytime, row)
                        if api_native_id:
                            current_page_ids.append(api_native_id)

                        row_id = api_native_id or number
                        tax_rate_data = {
                            "id": row_id,
                            "number": number or row_id,
                            "name": str(row.get("name", "") or ""),
                            "enable": str(row.get("enable", "") or ""),
                            "enable_title": str(row.get("enable_title", "") or ""),
                            "status": str(row.get("status", "") or ""),
                            "source_created_time": str(row.get("createtime", "") or ""),
                            "source_modified_time": str(row.get("modifytime", "") or ""),
                            "raw_data": json.dumps(row, ensure_ascii=False),
                        }
                        unique_rows[row_id] = tax_rate_data

                    unique_list = list(unique_rows.values())
                    unique_count = len(unique_list)
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.TaxRate,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=100,
                    )
                    _log_page_write_summary(
                        "Tax rates",
                        page_no=page_no,
                        fetched_rows=len(rows),
                        unique_rows=unique_count,
                        upserted_rows=count,
                    )
                    total_synced += count

                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Tax rates pagination repeated, breaking.")
                        break
                    prev_page_ids = current_page_ids

                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True

                if last_page:
                    break

                page_no += 1

            _save_incremental_sync_watermark(
                db_session,
                sync_name="Tax rate",
                module_key=TAX_RATE_LAST_SYNC_AT_KEY,
                description="Last successful tax rate sync watermark by modifytime",
                manual_time_filter=manual_time_filter,
                max_seen_modifytime=max_seen_modifytime,
                query_end_time=query_end_time,
                full_sync=force_full_sync,
            )
            logger.info(f"Finished. Total synced tax rates: {total_synced}")
            _save_tax_rate_sync_status(
                db_session,
                {
                    "status": "success",
                    "message": f"税率同步完成，共写入 {total_synced} 条记录",
                    "started_at": started_at,
                    "finished_at": datetime.utcnow().replace(microsecond=0).isoformat(),
                    "total_synced": total_synced,
                    "api_id": int(api_record.id),
                },
            )
            _save_sync_module_run_status(
                db_session,
                module_key=TAX_RATE_LAST_SYNC_AT_KEY,
                description="税率档案同步状态",
                status="success",
                message=f"税率同步完成，共写入 {total_synced} 条记录",
                started_at=datetime.fromisoformat(started_at),
                finished_at=datetime.now(),
            )

        except Exception as e:
            logger.error(f"Sync tax rates failed: {e}")
            try:
                _save_tax_rate_sync_status(
                    db_session,
                    {
                        "status": "failed",
                        "message": f"税率同步失败: {str(e)}",
                        "started_at": started_at,
                        "finished_at": datetime.utcnow().replace(microsecond=0).isoformat(),
                        "total_synced": 0,
                        "api_id": resolved_api_id,
                    },
                )
                _save_sync_module_run_status(
                    db_session,
                    module_key=TAX_RATE_LAST_SYNC_AT_KEY,
                    description="税率档案同步状态",
                    status="failed",
                    message=f"税率同步失败: {str(e)}",
                    started_at=datetime.fromisoformat(started_at),
                    finished_at=datetime.now(),
                )
            except Exception:
                pass
        finally:
            db_session.close()

    background_tasks.add_task(run_sync, user_ctx)
    return {
        "message": "Tax rate sync started",
        "api_id": _resolve_external_api_id(request_api_id, "KINGDEE_TAX_RATE_API_ID"),
    }

# ===================== Kingdee House Management =====================

@router.get("/api/finance/kd-houses", response_model=schemas.PaginatedKingdeeHouseResponse)
def get_kd_houses(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.KingdeeHouse)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.KingdeeHouse.number.ilike(search_filter)) |
            (models.KingdeeHouse.wtw8_number.ilike(search_filter)) |
            (models.KingdeeHouse.name.ilike(search_filter))
        )
    total = query.count()
    kd_houses = query.order_by(models.KingdeeHouse.wtw8_number).offset(skip).limit(limit).all()
    return {"items": kd_houses, "total": total}

@router.post("/api/finance/kd-houses/sync")
def sync_kd_houses(
    request: schemas.KingdeeHouseSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync kingdee houses using configured API"""
    from database import SessionLocal
    request_api_id = request.api_id
    force_full_sync = bool(request.full_sync)
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        started_at = datetime.now().replace(microsecond=0)
        _save_sync_module_run_status(
            db_session,
            module_key=KINGDEE_HOUSE_LAST_SYNC_AT_KEY,
            description="房号档案同步状态",
            status="running",
            message="房号档案同步执行中",
            started_at=started_at,
        )

        def mark_failed(message: str) -> None:
            _save_sync_module_run_status(
                db_session,
                module_key=KINGDEE_HOUSE_LAST_SYNC_AT_KEY,
                description="房号档案同步状态",
                status="failed",
                message=message,
                started_at=started_at,
                finished_at=datetime.now(),
            )
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record, _ = _load_external_api_by_id(
                db_session,
                request_api_id=request_api_id,
                env_key="KINGDEE_HOUSE_API_ID",
                sync_label="kd_houses",
            )
            if not api_record:
                mark_failed("未找到房号档案同步接口配置")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                mark_failed(f"房号档案同步服务不存在: {service_id}")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 mark_failed(f"房号档案同步认证失败: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            query_end_time, last_sync_at, manual_time_filter = _prepare_incremental_sync_context(
                db_session,
                body=body,
                sync_name="Kingdee house",
                module_key=KINGDEE_HOUSE_LAST_SYNC_AT_KEY,
                force_full_sync=force_full_sync,
                page_size_default=1000,
            )
             
            page_no = 1
            total_synced = 0
            prev_page_ids = []
            max_seen_modifytime = last_sync_at
             
            while True:
                body["pageNo"] = page_no
                logger.info(
                    "Syncing kd_houses page %s with pageSize=%s, modifytime=%s, start_createtime=%s, end_createtime=%s...",
                    page_no,
                    body.get("pageSize"),
                    body["data"].get("modifytime"),
                    body["data"].get("start_createtime"),
                    body["data"].get("end_createtime"),
                )
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {}
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                        else:
                            continue
                        max_seen_modifytime = _update_max_seen_modifytime(max_seen_modifytime, row)
                             
                        # Handle "Original Code" mapping
                        # Use number if available, fall back to wtw8_number
                        number = str(row.get("number", ""))
                        wtw8_num = str(row.get("wtw8_number", ""))
                        if not number:
                            number = wtw8_num
                            
                        kdhouse_data = {
                            "id": api_native_id,
                            "number": number,
                            "wtw8_number": wtw8_num,
                            "name": row.get("name", ""),
                            "tzqslx": str(row.get("wtw8_combofield_tzqslx", "")),
                            "splx": str(row.get("wtw8_combofield_splx", "")),
                            "createorg_name": str(row.get("createorg_name", "")),
                            "createorg_number": str(row.get("createorg_number", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[api_native_id] = kdhouse_data
                    
                    unique_list = list(unique_rows.values())
                    unique_count = len(unique_list)
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.KingdeeHouse,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
                    )
                    _log_page_write_summary(
                        "Kingdee houses",
                        page_no=page_no,
                        fetched_rows=len(rows),
                        unique_rows=unique_count,
                        upserted_rows=count,
                    )
                             
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            _save_incremental_sync_watermark(
                db_session,
                sync_name="Kingdee house",
                module_key=KINGDEE_HOUSE_LAST_SYNC_AT_KEY,
                description="Last successful Kingdee house sync watermark by modifytime",
                manual_time_filter=manual_time_filter,
                max_seen_modifytime=max_seen_modifytime,
                query_end_time=query_end_time,
                full_sync=force_full_sync,
            )
            logger.info(f"Finished. Total synced kd_houses: {total_synced}")
            _save_sync_module_run_status(
                db_session,
                module_key=KINGDEE_HOUSE_LAST_SYNC_AT_KEY,
                description="房号档案同步状态",
                status="success",
                message=f"房号档案同步完成，共写入 {total_synced} 条记录",
                started_at=started_at,
                finished_at=datetime.now(),
            )
                    
        except Exception as e:
             logger.error(f"Sync kd_houses failed: {e}")
             mark_failed(f"房号档案同步失败: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Kingdee House sync started"}

# ===================== Account Book Management =====================

@router.get("/api/finance/kd-account-books", response_model=schemas.PaginatedKingdeeAccountBookResponse)
def get_kd_account_books(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.KingdeeAccountBook)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.KingdeeAccountBook.number.ilike(search_filter)) |
            (models.KingdeeAccountBook.name.ilike(search_filter))
        )
    total = query.count()
    items = query.order_by(models.KingdeeAccountBook.number).offset(skip).limit(limit).all()
    return {"items": items, "total": total}

@router.post("/api/finance/kd-account-books/sync")
def sync_kd_account_books(
    request: schemas.KingdeeAccountBookSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync kingdee account books using configured API"""
    from database import SessionLocal
    request_api_id = request.api_id
    force_full_sync = bool(request.full_sync)
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        started_at = datetime.now().replace(microsecond=0)
        _save_sync_module_run_status(
            db_session,
            module_key=KINGDEE_ACCOUNT_BOOK_LAST_SYNC_AT_KEY,
            description="账簿档案同步状态",
            status="running",
            message="账簿档案同步执行中",
            started_at=started_at,
        )

        def mark_failed(message: str) -> None:
            _save_sync_module_run_status(
                db_session,
                module_key=KINGDEE_ACCOUNT_BOOK_LAST_SYNC_AT_KEY,
                description="账簿档案同步状态",
                status="failed",
                message=message,
                started_at=started_at,
                finished_at=datetime.now(),
            )
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record, _ = _load_external_api_by_id(
                db_session,
                request_api_id=request_api_id,
                env_key="KINGDEE_ACCOUNT_BOOK_API_ID",
                sync_label="kd_account_books",
            )
            if not api_record:
                mark_failed("未找到账簿档案同步接口配置")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                mark_failed(f"账簿档案同步服务不存在: {service_id}")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 mark_failed(f"账簿档案同步认证失败: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            query_end_time, last_sync_at, manual_time_filter = _prepare_incremental_sync_context(
                db_session,
                body=body,
                sync_name="Kingdee account book",
                module_key=KINGDEE_ACCOUNT_BOOK_LAST_SYNC_AT_KEY,
                force_full_sync=force_full_sync,
                page_size_default=1000,
            )
             
            page_no = 1
            total_synced = 0
            prev_page_ids = []
            max_seen_modifytime = last_sync_at
             
            while True:
                body["pageNo"] = page_no
                logger.info(
                    "Syncing kd_account_books page %s with pageSize=%s, modifytime=%s, start_createtime=%s, end_createtime=%s...",
                    page_no,
                    body.get("pageSize"),
                    body["data"].get("modifytime"),
                    body["data"].get("start_createtime"),
                    body["data"].get("end_createtime"),
                )
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {}
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                        else:
                            continue
                        max_seen_modifytime = _update_max_seen_modifytime(max_seen_modifytime, row)
                             
                        number = str(row.get("number", ""))
                            
                        account_book_data = {
                            "id": api_native_id,
                            "number": number,
                            "name": row.get("name", ""),
                            "org_number": str(row.get("org_number", "")),
                            "org_name": str(row.get("org_name", "")),
                            "accountingsys_number": str(row.get("accountingsys_number", "")),
                            "accountingsys_name": str(row.get("accountingsys_name", "")),
                            "booknature": str(row.get("booknature", "")),
                            "accounttable_name": str(row.get("accounttable_name", "")),
                            "basecurrency_name": str(row.get("basecurrency_name", "")),
                            "status": str(row.get("status", "")),
                            "enable": str(row.get("enable", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[api_native_id] = account_book_data
                    
                    unique_list = list(unique_rows.values())
                    unique_count = len(unique_list)
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.KingdeeAccountBook,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
                    )
                    _log_page_write_summary(
                        "Kingdee account books",
                        page_no=page_no,
                        fetched_rows=len(rows),
                        unique_rows=unique_count,
                        upserted_rows=count,
                    )
                             
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            _save_incremental_sync_watermark(
                db_session,
                sync_name="Kingdee account book",
                module_key=KINGDEE_ACCOUNT_BOOK_LAST_SYNC_AT_KEY,
                description="Last successful Kingdee account book sync watermark by modifytime",
                manual_time_filter=manual_time_filter,
                max_seen_modifytime=max_seen_modifytime,
                query_end_time=query_end_time,
                full_sync=force_full_sync,
            )
            logger.info(f"Finished. Total synced kd_account_books: {total_synced}")
            _save_sync_module_run_status(
                db_session,
                module_key=KINGDEE_ACCOUNT_BOOK_LAST_SYNC_AT_KEY,
                description="账簿档案同步状态",
                status="success",
                message=f"账簿档案同步完成，共写入 {total_synced} 条记录",
                started_at=started_at,
                finished_at=datetime.now(),
            )
                    
        except Exception as e:
             logger.error(f"Sync kd_account_books failed: {e}")
             mark_failed(f"账簿档案同步失败: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Kingdee Account Book sync started"}

# ===================== Auxiliary Data Management =====================

@router.get("/api/finance/auxiliary-data", response_model=schemas.PaginatedAuxiliaryDataResponse)
def get_auxiliary_data(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    categories: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.AuxiliaryData)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.AuxiliaryData.number.ilike(search_filter)) |
            (models.AuxiliaryData.name.ilike(search_filter)) |
            (models.AuxiliaryData.group_name.ilike(search_filter))
        )
    if categories:
        cat_list = [c.strip() for c in categories.split(",") if c.strip()]
        if cat_list:
            query = query.filter(
                (models.AuxiliaryData.group_number.in_(cat_list)) | 
                (models.AuxiliaryData.group_name.in_(cat_list))
            )
    total = query.count()
    items = query.order_by(models.AuxiliaryData.number).offset(skip).limit(limit).all()
    return {"items": items, "total": total}

@router.post("/api/finance/auxiliary-data/sync")
def sync_auxiliary_data(
    request: schemas.AuxiliaryDataSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync auxiliary data using configured API"""
    from database import SessionLocal
    request_api_id = request.api_id
    force_full_sync = bool(request.full_sync)
    
    category_numbers = request.categories or []

    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        started_at = datetime.now().replace(microsecond=0)
        _save_sync_module_run_status(
            db_session,
            module_key=AUXILIARY_DATA_LAST_SYNC_AT_KEY,
            description="辅助资料同步状态",
            status="running",
            message="辅助资料同步执行中",
            started_at=started_at,
        )

        def mark_failed(message: str) -> None:
            _save_sync_module_run_status(
                db_session,
                module_key=AUXILIARY_DATA_LAST_SYNC_AT_KEY,
                description="辅助资料同步状态",
                status="failed",
                message=message,
                started_at=started_at,
                finished_at=datetime.now(),
            )
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record, _ = _load_external_api_by_id(
                db_session,
                request_api_id=request_api_id,
                env_key="KINGDEE_AUXILIARY_DATA_API_ID",
                sync_label="auxiliary_data",
            )
            if not api_record:
                mark_failed("未找到辅助资料同步接口配置")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                mark_failed(f"辅助资料同步服务不存在: {service_id}")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 mark_failed(f"辅助资料同步认证失败: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            # Remove any specific group_number filter applied in template if we want to sync all or specific categories
            if "group_number" in body["data"]:
                del body["data"]["group_number"]
                
            local_category_numbers = list(category_numbers)
            if not local_category_numbers:
                # If no categories specified, fetch all from DB
                all_cats = db_session.query(models.AuxiliaryDataCategory.number).all()
                local_category_numbers = [c[0] for c in all_cats]
                
            if not local_category_numbers:
                logger.error("No categories available to sync. Please sync auxiliary data categories first.")
                return

            cat_str = "','".join(local_category_numbers)
            body["data"]["filter"] = f"group_number in ('{cat_str}')"
            query_end_time, last_sync_at, manual_time_filter = _prepare_incremental_sync_context(
                db_session,
                body=body,
                sync_name="Auxiliary data",
                module_key=AUXILIARY_DATA_LAST_SYNC_AT_KEY,
                force_full_sync=force_full_sync,
                page_size_default=1000,
            )
            
            page_no = 1
            total_synced = 0
            prev_page_ids = []
            max_seen_modifytime = last_sync_at
            
            while True:
                body["pageNo"] = page_no
                logger.info(
                    "Syncing auxiliary data page %s with pageSize=%s, modifytime=%s, start_createtime=%s, end_createtime=%s, filter=%s...",
                    page_no,
                    body.get("pageSize"),
                    body["data"].get("modifytime"),
                    body["data"].get("start_createtime"),
                    body["data"].get("end_createtime"),
                    body["data"].get("filter"),
                )
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    count = 0
                    current_page_ids = []
                    unique_rows = {}
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                        max_seen_modifytime = _update_max_seen_modifytime(max_seen_modifytime, row)
                             
                        number = str(row.get("number", ""))
                        if not number: continue
                        
                        group_number = str(row.get("group_number", ""))
                        if local_category_numbers and group_number not in local_category_numbers:
                            continue
                        
                        aux_data = {
                            "id": api_native_id or number,
                            "number": number,
                            "name": row.get("name", ""),
                            "issyspreset": bool(row.get("issyspreset")),
                            "ctrlstrategy": str(row.get("ctrlstrategy", "")),
                            "enable": str(row.get("enable", "")),
                            "group_number": str(row.get("group_number", "")),
                            "group_name": str(row.get("group_name", "")),
                            "parent_number": str(row.get("parent_number", "")),
                            "parent_name": str(row.get("parent_name", "")),
                            "createorg_number": str(row.get("createorg_number", "")),
                            "createorg_name":  str(row.get("createorg_name", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[api_native_id or number] = aux_data
                    
                    unique_list = list(unique_rows.values())
                    unique_count = len(unique_list)
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.AuxiliaryData,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
                    )
                    _log_page_write_summary(
                        "Auxiliary data",
                        page_no=page_no,
                        fetched_rows=len(rows),
                        unique_rows=unique_count,
                        upserted_rows=count,
                    )
                             
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            _save_incremental_sync_watermark(
                db_session,
                sync_name="Auxiliary data",
                module_key=AUXILIARY_DATA_LAST_SYNC_AT_KEY,
                description="Last successful auxiliary data sync watermark by modifytime",
                manual_time_filter=manual_time_filter,
                max_seen_modifytime=max_seen_modifytime,
                query_end_time=query_end_time,
                full_sync=force_full_sync,
            )
            logger.info(f"Finished. Total synced auxiliary_data: {total_synced}")
            _save_sync_module_run_status(
                db_session,
                module_key=AUXILIARY_DATA_LAST_SYNC_AT_KEY,
                description="辅助资料同步状态",
                status="success",
                message=f"辅助资料同步完成，共写入 {total_synced} 条记录",
                started_at=started_at,
                finished_at=datetime.now(),
            )
                    
        except Exception as e:
             logger.error(f"Sync auxiliary data failed: {e}")
             mark_failed(f"辅助资料同步失败: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Auxiliary Data sync started"}

# ===================== Auxiliary Data Category Management =====================

@router.get("/api/finance/auxiliary-data-categories", response_model=schemas.PaginatedAuxiliaryDataCategoryResponse)
def get_auxiliary_data_categories(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.AuxiliaryDataCategory)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.AuxiliaryDataCategory.number.ilike(search_filter)) |
            (models.AuxiliaryDataCategory.name.ilike(search_filter))
        )
    total = query.count()
    items = query.order_by(models.AuxiliaryDataCategory.number).offset(skip).limit(limit).all()
    return {"items": items, "total": total}

@router.post("/api/finance/auxiliary-data-categories/sync")
def sync_auxiliary_data_categories(
    request: schemas.AuxiliaryDataCategorySyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync auxiliary data categories using configured API"""
    from database import SessionLocal
    request_api_id = request.api_id
    force_full_sync = bool(request.full_sync)
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        started_at = datetime.now().replace(microsecond=0)
        _save_sync_module_run_status(
            db_session,
            module_key=AUXILIARY_DATA_CATEGORY_LAST_SYNC_AT_KEY,
            description="辅助资料分类同步状态",
            status="running",
            message="辅助资料分类同步执行中",
            started_at=started_at,
        )

        def mark_failed(message: str) -> None:
            _save_sync_module_run_status(
                db_session,
                module_key=AUXILIARY_DATA_CATEGORY_LAST_SYNC_AT_KEY,
                description="辅助资料分类同步状态",
                status="failed",
                message=message,
                started_at=started_at,
                finished_at=datetime.now(),
            )
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record, _ = _load_external_api_by_id(
                db_session,
                request_api_id=request_api_id,
                env_key="KINGDEE_AUXILIARY_DATA_CATEGORY_API_ID",
                sync_label="auxiliary_data_categories",
            )
            if not api_record:
                mark_failed("未找到辅助资料分类同步接口配置")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                mark_failed(f"辅助资料分类同步服务不存在: {service_id}")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 mark_failed(f"辅助资料分类同步认证失败: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            query_end_time, last_sync_at, manual_time_filter = _prepare_incremental_sync_context(
                db_session,
                body=body,
                sync_name="Auxiliary data category",
                module_key=AUXILIARY_DATA_CATEGORY_LAST_SYNC_AT_KEY,
                force_full_sync=force_full_sync,
                page_size_default=1000,
            )
             
            page_no = 1
            total_synced = 0
            prev_page_ids = []
            max_seen_modifytime = last_sync_at
             
            while True:
                body["pageNo"] = page_no
                logger.info(
                    "Syncing auxiliary data categories page %s with pageSize=%s, modifytime=%s, start_createtime=%s, end_createtime=%s...",
                    page_no,
                    body.get("pageSize"),
                    body["data"].get("modifytime"),
                    body["data"].get("start_createtime"),
                    body["data"].get("end_createtime"),
                )
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    count = 0
                    current_page_ids = []
                    unique_rows = {}
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                        max_seen_modifytime = _update_max_seen_modifytime(max_seen_modifytime, row)
                             
                        number = str(row.get("number", ""))
                        if not number: continue
                        
                        cat_data = {
                            "id": api_native_id or number,
                            "number": number,
                            "name": row.get("name") or "",
                            "fissyspreset": bool(row.get("fissyspreset") or row.get("issyspreset")),
                            "description": str(row.get("description", "")),
                            "ctrlstrategy": str(row.get("ctrlstrategy", "")),
                            "createorg_name": str(row.get("createorg_name", "")),
                            "createorg_number": str(row.get("createorg_number", "")),
                            "createorg_id": str(row.get("createorg_id", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[api_native_id or number] = cat_data
                    
                    unique_list = list(unique_rows.values())
                    unique_count = len(unique_list)
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.AuxiliaryDataCategory,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=100,
                    )
                    _log_page_write_summary(
                        "Auxiliary data categories",
                        page_no=page_no,
                        fetched_rows=len(rows),
                        unique_rows=unique_count,
                        upserted_rows=count,
                    )
                             
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            _save_incremental_sync_watermark(
                db_session,
                sync_name="Auxiliary data category",
                module_key=AUXILIARY_DATA_CATEGORY_LAST_SYNC_AT_KEY,
                description="Last successful auxiliary data category sync watermark by modifytime",
                manual_time_filter=manual_time_filter,
                max_seen_modifytime=max_seen_modifytime,
                query_end_time=query_end_time,
                full_sync=force_full_sync,
            )
            logger.info(f"Finished. Total synced auxiliary_data_categories: {total_synced}")
            _save_sync_module_run_status(
                db_session,
                module_key=AUXILIARY_DATA_CATEGORY_LAST_SYNC_AT_KEY,
                description="辅助资料分类同步状态",
                status="success",
                message=f"辅助资料分类同步完成，共写入 {total_synced} 条记录",
                started_at=started_at,
                finished_at=datetime.now(),
            )
                    
        except Exception as e:
             logger.error(f"Sync auxiliary data categories failed: {e}")
             mark_failed(f"辅助资料分类同步失败: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Auxiliary Data Category sync started"}

# ===================== Bank Account Management =====================

@router.get("/api/finance/kd-bank-accounts", response_model=schemas.PaginatedKingdeeBankAccountResponse)
def get_kd_bank_accounts(
    skip: int = 0, 
    limit: int = 100, 
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    query = db.query(models.KingdeeBankAccount)
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (models.KingdeeBankAccount.bankaccountnumber.ilike(search_filter)) |
            (models.KingdeeBankAccount.name.ilike(search_filter)) |
            (models.KingdeeBankAccount.acctname.ilike(search_filter)) |
            (models.KingdeeBankAccount.bank_name.ilike(search_filter))
        )
    total = query.count()
    items = query.order_by(models.KingdeeBankAccount.bankaccountnumber).offset(skip).limit(limit).all()
    return {"items": items, "total": total}

@router.post("/api/finance/kd-bank-accounts/sync")
def sync_kd_bank_accounts(
    request: schemas.KingdeeBankAccountSyncRequest, 
    background_tasks: BackgroundTasks, 
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync Kingdee bank accounts using configured API"""
    from database import SessionLocal
    request_api_id = request.api_id
    force_full_sync = bool(request.full_sync)
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        started_at = datetime.now().replace(microsecond=0)
        _save_sync_module_run_status(
            db_session,
            module_key=KINGDEE_BANK_ACCOUNT_LAST_SYNC_AT_KEY,
            description="银行账户同步状态",
            status="running",
            message="银行账户同步执行中",
            started_at=started_at,
        )

        def mark_failed(message: str) -> None:
            _save_sync_module_run_status(
                db_session,
                module_key=KINGDEE_BANK_ACCOUNT_LAST_SYNC_AT_KEY,
                description="银行账户同步状态",
                status="failed",
                message=message,
                started_at=started_at,
                finished_at=datetime.now(),
            )
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record, _ = _load_external_api_by_id(
                db_session,
                request_api_id=request_api_id,
                env_key="KINGDEE_BANK_ACCOUNT_API_ID",
                sync_label="bank_accounts",
            )
            if not api_record:
                mark_failed("未找到银行账户同步接口配置")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                mark_failed(f"银行账户同步服务不存在: {service_id}")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
                 mark_failed(f"银行账户同步认证失败: {e}")
                 return
                 
            # 3. Prepare URL & Headers
            full_url = api_record.url_path
            if not full_url.startswith("http"):
                full_url = (service.base_url or "") + full_url
            
            # 瑙ｆ瀽 URL 鍙橀噺
            url = resolve_variables(full_url or '', db_session, user_context=user_context)
            
            # 瑙ｆ瀽 Headers 鍙橀噺
            user_headers = api_record.request_headers or {}
            if isinstance(user_headers, str):
                try: user_headers = json.loads(user_headers)
                except: user_headers = {}
            user_headers = resolve_dict_variables(user_headers, db_session, user_context=user_context)
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
            
            # 瑙ｆ瀽 Body 鍙橀噺
            body = resolve_dict_variables(body, db_session, user_context=user_context)
                 
            query_end_time, last_sync_at, manual_time_filter = _prepare_incremental_sync_context(
                db_session,
                body=body,
                sync_name="Kingdee bank account",
                module_key=KINGDEE_BANK_ACCOUNT_LAST_SYNC_AT_KEY,
                force_full_sync=force_full_sync,
                page_size_default=1000,
            )
             
            page_no = 1
            total_synced = 0
            prev_page_ids = []
            max_seen_modifytime = last_sync_at
             
            while True:
                body["pageNo"] = page_no
                logger.info(
                    "Syncing kd_bank_accounts page %s with pageSize=%s, modifytime=%s, start_createtime=%s, end_createtime=%s...",
                    page_no,
                    body.get("pageSize"),
                    body["data"].get("modifytime"),
                    body["data"].get("start_createtime"),
                    body["data"].get("end_createtime"),
                )
                
                success = False
                data = {}
                
                for attempt in range(2):
                    headers = auth.get_auth_headers()
                    # 浣跨敤澶栭儴宸茶В鏋愬ソ鐨?user_headers
                    
                    if isinstance(user_headers, dict):
                        for k, v in user_headers.items():
                            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                                v = v.replace("{access_token}", service.access_token)
                            headers[k] = str(v)

                    try:
                        resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
                        auth_failed = False
                        if resp.status_code == 401:
                            auth_failed = True
                        else:
                            try:
                                resp_json = resp.json()
                                if isinstance(resp_json, dict) and str(resp_json.get("errorCode")) == "401":
                                    auth_failed = True
                            except:
                                pass

                        if auth_failed:
                            if attempt == 0:
                                auth.invalidate_token()
                                service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
                                auth.service_record = service
                                continue
                            else:
                                if resp.status_code != 200: resp.raise_for_status()
                                else: raise Exception(f"Auth Error: {resp.text}")

                        resp.raise_for_status()
                        try: data = resp.json()
                        except: data = {}
                        
                        success = True
                        break

                    except Exception as e:
                        if attempt == 1: raise e
                        if not auth_failed: raise e
                
                if not success:
                    break
                    
                rows = []
                last_page = True
                
                if "data" in data and isinstance(data["data"], dict):
                    if "rows" in data["data"]:
                        rows = data["data"]["rows"]
                    if "lastPage" in data["data"]:
                        last_page = str(data["data"]["lastPage"]).lower() == 'true'
                elif "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                elif "rows" in data:
                    rows = data["rows"]
                    if "lastPage" in data:
                        last_page = str(data["lastPage"]).lower() == 'true'
                elif isinstance(data, list):
                    rows = data
                    
                if isinstance(rows, list) and rows:
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {}
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                        else:
                            continue
                        max_seen_modifytime = _update_max_seen_modifytime(max_seen_modifytime, row)
                             
                        bank_account_data = {
                            "id": api_native_id,
                            "bankaccountnumber": str(row.get("bankaccountnumber", "")),
                            "name": str(row.get("name", "")),
                            "acctname": str(row.get("acctname", "")),
                            "company_number": str(row.get("company_number", "")),
                            "company_name": str(row.get("company_name", "")),
                            "openorg_number": str(row.get("openorg_number", "")),
                            "openorg_name": str(row.get("openorg_name", "")),
                            "defaultcurrency_number": str(row.get("defaultcurrency_number", "")),
                            "defaultcurrency_name": str(row.get("defaultcurrency_name", "")),
                            "accttype": str(row.get("accttype", "")),
                            "acctstyle": str(row.get("acctstyle", "")),
                            "finorgtype": str(row.get("finorgtype", "")),
                            "banktype_number": str(row.get("banktype_number", "")),
                            "banktype_name": str(row.get("banktype_name", "")),
                            "bank_number": str(row.get("bank_number", "")),
                            "bank_name": str(row.get("bank_name", "")),
                            "acctproperty_number": str(row.get("acctproperty_number", "")),
                            "acctproperty_name": str(row.get("acctproperty_name", "")),
                            "status": str(row.get("status", "")),
                            "acctstatus": str(row.get("acctstatus", "")),
                            "isdefaultrec": bool(row.get("isdefaultrec", False)),
                            "isdefaultpay": bool(row.get("isdefaultpay", False)),
                            "comment": str(row.get("comment", "")),
                            "raw_data": json.dumps(row)
                        }
                        unique_rows[api_native_id] = bank_account_data
                    
                    unique_list = list(unique_rows.values())
                    unique_count = len(unique_list)
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.KingdeeBankAccount,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
                    )
                    _log_page_write_summary(
                        "Kingdee bank accounts",
                        page_no=page_no,
                        fetched_rows=len(rows),
                        unique_rows=unique_count,
                        upserted_rows=count,
                    )
                             
                    total_synced += count
                    
                    if len(current_page_ids) > 0 and current_page_ids == prev_page_ids:
                        logger.info("Kingdee pagination out of bounds (repeated data), breaking loop.")
                        break
                    prev_page_ids = current_page_ids
                    
                    if len(rows) < body["pageSize"]:
                        last_page = True
                else:
                    last_page = True
                    
                if last_page:
                    break
                
                page_no += 1

            _save_incremental_sync_watermark(
                db_session,
                sync_name="Kingdee bank account",
                module_key=KINGDEE_BANK_ACCOUNT_LAST_SYNC_AT_KEY,
                description="Last successful Kingdee bank account sync watermark by modifytime",
                manual_time_filter=manual_time_filter,
                max_seen_modifytime=max_seen_modifytime,
                query_end_time=query_end_time,
                full_sync=force_full_sync,
            )
            logger.info(f"Finished. Total synced kd_bank_accounts: {total_synced}")
            _save_sync_module_run_status(
                db_session,
                module_key=KINGDEE_BANK_ACCOUNT_LAST_SYNC_AT_KEY,
                description="银行账户同步状态",
                status="success",
                message=f"银行账户同步完成，共写入 {total_synced} 条记录",
                started_at=started_at,
                finished_at=datetime.now(),
            )
                    
        except Exception as e:
             logger.error(f"Sync bank accounts failed: {e}")
             mark_failed(f"银行账户同步失败: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Bank account sync started"}

