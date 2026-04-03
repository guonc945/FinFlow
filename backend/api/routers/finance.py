import logging
from importlib import import_module
from typing import Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.orm import Session

import models
import schemas
from api.bootstrap import _upsert_rows
from api.dependencies import get_db, get_user_context
from utils.variable_parser import resolve_dict_variables, resolve_variables

router = APIRouter()
logger = logging.getLogger("project_sync")
TAX_RATE_SYNC_STATUS_KEY = "TAX_RATE_SYNC_STATUS"


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
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config (Try ExternalApi first, then fallback to GlobalVariable)
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            config = None
            service_id = None
            
            # Try to find the migrated API record
            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name == "会计科目同步接口").first()
            if api_record:
                config = {
                    "method": api_record.method,
                    "url": api_record.url_path,
                    "request_headers": api_record.request_headers,
                    "request_body": api_record.request_body,
                    "service_id": api_record.service_id
                }
                service_id = api_record.service_id
                logger.info("Using configuration from ExternalApi: 会计科目同步接口")
            else:
                # Fallback to legacy global variable
                config_var = db_session.query(models.GlobalVariable).filter(models.GlobalVariable.key == "ACCOUNTING_SUBJECT_CONFIG").first()
                if config_var:
                    config = json.loads(config_var.value)
                    service_id = config.get("service_id")
                    logger.info("Using legacy configuration from global_variables: ACCOUNTING_SUBJECT_CONFIG")
                else:
                    logger.error("No configuration found for accounting subjects (ExternalApi or GlobalVariable)")
                    return
                 
            if not service_id:
                 logger.error("Configuration missing service_id")
                 return
                 
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                 logger.error(f"Service with ID {service_id} not found")
                 return
                 
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            # Initial token fetch attempt
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
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

            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size

            # Incremental sync without truncating existing data
            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing accounting subjects page {page_no}...")
                
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
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.AccountingSubject,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
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

            logger.info(f"Finished. Total synced accounting subjects: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync failed outer: {e}")
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
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name == "查询金蝶云星空客户").first()
            if not api_record:
                logger.error("No configuration found for customers: '查询金蝶云星空客户' external API not found.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
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
                 
            # Ensure pagination parameters are present for Kingdee
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            # Clear table before syncing
            try:
                db_session.query(models.Customer).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing customers page {page_no}...")
                
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
                    
                if isinstance(rows, list) and rows:
                    
                    count = 0
                    current_page_ids = []
                    unique_rows = {} # 閸?Python 娓氀勭壌閹?number 閸樺鍣?
                    
                    for row in rows:
                        if not isinstance(row, dict): continue
                        
                        api_native_id = str(row.get("id"))
                        if api_native_id:
                            current_page_ids.append(api_native_id)
                            
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
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.Customer,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
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

            logger.info(f"Finished. Total synced customers: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync customers failed: {e}")
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
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(
                models.ExternalApi.name.in_(["查询供应商", "查询金蝶云星空供应商"])
            ).first()
            if not api_record:
                logger.error("No configuration found for suppliers: '查询供应商' external API not found.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
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
                 
            # Ensure pagination parameters are present for Kingdee
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            # Clear table before syncing
            try:
                db_session.query(models.Supplier).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing suppliers page {page_no}...")
                
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
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.Supplier,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
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

            logger.info(f"Finished. Total synced suppliers: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync suppliers failed: {e}")
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

    var = db.query(models.GlobalVariable).filter(models.GlobalVariable.key == TAX_RATE_SYNC_STATUS_KEY).first()
    if not var or not var.value:
        return {
            "status": "idle",
            "message": "尚未执行税率同步",
            "started_at": None,
            "finished_at": None,
            "total_synced": 0,
            "api_id": 32,
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
        "api_id": 32,
    }

@router.post("/api/finance/tax-rates/sync")
def sync_tax_rates(
    request: schemas.TaxRateSyncRequest,
    background_tasks: BackgroundTasks,
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db)
):
    """Sync tax rates using the configured Kingdee API (ExternalApi ID 32)."""
    from database import SessionLocal

    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        from datetime import datetime

        started_at = datetime.utcnow().replace(microsecond=0).isoformat()
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
                    "api_id": 32,
                },
            )

            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.id == 32).first()
            if not api_record:
                api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name == "获取税率").first()
            if not api_record:
                logger.error("No configuration found for tax rates: ExternalApi id=32 / name=获取税率 not found.")
                _save_tax_rate_sync_status(
                    db_session,
                    {
                        "status": "failed",
                        "message": "未找到 ID=32 或名称为“获取税率”的接口配置",
                        "started_at": started_at,
                        "finished_at": datetime.utcnow().replace(microsecond=0).isoformat(),
                        "total_synced": 0,
                        "api_id": 32,
                    },
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
                        "api_id": 32,
                    },
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
                        "api_id": 32,
                    },
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

            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}

            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 1000
            else:
                body["pageSize"] = page_size

            try:
                db_session.query(models.TaxRate).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear tax_rates table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []

            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing tax rates page {page_no}...")

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
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.TaxRate,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=100,
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
                        "api_id": 32,
                    },
                )
            except Exception:
                pass
        finally:
            db_session.close()

    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Tax rate sync started", "api_id": 32}

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
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name == "查询金蝶系统房号信息").first()
            if not api_record:
                logger.error("No configuration found for kd_houses: '查询金蝶系统房号信息' external API not found.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
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
                 
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            try:
                db_session.query(models.KingdeeHouse).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing kd_houses page {page_no}...")
                
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
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.KingdeeHouse,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
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

            logger.info(f"Finished. Total synced kd_houses: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync kd_houses failed: {e}")
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
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(
                models.ExternalApi.name.in_(["账簿列表查询", "查询金蝶系统账簿信息"])
            ).first()
            if not api_record:
                logger.error("No configuration found for kd_account_books: '账簿列表查询' external API not found.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
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
                 
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            try:
                db_session.query(models.KingdeeAccountBook).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing kd_account_books page {page_no}...")
                
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
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.KingdeeAccountBook,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
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

            logger.info(f"Finished. Total synced kd_account_books: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync kd_account_books failed: {e}")
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
    
    category_numbers = request.categories or []

    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name.in_(["辅助资料查询", "查询金蝶辅助资料"])).first()
            if not api_record:
                api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.url_path.like("%bos_assistantdata_detail/getList")).first()

            if not api_record:
                logger.error("No configuration found for auxiliary data: API configuration missing.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
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
            
            body_template = api_record.request_body
            body = {}
            try:
                if isinstance(body_template, str):
                    body = json.loads(body_template) if body_template else {}
                else:
                    body = body_template
            except:
                 body = {}
                 
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
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            try:
                if category_numbers:
                    db_session.query(models.AuxiliaryData).filter(models.AuxiliaryData.group_number.in_(category_numbers)).delete(synchronize_session=False)
                else:
                    db_session.query(models.AuxiliaryData).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing auxiliary data page {page_no}...")
                
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
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.AuxiliaryData,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
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

            logger.info(f"Finished. Total synced auxiliary_data: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync auxiliary data failed: {e}")
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
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name == "辅助资料分类").first()
            if not api_record:
                api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.url_path.like("%assistantdata/getList")).first()

            if not api_record:
                logger.error("No configuration found for auxiliary data categories: API configuration missing.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
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
                 
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 1000
            else:
                body["pageSize"] = page_size
            
            try:
                db_session.query(models.AuxiliaryDataCategory).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing auxiliary data categories page {page_no}...")
                
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
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.AuxiliaryDataCategory,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=100,
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

            logger.info(f"Finished. Total synced auxiliary_data_categories: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync auxiliary data categories failed: {e}")
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
    
    def run_sync(user_context: Dict[str, str]):
        db_session = SessionLocal()
        try:
            # 1. Get Config
            import json
            import requests
            from services.external_auth import ExternalAuthService
            
            api_record = db_session.query(models.ExternalApi).filter(
                models.ExternalApi.name.in_(["银行账户查询", "查询金蝶银行账号"])
            ).first()
            if not api_record:
                api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.url_path.like("%cas_bankaccount/getList")).first()

            if not api_record:
                logger.error("No configuration found for bank accounts: API configuration missing.")
                return
                
            service_id = api_record.service_id
            service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
            if not service:
                logger.error(f"Service with ID {service_id} not found")
                return
                
            # 2. Authenticate
            auth = ExternalAuthService(db=db_session, service_record=service, user_context=user_context)
            try:
                 auth.get_token() 
            except Exception as e:
                 logger.error(f"Auth failed: {e}")
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
                 
            if not body:
                body = {"data": {}}
            if "data" not in body:
                body["data"] = {}
                
            page_size = body.get("pageSize")
            if not page_size or not isinstance(page_size, int) or page_size <= 0:
                body["pageSize"] = 100
            else:
                body["pageSize"] = page_size
            
            # 閸氬本閸撳秵绔荤粚楦裤€?
            try:
                db_session.query(models.KingdeeBankAccount).delete()
                db_session.commit()
            except Exception as e:
                logger.warning(f"Failed to clear table: {e}")
                db_session.rollback()

            page_no = 1
            total_synced = 0
            prev_page_ids = []
            
            while True:
                body["pageNo"] = page_no
                logger.info(f"Syncing kd_bank_accounts page {page_no}...")
                
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
                    count += _upsert_rows(
                        db_session=db_session,
                        model=models.KingdeeBankAccount,
                        rows=unique_list,
                        conflict_fields=["id"],
                        immutable_fields={"id", "created_at"},
                        batch_size=50,
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

            logger.info(f"Finished. Total synced kd_bank_accounts: {total_synced}")
                    
        except Exception as e:
             logger.error(f"Sync bank accounts failed: {e}")
        finally:
            db_session.close()
            
    background_tasks.add_task(run_sync, user_ctx)
    return {"message": "Bank account sync started"}

