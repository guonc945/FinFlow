# -*- coding: utf-8 -*-
import csv
import io
from datetime import datetime
from importlib import import_module
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import String, cast, func, or_, text
from sqlalchemy.orm import Session

import models
import schemas
from api.bootstrap import _is_mssql
from api.dependencies import get_allowed_community_ids, get_current_user, get_db, get_user_context
from fetch_bills import sync_bills
from sync_tracker import tracker
from utils.variable_parser import resolve_dict_variables, resolve_variables

router = APIRouter()


def _main_attr(name: str):
    return getattr(import_module("main"), name)


def _reset_bill_voucher_binding_impl(*args, **kwargs):
    return _main_attr("_reset_bill_voucher_binding_impl")(*args, **kwargs)


def _decode_header_value(*args, **kwargs):
    return _main_attr("_decode_header_value")(*args, **kwargs)


def _get_bill_push_status_map(*args, **kwargs):
    return _main_attr("_get_bill_push_status_map")(*args, **kwargs)


def _build_bill_push_status_entry(*args, **kwargs):
    return _main_attr("_build_bill_push_status_entry")(*args, **kwargs)

@router.post("/api/bills/voucher/reset")
def reset_bill_voucher_binding(
    payload: schemas.BillVoucherResetRequest,
    x_account_book_id: Optional[str] = Header(None, alias="X-Account-Book-Id"),
    x_account_book_name: Optional[str] = Header(None, alias="X-Account-Book-Name"),
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    return _reset_bill_voucher_binding_impl(
        payload,
        x_account_book_id,
        x_account_book_name,
        x_account_book_number,
        current_user,
        db,
        allowed_community_ids,
    )


@router.post("/api/vouchers/query")
def query_voucher_by_id(
    payload: schemas.VoucherQueryRequest,
    user_ctx: Dict[str, str] = Depends(get_user_context),
    db: Session = Depends(get_db),
):
    """Query Kingdee voucher by internal id using ExternalApi named 凭证查询."""
    import requests
    import json as json_mod
    from services.external_auth import ExternalAuthService

    api_record = db.query(models.ExternalApi).filter(
        models.ExternalApi.name == "凭证查询",
        models.ExternalApi.is_active == True
    ).first()
    if not api_record:
        api_record = db.query(models.ExternalApi).filter(
            models.ExternalApi.url_path.ilike("%voucherQuery%"),
            models.ExternalApi.is_active == True
        ).first()
    if not api_record:
        raise HTTPException(status_code=404, detail="ExternalApi not found: 凭证查询")

    service = db.query(models.ExternalService).filter(models.ExternalService.id == api_record.service_id).first()
    if not service:
        raise HTTPException(status_code=404, detail="External service not found for 凭证查询")

    auth = ExternalAuthService(db=db, service_record=service, user_context=user_ctx)
    try:
        auth.get_token()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Auth failed: {exc}")

    full_url = api_record.url_path or ""
    if not full_url.startswith("http"):
        full_url = (service.base_url or "") + full_url
    url = resolve_variables(full_url or "", db, user_context=user_ctx)

    user_headers = api_record.request_headers or {}
    if isinstance(user_headers, str):
        try:
            user_headers = json_mod.loads(user_headers)
        except Exception:
            user_headers = {}
    user_headers = resolve_dict_variables(user_headers, db, user_context=user_ctx)

    headers = auth.get_auth_headers()
    for k, v in user_headers.items():
        if isinstance(v, str) and "{access_token}" in v and service.access_token:
            v = v.replace("{access_token}", service.access_token)
        headers[k] = str(v)

    body_template = api_record.request_body
    body: Dict[str, Any] = {}
    if body_template:
        if isinstance(body_template, str):
            try:
                body = json_mod.loads(body_template)
            except Exception:
                body = {}
        elif isinstance(body_template, dict):
            body = dict(body_template)
    body = resolve_dict_variables(body, db, user_context=user_ctx)

    if not body:
        body = {"data": {}}
    if "data" not in body or not isinstance(body["data"], dict):
        body["data"] = {}

    body["data"]["id"] = payload.voucher_id
    body["pageNo"] = payload.page_no or 1
    body["pageSize"] = payload.page_size or 10

    resp = requests.request(api_record.method or "POST", url, headers=headers, json=body)
    try:
        resp_data = resp.json()
    except Exception:
        resp_data = {"raw": resp.text}

    exists = False
    if isinstance(resp_data, dict):
        data_obj = resp_data.get("data")
        if isinstance(data_obj, dict):
            rows = data_obj.get("rows")
            if isinstance(rows, list) and len(rows) > 0:
                exists = True

    return {
        "success": bool(resp.ok),
        "status_code": resp.status_code,
        "voucher_id": payload.voucher_id,
        "exists": exists,
        "response": resp_data,
    }

@router.get("/api/bills")
def get_bills(
    search: Optional[str] = None, 
    community_ids: Optional[str] = None,
    status: Optional[str] = None,
    charge_items: Optional[str] = None,
    customer_name: Optional[str] = None,
    bill_id: Optional[str] = None,
    receipt_id: Optional[str] = None,
    house_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    in_month_start: Optional[str] = None,
    in_month_end: Optional[str] = None,
    pay_date_start: Optional[str] = None,
    pay_date_end: Optional[str] = None,
    pay_time_start: Optional[str] = None,
    pay_time_end: Optional[str] = None,
    deal_log_id: Optional[int] = None,
    skip: int = 0, 
    limit: int = 25, 
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    from sqlalchemy import func as sa_func
    from datetime import datetime
    
    # 瀛愭煡璇細鑱氬悎姣忎釜璐﹀崟鐨勫鎴峰悕绉?
    if _is_mssql():
        customer_subq = (
            db.query(
                models.BillUser.bill_id,
                models.BillUser.community_id,
                sa_func.max(models.BillUser.user_name).label('customer_name')
            )
            .group_by(models.BillUser.bill_id, models.BillUser.community_id)
            .subquery()
        )
    else:
        customer_subq = (
            db.query(
                models.BillUser.bill_id,
                models.BillUser.community_id,
                sa_func.string_agg(models.BillUser.user_name, ', ').label('customer_name')
            )
            .group_by(models.BillUser.bill_id, models.BillUser.community_id)
            .subquery()
        )
    
    query = db.query(
        models.Bill,
        models.ProjectList.proj_name,
        customer_subq.c.customer_name
    ).outerjoin(
        models.ProjectList, models.Bill.community_id == models.ProjectList.proj_id
    ).outerjoin(
        customer_subq,
        (models.Bill.id == customer_subq.c.bill_id) & 
        (models.Bill.community_id == customer_subq.c.community_id)
    )

    # 寮哄埗璐︾翱闅旂
    if allowed_community_ids:
        query = query.filter(models.Bill.community_id.in_(allowed_community_ids))
    else:
        return {"total": 0, "total_amount": 0.00, "items": []}

    # 缁村害绛涢€?
    if community_ids:
        # 鏀寔閫楀彿鍒嗛殧鐨勫涓洯鍖篒D
        try:
            ids = [int(cid.strip()) for cid in community_ids.split(",") if cid.strip()]
            if ids:
                query = query.filter(models.Bill.community_id.in_(ids))
        except ValueError:
            pass
            
    if customer_name:
        query = query.filter(customer_subq.c.customer_name.ilike(f"%{customer_name}%"))
    
    if status and status != '全部状态':
        query = query.filter(models.Bill.pay_status_str == status)
        
    if charge_items:
        c_items = [ci.strip() for ci in charge_items.split(",") if ci.strip()]
        if c_items:
            condition_list = []
            just_names = []
            for item in c_items:
                if '|' in item:
                    try:
                        pid, name = item.split('|', 1)
                        condition_list.append((models.Bill.community_id == int(pid)) & (models.Bill.charge_item_name == name))
                    except ValueError:
                        just_names.append(item)
                else:
                    just_names.append(item)
            
            # Combine all conditions
            all_conditions = list(condition_list)
            if just_names:
                all_conditions.append(models.Bill.charge_item_name.in_(just_names))
                
            if all_conditions:
                query = query.filter(or_(*all_conditions))
        
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            query = query.filter(models.Bill.created_at >= start_dt)
        except ValueError:
            pass
            
    if end_date:
        try:
            end_dt = datetime.strptime(f"{end_date} 23:59:59", '%Y-%m-%d %H:%M:%S')
            query = query.filter(models.Bill.created_at <= end_dt)
        except ValueError:
            pass

    if in_month_start:
        query = query.filter(models.Bill.in_month >= in_month_start)
    if in_month_end:
        query = query.filter(models.Bill.in_month <= in_month_end)

    if pay_date_start:
        try:
            date_start = datetime.strptime(pay_date_start, '%Y-%m-%d').date()
            query = query.filter(models.Bill.receive_date >= date_start)
        except ValueError:
            pass

    if pay_date_end:
        try:
            date_end = datetime.strptime(pay_date_end, '%Y-%m-%d').date()
            query = query.filter(models.Bill.receive_date <= date_end)
        except ValueError:
            pass

    if pay_time_start:
        try:
            pt_start = int(datetime.strptime(f"{pay_time_start} 00:00:00", '%Y-%m-%d %H:%M:%S').timestamp())
            query = query.filter(models.Bill.pay_time >= pt_start)
        except ValueError:
            pass

    if pay_time_end:
        try:
            pt_end = int(datetime.strptime(f"{pay_time_end} 23:59:59", '%Y-%m-%d %H:%M:%S').timestamp())
            query = query.filter(models.Bill.pay_time <= pt_end)
        except ValueError:
            pass

    if deal_log_id is not None:
        try:
            query = query.filter(models.Bill.deal_log_id == int(deal_log_id))
        except Exception:
            pass

    if bill_id:
        try:
            query = query.filter(models.Bill.id == int(bill_id))
        except Exception:
            pass

    if receipt_id:
        query = query.filter(models.Bill.receipt_id.ilike(f"%{receipt_id}%"))

    if house_name:
        like = f"%{house_name}%"
        query = query.filter(or_(
            models.Bill.full_house_name.ilike(like),
            models.Bill.bind_house_name.ilike(like),
            models.Bill.asset_name.ilike(like)
        ))

    if search:
        keyword = search.strip()
        if keyword:
            like = f"%{keyword}%"
            search_conditions = [
                models.Bill.receipt_id.ilike(like),
                models.Bill.full_house_name.ilike(like),
                models.Bill.bind_house_name.ilike(like),
                models.Bill.asset_name.ilike(like),
                customer_subq.c.customer_name.ilike(like),
            ]

            if keyword.isdigit():
                numeric_value = int(keyword)
                search_conditions.extend([
                    models.Bill.id == numeric_value,
                    models.Bill.deal_log_id == numeric_value,
                ])
            else:
                from sqlalchemy import cast, String as SAString

                search_conditions.extend([
                    cast(models.Bill.id, SAString).ilike(like),
                    cast(models.Bill.deal_log_id, SAString).ilike(like),
                ])

            query = query.filter(or_(*search_conditions))
    
    total = query.count()
    total_amount = query.with_entities(sa_func.sum(models.Bill.amount)).scalar()
    
    results = query.order_by(models.Bill.created_at.desc()).offset(skip).limit(limit).all()
    status_map = _get_bill_push_status_map(
        db,
        [{"bill_id": bill.id, "community_id": bill.community_id} for bill, _, _ in results],
        account_book_number=_decode_header_value(x_account_book_number) or None,
    )
    
    return {
        "total": total,
        "total_amount": float(total_amount) if total_amount else 0.00,
        "items": [{
            "id": bill.id,
            "community_id": bill.community_id,
            "community_name": proj_name or f"椤圭洰{bill.community_id}",
            "charge_item_name": bill.charge_item_name,
            "asset_name": bill.asset_name,
            "full_house_name": bill.full_house_name,
            "in_month": bill.in_month,
            "amount": float(bill.amount) if bill.amount else 0,
            "pay_status_str": bill.pay_status_str,
            "pay_time": bill.pay_time,
            "receive_date": bill.receive_date,
            "deal_log_id": bill.deal_log_id,
            "created_at": bill.created_at,
            "customer_name": customer_name or "",
            **status_map.get(
                (int(bill.id), int(bill.community_id)),
                _build_bill_push_status_entry(int(bill.id), int(bill.community_id)),
            ),
        } for bill, proj_name, customer_name in results]
    }


@router.get("/api/bills/export")
def export_bills(
    search: Optional[str] = None,
    community_ids: Optional[str] = None,
    status: Optional[str] = None,
    charge_items: Optional[str] = None,
    customer_name: Optional[str] = None,
    bill_id: Optional[str] = None,
    receipt_id: Optional[str] = None,
    house_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    in_month_start: Optional[str] = None,
    in_month_end: Optional[str] = None,
    pay_date_start: Optional[str] = None,
    pay_date_end: Optional[str] = None,
    pay_time_start: Optional[str] = None,
    pay_time_end: Optional[str] = None,
    deal_log_id: Optional[int] = None,
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    if _is_mssql():
        customer_subq = (
            db.query(
                models.BillUser.bill_id,
                models.BillUser.community_id,
                func.max(models.BillUser.user_name).label('customer_name')
            )
            .group_by(models.BillUser.bill_id, models.BillUser.community_id)
            .subquery()
        )
    else:
        customer_subq = (
            db.query(
                models.BillUser.bill_id,
                models.BillUser.community_id,
                func.string_agg(models.BillUser.user_name, ', ').label('customer_name')
            )
            .group_by(models.BillUser.bill_id, models.BillUser.community_id)
            .subquery()
        )

    query = db.query(
        models.Bill,
        models.ProjectList.proj_name,
        customer_subq.c.customer_name
    ).outerjoin(
        models.ProjectList, models.Bill.community_id == models.ProjectList.proj_id
    ).outerjoin(
        customer_subq,
        (models.Bill.id == customer_subq.c.bill_id) &
        (models.Bill.community_id == customer_subq.c.community_id)
    )

    if allowed_community_ids:
        query = query.filter(models.Bill.community_id.in_(allowed_community_ids))
    else:
        query = query.filter(text("1=0"))

    if community_ids:
        try:
            ids = [int(cid.strip()) for cid in community_ids.split(",") if cid.strip()]
            if ids:
                query = query.filter(models.Bill.community_id.in_(ids))
        except ValueError:
            pass

    if customer_name:
        query = query.filter(customer_subq.c.customer_name.ilike(f"%{customer_name}%"))

    if status and status != "全部状态":
        query = query.filter(models.Bill.pay_status_str == status)

    if charge_items:
        c_items = [ci.strip() for ci in charge_items.split(",") if ci.strip()]
        if c_items:
            condition_list = []
            just_names = []
            for item in c_items:
                if '|' in item:
                    try:
                        pid, name = item.split('|', 1)
                        condition_list.append((models.Bill.community_id == int(pid)) & (models.Bill.charge_item_name == name))
                    except ValueError:
                        just_names.append(item)
                else:
                    just_names.append(item)

            all_conditions = list(condition_list)
            if just_names:
                all_conditions.append(models.Bill.charge_item_name.in_(just_names))

            if all_conditions:
                query = query.filter(or_(*all_conditions))

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            query = query.filter(models.Bill.created_at >= start_dt)
        except ValueError:
            pass

    if end_date:
        try:
            end_dt = datetime.strptime(f"{end_date} 23:59:59", '%Y-%m-%d %H:%M:%S')
            query = query.filter(models.Bill.created_at <= end_dt)
        except ValueError:
            pass

    if in_month_start:
        query = query.filter(models.Bill.in_month >= in_month_start)
    if in_month_end:
        query = query.filter(models.Bill.in_month <= in_month_end)

    if pay_date_start:
        try:
            date_start = datetime.strptime(pay_date_start, '%Y-%m-%d').date()
            query = query.filter(models.Bill.receive_date >= date_start)
        except ValueError:
            pass

    if pay_date_end:
        try:
            date_end = datetime.strptime(pay_date_end, '%Y-%m-%d').date()
            query = query.filter(models.Bill.receive_date <= date_end)
        except ValueError:
            pass

    if pay_time_start:
        try:
            pt_start = int(datetime.strptime(f"{pay_time_start} 00:00:00", '%Y-%m-%d %H:%M:%S').timestamp())
            query = query.filter(models.Bill.pay_time >= pt_start)
        except ValueError:
            pass

    if pay_time_end:
        try:
            pt_end = int(datetime.strptime(f"{pay_time_end} 23:59:59", '%Y-%m-%d %H:%M:%S').timestamp())
            query = query.filter(models.Bill.pay_time <= pt_end)
        except ValueError:
            pass

    if deal_log_id is not None:
        try:
            query = query.filter(models.Bill.deal_log_id == int(deal_log_id))
        except Exception:
            pass

    if bill_id:
        try:
            query = query.filter(models.Bill.id == int(bill_id))
        except Exception:
            pass

    if receipt_id:
        query = query.filter(models.Bill.receipt_id.ilike(f"%{receipt_id}%"))

    if house_name:
        like = f"%{house_name}%"
        query = query.filter(or_(
            models.Bill.full_house_name.ilike(like),
            models.Bill.bind_house_name.ilike(like),
            models.Bill.asset_name.ilike(like)
        ))

    if search:
        keyword = search.strip()
        if keyword:
            like = f"%{keyword}%"
            search_conditions = [
                models.Bill.receipt_id.ilike(like),
                models.Bill.full_house_name.ilike(like),
                models.Bill.bind_house_name.ilike(like),
                models.Bill.asset_name.ilike(like),
                customer_subq.c.customer_name.ilike(like),
            ]

            if keyword.isdigit():
                numeric_value = int(keyword)
                search_conditions.extend([
                    models.Bill.id == numeric_value,
                    models.Bill.deal_log_id == numeric_value,
                ])
            else:
                search_conditions.extend([
                    cast(models.Bill.id, String).ilike(like),
                    cast(models.Bill.deal_log_id, String).ilike(like),
                ])

            query = query.filter(or_(*search_conditions))

    results = query.order_by(models.Bill.created_at.desc()).all()
    status_map = _get_bill_push_status_map(
        db,
        [{"bill_id": bill.id, "community_id": bill.community_id} for bill, _, _ in results],
        account_book_number=_decode_header_value(x_account_book_number) or None,
    )

    def _format_dt(value):
        if not value:
            return ""
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    def _format_timestamp(value):
        if value in (None, ""):
            return ""
        try:
            return datetime.fromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(value)

    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output)
    writer.writerow([
        "账单ID",
        "缴费ID",
        "收据ID",
        "园区",
        "房产名称",
        "房号",
        "客户名称",
        "收费项目",
        "所属月份",
        "收款金额",
        "收费状态",
        "支付日期",
        "支付时间",
        "创建时间",
        "推送状态",
        "凭证号",
    ])

    for bill, proj_name, customer_name_value in results:
        push_status = status_map.get(
            (int(bill.id), int(bill.community_id)),
            _build_bill_push_status_entry(int(bill.id), int(bill.community_id)),
        )
        writer.writerow([
            bill.id,
            bill.deal_log_id or "",
            bill.receipt_id or "",
            proj_name or bill.community_id,
            bill.asset_name or "",
            bill.full_house_name or "",
            customer_name_value or "",
            bill.charge_item_name or "",
            bill.in_month or "",
            float(bill.amount) if bill.amount else 0,
            bill.pay_status_str or "",
            bill.receive_date or "",
            _format_timestamp(bill.pay_time),
            _format_dt(bill.created_at),
            push_status.get("push_status_label", ""),
            push_status.get("voucher_number", "") or "",
        ])

    filename = f"bills_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    response = StreamingResponse(iter([output.getvalue()]), media_type="text/csv; charset=utf-8")
    response.headers["Content-Disposition"] = (
        f"attachment; filename={filename}; filename*=UTF-8''{quote(filename)}"
    )
    return response

@router.post("/api/bills/sync")
def sync_bills_endpoint(
    background_tasks: BackgroundTasks, 
    request: schemas.BillSyncRequest = None,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    """Sync bill data for the specified communities."""
    if request and request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids
        
    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")
        
    str_ids = [str(cid) for cid in community_ids]
    
    # Create a tracking task
    task_id = tracker.create_task(str_ids)
    
    background_tasks.add_task(sync_bills, str_ids, task_id)
    
    return {
        "message": "Bill synchronization started",
        "task_id": task_id,
        "community_ids": str_ids
    }

@router.get("/api/bills/sync/status/{task_id}")
def get_sync_status(task_id: str):
    """Get the current status of a sync task.
    """
    status = tracker.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")
    return status

@router.get("/api/bills/charge-items")
def get_bill_charge_items(
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids)
):
    """Get all distinct charge items from existing projects and charge items mapping."""
    if not allowed_community_ids:
        return []

    account_book_number = _decode_header_value(x_account_book_number) if x_account_book_number else None
    if not account_book_number:
        return []

    book_community_rows = (
        db.query(models.ProjectList.proj_id)
        .join(
            models.KingdeeAccountBook,
            cast(models.ProjectList.kingdee_account_book_id, String) == cast(models.KingdeeAccountBook.id, String),
        )
        .filter(models.KingdeeAccountBook.number == account_book_number)
        .all()
    )
    if not book_community_rows:
        return []

    allowed_set = {int(cid) for cid in allowed_community_ids}
    book_set = {int(row[0]) for row in book_community_rows}
    scoped_community_ids = list(allowed_set & book_set)
    if not scoped_community_ids:
        return []
        
    query = db.query(
        models.ChargeItem.item_name,
        models.ProjectList.proj_name,
        models.ProjectList.proj_id
    ).join(
        models.ProjectList, models.ChargeItem.communityid == cast(models.ProjectList.proj_id, String)
    ).filter(
        models.ProjectList.proj_id.in_(scoped_community_ids)
    )
    
    items = query.all()
    
    unique_items = []
    seen = set()
    for item in items:
        key = f"{item.proj_id}|{item.item_name}"
        if key not in seen and item.item_name:
            seen.add(key)
            proj_name = item.proj_name or f"鍥尯{item.proj_id}"
            unique_items.append({
                "value": key,
                "label": f"{item.item_name} + {proj_name}"
            })
            
    return unique_items

@router.get("/api/bills/{bill_id}")
def get_bill(
    bill_id: str,
    x_account_book_number: Optional[str] = Header(None, alias="X-Account-Book-Number"),
    db: Session = Depends(get_db)
):
    bill = db.query(models.Bill).filter(models.Bill.id == bill_id).first()
    if not bill:
        raise HTTPException(status_code=404, detail="Bill not found")

    push_status = _get_bill_push_status_map(
        db,
        [{"bill_id": bill.id, "community_id": bill.community_id}],
        account_book_number=_decode_header_value(x_account_book_number) or None,
    ).get(
        (int(bill.id), int(bill.community_id)),
        _build_bill_push_status_entry(int(bill.id), int(bill.community_id)),
    )
    
    return {
        "id": bill.id,
        "community_id": bill.community_id,
        "charge_item_id": bill.charge_item_id,
        "charge_item_name": bill.charge_item_name,
        "category_name": bill.category_name,
        "asset_name": bill.asset_name,
        "full_house_name": bill.full_house_name,
        "start_time": bill.start_time,
        "end_time": bill.end_time,
        "pay_time": bill.pay_time,
        "create_time": bill.create_time,
        "amount": float(bill.amount) if bill.amount else 0,
        "bill_amount": float(bill.bill_amount) if bill.bill_amount else 0,
        "discount_amount": float(bill.discount_amount) if bill.discount_amount else 0,
        "late_money_amount": float(bill.late_money_amount) if bill.late_money_amount else 0,
        "deposit_amount": float(bill.deposit_amount) if bill.deposit_amount else 0,
        "pay_status": bill.pay_status,
        "pay_status_str": bill.pay_status_str,
        "bill_type_str": bill.bill_type_str,
        "pay_type_str": bill.pay_type_str,
        "in_month": bill.in_month,
        "remark": bill.remark,
        "receipt_id": bill.receipt_id,
        **push_status,
    }


# ===================== Receipt Bills (收款账单) =====================

