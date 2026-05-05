# -*- coding: utf-8 -*-
import json
import uuid
from datetime import datetime
from importlib import import_module
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from sqlalchemy import and_
from sqlalchemy.orm import Session

import models
import schemas
from utils.api_config import require_api_id


def _main_attr(name: str):
    return getattr(import_module("main"), name)


def push_voucher_to_kingdee(
    payload: schemas.VoucherPushRequest,
    x_account_book_id: Optional[str],
    x_account_book_name: Optional[str],
    x_account_book_number: Optional[str],
    current_user: models.User,
    db: Session,
    allowed_community_ids: List[int],
):
    """
    Push previewed Kingdee voucher JSON to configured external API.
    Default target: configured ExternalApi id.
    """
    import requests
    import time
    from services.external_auth import ExternalAuthService

    if not isinstance(payload.kingdee_json, dict) or not payload.kingdee_json:
        raise HTTPException(status_code=400, detail="kingdee_json is required")
    _main_attr("_validate_voucher_json_amounts")(payload.kingdee_json)

    api_record: Optional[models.ExternalApi] = None
    if payload.api_id is not None:
        api_record = db.query(models.ExternalApi).filter(
            models.ExternalApi.id == payload.api_id,
            models.ExternalApi.is_active == True
        ).first()
        if not api_record:
            raise HTTPException(status_code=404, detail=f"External API not found or inactive: id={payload.api_id}")
    else:
        try:
            default_api_id = require_api_id("KINGDEE_VOUCHER_PUSH_API_ID")
        except ValueError:
            raise HTTPException(
                status_code=500,
                detail="Invalid KINGDEE_VOUCHER_PUSH_API_ID. Expected integer external_apis.id.",
            )
        api_record = db.query(models.ExternalApi).filter(
            models.ExternalApi.id == default_api_id,
            models.ExternalApi.is_active == True,
        ).first()
        if not api_record:
            raise HTTPException(
                status_code=404,
                detail=f"External API not found or inactive: id={default_api_id}"
            )

    service = db.query(models.ExternalService).filter(models.ExternalService.id == api_record.service_id).first()
    if not service or not service.is_active:
        raise HTTPException(status_code=404, detail=f"External service not found or inactive: id={api_record.service_id}")

    account_book_id = _main_attr("_decode_header_value")(x_account_book_id) or None
    account_book_name = _main_attr("_decode_header_value")(x_account_book_name) or None
    account_book_number = _main_attr("_decode_header_value")(x_account_book_number) or None
    tracked_refs = _main_attr("_normalize_bill_refs")(payload.bills)
    push_batch_no = (
        f"VP{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:8].upper()}"
        if tracked_refs else None
    )
    request_payload_text = json.dumps(payload.kingdee_json, ensure_ascii=False)

    if tracked_refs:
        if not allowed_community_ids:
            raise HTTPException(status_code=403, detail="No authorized communities for this account book")

        allowed_set = set(allowed_community_ids)
        unauthorized = [
            ref for ref in tracked_refs
            if int(ref["community_id"]) not in allowed_set
        ]
        if unauthorized:
            preview = ", ".join([f"{ref['community_id']}:{ref['bill_id']}" for ref in unauthorized[:10]])
            raise HTTPException(status_code=403, detail=f"Unauthorized bill communities: {preview}")

        bill_conditions = [
            and_(
                models.Bill.id == ref["bill_id"],
                models.Bill.community_id == ref["community_id"],
            )
            for ref in tracked_refs
        ]
        locked_bills = db.query(models.Bill).filter(or_(*bill_conditions)).with_for_update().all()
        locked_keys = {(int(b.id), int(b.community_id)) for b in locked_bills}
        missing_refs = [
            ref for ref in tracked_refs
            if (int(ref["bill_id"]), int(ref["community_id"])) not in locked_keys
        ]
        if missing_refs:
            preview = ", ".join([f"{ref['community_id']}:{ref['bill_id']}" for ref in missing_refs[:10]])
            raise HTTPException(status_code=404, detail=f"Bills not found: {preview}")

        tracked_status_map = _main_attr("_get_bill_push_status_map")(
            db,
            tracked_refs,
            account_book_number=account_book_number,
        )
        tracked_statuses = [
            tracked_status_map[(ref["bill_id"], ref["community_id"])]
            for ref in tracked_refs
        ]
        conflicts = _main_attr("_find_bill_push_conflicts")(tracked_statuses)
        if conflicts and not payload.force_push:
            preview = ", ".join(
                [
                    f"{item['community_id']}:{item['bill_id']}({item['push_status_label']})"
                    for item in conflicts[:10]
                ]
            )
            raise HTTPException(status_code=409, detail=f"Selected bills already pushed or pushing: {preview}")

        for ref in tracked_refs:
            db.add(models.BillVoucherPushRecord(
                bill_id=ref["bill_id"],
                community_id=ref["community_id"],
                push_batch_no=push_batch_no,
                push_status="pushing",
                account_book_id=account_book_id,
                account_book_name=account_book_name,
                account_book_number=account_book_number,
                api_id=api_record.id,
                api_name=api_record.name,
                pushed_by=current_user.id,
                message="Push request submitted",
                request_payload=request_payload_text,
            ))
        db.commit()

    org_name = current_user.organization.name if current_user.organization else "未分配"
    user_context = {
        "current_user_id": str(current_user.id),
        "current_username": current_user.username,
        "current_user_realname": current_user.real_name or current_user.username,
        "current_org_id": str(current_user.org_id) if current_user.org_id else "",
        "current_org_name": org_name,
        "current_account_book_id": account_book_id or "",
        "current_account_book_name": account_book_name or "",
        "current_account_book_number": account_book_number or "",
    }

    auth = ExternalAuthService(db=db, service_record=service, user_context=user_context)
    token = auth.get_token()
    base_headers = auth.get_auth_headers()

    custom_headers: Dict[str, Any] = {}
    if api_record.request_headers:
        try:
            parsed_headers = json.loads(api_record.request_headers) if isinstance(api_record.request_headers, str) else api_record.request_headers
            if isinstance(parsed_headers, dict):
                custom_headers = _main_attr("resolve_dict_variables")(parsed_headers, db, user_context=user_context)
        except Exception:
            custom_headers = {}

    def _merge_headers(token_value: str) -> Dict[str, str]:
        merged = {k: str(v) for k, v in (base_headers or {}).items()}
        for k, v in (custom_headers or {}).items():
            val = str(v)
            if "{access_token}" in val:
                val = val.replace("{access_token}", token_value)
            merged[k] = val
        return merged

    headers = _merge_headers(token)
    method = (api_record.method or "POST").upper()
    raw_path = (api_record.url_path or "").strip()
    if not raw_path:
        raise HTTPException(status_code=400, detail=f"External API {api_record.id} url_path is empty")

    if raw_path.startswith("http://") or raw_path.startswith("https://"):
        full_url = raw_path
    else:
        base = (service.base_url or "").strip()
        if base and raw_path and not base.endswith("/") and not raw_path.startswith("/"):
            full_url = f"{base}/{raw_path}"
        else:
            full_url = f"{base}{raw_path}"

    if not full_url:
        raise HTTPException(status_code=400, detail="External API url is empty")

    request_started = time.time()

    for attempt in range(2):
        try:
            if attempt > 0:
                auth.invalidate_token()
                db.commit()
                token = auth.get_token()
                headers = _merge_headers(token)

            req_kwargs: Dict[str, Any] = {"headers": headers, "timeout": 30}
            content_type = next((v for k, v in headers.items() if k.lower() == "content-type"), "").lower()
            if method == "GET":
                req_kwargs["params"] = payload.kingdee_json
            elif "application/x-www-form-urlencoded" in content_type:
                req_kwargs["data"] = payload.kingdee_json
            else:
                req_kwargs["json"] = payload.kingdee_json

            resp = requests.request(method, full_url, **req_kwargs)
            try:
                resp_data = resp.json()
            except Exception:
                resp_data = {"raw": resp.text}

            auth_failed = resp.status_code in [401, 602]
            if not auth_failed and isinstance(resp_data, dict):
                err_code = str(resp_data.get("errorCode") or resp_data.get("code") or "").strip()
                if err_code in ["401", "602"]:
                    auth_failed = True

            if auth_failed and attempt == 0:
                continue

            success = bool(resp.ok)
            message = "Push successful" if success else "Push failed"
            if isinstance(resp_data, dict):
                status_flag = resp_data.get("status")
                if status_flag is False:
                    success = False
                error_code = str(resp_data.get("errorCode") or "").strip()
                if error_code not in ("", "0", "None", "null"):
                    success = False
                data_obj = resp_data.get("data")
                if isinstance(data_obj, dict):
                    fail_count = str(data_obj.get("failCount") or "").strip()
                    if fail_count not in ("", "0", "None", "null"):
                        success = False

            binding = _main_attr("_extract_kingdee_voucher_result")(resp_data)
            if binding.get("bill_status") is False:
                success = False

            message = _main_attr("_extract_kingdee_push_message")(resp_data, message)
            response_payload_text = json.dumps(resp_data, ensure_ascii=False) if isinstance(resp_data, (dict, list)) else str(resp_data)

            if tracked_refs and push_batch_no:
                _main_attr("_finalize_bill_push_records")(
                    db=db,
                    push_batch_no=push_batch_no,
                    push_status="success" if success else "failed",
                    message=message,
                    response_payload=response_payload_text,
                    voucher_number=binding.get("voucher_number"),
                    voucher_id=binding.get("voucher_id"),
                )
                tracked_status_map = _main_attr("_get_bill_push_status_map")(
                    db,
                    tracked_refs,
                    account_book_number=account_book_number,
                )
                tracked_statuses = [
                    tracked_status_map[(ref["bill_id"], ref["community_id"])]
                    for ref in tracked_refs
                ]
            else:
                tracked_statuses = []

            duration_ms = round((time.time() - request_started) * 1000, 2)
            return {
                "success": success,
                "message": message,
                "status_code": resp.status_code,
                "duration_ms": duration_ms,
                "api_id": api_record.id,
                "api_name": api_record.name,
                "api_url": full_url,
                "push_batch_no": push_batch_no,
                "voucher_number": binding.get("voucher_number"),
                "voucher_id": binding.get("voucher_id"),
                "tracked_bills": tracked_statuses,
                "response": resp_data,
            }
        except Exception as exc:
            if attempt == 1:
                if tracked_refs and push_batch_no:
                    _main_attr("_finalize_bill_push_records")(
                        db=db,
                        push_batch_no=push_batch_no,
                        push_status="failed",
                        message=str(exc),
                        response_payload=str(exc),
                    )
                raise HTTPException(status_code=502, detail=f"Push voucher request failed: {str(exc)}")

    raise HTTPException(status_code=502, detail="Push voucher request failed")
