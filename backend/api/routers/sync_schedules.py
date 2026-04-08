# -*- coding: utf-8 -*-
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload

import database
import models
import schemas
from fetch_bills import sync_bills
from fetch_charge_items import sync_charge_items
from fetch_deposit_records import sync_deposit_records
from fetch_houses import sync_houses
from fetch_parks import sync_parks
from fetch_prepayment_records import sync_prepayment_records
from fetch_receipt_bills import sync_receipt_bills
from fetch_residents import sync_residents
from scripts.fetch_projects import main as fetch_projects_main
from api.routers.finance import (
    sync_accounting_subjects as finance_sync_accounting_subjects,
    sync_auxiliary_data as finance_sync_auxiliary_data,
    sync_auxiliary_data_categories as finance_sync_auxiliary_data_categories,
    sync_customers as finance_sync_customers,
    sync_kd_account_books as finance_sync_kd_account_books,
    sync_kd_bank_accounts as finance_sync_kd_bank_accounts,
    sync_kd_houses as finance_sync_kd_houses,
    sync_suppliers as finance_sync_suppliers,
    sync_tax_rates as finance_sync_tax_rates,
)
from api.voucher_preview_handlers import (
    preview_voucher_for_receipt as preview_voucher_for_receipt_handler,
)
from api.voucher_push_handlers import (
    push_voucher_to_kingdee as push_voucher_to_kingdee_handler,
)
from services.sync_schedule_service import (
    DEFAULT_TIMEZONE,
    RECEIPT_BILL_REQUIRED_TARGET_CODES,
    SyncScheduleService,
    compute_next_run_at,
    normalize_sync_target_codes,
    normalize_weekdays,
    parse_json_list,
    serialize_json_list,
    utcnow_naive,
)
from sync_tracker import tracker
from api.dependencies import _require_api_permission, get_current_user, get_db

router = APIRouter()
sync_schedule_service = SyncScheduleService(database.SessionLocal, database.engine)

SYNC_TARGET_DEFINITIONS = [
    {"code": "projects", "label": "马克园区档案", "system": "mark", "requires_community_ids": False},
    {"code": "charge_items", "label": "马克收费项目", "system": "mark", "requires_community_ids": True},
    {"code": "houses", "label": "马克房屋档案", "system": "mark", "requires_community_ids": True},
    {"code": "residents", "label": "马克住户档案", "system": "mark", "requires_community_ids": True},
    {"code": "parks", "label": "马克车位档案", "system": "mark", "requires_community_ids": True},
    {"code": "bills", "label": "马克运营账单", "system": "mark", "requires_community_ids": True},
    {
        "code": "receipt_bills",
        "label": "马克收款单",
        "system": "mark",
        "requires_community_ids": True,
        "forced_with": list(RECEIPT_BILL_REQUIRED_TARGET_CODES),
    },
    {"code": "deposit_records", "label": "马克押金记录", "system": "mark", "requires_community_ids": True},
    {"code": "prepayment_records", "label": "马克预存款记录", "system": "mark", "requires_community_ids": True},
    {"code": "accounting_subjects", "label": "金蝶会计科目", "system": "kingdee", "requires_community_ids": False},
    {"code": "customers", "label": "金蝶客户", "system": "kingdee", "requires_community_ids": False},
    {"code": "suppliers", "label": "金蝶供应商", "system": "kingdee", "requires_community_ids": False},
    {"code": "tax_rates", "label": "金蝶税率档案", "system": "kingdee", "requires_community_ids": False},
    {"code": "kd_houses", "label": "金蝶房号", "system": "kingdee", "requires_community_ids": False},
    {"code": "account_books", "label": "金蝶账簿", "system": "kingdee", "requires_community_ids": False},
    {"code": "auxiliary_data_categories", "label": "金蝶辅助资料分类", "system": "kingdee", "requires_community_ids": False},
    {"code": "auxiliary_data", "label": "金蝶辅助资料", "system": "kingdee", "requires_community_ids": False},
    {"code": "bank_accounts", "label": "金蝶银行账户", "system": "kingdee", "requires_community_ids": False},
    {
        "code": "receipt_voucher_auto_push",
        "label": "运管收款单",
        "system": "kingdee",
        "requires_community_ids": False,
        "requires_account_book": True,
        "auto_resolve_communities": True,
    },
]

SYNC_TARGET_MAP = {item["code"]: item for item in SYNC_TARGET_DEFINITIONS}


def _normalize_sync_time_text(value: Optional[str]) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        hour_str, minute_str = text.split(":", 1)
        hour = max(0, min(23, int(hour_str)))
        minute = max(0, min(59, int(minute_str)))
        return f"{hour:02d}:{minute:02d}"
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid time format, expected HH:MM")


def _normalize_schedule_community_ids(values: Optional[List[Any]]) -> List[int]:
    normalized: List[int] = []
    seen = set()
    for value in values or []:
        try:
            cid = int(value)
        except (TypeError, ValueError):
            continue
        if cid in seen:
            continue
        seen.add(cid)
        normalized.append(cid)
    return normalized


def _validate_sync_schedule_payload(payload: schemas.SyncScheduleBase | schemas.SyncScheduleUpdate) -> Dict[str, Any]:
    target_codes = normalize_sync_target_codes(payload.target_codes, valid_codes=set(SYNC_TARGET_MAP))
    if not target_codes:
        raise HTTPException(status_code=400, detail="At least one valid sync target must be selected")

    schedule_type = str(payload.schedule_type or "").strip()
    if schedule_type not in {"interval", "daily", "weekly"}:
        raise HTTPException(status_code=400, detail="Unsupported schedule type")

    interval_minutes = payload.interval_minutes
    daily_time = _normalize_sync_time_text(payload.daily_time)
    weekly_days = normalize_weekdays(payload.weekly_days)

    if schedule_type == "interval" and not interval_minutes:
        raise HTTPException(status_code=400, detail="Interval schedule requires interval_minutes")
    if schedule_type == "daily" and not daily_time:
        raise HTTPException(status_code=400, detail="Daily schedule requires daily_time")
    if schedule_type == "weekly":
        if not daily_time:
            raise HTTPException(status_code=400, detail="Weekly schedule requires daily_time")
        if not weekly_days:
            raise HTTPException(status_code=400, detail="Weekly schedule requires weekly_days")

    community_ids = _normalize_schedule_community_ids(payload.community_ids)
    requires_communities = any(SYNC_TARGET_MAP[code]["requires_community_ids"] for code in target_codes)
    if requires_communities and not community_ids:
        raise HTTPException(status_code=400, detail="Selected Mark targets require at least one community")

    account_book_number = str(payload.account_book_number or "").strip() or None
    requires_account_book = any(bool(SYNC_TARGET_MAP[code].get("requires_account_book")) for code in target_codes)
    if requires_account_book and not account_book_number:
        raise HTTPException(status_code=400, detail="Selected targets require an account book")

    timezone_name = str(payload.timezone or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    name = str(payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Schedule name is required")

    return {
        "name": name,
        "description": str(payload.description or "").strip() or None,
        "target_codes": target_codes,
        "community_ids": community_ids,
        "account_book_number": account_book_number,
        "account_book_name": str(payload.account_book_name or "").strip() or None,
        "schedule_type": schedule_type,
        "interval_minutes": interval_minutes if schedule_type == "interval" else None,
        "daily_time": daily_time if schedule_type in {"daily", "weekly"} else None,
        "weekly_days": weekly_days if schedule_type == "weekly" else [],
        "timezone": timezone_name,
        "enabled": bool(payload.enabled),
    }


def _serialize_sync_schedule(schedule: models.SyncSchedule) -> Dict[str, Any]:
    creator_name = None
    updater_name = None
    if schedule.creator:
        creator_name = schedule.creator.real_name or schedule.creator.username
    if schedule.updater:
        updater_name = schedule.updater.real_name or schedule.updater.username

    return {
        "id": schedule.id,
        "name": schedule.name,
        "description": schedule.description,
        "target_codes": normalize_sync_target_codes(
            parse_json_list(schedule.target_codes),
            valid_codes=set(SYNC_TARGET_MAP),
        ),
        "community_ids": _normalize_schedule_community_ids(parse_json_list(schedule.community_ids)),
        "account_book_number": schedule.account_book_number,
        "account_book_name": schedule.account_book_name,
        "schedule_type": schedule.schedule_type,
        "interval_minutes": schedule.interval_minutes,
        "daily_time": schedule.daily_time,
        "weekly_days": normalize_weekdays(parse_json_list(schedule.weekly_days)),
        "timezone": schedule.timezone or DEFAULT_TIMEZONE,
        "enabled": bool(schedule.enabled),
        "is_running": bool(schedule.is_running),
        "current_execution_id": schedule.current_execution_id,
        "last_run_at": schedule.last_run_at,
        "last_status": schedule.last_status,
        "last_message": schedule.last_message,
        "next_run_at": schedule.next_run_at,
        "created_by": schedule.created_by,
        "updated_by": schedule.updated_by,
        "created_at": schedule.created_at,
        "updated_at": schedule.updated_at,
        "creator_name": creator_name,
        "updater_name": updater_name,
    }


def _serialize_sync_schedule_execution(execution: models.SyncScheduleExecution) -> Dict[str, Any]:
    result_payload = []
    if execution.result_payload:
        try:
            parsed_payload = json.loads(execution.result_payload)
            if isinstance(parsed_payload, list):
                result_payload = parsed_payload
        except (TypeError, ValueError):
            result_payload = []

    triggered_by_name = None
    if execution.triggered_by_user:
        triggered_by_name = execution.triggered_by_user.real_name or execution.triggered_by_user.username

    return {
        "id": execution.id,
        "schedule_id": execution.schedule_id,
        "trigger_type": execution.trigger_type,
        "triggered_by": execution.triggered_by,
        "triggered_by_name": triggered_by_name,
        "status": execution.status,
        "started_at": execution.started_at,
        "finished_at": execution.finished_at,
        "total_targets": execution.total_targets or 0,
        "success_targets": execution.success_targets or 0,
        "failed_targets": execution.failed_targets or 0,
        "summary": execution.summary,
        "error_message": execution.error_message,
        "result_payload": result_payload,
        "created_at": execution.created_at,
        "updated_at": execution.updated_at,
    }


def _resolve_schedule_community_ids(schedule_data: Dict[str, Any]) -> List[int]:
    community_ids = _normalize_schedule_community_ids(schedule_data.get("community_ids"))
    if community_ids:
        return community_ids

    db = database.SessionLocal()
    try:
        return [row[0] for row in db.query(models.ProjectList.proj_id).order_by(models.ProjectList.proj_id).all()]
    finally:
        db.close()


def _resolve_schedule_account_book(
    db: Session,
    schedule_data: Dict[str, Any],
) -> models.KingdeeAccountBook:
    account_book_number = str(schedule_data.get("account_book_number") or "").strip()
    if not account_book_number:
        raise RuntimeError("Schedule account book is not configured.")

    account_book = (
        db.query(models.KingdeeAccountBook)
        .filter(models.KingdeeAccountBook.number == account_book_number)
        .first()
    )
    if not account_book:
        raise RuntimeError(f"Account book not found: {account_book_number}")
    return account_book


def _resolve_account_book_community_ids(
    db: Session,
    account_book: models.KingdeeAccountBook,
) -> List[int]:
    rows = (
        db.query(models.ProjectList.proj_id)
        .filter(models.ProjectList.kingdee_account_book_id == account_book.id)
        .order_by(models.ProjectList.proj_id.asc())
        .all()
    )
    return [int(row[0]) for row in rows if row and row[0] is not None]


def _resolve_schedule_operator(
    db: Session,
    user_context: Dict[str, str],
) -> models.User:
    user_id = str(user_context.get("current_user_id") or "").strip()
    operator: Optional[models.User] = None
    if user_id.isdigit():
        operator = db.query(models.User).filter(models.User.id == int(user_id)).first()

    if operator is None:
        username = str(user_context.get("current_username") or "").strip()
        if username:
            operator = db.query(models.User).filter(models.User.username == username).first()

    if operator is None:
        raise RuntimeError("Schedule operator user not found.")
    return operator


def _resolve_schedule_run_date(schedule_data: Dict[str, Any]):
    timezone_name = str(schedule_data.get("timezone") or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        return datetime.now(ZoneInfo(timezone_name)).date()
    except Exception:
        return datetime.now().date()


def _build_tracker_result(task_id: str, fallback_message: str) -> Dict[str, Any]:
    task_status = tracker.get_task_status(task_id) or {}
    tracker_status = str(task_status.get("status") or "").strip().lower()
    if tracker_status in {"completed"}:
        status = "success"
    elif tracker_status in {"failed", "partially_completed"}:
        status = "failed"
    else:
        status = "success"
    logs = task_status.get("logs") if isinstance(task_status.get("logs"), list) else []
    message = task_status.get("status") or fallback_message
    return {
        "status": status,
        "message": str(message),
        "task_id": task_id,
        "logs": logs,
    }


def _run_finance_sync_task(
    endpoint_callable,
    request_model,
    user_context: Dict[str, str],
) -> Dict[str, Any]:
    db = database.SessionLocal()
    try:
        background_tasks = BackgroundTasks()
        response = endpoint_callable(request_model, background_tasks, user_context, db)
        message = response.get("message") or response.get("detail") or "Sync started"
        logs = [{"type": "info", "message": str(message)}]
        for task in getattr(background_tasks, "tasks", []):
            task.func(*task.args, **task.kwargs)
        return {"status": "success", "message": str(message), "logs": logs}
    except Exception as exc:
        return {
            "status": "failed",
            "message": str(exc),
            "logs": [{"type": "error", "message": str(exc)}],
        }
    finally:
        db.close()


def _handle_projects_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    fetch_projects_main()
    return {
        "status": "success",
        "message": "Project sync completed",
        "logs": [{"type": "info", "message": "Project sync completed"}],
    }


def _handle_charge_items_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    sync_charge_items(community_ids)
    return {
        "status": "success",
        "message": f"Charge item sync completed for {len(community_ids)} communities",
        "logs": [{"type": "info", "message": f"Processed {len(community_ids)} communities"}],
    }


def _handle_houses_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    task_id = tracker.create_task(community_ids)
    sync_houses(community_ids, task_id)
    return _build_tracker_result(task_id, "House sync completed")


def _handle_residents_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    task_id = tracker.create_task(community_ids)
    sync_residents(community_ids, task_id)
    return _build_tracker_result(task_id, "Resident sync completed")


def _handle_parks_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    task_id = tracker.create_task(community_ids)
    sync_parks(community_ids, task_id)
    return _build_tracker_result(task_id, "Park sync completed")


def _handle_bills_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    task_id = tracker.create_task(community_ids)
    sync_bills(community_ids, task_id)
    return _build_tracker_result(task_id, "Bill sync completed")


def _handle_receipt_bills_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = [str(cid) for cid in _resolve_schedule_community_ids(schedule_data)]
    task_id = tracker.create_task(community_ids)
    sync_receipt_bills(community_ids, task_id)
    return _build_tracker_result(task_id, "Receipt bill sync completed")


def _handle_deposit_records_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = _resolve_schedule_community_ids(schedule_data)
    task_id = tracker.create_task(community_ids)
    sync_deposit_records(community_ids, task_id)
    return _build_tracker_result(task_id, "Deposit record sync completed")


def _handle_prepayment_records_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    community_ids = _resolve_schedule_community_ids(schedule_data)
    task_id = tracker.create_task(community_ids)
    sync_prepayment_records(community_ids, task_id)
    return _build_tracker_result(task_id, "Prepayment record sync completed")


def _handle_accounting_subjects_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        finance_sync_accounting_subjects,
        schemas.AccountingSubjectSyncRequest(),
        user_context,
    )


def _handle_customers_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        finance_sync_customers,
        schemas.CustomerSyncRequest(),
        user_context,
    )


def _handle_suppliers_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        finance_sync_suppliers,
        schemas.SupplierSyncRequest(),
        user_context,
    )


def _handle_tax_rates_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        finance_sync_tax_rates,
        schemas.TaxRateSyncRequest(),
        user_context,
    )


def _handle_kd_houses_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        finance_sync_kd_houses,
        schemas.KingdeeHouseSyncRequest(),
        user_context,
    )


def _handle_account_books_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        finance_sync_kd_account_books,
        schemas.KingdeeAccountBookSyncRequest(),
        user_context,
    )


def _handle_auxiliary_data_categories_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        finance_sync_auxiliary_data_categories,
        schemas.AuxiliaryDataCategorySyncRequest(),
        user_context,
    )


def _handle_auxiliary_data_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        finance_sync_auxiliary_data,
        schemas.AuxiliaryDataSyncRequest(),
        user_context,
    )


def _handle_bank_accounts_sync(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    return _run_finance_sync_task(
        finance_sync_kd_bank_accounts,
        schemas.KingdeeBankAccountSyncRequest(),
        user_context,
    )


def _handle_receipt_voucher_auto_push(schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    db = database.SessionLocal()
    try:
        account_book = _resolve_schedule_account_book(db, schedule_data)
        operator = _resolve_schedule_operator(db, user_context)
        community_ids = _resolve_account_book_community_ids(db, account_book)

        if not community_ids:
            return {
                "status": "failed",
                "message": f"账簿 {account_book.number} 未绑定任何园区，无法执行自动推送。",
                "logs": [{"type": "error", "message": f"账簿 {account_book.number} 未绑定任何园区。"}],
                "task_id": None,
            }

        run_date = _resolve_schedule_run_date(schedule_data)
        receipts = (
            db.query(models.ReceiptBill)
            .filter(
                models.ReceiptBill.community_id.in_(community_ids),
                models.ReceiptBill.deal_date == run_date,
            )
            .order_by(models.ReceiptBill.community_id.asc(), models.ReceiptBill.id.asc())
            .all()
        )

        if not receipts:
            return {
                "status": "success",
                "message": f"账簿 {account_book.number} 在 {run_date.isoformat()} 没有待处理收款单。",
                "logs": [
                    {
                        "type": "info",
                        "message": f"已扫描 {len(community_ids)} 个园区，未找到 {run_date.isoformat()} 的收款单。",
                    }
                ],
                "task_id": None,
            }

        preview_voucher = preview_voucher_for_receipt_handler
        push_voucher = push_voucher_to_kingdee_handler

        logs: List[Dict[str, str]] = []
        pushed_count = 0
        skipped_count = 0
        failed_count = 0

        for receipt in receipts:
            receipt_code = str(receipt.receipt_id or receipt.id)

            try:
                preview_result = preview_voucher(
                    int(receipt.id),
                    int(receipt.community_id),
                    str(account_book.id),
                    str(account_book.name or ""),
                    str(account_book.number or ""),
                    True,
                    operator,
                    db,
                    community_ids,
                )
            except HTTPException as exc:
                failed_count += 1
                logs.append({
                    "type": "error",
                    "message": f"收款单 {receipt_code} 预览失败：{exc.detail if isinstance(exc.detail, str) else str(exc.detail)}",
                })
                db.rollback()
                continue
            except Exception as exc:
                failed_count += 1
                logs.append({
                    "type": "error",
                    "message": f"收款单 {receipt_code} 预览异常：{str(exc)}",
                })
                db.rollback()
                continue

            if not preview_result.get("matched"):
                failed_count += 1
                logs.append({
                    "type": "error",
                    "message": f"收款单 {receipt_code} 未匹配到凭证模板，已跳过。",
                })
                continue

            if preview_result.get("push_blocked"):
                skipped_count += 1
                push_block_reason = str(preview_result.get("push_block_reason") or "").strip()
                logs.append({
                    "type": "warning",
                    "message": (
                        f"收款单 {receipt_code} 已存在推送记录，已跳过。"
                        f"{push_block_reason if not push_block_reason else ' ' + push_block_reason}"
                    ).strip(),
                })
                continue

            kingdee_json = preview_result.get("kingdee_json")
            if not isinstance(kingdee_json, dict) or not kingdee_json:
                failed_count += 1
                logs.append({
                    "type": "error",
                    "message": f"收款单 {receipt_code} 未生成有效金蝶 JSON，已跳过。",
                })
                continue

            payload = schemas.VoucherPushRequest(
                kingdee_json=kingdee_json,
                bills=preview_result.get("source_bills") or [],
                force_push=False,
            )

            try:
                push_result = push_voucher(
                    payload,
                    str(account_book.id),
                    str(account_book.name or ""),
                    str(account_book.number or ""),
                    operator,
                    db,
                    community_ids,
                )
            except HTTPException as exc:
                failed_count += 1
                logs.append({
                    "type": "error",
                    "message": f"收款单 {receipt_code} 推送失败：{exc.detail if isinstance(exc.detail, str) else str(exc.detail)}",
                })
                db.rollback()
                continue
            except Exception as exc:
                failed_count += 1
                logs.append({
                    "type": "error",
                    "message": f"收款单 {receipt_code} 推送异常：{str(exc)}",
                })
                db.rollback()
                continue

            if push_result.get("success"):
                pushed_count += 1
                voucher_number = str(push_result.get("voucher_number") or "").strip()
                logs.append({
                    "type": "info",
                    "message": (
                        f"收款单 {receipt_code} 推送成功。"
                        if not voucher_number
                        else f"收款单 {receipt_code} 推送成功，凭证号 {voucher_number}。"
                    ),
                })
            else:
                failed_count += 1
                logs.append({
                    "type": "error",
                    "message": f"收款单 {receipt_code} 推送失败：{push_result.get('message') or '未知错误'}",
                })

        scanned_count = len(receipts)
        status = "success" if failed_count == 0 else "failed"
        message = (
            f"账簿 {account_book.number} 自动推送完成：扫描 {scanned_count} 张，"
            f"成功 {pushed_count} 张，跳过 {skipped_count} 张，失败 {failed_count} 张。"
        )
        return {
            "status": status,
            "message": message,
            "logs": logs[:500],
            "task_id": None,
            "scanned_receipts": scanned_count,
            "pushed_receipts": pushed_count,
            "skipped_receipts": skipped_count,
            "failed_receipts": failed_count,
            "account_book_number": account_book.number,
            "run_date": run_date.isoformat(),
        }
    finally:
        db.close()

SYNC_TARGET_HANDLERS: Dict[str, Any] = {}


def _register_sync_target_handler(code: str, handler):
    SYNC_TARGET_HANDLERS[code] = handler
    sync_schedule_service.register_handler(code, handler)


def run_sync_target_handler(target_code: str, schedule_data: Dict[str, Any], user_context: Dict[str, str]) -> Dict[str, Any]:
    handler = SYNC_TARGET_HANDLERS.get(target_code)
    if not handler:
        raise RuntimeError(f"No sync handler registered for target '{target_code}'.")
    return handler(schedule_data, user_context)


_register_sync_target_handler("projects", _handle_projects_sync)
_register_sync_target_handler("charge_items", _handle_charge_items_sync)
_register_sync_target_handler("houses", _handle_houses_sync)
_register_sync_target_handler("residents", _handle_residents_sync)
_register_sync_target_handler("parks", _handle_parks_sync)
_register_sync_target_handler("bills", _handle_bills_sync)
_register_sync_target_handler("receipt_bills", _handle_receipt_bills_sync)
_register_sync_target_handler("deposit_records", _handle_deposit_records_sync)
_register_sync_target_handler("prepayment_records", _handle_prepayment_records_sync)
_register_sync_target_handler("accounting_subjects", _handle_accounting_subjects_sync)
_register_sync_target_handler("customers", _handle_customers_sync)
_register_sync_target_handler("suppliers", _handle_suppliers_sync)
_register_sync_target_handler("tax_rates", _handle_tax_rates_sync)
_register_sync_target_handler("kd_houses", _handle_kd_houses_sync)
_register_sync_target_handler("account_books", _handle_account_books_sync)
_register_sync_target_handler("auxiliary_data_categories", _handle_auxiliary_data_categories_sync)
_register_sync_target_handler("auxiliary_data", _handle_auxiliary_data_sync)
_register_sync_target_handler("bank_accounts", _handle_bank_accounts_sync)
_register_sync_target_handler("receipt_voucher_auto_push", _handle_receipt_voucher_auto_push)


@router.on_event("startup")
def start_sync_schedule_service():
    sync_schedule_service.start()


@router.on_event("shutdown")
def stop_sync_schedule_service():
    sync_schedule_service.stop()


@router.get("/api/sync-schedules/meta")
def get_sync_schedule_meta(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    return {
        "targets": SYNC_TARGET_DEFINITIONS,
        "schedule_types": [
            {"value": "interval", "label": "间隔执行"},
            {"value": "daily", "label": "每日执行"},
            {"value": "weekly", "label": "每周执行"},
        ],
        "weekdays": [
            {"value": "MON", "label": "周一"},
            {"value": "TUE", "label": "周二"},
            {"value": "WED", "label": "周三"},
            {"value": "THU", "label": "周四"},
            {"value": "FRI", "label": "周五"},
            {"value": "SAT", "label": "周六"},
            {"value": "SUN", "label": "周日"},
        ],
        "default_timezone": DEFAULT_TIMEZONE,
    }


@router.get("/api/sync-schedules")
def list_sync_schedules(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    schedules = (
        db.query(models.SyncSchedule)
        .options(
            joinedload(models.SyncSchedule.creator),
            joinedload(models.SyncSchedule.updater),
        )
        .order_by(models.SyncSchedule.created_at.desc(), models.SyncSchedule.id.desc())
        .all()
    )
    return [_serialize_sync_schedule(item) for item in schedules]


@router.post("/api/sync-schedules", response_model=schemas.SyncScheduleResponse)
def create_sync_schedule(
    payload: schemas.SyncScheduleCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    normalized = _validate_sync_schedule_payload(payload)
    schedule = models.SyncSchedule(
        name=normalized["name"],
        description=normalized["description"],
        target_codes=serialize_json_list(normalized["target_codes"]),
        community_ids=serialize_json_list(normalized["community_ids"]),
        account_book_number=normalized["account_book_number"],
        account_book_name=normalized["account_book_name"],
        schedule_type=normalized["schedule_type"],
        interval_minutes=normalized["interval_minutes"],
        daily_time=normalized["daily_time"],
        weekly_days=serialize_json_list(normalized["weekly_days"]),
        timezone=normalized["timezone"],
        enabled=normalized["enabled"],
        next_run_at=compute_next_run_at(
            schedule_type=normalized["schedule_type"],
            interval_minutes=normalized["interval_minutes"],
            daily_time=normalized["daily_time"],
            weekly_days=normalized["weekly_days"],
            timezone_name=normalized["timezone"],
            now_utc=utcnow_naive(),
        ) if normalized["enabled"] else None,
        created_by=current_user.id,
        updated_by=current_user.id,
    )
    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    return _serialize_sync_schedule(schedule)


@router.put("/api/sync-schedules/{schedule_id}", response_model=schemas.SyncScheduleResponse)
def update_sync_schedule(
    schedule_id: int,
    payload: schemas.SyncScheduleUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    schedule = db.query(models.SyncSchedule).filter(models.SyncSchedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Sync schedule not found")

    current_payload = schemas.SyncScheduleCreate(
        name=payload.name if payload.name is not None else schedule.name,
        description=payload.description if payload.description is not None else schedule.description,
        target_codes=payload.target_codes if payload.target_codes is not None else parse_json_list(schedule.target_codes),
        community_ids=payload.community_ids if payload.community_ids is not None else _normalize_schedule_community_ids(parse_json_list(schedule.community_ids)),
        account_book_number=payload.account_book_number if payload.account_book_number is not None else schedule.account_book_number,
        account_book_name=payload.account_book_name if payload.account_book_name is not None else schedule.account_book_name,
        schedule_type=payload.schedule_type if payload.schedule_type is not None else schedule.schedule_type,
        interval_minutes=payload.interval_minutes if payload.interval_minutes is not None else schedule.interval_minutes,
        daily_time=payload.daily_time if payload.daily_time is not None else schedule.daily_time,
        weekly_days=payload.weekly_days if payload.weekly_days is not None else normalize_weekdays(parse_json_list(schedule.weekly_days)),
        timezone=payload.timezone if payload.timezone is not None else (schedule.timezone or DEFAULT_TIMEZONE),
        enabled=payload.enabled if payload.enabled is not None else bool(schedule.enabled),
    )
    normalized = _validate_sync_schedule_payload(current_payload)

    schedule.name = normalized["name"]
    schedule.description = normalized["description"]
    schedule.target_codes = serialize_json_list(normalized["target_codes"])
    schedule.community_ids = serialize_json_list(normalized["community_ids"])
    schedule.account_book_number = normalized["account_book_number"]
    schedule.account_book_name = normalized["account_book_name"]
    schedule.schedule_type = normalized["schedule_type"]
    schedule.interval_minutes = normalized["interval_minutes"]
    schedule.daily_time = normalized["daily_time"]
    schedule.weekly_days = serialize_json_list(normalized["weekly_days"])
    schedule.timezone = normalized["timezone"]
    schedule.enabled = normalized["enabled"]
    schedule.updated_by = current_user.id

    if schedule.enabled:
        schedule.next_run_at = compute_next_run_at(
            schedule_type=normalized["schedule_type"],
            interval_minutes=normalized["interval_minutes"],
            daily_time=normalized["daily_time"],
            weekly_days=normalized["weekly_days"],
            timezone_name=normalized["timezone"],
            now_utc=utcnow_naive(),
        )
    else:
        schedule.next_run_at = None

    db.commit()
    db.refresh(schedule)
    return _serialize_sync_schedule(schedule)


@router.delete("/api/sync-schedules/{schedule_id}")
def delete_sync_schedule(
    schedule_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    schedule = db.query(models.SyncSchedule).filter(models.SyncSchedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Sync schedule not found")
    if schedule.is_running:
        raise HTTPException(status_code=400, detail="Running schedule cannot be deleted")

    db.delete(schedule)
    db.commit()
    return {"message": "Sync schedule deleted"}


@router.post("/api/sync-schedules/{schedule_id}/run")
def run_sync_schedule_now(
    schedule_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    try:
        payload = sync_schedule_service.trigger_execution(
            schedule_id=schedule_id,
            trigger_type="manual",
            user_id=current_user.id,
            advance_schedule=False,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return payload


@router.post("/api/sync-schedules/{schedule_id}/toggle", response_model=schemas.SyncScheduleResponse)
def toggle_sync_schedule(
    schedule_id: int,
    enabled: bool = Query(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    schedule = db.query(models.SyncSchedule).filter(models.SyncSchedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Sync schedule not found")

    schedule.enabled = enabled
    schedule.updated_by = current_user.id
    if enabled:
        schedule.next_run_at = compute_next_run_at(
            schedule_type=schedule.schedule_type,
            interval_minutes=schedule.interval_minutes,
            daily_time=schedule.daily_time,
            weekly_days=parse_json_list(schedule.weekly_days),
            timezone_name=schedule.timezone,
            now_utc=utcnow_naive(),
        )
    else:
        schedule.next_run_at = None

    db.commit()
    db.refresh(schedule)
    return _serialize_sync_schedule(schedule)


@router.get("/api/sync-schedules/{schedule_id}/executions")
def list_sync_schedule_executions(
    schedule_id: int,
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    schedule = db.query(models.SyncSchedule).filter(models.SyncSchedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Sync schedule not found")

    executions = (
        db.query(models.SyncScheduleExecution)
        .options(joinedload(models.SyncScheduleExecution.triggered_by_user))
        .filter(models.SyncScheduleExecution.schedule_id == schedule_id)
        .order_by(models.SyncScheduleExecution.started_at.desc(), models.SyncScheduleExecution.id.desc())
        .limit(limit)
        .all()
    )
    return [_serialize_sync_schedule_execution(item) for item in executions]


@router.get("/api/sync-schedules/executions/latest")
def list_latest_sync_schedule_executions(
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "sync_schedule.manage")
    executions = (
        db.query(models.SyncScheduleExecution)
        .options(
            joinedload(models.SyncScheduleExecution.triggered_by_user),
            joinedload(models.SyncScheduleExecution.schedule),
        )
        .order_by(models.SyncScheduleExecution.started_at.desc(), models.SyncScheduleExecution.id.desc())
        .limit(limit)
        .all()
    )

    result = []
    for execution in executions:
        item = _serialize_sync_schedule_execution(execution)
        item["schedule_name"] = execution.schedule.name if execution.schedule else ""
        result.append(item)
    return result



