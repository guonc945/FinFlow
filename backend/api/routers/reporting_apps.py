# -*- coding: utf-8 -*-
import json
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, joinedload

import models
import schemas
from api.dependencies import _require_api_permission, get_current_user, get_db, get_user_context
from api.routers.reporting_shared import serialize_reporting_dataset, serialize_reporting_report
from services.reporting_database import ReportingDatabaseError, ReportingDatabaseService, UnsafeQueryError, inject_report_filters


router = APIRouter()


@router.post("/api/reporting/reports/preview-draft")
def preview_reporting_report_draft(
    payload: schemas.ReportingReportDraftPreviewRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    user_ctx: Dict[str, str] = Depends(get_user_context),
):
    """Preview a report with draft configuration without saving it."""
    _require_api_permission(db, current_user, "reporting.manage")
    dataset = (
        db.query(models.ReportingDataset)
        .options(joinedload(models.ReportingDataset.connection))
        .filter(models.ReportingDataset.id == payload.dataset_id)
        .first()
    )
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if not dataset.is_active:
        raise HTTPException(status_code=400, detail="Dataset is inactive")
    if not dataset.connection or not dataset.connection.is_active:
        raise HTTPException(status_code=400, detail="Dataset connection is inactive")

    report_config = {}
    if payload.config_json:
        try:
            report_config = json.loads(payload.config_json)
        except json.JSONDecodeError:
            report_config = {}

    # Inject report filter WHERE clauses
    report_filters = report_config.get("filters") or []
    effective_sql = dataset.sql_text
    effective_params = dict(payload.params or {})
    if report_filters and effective_params:
        effective_sql, effective_params = inject_report_filters(effective_sql, report_filters, effective_params)

    effective_limit = payload.limit
    if effective_limit is None:
        try:
            default_limit = report_config.get("default_limit")
            effective_limit = int(default_limit) if default_limit is not None else None
        except (TypeError, ValueError):
            effective_limit = None

    try:
        raw_result = ReportingDatabaseService.execute_query(
            dataset.connection,
            sql_text=effective_sql,
            params_json=dataset.params_json,
            params=effective_params,
            limit=effective_limit,
            db_session=db,
            user_context=user_ctx,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    configured_columns = report_config.get("columns") or []
    ordered_visible_columns: List[str] = []
    if isinstance(configured_columns, list):
        ordered_visible_columns = [
            str(item.get("key", "")).strip()
            for item in configured_columns
            if isinstance(item, dict) and item.get("visible", True) and str(item.get("key", "")).strip()
        ]

    if not ordered_visible_columns:
        ordered_visible_columns = [str(col).strip() for col in (report_config.get("visible_columns") or []) if str(col).strip()]

    if ordered_visible_columns:
        visible_set = set(ordered_visible_columns)
        column_map = {col["name"]: col for col in raw_result["columns"] if col["name"] in visible_set}
        raw_result["columns"] = [column_map[key] for key in ordered_visible_columns if key in column_map]
        raw_result["rows"] = [
            {key: row[key] for key in ordered_visible_columns if key in row}
            for row in raw_result["rows"]
        ]
        raw_result["numeric_summary"] = {
            key: raw_result["numeric_summary"][key]
            for key in ordered_visible_columns
            if key in raw_result["numeric_summary"]
        }

    return {
        "report": {
            "dataset_id": payload.dataset_id,
            "report_type": payload.report_type,
            "config_json": payload.config_json,
        },
        "dataset": serialize_reporting_dataset(dataset).dict(),
        **raw_result,
    }


@router.get("/api/reporting/reports", response_model=List[schemas.ReportingReportResponse])
def list_reporting_reports(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    items = db.query(models.ReportingReport).options(joinedload(models.ReportingReport.dataset)).order_by(models.ReportingReport.id.desc()).all()
    return [serialize_reporting_report(item) for item in items]


@router.post("/api/reporting/reports", response_model=schemas.ReportingReportResponse)
def create_reporting_report(
    payload: schemas.ReportingReportCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    dataset = db.query(models.ReportingDataset).filter(models.ReportingDataset.id == payload.dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    report = models.ReportingReport(**payload.dict())
    db.add(report)
    db.commit()
    db.refresh(report)
    report = (
        db.query(models.ReportingReport)
        .options(joinedload(models.ReportingReport.dataset))
        .filter(models.ReportingReport.id == report.id)
        .first()
    )
    return serialize_reporting_report(report)


@router.put("/api/reporting/reports/{report_id}", response_model=schemas.ReportingReportResponse)
def update_reporting_report(
    report_id: int,
    payload: schemas.ReportingReportUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    report = db.query(models.ReportingReport).filter(models.ReportingReport.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    update_data = payload.dict(exclude_unset=True)
    if "dataset_id" in update_data:
        dataset = db.query(models.ReportingDataset).filter(models.ReportingDataset.id == update_data["dataset_id"]).first()
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")

    for key, value in update_data.items():
        setattr(report, key, value)

    db.commit()
    db.refresh(report)
    report = (
        db.query(models.ReportingReport)
        .options(joinedload(models.ReportingReport.dataset))
        .filter(models.ReportingReport.id == report.id)
        .first()
    )
    return serialize_reporting_report(report)


@router.delete("/api/reporting/reports/{report_id}")
def delete_reporting_report(
    report_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    report = db.query(models.ReportingReport).filter(models.ReportingReport.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    db.delete(report)
    db.commit()
    return {"message": "Report deleted"}


@router.post("/api/reporting/reports/{report_id}/run")
def run_reporting_report(
    report_id: int,
    payload: schemas.ReportingReportRunRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    user_ctx: Dict[str, str] = Depends(get_user_context),
):
    _require_api_permission(db, current_user, "reporting.manage")
    report = (
        db.query(models.ReportingReport)
        .options(joinedload(models.ReportingReport.dataset).joinedload(models.ReportingDataset.connection))
        .filter(models.ReportingReport.id == report_id)
        .first()
    )
    if not report or not report.is_active:
        raise HTTPException(status_code=404, detail="Report not found")

    dataset = report.dataset
    if not dataset or not dataset.is_active:
        raise HTTPException(status_code=400, detail="Dataset is inactive")
    if not dataset.connection or not dataset.connection.is_active:
        raise HTTPException(status_code=400, detail="Dataset connection is inactive")

    report_config = {}
    if report.config_json:
        try:
            report_config = json.loads(report.config_json)
        except json.JSONDecodeError:
            report_config = {}

    # Inject report filter WHERE clauses
    report_filters = report_config.get("filters") or []
    effective_sql = dataset.sql_text
    effective_params = dict(payload.params or {})
    if report_filters and effective_params:
        effective_sql, effective_params = inject_report_filters(effective_sql, report_filters, effective_params)

    effective_limit = payload.limit
    if effective_limit is None:
        try:
            default_limit = report_config.get("default_limit")
            effective_limit = int(default_limit) if default_limit is not None else None
        except (TypeError, ValueError):
            effective_limit = None

    try:
        raw_result = ReportingDatabaseService.execute_query(
            dataset.connection,
            sql_text=effective_sql,
            params_json=dataset.params_json,
            params=effective_params,
            limit=effective_limit,
            db_session=db,
            user_context=user_ctx,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    configured_columns = report_config.get("columns") or []
    ordered_visible_columns: List[str] = []
    if isinstance(configured_columns, list):
        ordered_visible_columns = [
            str(item.get("key", "")).strip()
            for item in configured_columns
            if isinstance(item, dict) and item.get("visible", True) and str(item.get("key", "")).strip()
        ]

    if not ordered_visible_columns:
        ordered_visible_columns = [str(col).strip() for col in (report_config.get("visible_columns") or []) if str(col).strip()]

    if ordered_visible_columns:
        visible_set = set(ordered_visible_columns)
        column_map = {col["name"]: col for col in raw_result["columns"] if col["name"] in visible_set}
        raw_result["columns"] = [column_map[key] for key in ordered_visible_columns if key in column_map]
        raw_result["rows"] = [
            {key: row[key] for key in ordered_visible_columns if key in row}
            for row in raw_result["rows"]
        ]
        raw_result["numeric_summary"] = {
            key: raw_result["numeric_summary"][key]
            for key in ordered_visible_columns
            if key in raw_result["numeric_summary"]
        }

    return {
        "report": serialize_reporting_report(report).dict(),
        "dataset": serialize_reporting_dataset(dataset).dict(),
        **raw_result,
    }
