# -*- coding: utf-8 -*-
import json
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload

import models
import schemas
from api.dependencies import _require_api_permission, get_current_user, get_db, get_user_context
from services.reporting_database import (
    ReportingDatabaseError,
    ReportingDatabaseService,
    UnsafeQueryError,
)
from utils.crypto import encrypt_value

router = APIRouter()

def _require_admin(user: models.User) -> None:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Only administrators can modify reporting resources")


def _serialize_reporting_connection(connection: models.ReportingDbConnection) -> schemas.ReportingDbConnectionResponse:
    return schemas.ReportingDbConnectionResponse(
        id=connection.id,
        name=connection.name,
        description=connection.description,
        db_type=connection.db_type,
        host=connection.host,
        port=connection.port,
        database_name=connection.database_name,
        schema_name=connection.schema_name,
        username=connection.username,
        connection_options=connection.connection_options,
        is_active=connection.is_active,
        has_password=bool(connection.password_enc),
        created_at=connection.created_at,
        updated_at=connection.updated_at,
    )


def _serialize_reporting_dataset(dataset: models.ReportingDataset) -> schemas.ReportingDatasetResponse:
    return schemas.ReportingDatasetResponse(
        id=dataset.id,
        connection_id=dataset.connection_id,
        connection_name=dataset.connection.name if dataset.connection else None,
        name=dataset.name,
        description=dataset.description,
        sql_text=dataset.sql_text,
        params_json=dataset.params_json,
        row_limit=dataset.row_limit,
        last_columns_json=dataset.last_columns_json,
        last_validated_at=dataset.last_validated_at,
        is_active=dataset.is_active,
        created_at=dataset.created_at,
        updated_at=dataset.updated_at,
    )


def _serialize_reporting_report(report: models.ReportingReport) -> schemas.ReportingReportResponse:
    return schemas.ReportingReportResponse(
        id=report.id,
        dataset_id=report.dataset_id,
        dataset_name=report.dataset.name if report.dataset else None,
        name=report.name,
        description=report.description,
        report_type=report.report_type,
        config_json=report.config_json,
        is_active=report.is_active,
        created_at=report.created_at,
        updated_at=report.updated_at,
    )


@router.get("/api/reporting/db-connections", response_model=List[schemas.ReportingDbConnectionResponse])
def list_reporting_db_connections(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    items = db.query(models.ReportingDbConnection).order_by(models.ReportingDbConnection.id.desc()).all()
    return [_serialize_reporting_connection(item) for item in items]


@router.post("/api/reporting/db-connections", response_model=schemas.ReportingDbConnectionResponse)
def create_reporting_db_connection(
    payload: schemas.ReportingDbConnectionCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    existing = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.name == payload.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Connection name already exists")

    connection = models.ReportingDbConnection(
        name=payload.name,
        description=payload.description,
        db_type=payload.db_type,
        host=payload.host,
        port=payload.port,
        database_name=payload.database_name,
        schema_name=payload.schema_name,
        username=payload.username,
        password_enc=encrypt_value(payload.password) if payload.password else None,
        connection_options=payload.connection_options,
        is_active=payload.is_active,
    )
    db.add(connection)
    db.commit()
    db.refresh(connection)
    return _serialize_reporting_connection(connection)


@router.post("/api/reporting/db-connections/test")
def test_reporting_db_connection(
    payload: schemas.ReportingDbConnectionCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    transient = models.ReportingDbConnection(
        name=payload.name,
        description=payload.description,
        db_type=payload.db_type,
        host=payload.host,
        port=payload.port,
        database_name=payload.database_name,
        schema_name=payload.schema_name,
        username=payload.username,
        password_enc=encrypt_value(payload.password) if payload.password else None,
        connection_options=payload.connection_options,
        is_active=payload.is_active,
    )
    try:
        return ReportingDatabaseService.test_connection(transient)
    except Exception as exc:
        return {"success": False, "message": str(exc)}


@router.put("/api/reporting/db-connections/{connection_id}", response_model=schemas.ReportingDbConnectionResponse)
def update_reporting_db_connection(
    connection_id: int,
    payload: schemas.ReportingDbConnectionUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    update_data = payload.dict(exclude_unset=True)
    if "name" in update_data and update_data["name"] != connection.name:
        exists = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.name == update_data["name"]).first()
        if exists:
            raise HTTPException(status_code=400, detail="Connection name already exists")

    password = update_data.pop("password", None) if "password" in update_data else None
    for key, value in update_data.items():
        setattr(connection, key, value)
    if "password" in payload.__fields_set__:
        connection.password_enc = encrypt_value(password) if password else None

    db.commit()
    db.refresh(connection)
    return _serialize_reporting_connection(connection)


@router.delete("/api/reporting/db-connections/{connection_id}")
def delete_reporting_db_connection(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    db.delete(connection)
    db.commit()
    return {"message": "Connection deleted"}


@router.get("/api/reporting/db-connections/{connection_id}/tables")
def list_reporting_db_tables(
    connection_id: int,
    schema_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        return {
            "connection_id": connection.id,
            "tables": ReportingDatabaseService.list_tables(connection, schema_name=schema_name),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/api/reporting/datasets", response_model=List[schemas.ReportingDatasetResponse])
def list_reporting_datasets(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    items = (
        db.query(models.ReportingDataset)
        .options(joinedload(models.ReportingDataset.connection))
        .order_by(models.ReportingDataset.id.desc())
        .all()
    )
    return [_serialize_reporting_dataset(item) for item in items]


@router.post("/api/reporting/datasets", response_model=schemas.ReportingDatasetResponse)
def create_reporting_dataset(
    payload: schemas.ReportingDatasetCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == payload.connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    dataset = models.ReportingDataset(**payload.dict())
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    dataset = (
        db.query(models.ReportingDataset)
        .options(joinedload(models.ReportingDataset.connection))
        .filter(models.ReportingDataset.id == dataset.id)
        .first()
    )
    return _serialize_reporting_dataset(dataset)


@router.put("/api/reporting/datasets/{dataset_id}", response_model=schemas.ReportingDatasetResponse)
def update_reporting_dataset(
    dataset_id: int,
    payload: schemas.ReportingDatasetUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    dataset = db.query(models.ReportingDataset).filter(models.ReportingDataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    update_data = payload.dict(exclude_unset=True)
    if "connection_id" in update_data:
        connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == update_data["connection_id"]).first()
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")

    for key, value in update_data.items():
        setattr(dataset, key, value)

    db.commit()
    db.refresh(dataset)
    dataset = (
        db.query(models.ReportingDataset)
        .options(joinedload(models.ReportingDataset.connection))
        .filter(models.ReportingDataset.id == dataset.id)
        .first()
    )
    return _serialize_reporting_dataset(dataset)


@router.delete("/api/reporting/datasets/{dataset_id}")
def delete_reporting_dataset(
    dataset_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    dataset = db.query(models.ReportingDataset).filter(models.ReportingDataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    db.delete(dataset)
    db.commit()
    return {"message": "Dataset deleted"}


@router.post("/api/reporting/datasets/{dataset_id}/preview")
def preview_reporting_dataset(
    dataset_id: int,
    payload: schemas.ReportingDatasetPreviewRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    user_ctx: Dict[str, str] = Depends(get_user_context),
):
    _require_api_permission(db, current_user, "reporting.manage")
    dataset = (
        db.query(models.ReportingDataset)
        .options(joinedload(models.ReportingDataset.connection))
        .filter(models.ReportingDataset.id == dataset_id)
        .first()
    )
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if not dataset.connection or not dataset.connection.is_active:
        raise HTTPException(status_code=400, detail="Dataset connection is inactive")

    try:
        result = ReportingDatabaseService.execute_dataset(
            dataset.connection,
            dataset,
            params=payload.params,
            limit=payload.limit,
            db_session=db,
            user_context=user_ctx,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return result


@router.post("/api/reporting/datasets/preview-draft")
def preview_reporting_dataset_draft(
    payload: schemas.ReportingDatasetDraftPreviewRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    user_ctx: Dict[str, str] = Depends(get_user_context),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = (
        db.query(models.ReportingDbConnection)
        .filter(models.ReportingDbConnection.id == payload.connection_id)
        .first()
    )
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    if not connection.is_active:
        raise HTTPException(status_code=400, detail="Connection is inactive")

    try:
        result = ReportingDatabaseService.execute_query(
            connection=connection,
            sql_text=payload.sql_text,
            params_json=payload.params_json,
            params=payload.params,
            limit=payload.limit,
            default_limit=payload.row_limit,
            db_session=db,
            user_context=user_ctx,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return result


@router.get("/api/reporting/reports", response_model=List[schemas.ReportingReportResponse])
def list_reporting_reports(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    items = db.query(models.ReportingReport).options(
        joinedload(models.ReportingReport.dataset)
    ).order_by(models.ReportingReport.id.desc()).all()
    return [_serialize_reporting_report(item) for item in items]


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
    return _serialize_reporting_report(report)


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
    return _serialize_reporting_report(report)


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
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    if not report.is_active:
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

    effective_limit = payload.limit
    if effective_limit is None:
        try:
            effective_limit = int(report_config.get("default_limit")) if report_config.get("default_limit") is not None else None
        except (TypeError, ValueError):
            effective_limit = None

    try:
        raw_result = ReportingDatabaseService.execute_dataset(
            dataset.connection,
            dataset,
            params=payload.params,
            limit=effective_limit,
            db_session=db,
            user_context=user_ctx,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    config = report_config

    visible_columns = [
        str(col).strip()
        for col in (config.get("visible_columns") or [])
        if str(col).strip()
    ]
    if visible_columns:
        raw_result["columns"] = [col for col in raw_result["columns"] if col["name"] in visible_columns]
        raw_result["rows"] = [
            {key: value for key, value in row.items() if key in visible_columns}
            for row in raw_result["rows"]
        ]
        raw_result["numeric_summary"] = {
            key: value for key, value in raw_result["numeric_summary"].items() if key in visible_columns
        }

    return {
        "report": _serialize_reporting_report(report).dict(),
        "dataset": _serialize_reporting_dataset(dataset).dict(),
        **raw_result,
    }


