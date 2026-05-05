# -*- coding: utf-8 -*-
import json
from datetime import datetime
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, joinedload

import models
import schemas
from api.dependencies import _require_api_permission, get_current_user, get_db, get_user_context
from api.routers.reporting_shared import serialize_reporting_dataset
from services.reporting_database import ReportingDatabaseError, ReportingDatabaseService, UnsafeQueryError, _json_default


router = APIRouter()


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
    return [serialize_reporting_dataset(item) for item in items]


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
    return serialize_reporting_dataset(dataset)


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
    return serialize_reporting_dataset(dataset)


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
        return ReportingDatabaseService.execute_dataset(
            dataset.connection,
            dataset,
            params=payload.params,
            limit=payload.limit,
            default_limit=dataset.row_limit,
            db_session=db,
            user_context=user_ctx,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/reporting/datasets/{dataset_id}/validate", response_model=schemas.ReportingDatasetValidationResponse)
def validate_reporting_dataset(
    dataset_id: int,
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
        result = ReportingDatabaseService.validate_query(
            connection=dataset.connection,
            sql_text=dataset.sql_text,
            params_json=dataset.params_json,
            default_limit=dataset.row_limit,
            db_session=db,
            user_context=user_ctx,
        )
        dataset.last_columns_json = json.dumps(result.get("columns") or [], ensure_ascii=False, default=_json_default)
        dataset.last_validated_at = datetime.utcnow()
        db.add(dataset)
        db.commit()
        return result
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/reporting/datasets/preview-draft")
def preview_reporting_dataset_draft(
    payload: schemas.ReportingDatasetDraftPreviewRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    user_ctx: Dict[str, str] = Depends(get_user_context),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == payload.connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    if not connection.is_active:
        raise HTTPException(status_code=400, detail="Connection is inactive")

    try:
        return ReportingDatabaseService.execute_query(
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


@router.post("/api/reporting/datasets/validate-draft", response_model=schemas.ReportingDatasetValidationResponse)
def validate_reporting_dataset_draft(
    payload: schemas.ReportingDatasetDraftPreviewRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    user_ctx: Dict[str, str] = Depends(get_user_context),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == payload.connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    if not connection.is_active:
        raise HTTPException(status_code=400, detail="Connection is inactive")

    try:
        return ReportingDatabaseService.validate_query(
            connection=connection,
            sql_text=payload.sql_text,
            params_json=payload.params_json,
            default_limit=payload.row_limit,
            db_session=db,
            user_context=user_ctx,
        )
    except (ReportingDatabaseError, UnsafeQueryError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
