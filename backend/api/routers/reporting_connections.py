# -*- coding: utf-8 -*-
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

import models
import schemas
from api.dependencies import _require_api_permission, get_current_user, get_db
from api.routers.reporting_shared import serialize_reporting_connection
from services.reporting_database import ReportingDatabaseService
from utils.crypto import encrypt_value


router = APIRouter()


@router.get("/api/reporting/db-connections", response_model=List[schemas.ReportingDbConnectionResponse])
def list_reporting_db_connections(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    items = db.query(models.ReportingDbConnection).order_by(models.ReportingDbConnection.id.desc()).all()
    return [serialize_reporting_connection(item) for item in items]


@router.get("/api/reporting/db-connections/{connection_id}", response_model=schemas.ReportingDbConnectionResponse)
def get_reporting_db_connection(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    item = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Connection not found")
    return serialize_reporting_connection(item)


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
    return serialize_reporting_connection(connection)


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


@router.get("/api/reporting/db-connections/{connection_id}/metadata", response_model=schemas.ReportingConnectionMetadataResponse)
def get_reporting_db_connection_metadata(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        return ReportingDatabaseService.get_connection_metadata(connection)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/api/reporting/db-connections/{connection_id}/schemas")
def list_reporting_db_schemas(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        return {"connection_id": connection.id, "schemas": ReportingDatabaseService.list_schemas(connection)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


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
    return serialize_reporting_connection(connection)


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
        return {"connection_id": connection.id, "tables": ReportingDatabaseService.list_tables(connection, schema_name=schema_name)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get(
    "/api/reporting/db-connections/{connection_id}/tables/{table_name}/columns",
    response_model=List[schemas.ReportingTableColumnResponse],
)
def get_reporting_table_columns(
    connection_id: int,
    table_name: str,
    schema_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    connection = db.query(models.ReportingDbConnection).filter(models.ReportingDbConnection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        return ReportingDatabaseService.get_table_columns(connection, table_name=table_name, schema_name=schema_name)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
