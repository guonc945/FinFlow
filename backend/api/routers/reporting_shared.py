# -*- coding: utf-8 -*-
from fastapi import HTTPException

import models
import schemas


def require_reporting_manage(user: models.User) -> None:
    if user.role != "admin" and "reporting.manage" not in (user.api_keys or "").split(","):
        raise HTTPException(status_code=403, detail="Forbidden")


def serialize_reporting_connection(connection: models.ReportingDbConnection) -> schemas.ReportingDbConnectionResponse:
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


def serialize_reporting_dataset(dataset: models.ReportingDataset) -> schemas.ReportingDatasetResponse:
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


def serialize_reporting_report(report: models.ReportingReport) -> schemas.ReportingReportResponse:
    return schemas.ReportingReportResponse(
        id=report.id,
        dataset_id=report.dataset_id,
        dataset_name=report.dataset.name if report.dataset else None,
        name=report.name,
        description=report.description,
        report_type=report.report_type,
        config_json=report.config_json,
        category_id=report.category_id,
        category_name=report.category.name if report.category else None,
        is_active=report.is_active,
        created_at=report.created_at,
        updated_at=report.updated_at,
    )
