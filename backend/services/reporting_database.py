import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session

import models
from utils.crypto import decrypt_value
from utils.variable_parser import build_variable_map, resolve_dict_variables, resolve_variables


class ReportingDatabaseError(Exception):
    """Base error for reporting database operations."""


class UnsafeQueryError(ReportingDatabaseError):
    """Raised when a dataset SQL is not read-only."""


def _normalize_sql(sql_text: str) -> str:
    sql = (sql_text or "").strip()
    if not sql:
        raise ReportingDatabaseError("SQL text cannot be empty")
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    sql = re.sub(r"--.*?$", " ", sql, flags=re.M)
    return sql.strip().rstrip(";").strip()


def _ensure_readonly_sql(sql_text: str) -> str:
    normalized = _normalize_sql(sql_text)
    lowered = normalized.lower()
    if ";" in normalized:
        raise UnsafeQueryError("Only a single SQL statement is allowed")
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise UnsafeQueryError("Only SELECT or WITH queries are allowed")

    banned = re.compile(
        r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|merge|call|execute|exec|copy)\b",
        flags=re.I,
    )
    if banned.search(lowered):
        raise UnsafeQueryError("Only read-only SQL is supported")
    return normalized


def _loads_json(raw: Optional[str], fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ReportingDatabaseError(f"Invalid JSON configuration: {exc}") from exc


def _infer_value_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, (dict, list)):
        return "json"
    return "string"


class ReportingDatabaseService:
    @staticmethod
    def build_url(connection: models.ReportingDbConnection) -> str:
        db_type = (connection.db_type or "postgresql").strip().lower()
        if db_type == "sqlite":
            db_name = (connection.database_name or "").strip() or ":memory:"
            if db_name == ":memory:":
                return "sqlite:///:memory:"
            db_path = Path(db_name)
            if not db_path.is_absolute():
                db_path = Path.cwd() / db_path
            return f"sqlite:///{db_path.as_posix()}"

        host = (connection.host or "").strip()
        database_name = (connection.database_name or "").strip()
        username = quote_plus((connection.username or "").strip())
        password = quote_plus(decrypt_value(connection.password_enc or ""))

        if not host or not database_name:
            raise ReportingDatabaseError("Host and database name are required")

        port = f":{connection.port}" if connection.port else ""
        auth_part = ""
        if username:
            auth_part = username
            if password:
                auth_part += f":{password}"
            auth_part += "@"

        if db_type in {"postgresql", "postgres", "pgsql"}:
            return f"postgresql+psycopg2://{auth_part}{host}{port}/{database_name}"
        if db_type in {"mysql", "mariadb"}:
            return f"mysql+pymysql://{auth_part}{host}{port}/{database_name}"
        if db_type in {"sqlserver", "mssql"}:
            return (
                f"mssql+pyodbc://{auth_part}{host}{port}/{database_name}"
                "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
            )
        raise ReportingDatabaseError(f"Unsupported db_type: {connection.db_type}")

    @staticmethod
    def create_engine_for(connection: models.ReportingDbConnection):
        url = ReportingDatabaseService.build_url(connection)
        connect_args = {}
        if url.startswith("sqlite:///"):
            connect_args["check_same_thread"] = False
        return create_engine(url, pool_pre_ping=True, connect_args=connect_args)

    @staticmethod
    def test_connection(connection: models.ReportingDbConnection) -> Dict[str, Any]:
        engine = ReportingDatabaseService.create_engine_for(connection)
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return {"success": True, "message": "Connection successful"}
        finally:
            engine.dispose()

    @staticmethod
    def list_tables(connection: models.ReportingDbConnection, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
        engine = ReportingDatabaseService.create_engine_for(connection)
        try:
            inspector = inspect(engine)
            schema = schema_name or connection.schema_name or None
            tables = []
            for table_name in inspector.get_table_names(schema=schema):
                columns = inspector.get_columns(table_name, schema=schema)
                tables.append(
                    {
                        "table_name": table_name,
                        "schema_name": schema,
                        "columns": [
                            {"name": col["name"], "type": str(col.get("type") or "")}
                            for col in columns
                        ],
                    }
                )
            return tables
        finally:
            engine.dispose()

    @staticmethod
    def execute_dataset(
        connection: models.ReportingDbConnection,
        dataset: models.ReportingDataset,
        params: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        db_session: Optional[Session] = None,
        user_context: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        return ReportingDatabaseService.execute_query(
            connection=connection,
            sql_text=dataset.sql_text,
            params_json=dataset.params_json,
            params=params,
            limit=limit,
            default_limit=dataset.row_limit,
            db_session=db_session,
            user_context=user_context,
        )

    @staticmethod
    def execute_query(
        connection: models.ReportingDbConnection,
        sql_text: str,
        params_json: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        default_limit: Optional[int] = None,
        db_session: Optional[Session] = None,
        user_context: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        preloaded_vars: Optional[Dict[str, str]] = None
        if db_session is not None:
            preloaded_vars = build_variable_map(db_session, user_context=user_context)
            sql_text = resolve_variables(
                sql_text,
                db_session,
                preloaded_vars=preloaded_vars,
                user_context=user_context,
            )

        sql_text = _ensure_readonly_sql(sql_text)
        engine = ReportingDatabaseService.create_engine_for(connection)
        effective_limit = max(1, min(limit or default_limit or 500, 5000))
        merged_params = {}
        merged_params.update(_loads_json(params_json, {}))
        merged_params.update(params or {})
        if db_session is not None and merged_params:
            merged_params = resolve_dict_variables(
                merged_params,
                db_session,
                preloaded_vars=preloaded_vars,
                user_context=user_context,
            )

        try:
            with engine.connect() as conn:
                result = conn.execute(text(sql_text), merged_params)
                rows = [dict(row) for row in result.mappings().fetchmany(effective_limit)]
                columns = list(result.keys())
        finally:
            engine.dispose()

        column_meta = []
        for name in columns:
            sample = next((row.get(name) for row in rows if row.get(name) is not None), None)
            column_meta.append(
                {
                    "name": name,
                    "type": _infer_value_type(sample),
                    "sample": sample,
                }
            )

        numeric_summary: Dict[str, float] = {}
        for column in columns:
            values = [row.get(column) for row in rows if isinstance(row.get(column), (int, float))]
            if values:
                numeric_summary[column] = float(sum(values))

        return {
            "columns": column_meta,
            "rows": rows,
            "row_count": len(rows),
            "limit": effective_limit,
            "numeric_summary": numeric_summary,
        }
