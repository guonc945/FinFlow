# -*- coding: utf-8 -*-
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

try:
    import pyodbc
except ImportError:  # pragma: no cover - optional until SQL Server is used
    pyodbc = None

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session

import models
from utils.crypto import decrypt_value
from utils.variable_parser import build_variable_map, resolve_dict_variables, resolve_variables


class ReportingDatabaseError(Exception):
    """Base error for reporting database operations."""


class UnsafeQueryError(ReportingDatabaseError):
    """Raised when a dataset SQL is not read-only."""


def inject_report_filters(sql_text: str, filters: List[Dict[str, Any]], params: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
    """Inject report filter conditions as WHERE clauses into the SQL.

    For each filter whose key matches a user-provided param value, a
    ``WHERE column = :_filter_<key>`` condition is appended.  Existing
    ``WHERE`` / ``GROUP BY`` / ``ORDER BY`` clauses are handled so the
    injected conditions are placed correctly.

    Returns the modified SQL text and the merged params dict.
    """
    if not filters or not params:
        return sql_text, params

    conditions: List[str] = []
    new_params: Dict[str, Any] = dict(params)

    for flt in filters:
        key = (flt.get("key") or "").strip()
        if not key or key not in params:
            continue
        value = params[key]
        if value is None or value == "":
            continue
        # Sanitize key to only allow safe identifier characters
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            continue
        param_name = f"_filter_{key}"
        flt_type = (flt.get("type") or "text").strip().lower()
        col_ref = f'"{key}"' if re.search(r"[^A-Za-z0-9_]", key) else key

        if flt_type == "number":
            try:
                float(value)
            except (TypeError, ValueError):
                continue
            conditions.append(f"{col_ref} = :{param_name}")
            new_params[param_name] = value
        elif flt_type == "date":
            conditions.append(f"{col_ref} = :{param_name}")
            new_params[param_name] = str(value)
        elif flt_type == "select":
            conditions.append(f"{col_ref} = :{param_name}")
            new_params[param_name] = str(value)
        else:
            # text – use LIKE for partial match
            conditions.append(f"{col_ref} LIKE :{param_name}")
            new_params[param_name] = f"%{value}%"

    if not conditions:
        return sql_text, params

    where_clause = " AND ".join(conditions)

    # Determine where to inject the WHERE clause
    upper = sql_text.upper()
    where_pos = upper.find("WHERE")
    group_pos = upper.find("GROUP BY")
    order_pos = upper.find("ORDER BY")
    having_pos = upper.find("HAVING")
    limit_pos = upper.rfind("LIMIT")

    # Find the earliest clause keyword after the main SELECT body
    later_clauses = [pos for pos in [group_pos, order_pos, having_pos, limit_pos] if pos > 0]
    earliest_later = min(later_clauses) if later_clauses else len(sql_text)

    if where_pos > 0 and where_pos < earliest_later:
        # Already has WHERE – append with AND
        inject_pos = where_pos + 5  # len("WHERE")
        sql_text = sql_text[:inject_pos] + f" ({where_clause}) AND" + sql_text[inject_pos:]
    else:
        # No WHERE – inject before the first later clause or at the end
        sql_text = sql_text[:earliest_later].rstrip() + f" WHERE {where_clause} " + sql_text[earliest_later:]

    return sql_text, new_params


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _convert_decimal_in_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _convert_decimal_in_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_convert_decimal_in_value(item) for item in value]
    return value


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


def _split_param_payload(raw: Optional[str]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    loaded = _loads_json(raw, {})
    if not isinstance(loaded, dict):
        raise ReportingDatabaseError("params_json must be a JSON object")
    meta = loaded.get("__meta__")
    defaults = {key: value for key, value in loaded.items() if key != "__meta__"}
    return defaults, meta if isinstance(meta, dict) else {}


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


def _extract_named_params(sql_text: str) -> List[str]:
    normalized = _normalize_sql(sql_text)
    matches = re.findall(r":([A-Za-z_][A-Za-z0-9_]*)", normalized)
    ordered: List[str] = []
    seen = set()
    for item in matches:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered


def _get_installed_odbc_drivers() -> List[str]:
    if pyodbc is None:
        return []
    try:
        return [item for item in pyodbc.drivers() if item]
    except Exception:
        return []


def _select_sqlserver_driver(options: Dict[str, Any]) -> str:
    configured = str(options.get("driver") or "").strip()
    installed = _get_installed_odbc_drivers()
    if configured:
        if not installed or configured in installed:
            return configured
        raise ReportingDatabaseError(
            f"Configured SQL Server ODBC driver not found: {configured}. "
            f"Installed drivers: {', '.join(installed) or 'none'}"
        )

    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    for driver_name in preferred:
        if driver_name in installed:
            return driver_name

    if installed:
        return installed[-1]

    raise ReportingDatabaseError(
        "No SQL Server ODBC driver is installed. Please install "
        "'ODBC Driver 17 for SQL Server' or newer, or specify a valid driver in connection_options."
    )


class ReportingDatabaseService:
    @staticmethod
    def parse_connection_options(connection: models.ReportingDbConnection) -> Dict[str, Any]:
        raw = _loads_json(connection.connection_options, {})
        if not isinstance(raw, dict):
            raise ReportingDatabaseError("connection_options must be a JSON object")
        return raw

    @staticmethod
    def build_url(connection: models.ReportingDbConnection) -> str:
        db_type = (connection.db_type or "sqlserver").strip().lower()
        options = ReportingDatabaseService.parse_connection_options(connection)
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
            query = ""
            sslmode = str(options.get("sslmode") or "").strip()
            application_name = str(options.get("application_name") or "").strip()
            query_parts = []
            if sslmode:
                query_parts.append(f"sslmode={quote_plus(sslmode)}")
            if application_name:
                query_parts.append(f"application_name={quote_plus(application_name)}")
            if query_parts:
                query = "?" + "&".join(query_parts)
            return f"postgresql+psycopg2://{auth_part}{host}{port}/{database_name}{query}"
        if db_type in {"mysql", "mariadb"}:
            charset = str(options.get("charset") or "").strip()
            query = f"?charset={quote_plus(charset)}" if charset else ""
            return f"mysql+pymysql://{auth_part}{host}{port}/{database_name}{query}"
        if db_type in {"sqlserver", "mssql"}:
            driver = quote_plus(_select_sqlserver_driver(options))
            encrypt = quote_plus(str(options.get("encrypt") or "yes"))
            trust_cert = quote_plus(str(options.get("trust_server_certificate") or "yes"))
            return (
                f"mssql+pyodbc://{auth_part}{host}{port}/{database_name}"
                f"?driver={driver}&TrustServerCertificate={trust_cert}&Encrypt={encrypt}"
            )
        raise ReportingDatabaseError(f"Unsupported db_type: {connection.db_type}")

    @staticmethod
    def create_engine_for(connection: models.ReportingDbConnection):
        url = ReportingDatabaseService.build_url(connection)
        connect_args = {}
        if url.startswith("sqlite:///"):
            connect_args["check_same_thread"] = False
        options = ReportingDatabaseService.parse_connection_options(connection)
        timeout = options.get("connect_timeout")
        if timeout is not None:
            try:
                connect_args["connect_timeout"] = int(timeout)
            except (TypeError, ValueError):
                pass
        return create_engine(url, pool_pre_ping=True, connect_args=connect_args)

    @staticmethod
    def test_connection(connection: models.ReportingDbConnection) -> Dict[str, Any]:
        engine = ReportingDatabaseService.create_engine_for(connection)
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            metadata = ReportingDatabaseService.get_connection_metadata(connection)
            return {"success": True, "message": "Connection successful", "metadata": metadata}
        finally:
            engine.dispose()

    @staticmethod
    def list_schemas(connection: models.ReportingDbConnection) -> List[str]:
        engine = ReportingDatabaseService.create_engine_for(connection)
        try:
            inspector = inspect(engine)
            schemas = [item for item in inspector.get_schema_names() if item]
            return sorted(set(schemas))
        finally:
            engine.dispose()

    @staticmethod
    def get_connection_metadata(connection: models.ReportingDbConnection) -> Dict[str, Any]:
        engine = ReportingDatabaseService.create_engine_for(connection)
        db_type = (connection.db_type or "").strip().lower()
        try:
            inspector = inspect(engine)
            available_schemas = [item for item in inspector.get_schema_names() if item]
            schema = connection.schema_name or None
            table_names = inspector.get_table_names(schema=schema)
            view_names = inspector.get_view_names(schema=schema)
            server_version = None
            current_schema = schema
            with engine.connect() as conn:
                if db_type in {"postgresql", "postgres", "pgsql"}:
                    server_version = conn.execute(text("select version()")).scalar()
                    current_schema = current_schema or conn.execute(text("select current_schema()")).scalar()
                elif db_type in {"mysql", "mariadb"}:
                    server_version = conn.execute(text("select version()")).scalar()
                    current_schema = current_schema or connection.database_name
                elif db_type in {"sqlserver", "mssql"}:
                    server_version = conn.execute(text("select @@VERSION")).scalar()
                    current_schema = current_schema or "dbo"
                elif db_type == "sqlite":
                    server_version = conn.execute(text("select sqlite_version()")).scalar()
                    current_schema = None
            return {
                "connection_id": connection.id,
                "db_type": connection.db_type,
                "database_name": connection.database_name,
                "schema_name": connection.schema_name,
                "server_version": str(server_version) if server_version is not None else None,
                "current_schema": current_schema,
                "available_schemas": sorted(set(available_schemas)),
                "table_count": len(table_names),
                "view_count": len(view_names),
            }
        finally:
            engine.dispose()

    @staticmethod
    def list_tables(connection: models.ReportingDbConnection, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
        engine = ReportingDatabaseService.create_engine_for(connection)
        try:
            inspector = inspect(engine)
            db_type = (connection.db_type or "").strip().lower()
            schema = schema_name or connection.schema_name or None
            if schema is None and db_type in {"sqlserver", "mssql"}:
                schema = "dbo"
            tables = []
            for table_name in inspector.get_table_names(schema=schema):
                tables.append(
                    {
                        "table_name": table_name,
                        "schema_name": schema,
                        "object_type": "table",
                        "columns": [],
                    }
                )
            for view_name in inspector.get_view_names(schema=schema):
                tables.append(
                    {
                        "table_name": view_name,
                        "schema_name": schema,
                        "object_type": "view",
                        "columns": [],
                    }
                )
            return tables
        finally:
            engine.dispose()

    @staticmethod
    def get_table_columns(connection: models.ReportingDbConnection, table_name: str, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
        engine = ReportingDatabaseService.create_engine_for(connection)
        try:
            inspector = inspect(engine)
            db_type = (connection.db_type or "").strip().lower()
            schema = schema_name or connection.schema_name or None
            if schema is None and db_type in {"sqlserver", "mssql"}:
                schema = "dbo"
            columns = inspector.get_columns(table_name, schema=schema)
            return [
                {
                    "name": col["name"],
                    "type": str(col.get("type") or ""),
                    "nullable": bool(col.get("nullable", True)),
                    "default": None if col.get("default") is None else str(col.get("default")),
                }
                for col in columns
            ]
        finally:
            engine.dispose()

    @staticmethod
    def execute_dataset(
        connection: models.ReportingDbConnection,
        dataset: models.ReportingDataset,
        params: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        default_limit: Optional[int] = None,
        db_session: Optional[Session] = None,
        user_context: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        return ReportingDatabaseService.execute_query(
            connection=connection,
            sql_text=dataset.sql_text,
            params_json=dataset.params_json,
            params=params,
            limit=limit,
            default_limit=default_limit,
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
        effective_limit = limit if limit is not None else default_limit
        merged_params = {}
        defaults, _ = _split_param_payload(params_json)
        merged_params.update(defaults)
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
                rows = [dict(row) for row in (result.mappings().fetchmany(effective_limit) if effective_limit else result.mappings().all())]
                columns = list(result.keys())
        finally:
            engine.dispose()

        rows = _convert_decimal_in_value(rows)

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

    @staticmethod
    def validate_query(
        connection: models.ReportingDbConnection,
        sql_text: str,
        params_json: Optional[str] = None,
        default_limit: Optional[int] = None,
        db_session: Optional[Session] = None,
        user_context: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        extracted_params = _extract_named_params(sql_text)
        defaults, _ = _split_param_payload(params_json)
        preview = ReportingDatabaseService.execute_query(
            connection=connection,
            sql_text=sql_text,
            params_json=params_json,
            params=defaults,
            limit=min(default_limit or 100, 100),
            default_limit=default_limit,
            db_session=db_session,
            user_context=user_context,
        )
        warnings: List[str] = []
        missing_defaults = [item for item in extracted_params if item not in defaults]
        if missing_defaults:
            warnings.append("以下 SQL 参数未在参数 JSON 中提供默认值: " + ", ".join(missing_defaults))
        return {
            "connection_id": connection.id,
            "sql_text": sql_text,
            "normalized_sql": _ensure_readonly_sql(sql_text),
            "extracted_params": extracted_params,
            "resolved_defaults": defaults,
            "columns": preview["columns"],
            "preview_row_count": preview["row_count"],
            "limit": preview["limit"],
            "warnings": warnings,
        }
