import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, create_engine, inspect, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.sql.sqltypes import DATE, DATETIME, Date, DateTime, String, Text


BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = BASE_DIR.parent
load_dotenv(BASE_DIR / ".env")
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

import models


def _detect_sqlserver_driver() -> str:
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]
    try:
        import pyodbc  # type: ignore

        installed = set(pyodbc.drivers())
        for driver in preferred:
            if driver in installed:
                return driver
    except Exception:
        pass
    return preferred[0]


def _split_kv_line(line: str) -> Tuple[str, str]:
    parts = re.split(r"[:\uFF1A]", line, maxsplit=1)
    if len(parts) != 2:
        return "", ""
    key = parts[0].strip()
    value = parts[1].strip().rstrip(",;\uFF0C\uFF1B").strip()
    return key, value


def _parse_database_txt(file_path: Path) -> Dict[str, str]:
    if not file_path.exists():
        return {}

    content = file_path.read_text(encoding="utf-8", errors="ignore")
    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    parsed: Dict[str, str] = {}

    # First try key-word based parsing.
    # Note: avoid broad "name" matches for username; database labels often contain "name".
    for line in lines:
        key, value = _split_kv_line(line)
        if not key or not value:
            continue
        key_l = key.lower()
        if "host" in key_l or "server" in key_l:
            parsed["host"] = value
        elif "port" in key_l:
            parsed["port"] = value
        elif (
            "user" in key_l
            or "username" in key_l
            or "account" in key_l
            or "登录名" in key
            or "账号" in key
            or "用户名" in key
        ):
            parsed["user"] = value
        elif "pass" in key_l:
            parsed["password"] = value
        elif (
            "db" in key_l
            or "database" in key_l
            or "库名" in key
            or "数据库" in key
        ):
            parsed["database"] = value

    # Fallback for files that use non-ASCII labels but fixed line order.
    ordered_values: List[str] = []
    for line in lines:
        key, value = _split_kv_line(line)
        if value:
            ordered_values.append(value)
            continue
        # Support plain value-per-line files (host, port, user, password, [database]).
        if not key and line:
            ordered_values.append(line.strip().rstrip(",;\uFF0C\uFF1B").strip())
    if "host" not in parsed and len(ordered_values) >= 1:
        parsed["host"] = ordered_values[0]
    if "port" not in parsed and len(ordered_values) >= 2:
        parsed["port"] = ordered_values[1]
    if "user" not in parsed and len(ordered_values) >= 3:
        parsed["user"] = ordered_values[2]
    if "password" not in parsed and len(ordered_values) >= 4:
        parsed["password"] = ordered_values[3]
    if "database" not in parsed and len(ordered_values) >= 5:
        parsed["database"] = ordered_values[4]

    return parsed


def _build_postgres_source_url(args: argparse.Namespace) -> URL:
    source_url = (args.source_url or "").strip()
    if source_url:
        return make_url(source_url)

    host = (args.source_host or "").strip() or os.getenv("SOURCE_DB_HOST", "").strip()
    port = (args.source_port or "").strip() or os.getenv("SOURCE_DB_PORT", "5432").strip() or "5432"
    db_name = (args.source_db_name or "").strip() or os.getenv("SOURCE_DB_NAME", "").strip()
    user = (args.source_db_user or "").strip() or os.getenv("SOURCE_DB_USER", "").strip()
    password = (args.source_db_password or "").strip() or os.getenv("SOURCE_DB_PASSWORD", "").strip()

    if not all([host, db_name, user]):
        raise RuntimeError(
            "Source PostgreSQL connection is incomplete. "
            "Please pass --source-url or --source-host/--source-db-name/--source-db-user."
        )

    return URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=db_name,
    )


def _build_sqlserver_target_url(args: argparse.Namespace) -> URL:
    target_url = (args.target_url or "").strip()
    if target_url:
        return make_url(target_url)

    config_path = Path(args.db_config_file or (PROJECT_DIR / "database.txt"))
    cfg = _parse_database_txt(config_path)

    host = (args.target_host or "").strip() or cfg.get("host") or os.getenv("DB_HOST", "localhost")
    port = (args.target_port or "").strip() or cfg.get("port") or os.getenv("DB_PORT", "1433")
    db_name = (args.target_db_name or "").strip() or cfg.get("database") or os.getenv("DB_NAME", "finflow")
    user = (args.target_db_user or "").strip() or cfg.get("user") or os.getenv("DB_USER", "admin")
    password = (args.target_db_password or "").strip() or cfg.get("password") or os.getenv("DB_PASSWORD", "")
    driver = (args.target_driver or "").strip() or os.getenv("DB_DRIVER", _detect_sqlserver_driver())

    return URL.create(
        "mssql+pyodbc",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=db_name,
        query={
            "driver": driver,
            "TrustServerCertificate": "yes",
            "Encrypt": "yes",
        },
    )


def _mask_url(url: URL) -> str:
    rendered = url.render_as_string(hide_password=True)
    return re.sub(r"driver=[^&]+", "driver=<hidden>", rendered, flags=re.I)


def _mssql_qualified_name(table: Table, default_schema: str) -> str:
    schema = table.schema or default_schema
    return f"[{schema}].[{table.name}]"


def _find_identity_column(conn: Connection, table: Table, default_schema: str) -> Optional[str]:
    if conn.dialect.name != "mssql":
        return None

    schema = table.schema or default_schema
    query = text(
        """
        SELECT c.name
        FROM sys.columns AS c
        INNER JOIN sys.tables AS t ON c.object_id = t.object_id
        INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
        WHERE t.name = :table_name
          AND s.name = :schema_name
          AND c.is_identity = 1
        """
    )
    row = conn.execute(query, {"table_name": table.name, "schema_name": schema}).fetchone()
    return row[0] if row else None


def _iter_batches(rows: Iterable, batch_size: int) -> Iterable[List]:
    batch: List = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _copy_table_data(
    source_engine: Engine,
    target_engine: Engine,
    source_table: Table,
    target_table: Table,
    batch_size: int,
    truncate_target: bool,
) -> int:
    source_columns = {col.name for col in source_table.columns}
    common_columns = [col.name for col in target_table.columns if col.name in source_columns]
    if not common_columns:
        return 0

    default_schema = inspect(target_engine).default_schema_name or "dbo"

    with source_engine.connect() as source_conn:
        select_stmt = select(*(source_table.c[col] for col in common_columns))
        result = source_conn.execution_options(stream_results=True).execute(select_stmt)

        with target_engine.begin() as target_conn:
            qualified_name = _mssql_qualified_name(target_table, default_schema)
            if truncate_target:
                target_conn.execute(text(f"DELETE FROM {qualified_name}"))

            identity_col = _find_identity_column(target_conn, target_table, default_schema)
            identity_enabled = False
            if identity_col and identity_col in common_columns:
                target_conn.execute(text(f"SET IDENTITY_INSERT {qualified_name} ON"))
                identity_enabled = True

            inserted = 0
            try:
                insert_stmt = target_table.insert()
                for raw_batch in _iter_batches(result.mappings(), batch_size):
                    payload = []
                    for row in raw_batch:
                        out_row: Dict[str, object] = {}
                        for col in common_columns:
                            value = row.get(col)
                            target_col = target_table.c[col]
                            target_type = target_col.type

                            # SQL Server text columns: serialize JSON/list/dict payloads.
                            if isinstance(value, (dict, list)):
                                if isinstance(target_type, (String, Text)) or "CHAR" in str(target_type).upper() or "TEXT" in str(target_type).upper():
                                    value = json.dumps(value, ensure_ascii=False)

                            # Normalize timezone-aware datetime for SQL Server DateTime columns.
                            if hasattr(value, "tzinfo") and getattr(value, "tzinfo", None) is not None:
                                if isinstance(target_type, (Date, DATE)):
                                    value = value.date()
                                elif isinstance(target_type, (DateTime, DATETIME)):
                                    value = value.replace(tzinfo=None)

                            # If target is DATE but value is datetime, downcast to date.
                            if isinstance(target_type, (Date, DATE)) and hasattr(value, "date"):
                                try:
                                    value = value.date()
                                except Exception:
                                    pass

                            out_row[col] = value
                        payload.append(out_row)
                    if payload:
                        target_conn.execute(insert_stmt, payload)
                        inserted += len(payload)
            finally:
                if identity_enabled:
                    target_conn.execute(text(f"SET IDENTITY_INSERT {qualified_name} OFF"))

            return inserted


def _get_source_community_ids(source_engine: Engine, table_name: str) -> List[int]:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name):
        raise ValueError(f"Invalid table name: {table_name}")

    with source_engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT DISTINCT val
                FROM (
                    SELECT community_id AS val
                    FROM {table_name}
                    WHERE community_id IS NOT NULL
                    UNION
                    SELECT proj_id AS val
                    FROM projects_lists
                    WHERE proj_id IS NOT NULL
                    UNION
                    SELECT community_id AS val
                    FROM community_mapping
                    WHERE community_id IS NOT NULL
                ) AS u
                ORDER BY val
                """
            )
        ).fetchall()
    values: List[int] = []
    for row in rows:
        try:
            values.append(int(row[0]))
        except Exception:
            continue
    return values


def _mssql_get_partition_boundaries(conn: Connection, pf_name: str) -> List[int]:
    rows = conn.execute(
        text(
            """
            SELECT TRY_CONVERT(BIGINT, prv.value) AS boundary_value
            FROM sys.partition_range_values prv
            JOIN sys.partition_functions pf ON prv.function_id = pf.function_id
            WHERE pf.name = :pf_name
            ORDER BY TRY_CONVERT(BIGINT, prv.value)
            """
        ),
        {"pf_name": pf_name},
    ).fetchall()
    vals: List[int] = []
    for row in rows:
        try:
            vals.append(int(row.boundary_value))
        except Exception:
            continue
    return vals


def _mssql_collect_referencing_fk_sql(conn: Connection, ref_table_name: str) -> List[Tuple[str, str, str]]:
    fk_rows = conn.execute(
        text(
            """
            SELECT
                fk.object_id AS fk_id,
                fk.name AS fk_name,
                cs.name AS child_schema,
                ct.name AS child_table,
                rs.name AS ref_schema,
                rt.name AS ref_table,
                fk.delete_referential_action_desc AS delete_action,
                fk.update_referential_action_desc AS update_action
            FROM sys.foreign_keys fk
            JOIN sys.tables ct ON fk.parent_object_id = ct.object_id
            JOIN sys.schemas cs ON ct.schema_id = cs.schema_id
            JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
            JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
            WHERE rt.name = :ref_table
            ORDER BY fk.name
            """
        ),
        {"ref_table": ref_table_name},
    ).fetchall()

    definitions: List[Tuple[str, str, str]] = []
    for fk in fk_rows:
        cols = conn.execute(
            text(
                """
                SELECT
                    pc.name AS parent_col,
                    rc.name AS ref_col
                FROM sys.foreign_key_columns fkc
                JOIN sys.columns pc
                  ON fkc.parent_object_id = pc.object_id
                 AND fkc.parent_column_id = pc.column_id
                JOIN sys.columns rc
                  ON fkc.referenced_object_id = rc.object_id
                 AND fkc.referenced_column_id = rc.column_id
                WHERE fkc.constraint_object_id = :fk_id
                ORDER BY fkc.constraint_column_id
                """
            ),
            {"fk_id": fk.fk_id},
        ).fetchall()
        if not cols:
            continue

        parent_cols = ", ".join(f"[{c.parent_col}]" for c in cols)
        ref_cols = ", ".join(f"[{c.ref_col}]" for c in cols)
        drop_sql = f"ALTER TABLE [{fk.child_schema}].[{fk.child_table}] DROP CONSTRAINT [{fk.fk_name}]"
        delete_action = str(fk.delete_action or "NO_ACTION").replace("_", " ")
        update_action = str(fk.update_action or "NO_ACTION").replace("_", " ")
        create_sql = (
            f"ALTER TABLE [{fk.child_schema}].[{fk.child_table}] WITH CHECK "
            f"ADD CONSTRAINT [{fk.fk_name}] FOREIGN KEY ({parent_cols}) "
            f"REFERENCES [{fk.ref_schema}].[{fk.ref_table}] ({ref_cols}) "
            f"ON DELETE {delete_action} ON UPDATE {update_action}"
        )
        check_sql = f"ALTER TABLE [{fk.child_schema}].[{fk.child_table}] CHECK CONSTRAINT [{fk.fk_name}]"
        definitions.append((drop_sql, create_sql, check_sql))

    return definitions


def _mssql_collect_indexes_on_column_sql(
    conn: Connection,
    table_name: str,
    column_name: str,
) -> List[Tuple[str, str]]:
    idx_rows = conn.execute(
        text(
            """
            SELECT
                i.index_id,
                i.name AS index_name,
                s.name AS schema_name,
                t.name AS table_name,
                i.type_desc,
                i.is_unique,
                i.filter_definition
            FROM sys.indexes i
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = :table_name
              AND i.is_primary_key = 0
              AND i.is_hypothetical = 0
              AND i.name IS NOT NULL
              AND EXISTS (
                    SELECT 1
                    FROM sys.index_columns ic
                    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    WHERE ic.object_id = i.object_id
                      AND ic.index_id = i.index_id
                      AND c.name = :column_name
                )
            ORDER BY i.name
            """
        ),
        {"table_name": table_name, "column_name": column_name},
    ).fetchall()

    statements: List[Tuple[str, str]] = []
    for idx in idx_rows:
        col_rows = conn.execute(
            text(
                """
                SELECT
                    c.name AS col_name,
                    ic.is_descending_key,
                    ic.is_included_column,
                    ic.key_ordinal,
                    ic.index_column_id
                FROM sys.index_columns ic
                JOIN sys.columns c
                  ON ic.object_id = c.object_id
                 AND ic.column_id = c.column_id
                JOIN sys.tables t ON ic.object_id = t.object_id
                WHERE t.name = :table_name
                  AND ic.index_id = :index_id
                ORDER BY ic.key_ordinal, ic.index_column_id
                """
            ),
            {"table_name": table_name, "index_id": idx.index_id},
        ).fetchall()

        key_cols: List[str] = []
        include_cols: List[str] = []
        for col in col_rows:
            if col.is_included_column:
                include_cols.append(f"[{col.col_name}]")
            else:
                direction = "DESC" if col.is_descending_key else "ASC"
                key_cols.append(f"[{col.col_name}] {direction}")
        if not key_cols:
            continue

        unique_sql = "UNIQUE " if idx.is_unique else ""
        index_type = "CLUSTERED" if str(idx.type_desc).upper().startswith("CLUSTERED") else "NONCLUSTERED"
        include_sql = f" INCLUDE ({', '.join(include_cols)})" if include_cols else ""
        filter_sql = f" WHERE {idx.filter_definition}" if idx.filter_definition else ""

        drop_sql = f"DROP INDEX [{idx.index_name}] ON [{idx.schema_name}].[{idx.table_name}]"
        create_sql = (
            f"CREATE {unique_sql}{index_type} INDEX [{idx.index_name}] "
            f"ON [{idx.schema_name}].[{idx.table_name}] ({', '.join(key_cols)})"
            f"{include_sql}{filter_sql}"
        )
        statements.append((drop_sql, create_sql))
    return statements


def _mssql_get_column_type(conn: Connection, table_name: str, column_name: str) -> Optional[str]:
    row = conn.execute(
        text(
            """
            SELECT UPPER(ty.name) AS type_name
            FROM sys.columns c
            JOIN sys.tables t ON c.object_id = t.object_id
            JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            WHERE t.name = :table_name
              AND c.name = :column_name
            """
        ),
        {"table_name": table_name, "column_name": column_name},
    ).fetchone()
    return row.type_name if row else None


def _ensure_mssql_bills_community_bigint_chain(target_engine: Engine) -> None:
    if target_engine.dialect.name != "mssql":
        return

    with target_engine.begin() as conn:
        tables = {row[0] for row in conn.execute(text("SELECT name FROM sys.tables")).fetchall()}
        if "bills" not in tables:
            return

        bills_cid_type = _mssql_get_column_type(conn, "bills", "community_id")
        bill_users_cid_type = (
            _mssql_get_column_type(conn, "bill_users", "community_id")
            if "bill_users" in tables
            else None
        )
        bill_voucher_cid_type = (
            _mssql_get_column_type(conn, "bill_voucher_push_records", "community_id")
            if "bill_voucher_push_records" in tables
            else None
        )

        needs_fix = (
            bills_cid_type != "BIGINT"
            or ("bill_users" in tables and bill_users_cid_type != "BIGINT")
            or ("bill_voucher_push_records" in tables and bill_voucher_cid_type != "BIGINT")
        )
        if not needs_fix:
            return

        print("[INFO] Aligning bills community_id FK chain to BIGINT on SQL Server...")

        fk_sql_defs = _mssql_collect_referencing_fk_sql(conn, "bills")
        for drop_sql, _, _ in fk_sql_defs:
            conn.execute(text(drop_sql))

        post_alter_index_sql: List[str] = []
        for child_table in ("bill_users", "bill_voucher_push_records"):
            if child_table not in tables:
                continue
            idx_sql_defs = _mssql_collect_indexes_on_column_sql(conn, child_table, "community_id")
            for drop_sql, create_sql in idx_sql_defs:
                conn.execute(text(drop_sql))
                post_alter_index_sql.append(create_sql)

        pk_row = conn.execute(
            text(
                """
                SELECT kc.name AS pk_name
                FROM sys.key_constraints kc
                JOIN sys.tables t ON kc.parent_object_id = t.object_id
                WHERE kc.type = 'PK' AND t.name = 'bills'
                """
            )
        ).fetchone()
        pk_name = pk_row.pk_name if pk_row else None
        pk_cols_rows = conn.execute(
            text(
                """
                SELECT c.name AS col_name
                FROM sys.indexes i
                JOIN sys.tables t ON i.object_id = t.object_id
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE t.name = 'bills'
                  AND i.is_primary_key = 1
                ORDER BY ic.key_ordinal
                """
            )
        ).fetchall()
        pk_cols = [row.col_name for row in pk_cols_rows]

        if pk_name:
            conn.execute(text(f"ALTER TABLE [dbo].[bills] DROP CONSTRAINT [{pk_name}]"))

        temp_clustered_name = "IX_bills_tmp_clustered_cid_upgrade"
        temp_clustered_exists = conn.execute(
            text(
                """
                SELECT 1
                FROM sys.indexes i
                JOIN sys.tables t ON i.object_id = t.object_id
                WHERE t.name = 'bills' AND i.name = :idx_name
                """
            ),
            {"idx_name": temp_clustered_name},
        ).fetchone()
        if not temp_clustered_exists:
            conn.execute(
                text(
                    f"CREATE CLUSTERED INDEX [{temp_clustered_name}] "
                    f"ON [dbo].[bills] ([id] ASC, [community_id] ASC) ON [PRIMARY]"
                )
            )

        ps_exists = conn.execute(
            text("SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_bills_community_id'")
        ).fetchone()
        if ps_exists:
            conn.execute(text("DROP PARTITION SCHEME [ps_bills_community_id]"))

        pf_exists = conn.execute(
            text("SELECT 1 FROM sys.partition_functions WHERE name = 'pf_bills_community_id'")
        ).fetchone()
        if pf_exists:
            conn.execute(text("DROP PARTITION FUNCTION [pf_bills_community_id]"))

        temp_clustered_exists = conn.execute(
            text(
                """
                SELECT 1
                FROM sys.indexes i
                JOIN sys.tables t ON i.object_id = t.object_id
                WHERE t.name = 'bills' AND i.name = :idx_name
                """
            ),
            {"idx_name": temp_clustered_name},
        ).fetchone()
        if temp_clustered_exists:
            conn.execute(text(f"DROP INDEX [{temp_clustered_name}] ON [dbo].[bills]"))

        conn.execute(text("ALTER TABLE [dbo].[bills] ALTER COLUMN [community_id] BIGINT NOT NULL"))
        if "bill_users" in tables and _mssql_get_column_type(conn, "bill_users", "community_id") is not None:
            conn.execute(text("ALTER TABLE [dbo].[bill_users] ALTER COLUMN [community_id] BIGINT NOT NULL"))
        if "bill_voucher_push_records" in tables and _mssql_get_column_type(conn, "bill_voucher_push_records", "community_id") is not None:
            conn.execute(text("ALTER TABLE [dbo].[bill_voucher_push_records] ALTER COLUMN [community_id] BIGINT NOT NULL"))

        for create_sql in post_alter_index_sql:
            conn.execute(text(create_sql))

        if pk_name and pk_cols:
            pk_cols_sql = ", ".join(f"[{c}] ASC" for c in pk_cols)
            conn.execute(
                text(
                    f"ALTER TABLE [dbo].[bills] ADD CONSTRAINT [{pk_name}] "
                    f"PRIMARY KEY CLUSTERED ({pk_cols_sql}) ON [PRIMARY]"
                )
            )

        for _, create_sql, check_sql in fk_sql_defs:
            conn.execute(text(create_sql))
            conn.execute(text(check_sql))

        print("[INFO] bills community_id chain aligned to BIGINT.")


def _ensure_mssql_table_partition_on_community(
    source_engine: Engine,
    target_engine: Engine,
    table_name: str,
    pf_name: str,
    ps_name: str,
    partition_key_sql_type: str = "INT",
) -> None:
    if target_engine.dialect.name != "mssql":
        return

    community_ids = _get_source_community_ids(source_engine, table_name)
    if not community_ids:
        community_ids = [1]
    boundaries_sql = ", ".join(str(v) for v in sorted(set(community_ids)))
    partition_key_sql_type = (partition_key_sql_type or "INT").upper()
    if partition_key_sql_type not in {"INT", "BIGINT"}:
        raise ValueError(f"Unsupported partition key SQL type: {partition_key_sql_type}")

    with target_engine.begin() as conn:
        table_exists = conn.execute(
            text("SELECT 1 FROM sys.tables WHERE name = :table_name"),
            {"table_name": table_name},
        ).fetchone()
        if not table_exists:
            return

        pk_row = conn.execute(
            text(
                """
                SELECT kc.name AS pk_name
                FROM sys.key_constraints kc
                JOIN sys.tables t ON kc.parent_object_id = t.object_id
                WHERE kc.type = 'PK' AND t.name = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchone()
        if not pk_row:
            return
        pk_name = pk_row.pk_name

        pk_cols_rows = conn.execute(
            text(
                """
                SELECT c.name AS col_name
                FROM sys.indexes i
                JOIN sys.tables t ON i.object_id = t.object_id
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE t.name = :table_name
                  AND i.is_primary_key = 1
                ORDER BY ic.key_ordinal
                """
            ),
            {"table_name": table_name},
        ).fetchall()
        pk_cols = [row.col_name for row in pk_cols_rows]
        if not pk_cols:
            return
        if "community_id" not in pk_cols:
            raise RuntimeError(
                f"{table_name} primary key does not include community_id; cannot partition by community_id."
            )

        partitioned_row = conn.execute(
            text(
                """
                SELECT ps.name AS partition_scheme
                FROM sys.indexes i
                JOIN sys.tables t ON i.object_id = t.object_id
                LEFT JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
                LEFT JOIN sys.partition_schemes ps ON ds.data_space_id = ps.data_space_id
                WHERE t.name = :table_name
                  AND i.is_primary_key = 1
                """
            ),
            {"table_name": table_name},
        ).fetchone()
        if partitioned_row and partitioned_row.partition_scheme == ps_name:
            existing_values = set(_mssql_get_partition_boundaries(conn, pf_name))
            desired_values = sorted(set(community_ids))
            split_count = 0
            for value in desired_values:
                if value in existing_values:
                    continue
                conn.execute(text(f"ALTER PARTITION SCHEME [{ps_name}] NEXT USED [PRIMARY]"))
                conn.execute(text(f"ALTER PARTITION FUNCTION [{pf_name}]() SPLIT RANGE ({value})"))
                split_count += 1
            if split_count > 0:
                print(f"[INFO] {table_name} partition function expanded with {split_count} new boundaries.")
            else:
                print(f"[INFO] {table_name} already partitioned on SQL Server partition scheme.")
            return

        fk_sql_defs = _mssql_collect_referencing_fk_sql(conn, table_name)
        for drop_sql, _, _ in fk_sql_defs:
            conn.execute(text(drop_sql))

        pf_exists = conn.execute(
            text("SELECT 1 FROM sys.partition_functions WHERE name = :n"),
            {"n": pf_name},
        ).fetchone()
        pf_type_row = conn.execute(
            text(
                """
                SELECT UPPER(ty.name) AS pf_type
                FROM sys.partition_functions pf
                JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
                JOIN sys.types ty ON ty.system_type_id = pp.system_type_id AND ty.user_type_id = pp.system_type_id
                WHERE pf.name = :n
                """
            ),
            {"n": pf_name},
        ).fetchone()
        if pf_type_row and pf_type_row.pf_type != partition_key_sql_type:
            raise RuntimeError(
                f"Partition function {pf_name} type is {pf_type_row.pf_type}, expected {partition_key_sql_type}."
            )
        if not pf_exists:
            conn.execute(
                text(
                    f"CREATE PARTITION FUNCTION [{pf_name}] ({partition_key_sql_type}) "
                    f"AS RANGE LEFT FOR VALUES ({boundaries_sql})"
                )
            )

        ps_exists = conn.execute(
            text("SELECT 1 FROM sys.partition_schemes WHERE name = :n"),
            {"n": ps_name},
        ).fetchone()
        if not ps_exists:
            conn.execute(
                text(
                    f"CREATE PARTITION SCHEME [{ps_name}] "
                    f"AS PARTITION [{pf_name}] ALL TO ([PRIMARY])"
                )
            )

        conn.execute(text(f"ALTER TABLE [dbo].[{table_name}] DROP CONSTRAINT [{pk_name}]"))
        pk_cols_sql = ", ".join(f"[{c}] ASC" for c in pk_cols)
        conn.execute(
            text(
                f"ALTER TABLE [dbo].[{table_name}] ADD CONSTRAINT [{pk_name}] "
                f"PRIMARY KEY CLUSTERED ({pk_cols_sql}) "
                f"ON [{ps_name}]([community_id])"
            )
        )

        for _, create_sql, check_sql in fk_sql_defs:
            conn.execute(text(create_sql))
            conn.execute(text(check_sql))

    print(f"[INFO] SQL Server {table_name} table partitioning has been applied.")


def _disable_mssql_constraints(engine: Engine) -> None:
    if engine.dialect.name != "mssql":
        return
    with engine.begin() as conn:
        conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'"))


def _enable_mssql_constraints(engine: Engine) -> None:
    if engine.dialect.name != "mssql":
        return
    with engine.begin() as conn:
        # Re-enable constraints for future writes. We intentionally avoid forcing
        # full historical validation because source data may contain legacy FK issues.
        conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL'"))


def _harmonize_mssql_target_schema(engine: Engine) -> None:
    if engine.dialect.name != "mssql":
        return

    with engine.begin() as conn:
        tables = {row[0] for row in conn.execute(text("SELECT name FROM sys.tables")).fetchall()}

        if "charge_items" in tables:
            col = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM sys.columns c
                    JOIN sys.tables t ON c.object_id = t.object_id
                    WHERE t.name='charge_items' AND c.name='updated_at'
                    """
                )
            ).fetchone()
            if not col:
                conn.execute(text("ALTER TABLE [dbo].[charge_items] ADD [updated_at] DATETIME2 NULL"))

        if "projects_lists" in tables:
            col = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM sys.columns c
                    JOIN sys.tables t ON c.object_id = t.object_id
                    WHERE t.name='projects_lists' AND c.name='updated_at'
                    """
                )
            ).fetchone()
            if not col:
                conn.execute(text("ALTER TABLE [dbo].[projects_lists] ADD [updated_at] DATETIME2 NULL"))

        if "bills" in tables:
            bill_cols = {
                row[0]
                for row in conn.execute(
                    text(
                        """
                        SELECT c.name
                        FROM sys.columns c
                        JOIN sys.tables t ON c.object_id = t.object_id
                        WHERE t.name='bills'
                        """
                    )
                ).fetchall()
            }
            if "bill_month" in bill_cols:
                conn.execute(text("ALTER TABLE [dbo].[bills] ALTER COLUMN [bill_month] DATE NULL"))
            if "receive_date" in bill_cols:
                conn.execute(text("ALTER TABLE [dbo].[bills] ALTER COLUMN [receive_date] DATETIME2 NULL"))
            for col_name in ["start_time", "end_time", "pay_time", "deal_log_id", "create_time"]:
                if col_name in bill_cols:
                    conn.execute(text(f"ALTER TABLE [dbo].[bills] ALTER COLUMN [{col_name}] BIGINT NULL"))


def migrate(args: argparse.Namespace) -> None:
    source_url = _build_postgres_source_url(args)
    target_url = _build_sqlserver_target_url(args)

    source_engine = create_engine(source_url, pool_pre_ping=True)
    target_engine = create_engine(target_url, pool_pre_ping=True, connect_args={"timeout": 30})

    print(f"[INFO] Source: {_mask_url(source_url)}")
    print(f"[INFO] Target: {_mask_url(target_url)}")

    if not args.skip_create_tables:
        print("[INFO] Creating target schema from SQLAlchemy models...")
        models.Base.metadata.create_all(bind=target_engine)

    if target_engine.dialect.name == "mssql":
        print("[INFO] Harmonizing SQL Server target schema...")
        _harmonize_mssql_target_schema(target_engine)
        _ensure_mssql_bills_community_bigint_chain(target_engine)

    if target_engine.dialect.name == "mssql":
        print("[INFO] Ensuring bills partitioning on SQL Server...")
        _ensure_mssql_table_partition_on_community(
            source_engine,
            target_engine,
            table_name="bills",
            pf_name="pf_bills_community_id",
            ps_name="ps_bills_community_id",
            partition_key_sql_type="BIGINT",
        )
        print("[INFO] Ensuring receipt_bills partitioning on SQL Server...")
        _ensure_mssql_table_partition_on_community(
            source_engine,
            target_engine,
            table_name="receipt_bills",
            pf_name="pf_receipt_bills_community_id",
            ps_name="ps_receipt_bills_community_id",
            partition_key_sql_type="INT",
        )

    source_meta = MetaData()
    source_meta.reflect(bind=source_engine)
    target_meta = MetaData()
    target_meta.reflect(bind=target_engine)

    target_by_name = {table.name.lower(): table for table in target_meta.sorted_tables}
    table_pairs: List[Tuple[Table, Table]] = []
    for source_table in source_meta.sorted_tables:
        target_table = target_by_name.get(source_table.name.lower())
        if target_table is None:
            continue
        table_pairs.append((source_table, target_table))

    if not table_pairs:
        raise RuntimeError("No overlapping tables found between source and target databases.")

    total_rows = 0
    constraints_toggled = False
    try:
        if target_engine.dialect.name == "mssql":
            print("[INFO] Temporarily disabling SQL Server constraints for bulk copy...")
            _disable_mssql_constraints(target_engine)
            constraints_toggled = True

        for source_table, target_table in table_pairs:
            copied = _copy_table_data(
                source_engine=source_engine,
                target_engine=target_engine,
                source_table=source_table,
                target_table=target_table,
                batch_size=args.batch_size,
                truncate_target=args.truncate_target,
            )
            total_rows += copied
            print(f"[INFO] {source_table.name}: copied {copied} rows")
    finally:
        if constraints_toggled:
            print("[INFO] Re-enabling SQL Server constraints...")
            _enable_mssql_constraints(target_engine)

    print(f"[DONE] Migration finished. Total copied rows: {total_rows}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Migrate data from PostgreSQL to SQL Server 2016.")

    parser.add_argument("--source-url", default="")
    parser.add_argument("--source-host", default="")
    parser.add_argument("--source-port", default="5432")
    parser.add_argument("--source-db-name", default="")
    parser.add_argument("--source-db-user", default="")
    parser.add_argument("--source-db-password", default="")

    parser.add_argument("--target-url", default="")
    parser.add_argument("--target-host", default="")
    parser.add_argument("--target-port", default="")
    parser.add_argument("--target-db-name", default="")
    parser.add_argument("--target-db-user", default="")
    parser.add_argument("--target-db-password", default="")
    parser.add_argument("--target-driver", default=_detect_sqlserver_driver())
    parser.add_argument("--db-config-file", default=str(PROJECT_DIR / "database.txt"))

    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--skip-create-tables", action="store_true")
    parser.add_argument("--truncate-target", action="store_true")
    return parser


if __name__ == "__main__":
    parser = build_arg_parser()
    args = parser.parse_args()
    migrate(args)


