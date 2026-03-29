from __future__ import annotations

from typing import Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Connection

import database
import models


DEFAULT_PARTITION_TABLES: Sequence[Tuple[str, str, str, str]] = (
    ("bills", "pf_bills_community_id", "ps_bills_community_id", "BIGINT"),
    ("receipt_bills", "pf_receipt_bills_community_id", "ps_receipt_bills_community_id", "INT"),
)


def _is_mssql() -> bool:
    return database.engine.dialect.name == "mssql"


def _normalize_ids(values: Optional[Iterable[int]]) -> List[int]:
    normalized: List[int] = []
    seen = set()
    for raw in values or []:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return sorted(normalized)


def _collect_community_ids_from_db() -> List[int]:
    db = database.SessionLocal()
    try:
        ids = set()
        for row in db.query(models.ProjectList.proj_id).all():
            try:
                ids.add(int(row.proj_id))
            except (TypeError, ValueError):
                continue
        for row in db.query(models.CommunityMapping.community_id).all():
            try:
                ids.add(int(row.community_id))
            except (TypeError, ValueError):
                continue
        return sorted(ids)
    finally:
        db.close()


def _table_exists(conn: Connection, table_name: str) -> bool:
    row = conn.execute(
        text("SELECT 1 FROM sys.tables WHERE name = :table_name"),
        {"table_name": table_name},
    ).fetchone()
    return bool(row)


def _get_pk_info(conn: Connection, table_name: str) -> Tuple[Optional[str], List[str]]:
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
        return None, []

    col_rows = conn.execute(
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
    return pk_row.pk_name, [row.col_name for row in col_rows]


def _get_pk_partition_scheme(conn: Connection, table_name: str) -> Optional[str]:
    row = conn.execute(
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
    return row.partition_scheme if row and row.partition_scheme else None


def _get_column_sql_type(conn: Connection, table_name: str, column_name: str) -> Optional[str]:
    row = conn.execute(
        text(
            """
            SELECT UPPER(ty.name) AS type_name
            FROM sys.columns c
            JOIN sys.tables t ON c.object_id = t.object_id
            JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            WHERE t.name = :table_name AND c.name = :column_name
            """
        ),
        {"table_name": table_name, "column_name": column_name},
    ).fetchone()
    return row.type_name if row else None


def _get_pf_boundaries(conn: Connection, pf_name: str) -> List[int]:
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
    values: List[int] = []
    for row in rows:
        try:
            values.append(int(row.boundary_value))
        except Exception:
            continue
    return values


def _collect_fk_defs(conn: Connection, ref_table_name: str) -> List[Tuple[str, str, str]]:
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


def _ensure_pf_ps(
    conn: Connection,
    pf_name: str,
    ps_name: str,
    boundaries: List[int],
    partition_key_sql_type: str,
) -> None:
    boundaries_sql = ", ".join(str(v) for v in boundaries)
    partition_key_sql_type = (partition_key_sql_type or "INT").upper()
    if partition_key_sql_type not in {"INT", "BIGINT"}:
        raise ValueError(f"Unsupported partition key SQL type: {partition_key_sql_type}")

    pf_exists = conn.execute(
        text("SELECT 1 FROM sys.partition_functions WHERE name = :name"),
        {"name": pf_name},
    ).fetchone()
    pf_type_row = conn.execute(
        text(
            """
            SELECT UPPER(ty.name) AS pf_type
            FROM sys.partition_functions pf
            JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
            JOIN sys.types ty ON ty.system_type_id = pp.system_type_id AND ty.user_type_id = pp.system_type_id
            WHERE pf.name = :name
            """
        ),
        {"name": pf_name},
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
        text("SELECT 1 FROM sys.partition_schemes WHERE name = :name"),
        {"name": ps_name},
    ).fetchone()
    if not ps_exists:
        conn.execute(
            text(
                f"CREATE PARTITION SCHEME [{ps_name}] "
                f"AS PARTITION [{pf_name}] ALL TO ([PRIMARY])"
            )
        )


def _expand_pf_boundaries(conn: Connection, pf_name: str, ps_name: str, boundaries: List[int]) -> int:
    existing = set(_get_pf_boundaries(conn, pf_name))
    split_count = 0
    for value in boundaries:
        if value in existing:
            continue
        conn.execute(text(f"ALTER PARTITION SCHEME [{ps_name}] NEXT USED [PRIMARY]"))
        conn.execute(text(f"ALTER PARTITION FUNCTION [{pf_name}]() SPLIT RANGE ({value})"))
        split_count += 1
    return split_count


def ensure_table_partition_on_community(
    table_name: str,
    partition_function_name: str,
    partition_scheme_name: str,
    community_ids: Optional[Iterable[int]] = None,
    partition_key_sql_type: Optional[str] = None,
) -> bool:
    if not _is_mssql():
        return False

    boundaries = _normalize_ids(community_ids) or _collect_community_ids_from_db()
    if not boundaries:
        boundaries = [1]

    changed = False
    with database.engine.begin() as conn:
        if not _table_exists(conn, table_name):
            return False

        pk_name, pk_cols = _get_pk_info(conn, table_name)
        if not pk_name or not pk_cols:
            return False
        if "community_id" not in pk_cols:
            return False
        if not partition_key_sql_type:
            partition_key_sql_type = _get_column_sql_type(conn, table_name, "community_id") or "INT"

        current_scheme = _get_pk_partition_scheme(conn, table_name)
        if current_scheme == partition_scheme_name:
            split_count = _expand_pf_boundaries(conn, partition_function_name, partition_scheme_name, boundaries)
            return split_count > 0

        fk_defs = _collect_fk_defs(conn, table_name)
        for drop_sql, _, _ in fk_defs:
            conn.execute(text(drop_sql))

        _ensure_pf_ps(
            conn,
            partition_function_name,
            partition_scheme_name,
            boundaries,
            partition_key_sql_type=partition_key_sql_type,
        )
        _expand_pf_boundaries(conn, partition_function_name, partition_scheme_name, boundaries)

        conn.execute(text(f"ALTER TABLE [dbo].[{table_name}] DROP CONSTRAINT [{pk_name}]"))
        pk_cols_sql = ", ".join(f"[{col}] ASC" for col in pk_cols)
        conn.execute(
            text(
                f"ALTER TABLE [dbo].[{table_name}] ADD CONSTRAINT [{pk_name}] "
                f"PRIMARY KEY CLUSTERED ({pk_cols_sql}) "
                f"ON [{partition_scheme_name}]([community_id])"
            )
        )

        for _, create_sql, check_sql in fk_defs:
            conn.execute(text(create_sql))
            conn.execute(text(check_sql))

        changed = True

    return changed


def ensure_default_financial_partitions(community_ids: Optional[Iterable[int]] = None) -> bool:
    changed = False
    for table_name, pf_name, ps_name, partition_key_sql_type in DEFAULT_PARTITION_TABLES:
        changed = ensure_table_partition_on_community(
            table_name,
            pf_name,
            ps_name,
            community_ids,
            partition_key_sql_type=partition_key_sql_type,
        ) or changed
    return changed
