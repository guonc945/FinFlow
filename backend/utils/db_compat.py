# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple, Type

from sqlalchemy import and_, text
from sqlalchemy.orm import Session


ModelType = Type[Any]


def _is_mssql(db: Session) -> bool:
    bind = db.get_bind()
    return bool(bind and bind.dialect and bind.dialect.name.lower().startswith("mssql"))


def _quote_identifier(name: str) -> str:
    return f"[{str(name).replace(']', ']]')}]"


def _qualified_table_name(model: ModelType) -> str:
    table = model.__table__
    if table.schema:
        return f"{_quote_identifier(table.schema)}.{_quote_identifier(table.name)}"
    return _quote_identifier(table.name)


def _mssql_identity_column_names(db: Session, model: ModelType) -> Set[str]:
    table = model.__table__
    table_name = table.name
    schema_name = table.schema or "dbo"
    full_name = f"{schema_name}.{table_name}"

    sql = """
SELECT c.name
FROM sys.columns AS c
WHERE c.object_id = OBJECT_ID(:full_name)
  AND c.is_identity = 1
"""
    rows = db.execute(text(sql), {"full_name": full_name}).fetchall()
    return {str(row[0]) for row in rows if row and row[0]}


def _iter_chunks(items: List[Dict[str, Any]], size: int):
    if size <= 0:
        raise ValueError("size must be positive")
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _mssql_atomic_upsert(
    db: Session,
    model: ModelType,
    key_values: Dict[str, Any],
    values: Dict[str, Any],
    immutable_fields: Iterable[str] | None = None,
):
    if not key_values:
        raise ValueError("key_values is required for upsert")

    immutable = set(immutable_fields or [])
    table_name = _qualified_table_name(model)

    key_fields = list(key_values.keys())
    update_fields = [field for field in values.keys() if field not in immutable]
    insert_fields = list(dict.fromkeys([*key_fields, *values.keys()]))

    where_clause = " AND ".join(
        f"{_quote_identifier(field)} = :k_{field}" for field in key_fields
    )
    insert_columns = ", ".join(_quote_identifier(field) for field in insert_fields)
    insert_values = ", ".join(f":i_{field}" for field in insert_fields)

    identity_columns = _mssql_identity_column_names(db, model)
    has_identity_insert = any(col in identity_columns for col in insert_fields)

    params: Dict[str, Any] = {}
    for field, val in key_values.items():
        params[f"k_{field}"] = val
        params[f"i_{field}"] = val
    for field, val in values.items():
        params[f"i_{field}"] = val
        if field in update_fields:
            params[f"u_{field}"] = val

    inserted = False
    if update_fields:
        set_clause = ", ".join(
            f"{_quote_identifier(field)} = :u_{field}" for field in update_fields
        )
        update_sql = f"""
UPDATE {table_name} WITH (UPDLOCK, HOLDLOCK)
SET {set_clause}
WHERE {where_clause};
"""
        updated_count = db.execute(text(update_sql), params).rowcount or 0
        if updated_count <= 0:
            core_insert_sql = f"""
INSERT INTO {table_name} ({insert_columns})
SELECT {insert_values}
WHERE NOT EXISTS (
    SELECT 1
    FROM {table_name} WITH (UPDLOCK, HOLDLOCK)
    WHERE {where_clause}
);
"""
            if has_identity_insert:
                db.execute(text(f"SET IDENTITY_INSERT {table_name} ON;"))
                try:
                    inserted = (db.execute(text(core_insert_sql), params).rowcount or 0) > 0
                finally:
                    db.execute(text(f"SET IDENTITY_INSERT {table_name} OFF;"))
            else:
                inserted = (db.execute(text(core_insert_sql), params).rowcount or 0) > 0
    else:
        core_insert_sql = f"""
INSERT INTO {table_name} ({insert_columns})
SELECT {insert_values}
WHERE NOT EXISTS (
    SELECT 1
    FROM {table_name} WITH (UPDLOCK, HOLDLOCK)
    WHERE {where_clause}
);
"""
        if has_identity_insert:
            db.execute(text(f"SET IDENTITY_INSERT {table_name} ON;"))
            try:
                inserted = (db.execute(text(core_insert_sql), params).rowcount or 0) > 0
            finally:
                db.execute(text(f"SET IDENTITY_INSERT {table_name} OFF;"))
        else:
            inserted = (db.execute(text(core_insert_sql), params).rowcount or 0) > 0

    filters = [getattr(model, key) == val for key, val in key_values.items()]
    instance = db.query(model).filter(and_(*filters)).first()
    return instance, inserted


def _mssql_bulk_upsert(
    db: Session,
    model: ModelType,
    rows: List[Dict[str, Any]],
    key_fields: Tuple[str, ...],
    immutable_fields: Iterable[str] | None = None,
):
    if not rows:
        return
    if not key_fields:
        raise ValueError("key_fields is required for bulk upsert")

    immutable = set(immutable_fields or [])
    table_name = _qualified_table_name(model)
    key_set = set(key_fields)

    columns: List[str] = list(key_fields)
    seen = set(columns)
    for row in rows:
        for field in row.keys():
            if field not in seen:
                seen.add(field)
                columns.append(field)

    if any(col not in row for row in rows for col in key_fields):
        raise ValueError("each row must include all key_fields")

    update_fields = [c for c in columns if c not in key_set and c not in immutable]
    # SQL Server has a hard limit of 2100 bound parameters per statement.
    max_rows_per_chunk = max(1, 1800 // max(1, len(columns)))

    for chunk_index, chunk_rows in enumerate(_iter_chunks(rows, max_rows_per_chunk)):
        params: Dict[str, Any] = {}
        values_sql_rows = []
        for row_index, row in enumerate(chunk_rows):
            placeholders = []
            for col in columns:
                param_name = f"b{chunk_index}_{row_index}_{col}"
                placeholders.append(f":{param_name}")
                params[param_name] = row.get(col)
            values_sql_rows.append(f"({', '.join(placeholders)})")

        src_cols = ", ".join(_quote_identifier(c) for c in columns)
        src_values = ",\n    ".join(values_sql_rows)
        join_predicate = " AND ".join(
            f"tgt.{_quote_identifier(k)} = src.{_quote_identifier(k)}" for k in key_fields
        )
        not_exists_predicate = " AND ".join(
            f"existing.{_quote_identifier(k)} = src.{_quote_identifier(k)}" for k in key_fields
        )
        cte = f"WITH src ({src_cols}) AS (SELECT * FROM (VALUES {src_values}) AS v ({src_cols}))"

        if update_fields:
            update_set = ", ".join(
                f"tgt.{_quote_identifier(c)} = src.{_quote_identifier(c)}" for c in update_fields
            )
            update_sql = f"""
{cte}
UPDATE tgt
SET {update_set}
FROM {table_name} AS tgt WITH (UPDLOCK, HOLDLOCK)
JOIN src ON {join_predicate};
"""
            db.execute(text(update_sql), params)

        insert_cols = ", ".join(_quote_identifier(c) for c in columns)
        insert_select_cols = ", ".join(f"src.{_quote_identifier(c)}" for c in columns)
        insert_sql = f"""
{cte}
INSERT INTO {table_name} ({insert_cols})
SELECT {insert_select_cols}
FROM src
WHERE NOT EXISTS (
    SELECT 1
    FROM {table_name} AS existing WITH (UPDLOCK, HOLDLOCK)
    WHERE {not_exists_predicate}
);
"""
        db.execute(text(insert_sql), params)


def upsert_model_row(
    db: Session,
    model: ModelType,
    key_values: Dict[str, Any],
    values: Dict[str, Any],
    immutable_fields: Iterable[str] | None = None,
):
    """Insert or update one ORM row using conflict key fields."""
    if _is_mssql(db):
        return _mssql_atomic_upsert(db, model, key_values, values, immutable_fields)

    immutable = set(immutable_fields or [])

    if not key_values:
        raise ValueError("key_values is required for upsert")

    filters = [getattr(model, key) == val for key, val in key_values.items()]
    existing = db.query(model).filter(and_(*filters)).first()

    if existing is None:
        payload = dict(values)
        payload.update(key_values)
        instance = model(**payload)
        db.add(instance)
        return instance, True

    for field, value in values.items():
        if field in immutable:
            continue
        setattr(existing, field, value)

    return existing, False


def upsert_model_rows(
    db: Session,
    model: ModelType,
    rows: Sequence[Dict[str, Any]],
    key_fields: Sequence[str],
    immutable_fields: Iterable[str] | None = None,
):
    """Bulk insert-or-update rows by key fields."""
    normalized_rows = [dict(row) for row in rows if row]
    key_tuple = tuple(key_fields)
    if not normalized_rows:
        return
    if not key_tuple:
        raise ValueError("key_fields is required for bulk upsert")

    if _is_mssql(db):
        _mssql_bulk_upsert(db, model, normalized_rows, key_tuple, immutable_fields)
        return

    for row in normalized_rows:
        key_values = {k: row[k] for k in key_tuple}
        values = {k: v for k, v in row.items() if k not in key_values}
        upsert_model_row(db, model, key_values, values, immutable_fields)


def fetch_all_project_ids(db: Session, project_model: ModelType) -> Sequence[int]:
    """Return all project IDs from projects list table as ints when possible."""
    project_ids = []
    for row in db.query(project_model.proj_id).all():
        try:
            project_ids.append(int(row.proj_id))
        except (TypeError, ValueError):
            continue
    return project_ids
