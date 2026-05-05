from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

try:
    import pyodbc  # type: ignore
except ImportError as exc:  # pragma: no cover
    raise SystemExit("pyodbc is required to run this migration.") from exc


BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _detect_sqlserver_driver() -> str:
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]
    try:
        installed = set(pyodbc.drivers())
        for driver in preferred:
            if driver in installed:
                return driver
    except Exception:
        pass
    return preferred[0]


def _connect() -> pyodbc.Connection:
    host = os.getenv("DB_HOST", "localhost").strip()
    port = os.getenv("DB_PORT", "1433").strip()
    database = os.getenv("DB_NAME", "finflow").strip()
    user = os.getenv("DB_USER", "sa").strip()
    password = os.getenv("DB_PASSWORD", "").strip()
    driver = os.getenv("DB_DRIVER", _detect_sqlserver_driver()).strip() or _detect_sqlserver_driver()
    encrypt = os.getenv("DB_ENCRYPT", "yes").strip()
    trust_cert = os.getenv("DB_TRUST_SERVER_CERT", "yes").strip()
    timeout = int(os.getenv("DB_TIMEOUT", "30"))

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert};"
    )
    return pyodbc.connect(conn_str, timeout=timeout, autocommit=False)


MIGRATION_SQL = """
SET NOCOUNT ON;

IF OBJECT_ID('dbo.bills', 'U') IS NULL
    THROW 51000, 'Table dbo.bills does not exist.', 1;

IF COL_LENGTH('dbo.bills', 'last_seen_at') IS NULL
    ALTER TABLE dbo.bills ADD last_seen_at DATETIME2 NULL;

IF COL_LENGTH('dbo.bills', 'source_deleted') IS NULL
    ALTER TABLE dbo.bills
    ADD source_deleted BIT NOT NULL
        CONSTRAINT DF_bills_source_deleted DEFAULT 0;

IF COL_LENGTH('dbo.bills', 'source_deleted_at') IS NULL
    ALTER TABLE dbo.bills ADD source_deleted_at DATETIME2 NULL;

IF COL_LENGTH('dbo.bills', 'source_deleted') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1
       FROM sys.default_constraints
       WHERE name = 'DF_bills_source_deleted'
   )
    ALTER TABLE dbo.bills
    ADD CONSTRAINT DF_bills_source_deleted DEFAULT 0 FOR source_deleted;

UPDATE dbo.bills
SET source_deleted = 0
WHERE source_deleted IS NULL;

UPDATE dbo.bills
SET last_seen_at = COALESCE(last_seen_at, updated_at, created_at, GETDATE())
WHERE last_seen_at IS NULL;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.bills')
      AND name = 'ix_bills_community_source_deleted'
)
    CREATE INDEX ix_bills_community_source_deleted
    ON dbo.bills (community_id, source_deleted);

SELECT
    COL_LENGTH('dbo.bills', 'last_seen_at') AS last_seen_at_len,
    COL_LENGTH('dbo.bills', 'source_deleted') AS source_deleted_len,
    COL_LENGTH('dbo.bills', 'source_deleted_at') AS source_deleted_at_len,
    (
        SELECT COUNT(1)
        FROM sys.indexes
        WHERE object_id = OBJECT_ID('dbo.bills')
          AND name = 'ix_bills_community_source_deleted'
    ) AS idx_exists,
    (
        SELECT COUNT(1)
        FROM dbo.bills
        WHERE last_seen_at IS NULL
    ) AS missing_last_seen,
    (
        SELECT COUNT(1)
        FROM dbo.bills
        WHERE source_deleted IS NULL
    ) AS missing_source_deleted;
"""


def main() -> int:
    if os.getenv("DB_DIALECT", "postgresql").strip().lower() not in {"mssql", "sqlserver", "sql_server"}:
        raise SystemExit("This migration is intended for SQL Server only.")

    with _connect() as conn:
        cursor = conn.cursor()
        print("[info] connected to SQL Server, migrating bills soft-delete fields", flush=True)
        print(f"[info] started_at={datetime.now().isoformat(timespec='seconds')}", flush=True)
        cursor.execute(MIGRATION_SQL)
        conn.commit()
        summary = cursor.fetchone()
        print(
            "[done] migration complete "
            f"(last_seen_at_len={int(summary[0] or 0)}, "
            f"source_deleted_len={int(summary[1] or 0)}, "
            f"source_deleted_at_len={int(summary[2] or 0)}, "
            f"idx_exists={int(summary[3] or 0)}, "
            f"missing_last_seen={int(summary[4] or 0)}, "
            f"missing_source_deleted={int(summary[5] or 0)})"
        , flush=True)

        if _truthy(os.getenv("MARKI_BILL_FORCE_FULL_SYNC")):
            print("[info] MARKI_BILL_FORCE_FULL_SYNC is enabled in current env", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
