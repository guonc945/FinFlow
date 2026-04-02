import os
from typing import Dict
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base, sessionmaker


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_DIALECT = os.getenv("DB_DIALECT", "postgresql").strip().lower()


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


DB_DRIVER = os.getenv("DB_DRIVER", _detect_sqlserver_driver()).strip()


# 直接从环境变量读取数据库配置
DB_HOST = os.getenv("DB_HOST", "localhost").strip()
DB_PORT = os.getenv("DB_PORT", "1433").strip()
DB_NAME = os.getenv("DB_NAME", "finflow").strip()
DB_USER = os.getenv("DB_USER", "admin").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "").strip()


def _build_database_url() -> str | URL:
    if DATABASE_URL:
        return DATABASE_URL

    if DB_DIALECT in {"mssql", "sqlserver", "sql_server"}:
        return URL.create(
            "mssql+pyodbc",
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_NAME,
            query={
                "driver": DB_DRIVER,
                "TrustServerCertificate": os.getenv("DB_TRUST_SERVER_CERT", "yes"),
                "Encrypt": os.getenv("DB_ENCRYPT", "yes"),
            },
        )

    if DB_DIALECT == "sqlite":
        db_name = DB_NAME if DB_NAME else "finflow.db"
        if db_name == ":memory:":
            return "sqlite:///:memory:"
        db_path = Path(db_name)
        if not db_path.is_absolute():
            db_path = BASE_DIR / db_path
        return f"sqlite:///{db_path.as_posix()}"

    return URL.create(
        "postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=int(DB_PORT),
        database=DB_NAME,
    )


_connect_args: Dict[str, object] = {}
if DB_DIALECT == "sqlite":
    _connect_args["check_same_thread"] = False
if DB_DIALECT in {"mssql", "sqlserver", "sql_server"}:
    _connect_args["timeout"] = int(os.getenv("DB_TIMEOUT", "30"))

engine = create_engine(_build_database_url(), pool_pre_ping=True, connect_args=_connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
