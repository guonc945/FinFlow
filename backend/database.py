import os
import re
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base, sessionmaker


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_DIALECT = os.getenv("DB_DIALECT", "postgresql").strip().lower()
DB_CONFIG_FILE = os.getenv("DB_CONFIG_FILE", str((BASE_DIR.parent / "database.txt").resolve())).strip()
DB_USE_CONFIG_FILE = os.getenv("DB_USE_CONFIG_FILE", "true").strip().lower() in {"1", "true", "yes", "on"}


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


def _parse_db_config_file(file_path: str) -> Dict[str, str]:
    if not file_path:
        return {}
    path = Path(file_path)
    if not path.is_absolute():
        path = (BASE_DIR / path).resolve()
    if not path.exists() or not path.is_file():
        return {}

    raw = path.read_text(encoding="utf-8", errors="ignore")
    lines = [line.strip() for line in raw.splitlines() if line.strip()]

    config: Dict[str, str] = {}
    for line in lines:
        parts = re.split(r"[:\uFF1A]", line, maxsplit=1)
        if len(parts) != 2:
            continue
        key = parts[0].strip().lower()
        value = parts[1].strip().rstrip(",;\uFF0C\uFF1B").strip()
        if not value:
            continue

        if "host" in key or "server" in key:
            config["DB_HOST"] = value
        elif "port" in key:
            config["DB_PORT"] = value
        elif (
            "user" in key
            or "account" in key
            or "username" in key
            or "登录名" in key
            or "账号" in key
            or "用户名" in key
        ):
            config["DB_USER"] = value
        elif "pass" in key:
            config["DB_PASSWORD"] = value
        elif "db" in key or "database" in key or "库名" in key or "数据库" in key:
            config["DB_NAME"] = value

    # Fallback for non-ASCII labels with known line order: host, port, user, password.
    values = []
    for line in lines:
        parts = re.split(r"[:\uFF1A]", line, maxsplit=1)
        if len(parts) == 2:
            value = parts[1].strip().rstrip(",;\uFF0C\uFF1B").strip()
            if value:
                values.append(value)
            continue

        # Support plain value-per-line files (host, port, user, password, [database]).
        plain_value = line.strip().rstrip(",;\uFF0C\uFF1B").strip()
        if plain_value:
            values.append(plain_value)

    if "DB_HOST" not in config and len(values) >= 1:
        config["DB_HOST"] = values[0]
    if "DB_PORT" not in config and len(values) >= 2:
        config["DB_PORT"] = values[1]
    if "DB_USER" not in config and len(values) >= 3:
        config["DB_USER"] = values[2]
    if "DB_PASSWORD" not in config and len(values) >= 4:
        config["DB_PASSWORD"] = values[3]
    if "DB_NAME" not in config and len(values) >= 5:
        config["DB_NAME"] = values[4]

    return config


_file_db_config = _parse_db_config_file(DB_CONFIG_FILE) if DB_USE_CONFIG_FILE else {}
_prefer_file_values = DB_DIALECT in {"mssql", "sqlserver", "sql_server"}


def _db_setting(name: str, default: str) -> str:
    if _prefer_file_values:
        file_value = _file_db_config.get(name, "").strip()
        if file_value:
            return file_value

    env_value = os.getenv(name, "").strip()
    if env_value:
        return env_value

    file_value = _file_db_config.get(name, "").strip()
    if file_value:
        return file_value

    return default


DB_HOST = _db_setting("DB_HOST", "localhost")
DB_PORT = _db_setting("DB_PORT", "1433" if _prefer_file_values else "5432")
DB_NAME = _db_setting("DB_NAME", "finflow")
DB_USER = _db_setting("DB_USER", "admin" if _prefer_file_values else "postgres")
DB_PASSWORD = _db_setting("DB_PASSWORD", "")


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
