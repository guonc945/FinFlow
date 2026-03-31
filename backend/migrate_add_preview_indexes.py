from sqlalchemy import inspect, text

from database import engine


INDEX_NAME = "ix_bills_community_deal_log"


def index_exists() -> bool:
    inspector = inspect(engine)
    try:
        indexes = inspector.get_indexes("bills")
    except Exception:
        return False
    return any((idx.get("name") or "").lower() == INDEX_NAME.lower() for idx in indexes)


def create_index() -> None:
    if index_exists():
        print(f"Index already exists: {INDEX_NAME}")
        return

    dialect = engine.dialect.name.lower()
    ddl = None

    if dialect in {"mssql"}:
        ddl = f"CREATE INDEX {INDEX_NAME} ON bills (community_id, deal_log_id)"
    elif dialect in {"postgresql"}:
        ddl = f"CREATE INDEX IF NOT EXISTS {INDEX_NAME} ON bills (community_id, deal_log_id)"
    elif dialect in {"sqlite"}:
        ddl = f"CREATE INDEX IF NOT EXISTS {INDEX_NAME} ON bills (community_id, deal_log_id)"
    else:
        ddl = f"CREATE INDEX {INDEX_NAME} ON bills (community_id, deal_log_id)"

    with engine.begin() as conn:
        conn.execute(text(ddl))

    print(f"Index created: {INDEX_NAME}")


if __name__ == "__main__":
    create_index()
