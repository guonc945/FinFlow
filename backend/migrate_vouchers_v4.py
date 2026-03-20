from sqlalchemy import create_engine, inspect, text
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "finflow")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

TARGET_COLUMNS = [
    "display_condition_expr",
    "amount_expr",
    "summary_expr",
    "currency_expr",
    "localrate_expr",
]

with engine.begin() as conn:
    print("Migrating voucher_entry_rule expression columns to TEXT...")

    inspector = inspect(engine)
    existing_columns = {col["name"]: col for col in inspector.get_columns("voucher_entry_rule")}

    for column_name in TARGET_COLUMNS:
        if column_name not in existing_columns:
            print(f"Skipping missing column: {column_name}")
            continue

        conn.execute(
            text(f"ALTER TABLE voucher_entry_rule ALTER COLUMN {column_name} TYPE TEXT")
        )
        print(f"Altered {column_name} -> TEXT")

    print("Migration completed.")
