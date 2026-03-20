from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from sqlalchemy import inspect

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "finflow")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

with engine.begin() as conn:
    print("Migrating voucher rules for display_condition_expr...")

    inspector = inspect(engine)
    cols = [c['name'] for c in inspector.get_columns('voucher_entry_rule')]

    if 'display_condition_expr' not in cols:
        conn.execute(text("ALTER TABLE voucher_entry_rule ADD COLUMN display_condition_expr VARCHAR(255) DEFAULT '';"))
        conn.execute(text("UPDATE voucher_entry_rule SET display_condition_expr = '' WHERE display_condition_expr IS NULL;"))
        print("Added display_condition_expr column.")
    else:
        print("Column display_condition_expr already exists.")

    print("Migration completed.")
