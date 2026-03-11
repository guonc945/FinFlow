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
    print("Migrating voucher tables...")
    
    inspector = inspect(engine)
    
    # Tables
    v_cols = [c['name'] for c in inspector.get_columns('voucher_template')]
    r_cols = [c['name'] for c in inspector.get_columns('voucher_entry_rule')]
    
    # Voucher Template Columns
    if 'book_number_expr' not in v_cols:
        conn.execute(text("ALTER TABLE voucher_template ADD COLUMN book_number_expr VARCHAR(100) DEFAULT '''BU-35256''';"))
    if 'vouchertype_number_expr' not in v_cols:
        conn.execute(text("ALTER TABLE voucher_template ADD COLUMN vouchertype_number_expr VARCHAR(100) DEFAULT '''0001''';"))
    if 'attachment_expr' not in v_cols:
        conn.execute(text("ALTER TABLE voucher_template ADD COLUMN attachment_expr VARCHAR(100) DEFAULT '0';"))
    if 'priority' not in v_cols:
        conn.execute(text("ALTER TABLE voucher_template ADD COLUMN priority INTEGER DEFAULT 100;"))
        
    # Entry Rule Columns
    if 'currency_expr' not in r_cols:
        conn.execute(text("ALTER TABLE voucher_entry_rule ADD COLUMN currency_expr VARCHAR(100) DEFAULT '''CNY''';"))
    if 'localrate_expr' not in r_cols:
        conn.execute(text("ALTER TABLE voucher_entry_rule ADD COLUMN localrate_expr VARCHAR(100) DEFAULT '1';"))
        
    print("Migration completed.")
