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
    print("Adding missing columns to external_apis...")
    
    inspector = inspect(engine)
    existing_columns = [c['name'] for c in inspector.get_columns('external_apis')]
    
    columns_to_add = {
        'request_headers': "TEXT",
        'request_body': "TEXT",
        'response_example': "TEXT",
        'notes': "TEXT"
    }
    
    for col_name, col_type in columns_to_add.items():
        if col_name not in existing_columns:
            conn.execute(text(f"ALTER TABLE external_apis ADD COLUMN {col_name} {col_type};"))
            print(f"Added column: {col_name}")
        else:
            print(f"Column already exists: {col_name}")
    
    print("Migration completed.")
