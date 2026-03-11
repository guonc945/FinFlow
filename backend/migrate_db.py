from sqlalchemy import create_engine, text
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

with engine.begin() as conn: # engine.begin() handles committing automatically
    print("Adding missing columns to external_services...")
    
    # Check current columns first to avoid errors
    from sqlalchemy import inspect
    inspector = inspect(engine)
    existing_columns = [c['name'] for c in inspector.get_columns('external_services')]
    
    if 'auth_method' not in existing_columns:
        conn.execute(text("ALTER TABLE external_services ADD COLUMN auth_method VARCHAR(10) DEFAULT 'POST';"))
        print("Added auth_method")
    
    if 'auth_headers' not in existing_columns:
        conn.execute(text("ALTER TABLE external_services ADD COLUMN auth_headers TEXT;"))
        print("Added auth_headers")
        
    if 'auth_body' not in existing_columns:
        conn.execute(text("ALTER TABLE external_services ADD COLUMN auth_body TEXT;"))
        print("Added auth_body")
    
    print("Migration completed.")
