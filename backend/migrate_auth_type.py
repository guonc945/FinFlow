import models
from database import engine
from sqlalchemy import text

def migrate_db():
    print("Migrating database...")
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Check if column exists
            result = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='external_services' AND column_name='auth_type'"))
            if not result.fetchone():
                print("Adding 'auth_type' column to 'external_services'...")
                conn.execute(text("ALTER TABLE external_services ADD COLUMN auth_type VARCHAR(20) DEFAULT 'oauth2'"))
            else:
                print("'auth_type' column already exists.")
                
            trans.commit()
            print("Migration successful.")
        except Exception as e:
            trans.rollback()
            print(f"Migration failed: {e}")

if __name__ == "__main__":
    migrate_db()
