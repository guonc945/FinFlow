import psycopg2
from dotenv import load_dotenv
import os

def migrate():
    load_dotenv()
    
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )
    
    try:
        cursor = conn.cursor()
        
        # Add column to parks
        cursor.execute("""
        ALTER TABLE parks 
        ADD COLUMN IF NOT EXISTS kingdee_house_id VARCHAR(50);
        """)
        
        conn.commit()
        print("Migration successful: Added kingdee_house_id to parks table.")
        
    except Exception as e:
        conn.rollback()
        print(f"Migration failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    migrate()
