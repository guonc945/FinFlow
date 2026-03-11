import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def migrate():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
    
    try:
        # 1. 给 bills 表添加字段
        print("Adding receive_date column to bills table...")
        cursor.execute("ALTER TABLE bills ADD COLUMN IF NOT EXISTS receive_date TIMESTAMP;")
        conn.commit()
        
        # 2. 提取存量内容
        print("Populating receive_date from existing pay_time data...")
        # pay_time 是 Unix 时间戳 (seconds), PostgreSQL to_timestamp() 将之转换为 timestamp
        cursor.execute("""
            UPDATE bills 
            SET receive_date = to_timestamp(pay_time)
            WHERE pay_time IS NOT NULL AND receive_date IS NULL;
        """)
        conn.commit()
        print(f"Migration successful. Updated {cursor.rowcount} records.")
        
    except Exception as e:
        conn.rollback()
        print(f"Migration failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    migrate()
