import os
import psycopg2
from dotenv import load_dotenv

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
        # 1. 查找并删除外键约束
        print("Dropping foreign key constraints...")
        
        # 查找引用 bills 表的外键
        cursor.execute("""
            SELECT DISTINCT tc.table_name, tc.constraint_name 
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
              AND ccu.table_name = 'bills';
        """)
        fks = cursor.fetchall()
        for table_name, constraint_name in fks:
            print(f"Dropping FK {constraint_name} on {table_name}")
            cursor.execute(f"ALTER TABLE {table_name} DROP CONSTRAINT {constraint_name};")

        # 2. 修改列类型
        print("Altring columns to BIGINT...")
        
        tables_to_fix = ['bills', 'bill_users', 'bill_voucher_push_records']
        for table in tables_to_fix:
            print(f"Fixing table {table}...")
            if table == 'bills':
                cursor.execute(f"ALTER TABLE bills ALTER COLUMN id TYPE BIGINT;")
            else:
                cursor.execute(f"ALTER TABLE {table} ALTER COLUMN bill_id TYPE BIGINT;")

        # 3. 重新建立外键约束
        print("Re-creating foreign key constraints...")
        
        cursor.execute("""
            ALTER TABLE bill_users 
            ADD CONSTRAINT bill_users_bill_id_fkey 
            FOREIGN KEY (bill_id, community_id) 
            REFERENCES bills (id, community_id) ON DELETE CASCADE;
        """)
        
        cursor.execute("""
            ALTER TABLE bill_voucher_push_records 
            ADD CONSTRAINT bill_voucher_push_records_bill_id_fkey 
            FOREIGN KEY (bill_id, community_id) 
            REFERENCES bills (id, community_id) ON DELETE CASCADE;
        """)
        
        conn.commit()
        print("Migration successful: All bill-related IDs are now BIGINT.")
        
    except Exception as e:
        conn.rollback()
        print(f"Migration failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    migrate()
unlink_path = "d:/FinFlow/backend/migrate_bigint_ids.py"
if os.path.exists(unlink_path):
    os.remove(unlink_path)
