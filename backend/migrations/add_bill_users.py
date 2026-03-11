"""
迁移脚本：创建 bill_users 从表，并从现有 bills.user_list JSON 数据回填
"""
import json
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def migrate():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 1. 删除旧表重建（因为可能已存在但类型不对）
        print("正在创建 bill_users 表...")
        cursor.execute("DROP TABLE IF EXISTS bill_users CASCADE;")
        cursor.execute("""
            CREATE TABLE bill_users (
                id SERIAL PRIMARY KEY,
                bill_id BIGINT NOT NULL,
                community_id BIGINT NOT NULL,
                user_id INTEGER,
                user_name VARCHAR(255),
                is_system INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                CONSTRAINT fk_bill_users_bill
                    FOREIGN KEY (bill_id, community_id)
                    REFERENCES bills (id, community_id)
                    ON DELETE CASCADE
            );
        """)

        # 2. 创建索引
        print("正在创建索引...")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_bill_users_bill_id ON bill_users (bill_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_bill_users_bill_community ON bill_users (bill_id, community_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_bill_users_user_name ON bill_users (user_name);")

        conn.commit()
        print("表和索引创建完成。")

        # 3. 从现有 bills.user_list 回填数据
        print("正在回填历史数据...")
        cursor.execute("SELECT id, community_id, user_list FROM bills WHERE user_list IS NOT NULL AND user_list != '[]'")
        rows = cursor.fetchall()
        
        inserted = 0
        errors = 0
        for bill_id, community_id, user_list_raw in rows:
            try:
                # PostgreSQL 可能已经自动解析为 list，也可能是 JSON 字符串
                if isinstance(user_list_raw, str):
                    users = json.loads(user_list_raw)
                elif isinstance(user_list_raw, list):
                    users = user_list_raw
                else:
                    continue
                    
                if not isinstance(users, list):
                    continue
                for u in users:
                    user_id = u.get("id")
                    user_name = u.get("name", "")
                    is_system = u.get("isSystem", 0)
                    
                    # 检查是否已存在（避免重复）
                    cursor.execute(
                        "SELECT 1 FROM bill_users WHERE bill_id = %s AND community_id = %s AND user_id = %s",
                        (bill_id, community_id, user_id)
                    )
                    if cursor.fetchone():
                        continue
                    
                    cursor.execute(
                        """INSERT INTO bill_users (bill_id, community_id, user_id, user_name, is_system)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (bill_id, community_id, user_id, user_name, is_system)
                    )
                    inserted += 1
            except (json.JSONDecodeError, TypeError) as e:
                errors += 1
                continue
        
        conn.commit()
        print(f"回填完成：成功插入 {inserted} 条客户记录，{errors} 条解析失败。")
        print("迁移全部完成！")

    except Exception as e:
        conn.rollback()
        print(f"迁移失败: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    migrate()
