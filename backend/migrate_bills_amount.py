import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def migrate_bills():
    """将 bills 表里所有的历史金额字段从分转换为元（除以 100 并保留两位小数）"""
    print("Connecting to database...")
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
    
    # 通过 SQL 直接处理所有的金额字段，将它们强转为 numeric 后除以 100 并四舍五入
    query = """
    UPDATE bills SET
        amount = ROUND(CAST(amount AS numeric) / 100.0, 2),
        bill_amount = ROUND(CAST(bill_amount AS numeric) / 100.0, 2),
        discount_amount = ROUND(CAST(discount_amount AS numeric) / 100.0, 2),
        late_money_amount = ROUND(CAST(late_money_amount AS numeric) / 100.0, 2),
        deposit_amount = ROUND(CAST(deposit_amount AS numeric) / 100.0, 2),
        second_pay_amount = ROUND(CAST(second_pay_amount AS numeric) / 100.0, 2)
    """
    
    try:
        cursor.execute(query)
        conn.commit()
        print(f"迁移成功！总共更新了 {cursor.rowcount} 条账单记录。")
    except Exception as e:
        conn.rollback()
        print(f"迁移失败: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    migrate_bills()
