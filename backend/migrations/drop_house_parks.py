# -*- coding: utf-8 -*-
"""
迁移脚本：清理历史冗余表 house_parks（方案 A）。

说明：
- 方案 A 中，房屋 - 车位关系以 parks.house_fk -> houses.id 为准。
- 旧的 house_parks 表不再使用，可安全删除。
"""

import os
import psycopg2
from dotenv import load_dotenv


def get_db_connection():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "finflow"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def drop_house_parks():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'house_parks'
            )
            """
        )
        exists = cursor.fetchone()[0]
        if not exists:
            print("house_parks 表不存在，跳过。")
            conn.commit()
            return

        print("正在删除 house_parks 表（CASCADE）...")
        cursor.execute("DROP TABLE IF EXISTS house_parks CASCADE;")
        conn.commit()
        print("删除完成。")
    except Exception as e:
        conn.rollback()
        print(f"删除失败: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    drop_house_parks()

