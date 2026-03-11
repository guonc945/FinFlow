import json
import psycopg2
from dotenv import load_dotenv
import os
import logging
from utils.marki_client import marki_client, get_api_url
from sync_tracker import tracker
from database import SessionLocal
from models import ExternalApi
from utils.variable_parser import resolve_dict_variables, build_variable_map

load_dotenv()

logger = logging.getLogger("park_sync")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "finflow"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "")
    )

def create_table_if_not_exists():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS parks (
                id SERIAL PRIMARY KEY,
                park_id VARCHAR(50) NOT NULL,
                community_id VARCHAR(50) NOT NULL,
                community_name VARCHAR(255),
                name VARCHAR(255) NOT NULL,
                park_type_name VARCHAR(50),
                state INTEGER,
                user_name VARCHAR(255),
                house_name VARCHAR(255),
                house_id VARCHAR(50),
                house_fk INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("ALTER TABLE parks ADD COLUMN IF NOT EXISTS house_id VARCHAR(50)")
        cursor.execute("ALTER TABLE parks ADD COLUMN IF NOT EXISTS house_fk INTEGER")
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS udx_parks_park_com ON parks (park_id, community_id)
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_parks_house_id ON parks (house_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_parks_house_fk ON parks (house_fk)")
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"创建 parks 表失败: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_parks(data_list, community_name=None):
    create_table_if_not_exists()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    inserted_count = 0
    skipped_count = 0
    
    try:
        for item in data_list:
            park_id = str(item.get("id", ""))
            
            # 优先从 API 返回项中获取园区 ID 和名称
            item_community_id = item.get("communityId")
            item_community_name = item.get("communityName")
            
            # 如果项中没有，则使用外部传入的值
            community_id = str(item_community_id) if item_community_id is not None else str(item.get("communityId", ""))
            current_community_name = item_community_name if item_community_name is not None else community_name
            
            name = str(item.get("name", ""))
            park_type_name = str(item.get("parkTypeName", ""))
            state = item.get("state")
            
            user_item = item.get("userItem")
            user_name = user_item.get("name") if user_item and isinstance(user_item, dict) else None
            
            house_item = item.get("houseItem")
            house_name = house_item.get("name") if house_item and isinstance(house_item, dict) else None
            house_id = str(house_item.get("id")) if house_item and isinstance(house_item, dict) and house_item.get("id") is not None else None
            
            if not park_id or not community_id or not name:
                logger.warning(f"跳过数据不完整: {item}")
                skipped_count += 1
                continue
            
            cursor.execute(
                """
                INSERT INTO parks (park_id, community_id, community_name, name, park_type_name, state, user_name, house_name, house_id, house_fk) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (park_id, community_id) DO UPDATE SET
                    community_name = EXCLUDED.community_name,
                    name = EXCLUDED.name,
                    park_type_name = EXCLUDED.park_type_name,
                    state = EXCLUDED.state,
                    user_name = EXCLUDED.user_name,
                    house_name = EXCLUDED.house_name,
                    house_id = EXCLUDED.house_id,
                    house_fk = EXCLUDED.house_fk
                """,
                (park_id, community_id, current_community_name, name, park_type_name, state, user_name, house_name, house_id, None)
            )

            # 回填 house_fk（如果 houses 已同步）
            if house_id:
                cursor.execute("SELECT id FROM houses WHERE house_id = %s LIMIT 1", (house_id,))
                row = cursor.fetchone()
                if row:
                    cursor.execute(
                        "UPDATE parks SET house_fk = %s WHERE park_id = %s AND community_id = %s",
                        (row[0], park_id, community_id),
                    )
            inserted_count += 1
        
        conn.commit()
        return {"inserted": inserted_count, "skipped": skipped_count}
        
    except Exception as e:
        conn.rollback()
        logger.error(f"数据库操作失败: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def sync_parks_for_community(community_id: str, task_id: str = None):
    """同步单个园区的车位信息（带分页）"""
    page = 1
    total_inserted = 0
    total_skipped = 0
    page_size = 100 
    
    base_url = get_api_url("getParkList")
    
    msg = f"正在同步园区 {community_id} 的车位档案..."
    logger.info(msg)
    if task_id:
        tracker.add_log(task_id, msg, "info")

    # 获取园区名称
    community_name = None
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT proj_name FROM projects_lists WHERE proj_id = %s", (community_id,))
        row = cursor.fetchone()
        if row:
            community_name = row[0]
    except Exception as e:
        logger.error(f"查询园区名称失败: {e}")
    finally:
        cursor.close()
        conn.close()

    db_session = SessionLocal()
    api_config = db_session.query(ExternalApi).filter(ExternalApi.name == "getParkList").first()
    
    # Pre-parse common variables
    preloaded_vars = build_variable_map(db_session)
    preloaded_vars.update({
        "communityID": community_id,
        "pageSize": str(page_size)
    })

    while True:
        current_vars = preloaded_vars.copy()
        current_vars.update({
            "page": str(page)
        })

        params = {}
        json_body = {}
        method = "GET"

        if api_config:
            method = api_config.method or "GET"
            # Parse configured body (Query parameters for GET)
            base_body = {}
            if api_config.request_body:
                try:
                    base_body = json.loads(api_config.request_body)
                except:
                    logger.error("Failed to parse request_body JSON from database")
            
            # Resolve variables in body
            resolved_body = resolve_dict_variables(base_body, db_session, preloaded_vars=current_vars)
            
            if method == "GET":
                params = resolved_body
            else:
                json_body = resolved_body
        else:
            # Fallback if config not found
            params = {
                "communityID": community_id,
                "page": page,
                "pageSize": page_size
            }
        
        try:
            result = marki_client.request(method, base_url, params=params, json_data=json_body)
        except Exception as e:
            msg = f"园区 {community_id} 第 {page} 页请求失败: {e}"
            logger.error(msg)
            if task_id:
                tracker.add_log(task_id, msg, "error")
            break

        data_list = []
        if isinstance(result, dict):
            if "data" in result:
                if isinstance(result["data"], list):
                    data_list = result["data"]
                elif isinstance(result["data"], dict) and "list" in result["data"]:
                    data_list = result["data"]["list"]
            elif "list" in result:
                 data_list = result["list"]
        elif isinstance(result, list):
            data_list = result
            
        if not data_list:
            break
            
        counts = insert_parks(data_list, community_name)
        total_inserted += counts["inserted"]
        total_skipped += counts["skipped"]
        
        msg = f"园区 {community_id} - 第 {page} 页: 处理 {len(data_list)} 条 (新增 {counts['inserted']})"
        logger.info(msg)
        if task_id:
            tracker.add_log(task_id, msg, "info")
            
        if len(data_list) < page_size:
            break
            
        page += 1
        
    db_session.close()
        
    return total_inserted

def sync_parks(community_ids: list = None, task_id: str = None):
    if not community_ids:
        # If no specific IDs provided, fetch all from projects list in DB
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT proj_id FROM projects_lists")
            community_ids = [str(row[0]) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to retrieve community_ids from DB: {e}")
            community_ids = []
        finally:
            if 'cur' in locals():
                cur.close()
            conn.close()

        if not community_ids:
            # Fallback to env var or default
            fallback_var = os.getenv('MARKI_SYSTEM_ID', '')
            if fallback_var:
                community_ids = [fallback_var]
            else:
                logger.warning("No community IDs provided and none found in DB.")
                return 0
        
    if task_id:
        tracker.update_status(task_id, "running")
        tracker.add_log(task_id, f"开始同步 {len(community_ids)} 个园区的车位档案", "info")
        
    total_all = 0
    for i, cid in enumerate(community_ids):
        try:
            if task_id:
                tracker.update_progress(task_id, i, f"园区 ID: {cid}")
            
            inserted = sync_parks_for_community(str(cid), task_id)
            total_all += inserted
        except Exception as e:
            msg = f"同步园区 {cid} 发生异常: {e}"
            logger.error(msg)
            if task_id:
                tracker.add_log(task_id, msg, "error")
            continue
            
    if task_id:
        tracker.update_progress(task_id, len(community_ids), "同步完成")
        tracker.update_status(task_id, "completed")
        tracker.add_log(task_id, f"同步完成。共新增 {total_all} 个车位档案。", "info")
        
    return total_all

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sync_parks(["10956"])
