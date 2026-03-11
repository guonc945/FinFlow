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

logger = logging.getLogger("house_sync")

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
            CREATE TABLE IF NOT EXISTS houses (
                id SERIAL PRIMARY KEY,
                house_id VARCHAR(50) UNIQUE NOT NULL,
                community_id VARCHAR(50) NOT NULL,
                community_name VARCHAR(255),
                house_name VARCHAR(255) NOT NULL,
                building_id BIGINT,
                building_name VARCHAR(255),
                unit_id BIGINT,
                unit_name VARCHAR(255),
                layer INTEGER,
                building_size DECIMAL(10, 2),
                usable_size DECIMAL(10, 2),
                floor_name VARCHAR(255),
                area DECIMAL(10, 2),
                user_num INTEGER,
                charge_num INTEGER,
                park_num INTEGER,
                car_num INTEGER,
                combina_name VARCHAR(255),
                create_uid BIGINT,
                disable BOOLEAN DEFAULT FALSE,
                expand TEXT,
                expand_info TEXT,
                tag_list TEXT,
                attachment_list TEXT,
                house_type_name VARCHAR(100),
                house_status_name VARCHAR(100),
                kingdee_house_id VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 增量补列（兼容历史数据库）
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS building_id BIGINT")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS unit_id BIGINT")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS unit_name VARCHAR(255)")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS layer INTEGER")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS building_size DECIMAL(10, 2)")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS usable_size DECIMAL(10, 2)")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS user_num INTEGER")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS charge_num INTEGER")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS park_num INTEGER")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS car_num INTEGER")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS combina_name VARCHAR(255)")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS create_uid BIGINT")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS disable BOOLEAN DEFAULT FALSE")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS expand TEXT")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS expand_info TEXT")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS tag_list TEXT")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS attachment_list TEXT")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS house_type_name VARCHAR(100)")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS house_status_name VARCHAR(100)")
        cursor.execute("ALTER TABLE houses ADD COLUMN IF NOT EXISTS kingdee_house_id VARCHAR(50)")

        # 维度表：房屋绑定住户（userList）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS house_users (
                id SERIAL PRIMARY KEY,
                house_fk INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
                origin_id BIGINT,
                item_id BIGINT NOT NULL,
                name VARCHAR(255),
                item_type INTEGER,
                licence VARCHAR(100),
                park_name VARCHAR(255),
                owner_name VARCHAR(255),
                owner_phone VARCHAR(50),
                charge_item_info TEXT,
                start_time BIGINT,
                end_time BIGINT,
                community_name VARCHAR(255),
                natural_period BIGINT,
                period_type INTEGER,
                period_num INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS udx_house_users_house_item ON house_users (house_fk, item_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_house_users_owner_phone ON house_users (owner_phone)")

        # 方案 A：车位关系由 parks 表维护（house_fk 外键），不再创建 house_parks 冗余表
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'parks'
            )
        """)
        parks_exists = cursor.fetchone()[0]
        if parks_exists:
            cursor.execute("ALTER TABLE parks ADD COLUMN IF NOT EXISTS house_id VARCHAR(50)")
            cursor.execute("ALTER TABLE parks ADD COLUMN IF NOT EXISTS house_fk INTEGER")
            cursor.execute("CREATE INDEX IF NOT EXISTS ix_parks_house_id ON parks (house_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS ix_parks_house_fk ON parks (house_fk)")
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"创建 houses 表失败: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_houses(data_list, community_name=None):
    create_table_if_not_exists()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    inserted_count = 0
    skipped_count = 0
    
    try:
        for item in data_list:
            house_id = str(item.get("id", ""))
            # 优先从 API 返回项中获取园区 ID 和名称
            item_community_id = item.get("communityID")
            item_community_name = item.get("communityName")
            
            # 如果项中没有，则使用外部传入的值（或保留原逻辑）
            community_id = str(item_community_id) if item_community_id is not None else str(item.get("communityID", ""))
            current_community_name = item_community_name if item_community_name is not None else community_name
            
            house_name = str(item.get("name", item.get("houseName", "")))

            building_id = item.get("buildingID")
            building_name = str(item.get("buildingName", ""))
            unit_id = item.get("unitID")
            unit_name = str(item.get("unitName", ""))
            layer = item.get("layer")

            building_size = item.get("buildingSize")
            usable_size = item.get("usableSize")

            # 兼容旧字段：如果没有 floorName，则用 layer
            floor_name = str(layer) if layer is not None else str(item.get("floorName", ""))
            area = (
                item.get("buildArea")
                or item.get("area")
                or building_size
                or usable_size
                or 0.0
            )

            user_num = item.get("userNum")
            charge_num = item.get("chargeNum")
            park_num = item.get("parkNum")
            car_num = item.get("carNum")
            combina_name = item.get("combinaName")
            create_uid = item.get("createUid")
            disable = item.get("disable")

            def _to_json_str(v):
                if v is None:
                    return None
                if isinstance(v, str):
                    return v
                try:
                    return json.dumps(v, ensure_ascii=False)
                except Exception:
                    return str(v)

            expand_str = _to_json_str(item.get("expand"))
            expand_info_str = _to_json_str(item.get("ExpandInfo"))
            tag_list_str = _to_json_str(item.get("tagList"))
            attachment_list_str = _to_json_str(item.get("attachmentList"))

            house_type_name = item.get("houseTypeName")
            house_status_name = item.get("houseStatusName")
            
            if not house_id or not community_id or not house_name:
                logger.warning(f"跳过数据不完整: {item}")
                skipped_count += 1
                continue
            
            cursor.execute(
                """
                INSERT INTO houses (
                    house_id, community_id, community_name, house_name,
                    building_id, building_name, unit_id, unit_name, layer,
                    building_size, usable_size, floor_name, area,
                    user_num, charge_num, park_num, car_num,
                    combina_name, create_uid, disable, expand, expand_info,
                    tag_list, attachment_list, house_type_name, house_status_name
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (house_id) DO UPDATE SET
                    community_id = EXCLUDED.community_id,
                    community_name = EXCLUDED.community_name,
                    house_name = EXCLUDED.house_name,
                    building_id = EXCLUDED.building_id,
                    building_name = EXCLUDED.building_name,
                    unit_id = EXCLUDED.unit_id,
                    unit_name = EXCLUDED.unit_name,
                    layer = EXCLUDED.layer,
                    building_size = EXCLUDED.building_size,
                    usable_size = EXCLUDED.usable_size,
                    floor_name = EXCLUDED.floor_name,
                    area = EXCLUDED.area,
                    user_num = EXCLUDED.user_num,
                    charge_num = EXCLUDED.charge_num,
                    park_num = EXCLUDED.park_num,
                    car_num = EXCLUDED.car_num,
                    combina_name = EXCLUDED.combina_name,
                    create_uid = EXCLUDED.create_uid,
                    disable = EXCLUDED.disable,
                    expand = EXCLUDED.expand,
                    expand_info = EXCLUDED.expand_info,
                    tag_list = EXCLUDED.tag_list,
                    attachment_list = EXCLUDED.attachment_list,
                    house_type_name = EXCLUDED.house_type_name,
                    house_status_name = EXCLUDED.house_status_name
                RETURNING id
                """,
                (
                    house_id, community_id, current_community_name, house_name,
                    building_id, building_name, unit_id, unit_name, layer,
                    building_size, usable_size, floor_name, area,
                    user_num, charge_num, park_num, car_num,
                    combina_name, create_uid, disable, expand_str, expand_info_str,
                    tag_list_str, attachment_list_str, house_type_name, house_status_name,
                )
            )
            house_pk_row = cursor.fetchone()
            house_pk = house_pk_row[0] if house_pk_row else None

            if house_pk:
                cursor.execute("DELETE FROM house_users WHERE house_fk = %s", (house_pk,))
                for u in (item.get("userList") or []):
                    cursor.execute(
                        """
                        INSERT INTO house_users (
                            house_fk, origin_id, item_id, name, item_type,
                            licence, park_name, owner_name, owner_phone,
                            charge_item_info, start_time, end_time, community_name,
                            natural_period, period_type, period_num
                        )
                        VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s
                        )
                        ON CONFLICT (house_fk, item_id) DO UPDATE SET
                            origin_id = EXCLUDED.origin_id,
                            name = EXCLUDED.name,
                            item_type = EXCLUDED.item_type,
                            licence = EXCLUDED.licence,
                            park_name = EXCLUDED.park_name,
                            owner_name = EXCLUDED.owner_name,
                            owner_phone = EXCLUDED.owner_phone,
                            charge_item_info = EXCLUDED.charge_item_info,
                            start_time = EXCLUDED.start_time,
                            end_time = EXCLUDED.end_time,
                            community_name = EXCLUDED.community_name,
                            natural_period = EXCLUDED.natural_period,
                            period_type = EXCLUDED.period_type,
                            period_num = EXCLUDED.period_num
                        """,
                        (
                            house_pk,
                            u.get("originId"),
                            u.get("id"),
                            u.get("name"),
                            u.get("itemType"),
                            u.get("licence"),
                            u.get("parkName"),
                            u.get("ownerName"),
                            u.get("ownerPhone"),
                            u.get("chargeItemInfo"),
                            u.get("startTime"),
                            u.get("endTime"),
                            u.get("communityName"),
                            u.get("naturalPeriod"),
                            u.get("periodType"),
                            u.get("periodNum"),
                        ),
                    )

                # 方案 A：车位关系由 parks 表维护，反向回填 parks.house_fk
                try:
                    cursor.execute(
                        "UPDATE parks SET house_fk = %s WHERE house_id = %s",
                        (house_pk, house_id),
                    )
                except Exception:
                    pass

                # 方案 A：house_parks 冗余表已清理
            inserted_count += 1
            logger.info(f"已同步/更新房屋: {house_name} (ID: {house_id})")
        
        conn.commit()
        logger.info(f"同步房屋完成: 成功 {inserted_count} 条, 更新/跳过 {skipped_count} 条")
        return {"inserted": inserted_count, "skipped": skipped_count}
        
    except Exception as e:
        conn.rollback()
        logger.error(f"数据库操作失败: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def sync_houses_for_community(community_id: str, task_id: str = None):
    """同步单个园区的房屋信息（带分页）"""
    page = 1
    total_inserted = 0
    total_skipped = 0
    page_size = 100 # 拉大点提高效率
    
    base_url = get_api_url("getHouseList")
    
    msg = f"正在同步园区 {community_id} 的房屋档案..."
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
    api_config = db_session.query(ExternalApi).filter(ExternalApi.name == "getHouseList").first()
    
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
            
        counts = insert_houses(data_list, community_name)
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

def sync_houses(community_ids: list = None, task_id: str = None):
    """
    同步多个园区的房屋档案
    Args:
        community_ids: 园区 ID 列表
        task_id: 进度跟踪 ID
    """
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
        tracker.add_log(task_id, f"开始同步 {len(community_ids)} 个园区的房屋档案", "info")
        
    total_all = 0
    for i, cid in enumerate(community_ids):
        try:
            if task_id:
                tracker.update_progress(task_id, i, f"园区 ID: {cid}")
            
            inserted = sync_houses_for_community(str(cid), task_id)
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
        tracker.add_log(task_id, f"同步完成。共新增 {total_all} 个房屋档案。", "info")
        
    return total_all

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sync_houses(["10956"])
