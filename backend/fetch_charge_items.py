import json
import psycopg2
from dotenv import load_dotenv
import os
import logging
from utils.marki_client import marki_client, get_api_url
from database import SessionLocal
from models import ExternalApi
from utils.variable_parser import resolve_dict_variables, build_variable_map

load_dotenv()

logger = logging.getLogger("charge_item_sync")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def insert_charge_items(data_list):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    inserted_count = 0
    skipped_count = 0
    
    try:
        for item in data_list:
            # 马克联返回的 ID 可能是字符串也可能是整数，强制转为整数以匹配 DB INTEGER
            try:
                item_raw_id = item.get("id")
                if item_raw_id is None:
                    continue
                item_id = int(item_raw_id)
            except (ValueError, TypeError):
                logger.warning(f"跳过无效的 item_id: {item.get('id')}")
                continue

            # communityid 在 DB 中是 VARCHAR(20)，强制转为字符串
            community_id = str(item.get("communityID") or item.get("communityId") or "")
            item_name = item.get("name")
            charge_type = item.get("chargeType")
            charge_type_str = item.get("chargeTypeStr")
            category_id = item.get("categoryId")
            category_name = item.get("categoryName")
            period_type_str = item.get("periodTypeStr")
            remark = item.get("remark")
            
            if not item_id or not community_id or not item_name:
                logger.warning(f"跳过数据不完整: {item}")
                skipped_count += 1
                continue
            
            # 使用 UPSERT
            cursor.execute(
                """
                INSERT INTO charge_items 
                (item_id, communityid, item_name, charge_type, charge_type_str, category_id, category_name, period_type_str, remark) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (item_id) 
                DO UPDATE SET 
                    communityid = EXCLUDED.communityid,
                    item_name = EXCLUDED.item_name,
                    charge_type = EXCLUDED.charge_type,
                    charge_type_str = EXCLUDED.charge_type_str,
                    category_id = EXCLUDED.category_id,
                    category_name = EXCLUDED.category_name,
                    period_type_str = EXCLUDED.period_type_str,
                    remark = EXCLUDED.remark
                """,
                (item_id, community_id, item_name, charge_type, charge_type_str, category_id, category_name, period_type_str, remark)
            )
            inserted_count += 1
            logger.info(f"已同步/更新项目: {item_name} (ID: {item_id})")
        
        conn.commit()
        logger.info(f"同步完成: 成功处理 {inserted_count} 条, 跳过 {skipped_count} 条")
        return inserted_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"数据库操作失败: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def sync_charge_items_for_community(community_id: int):
    """从马克联同步收费项目"""
    base_url = get_api_url("getChargeItemList")
    
    db_session = SessionLocal()
    api_config = db_session.query(ExternalApi).filter(ExternalApi.name == "getChargeItemList").first()
    
    # Pre-parse variables
    preloaded_vars = build_variable_map(db_session)
    preloaded_vars.update({
        "communityID": str(community_id),
        "pageSize": "500"
    })

    params = {}
    json_body = {}
    method = "GET"

    if api_config:
        method = api_config.method or "GET"
        base_body = {}
        if api_config.request_body:
            try:
                base_body = json.loads(api_config.request_body)
            except:
                logger.error("Failed to parse request_body JSON from database")
        
        resolved_body = resolve_dict_variables(base_body, db_session, preloaded_vars=preloaded_vars)
        if method == "GET":
            params = resolved_body
        else:
            json_body = resolved_body
    else:
        params = {
            "communityID": community_id,
            "categoryIds": "",
            "page": 1,
            "pageSize": 500,
            "version": 1
        }
    
    logger.info(f"开始同步园区 {community_id} 的收费项目...")
    try:
        result = marki_client.request(method, base_url, params=params, json_data=json_body)
    finally:
        db_session.close()
    
    data_list = []
    if "data" in result:
        if isinstance(result["data"], list):
            data_list = result["data"]
        elif isinstance(result["data"], dict) and "list" in result["data"] and isinstance(result["data"]["list"], list):
            data_list = result["data"]["list"]
    elif isinstance(result, list):
        data_list = result
    
    if data_list:
        return insert_charge_items(data_list)
    else:
        logger.warning(f"园区 {community_id} 未找到任何有效收费项目")
        return 0

def sync_charge_items(community_ids: list = None):
    if not community_ids:
        # If no specific IDs provided, fetch all from projects list in DB
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT proj_id FROM projects_lists")
            community_ids = [row[0] for row in cur.fetchall()]
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
                community_ids = [int(fallback_var)]
            else:
                logger.warning("No community IDs provided and none found in DB.")
                return 0
                
    total_all = 0
    for cid in community_ids:
        try:
            total_all += sync_charge_items_for_community(cid)
        except Exception as e:
            logger.error(f"同步园区 {cid} 收费项目异常: {e}")
    return total_all

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sync_charge_items()
