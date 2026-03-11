import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os
import time
from sync_tracker import tracker
from utils.marki_client import marki_client, get_api_url

load_dotenv()

# Moved inside functions to ensure DB is available when calling get_api_url
# url = get_api_url("getBillList")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def validate_timestamp(timestamp):
    """验证时间戳是否有效，如果是无效则返回None"""
    try:
        if timestamp is None:
            return None
        if isinstance(timestamp, str):
            ts = timestamp.strip()
            if not ts:
                return None
            if not ts.isdigit():
                return None
            timestamp = int(ts)
        timestamp = int(timestamp)
        if timestamp > 0:
            return timestamp
    except Exception:
        return None
    return None


def normalize_datetime(value):
    """Normalize Marki date/time values to python datetime (or None)."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)) and value > 0:
            return datetime.fromtimestamp(int(value))
        if isinstance(value, str):
            v = value.strip()
            if not v:
                return None
            if v.isdigit():
                return datetime.fromtimestamp(int(v))
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except Exception:
                return None
    except Exception:
        return None
    return None

def format_amount(val):
    """将金额从分转为元，使用 Decimal 确保金融精度（除以100并保留2位小数）"""
    if val is not None:
        try:
            from decimal import Decimal, ROUND_HALF_UP
            # 如果是浮点数传入，先转成字符串确保不丢失原有的显示精度
            dec_val = Decimal(str(val))
            result = dec_val / Decimal('100')
            # 转换为 float 返回，因为最终插入 DB 时 psycopg2 也会处理，
            # 若强需求可以是 float 或者 Decimal，返回 float 加 round 主要是消除显示问题
            return float(result.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        except (ValueError, TypeError, Exception):
            return val
    return None

def insert_bills_data(data_list, community_id_context=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    inserted_count = 0
    skipped_count = 0
    
    try:
        for item in data_list:
            # 必要字段
            bill_id = item.get("id")
            # 优先使用传入的上下文ID，确保数据正确归类
            community_id = community_id_context or item.get("communityId") or item.get("communityID")
            
            if bill_id is None or community_id is None:
                skipped_count += 1
                continue

            try:
                bill_id = int(bill_id)
                community_id = int(community_id)
            except Exception:
                skipped_count += 1
                continue
            
            # 基础映射字段（缺失则为 None）
            charge_item_id = item.get("chargeItemID")
            ci_snapshot_id = item.get("ciSnapshotId")
            charge_item_name = item.get("chargeItemName")
            charge_item_type = item.get("chargeItemType")
            category_name = item.get("categoryName")
            asset_id = item.get("assetID")
            asset_name = item.get("assetName")
            asset_type = item.get("assetType")
            asset_type_str = item.get("assetTypeStr")
            house_id = item.get("houseId")
            full_house_name = item.get("FullHouseName")
            bind_house_id = item.get("bindHouseInfo", {}).get("id")
            bind_house_name = item.get("bindHouseInfo", {}).get("name")
            park_id = item.get("parkId")
            park_name = item.get("parkName")
            bill_month = None
            in_month = item.get("inMonth")
            start_time = validate_timestamp(item.get("startTime"))
            end_time = validate_timestamp(item.get("endTime"))
            pay_time = validate_timestamp(item.get("payTime"))
            receive_date = None
            if pay_time:
                receive_date = datetime.fromtimestamp(pay_time).date()
            create_time = validate_timestamp(item.get("createTime"))
            amount = format_amount(item.get("amount"))
            bill_amount = format_amount(item.get("billAmount"))
            discount_amount = format_amount(item.get("discountAmount"))
            late_money_amount = format_amount(item.get("lateMoneyAmount"))
            deposit_amount = format_amount(item.get("depositAmount"))
            second_pay_amount = format_amount(item.get("secondPayAmount"))
            pay_status = item.get("payStatus")
            pay_status_str = item.get("payStatusStr")
            pay_type = item.get("payType")
            pay_type_str = item.get("payTypeStr")
            second_pay_channel = item.get("secondPayChannel")
            bill_type = item.get("billType")
            bill_type_str = item.get("billTypeStr")
            deal_log_id = item.get("dealLogId")
            receipt_id = item.get("receiptId")
            sub_mch_id = item.get("subMchId")
            sub_mch_name = item.get("subMchName")
            bad_bill_state = item.get("badBillState")
            is_bad_bill = item.get("isBadBill")
            has_split = item.get("hasSplit")
            split_desc = item.get("splitDesc")
            visible_type = item.get("visibleInfo", {}).get("visibleType")
            visible_desc_str = item.get("visibleInfo", {}).get("visibleDescStr")
            can_revoke = item.get("canRevoke")
            version = item.get("version")
            meter_type = item.get("meterType")
            snapshot_size = item.get("snapshotSize")
            now_size = item.get("nowSize")
            remark = item.get("remark")
            bind_toll = json.dumps(item.get("bindToll", []), ensure_ascii=False)
            user_list = json.dumps(item.get("userList", []), ensure_ascii=False)
            last_op_time = normalize_datetime(item.get("lastOpTime"))
            
            # 插入语句（列顺序与模型保持一致）
            cursor.execute(
                """
                INSERT INTO bills (
                    id, community_id, charge_item_id, ci_snapshot_id, charge_item_name, charge_item_type, category_name,
                    asset_id, asset_name, asset_type, asset_type_str,
                    house_id, full_house_name, bind_house_id, bind_house_name,
                    park_id, park_name,
                    bill_month, in_month, start_time, end_time,
                    amount, bill_amount, discount_amount, late_money_amount, deposit_amount, second_pay_amount,
                    pay_status, pay_status_str, pay_type, pay_type_str, pay_time, receive_date, second_pay_channel,
                    bill_type, bill_type_str, deal_log_id, receipt_id, sub_mch_id, sub_mch_name,
                    bad_bill_state, is_bad_bill, has_split, split_desc,
                    visible_type, visible_desc_str, can_revoke, version, meter_type, snapshot_size, now_size,
                    remark, bind_toll, user_list, create_time, last_op_time
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (id, community_id) DO UPDATE SET
                    charge_item_id = EXCLUDED.charge_item_id,
                    ci_snapshot_id = EXCLUDED.ci_snapshot_id,
                    charge_item_name = EXCLUDED.charge_item_name,
                    charge_item_type = EXCLUDED.charge_item_type,
                    category_name = EXCLUDED.category_name,
                    asset_id = EXCLUDED.asset_id,
                    asset_name = EXCLUDED.asset_name,
                    asset_type = EXCLUDED.asset_type,
                    asset_type_str = EXCLUDED.asset_type_str,
                    house_id = EXCLUDED.house_id,
                    full_house_name = EXCLUDED.full_house_name,
                    bind_house_id = EXCLUDED.bind_house_id,
                    bind_house_name = EXCLUDED.bind_house_name,
                    park_id = EXCLUDED.park_id,
                    park_name = EXCLUDED.park_name,
                    bill_month = EXCLUDED.bill_month,
                    in_month = EXCLUDED.in_month,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    amount = EXCLUDED.amount,
                    bill_amount = EXCLUDED.bill_amount,
                    discount_amount = EXCLUDED.discount_amount,
                    late_money_amount = EXCLUDED.late_money_amount,
                    deposit_amount = EXCLUDED.deposit_amount,
                    second_pay_amount = EXCLUDED.second_pay_amount,
                    pay_status = EXCLUDED.pay_status,
                    pay_status_str = EXCLUDED.pay_status_str,
                    pay_type = EXCLUDED.pay_type,
                    pay_type_str = EXCLUDED.pay_type_str,
                    pay_time = EXCLUDED.pay_time,
                    receive_date = EXCLUDED.receive_date,
                    second_pay_channel = EXCLUDED.second_pay_channel,
                    bill_type = EXCLUDED.bill_type,
                    bill_type_str = EXCLUDED.bill_type_str,
                    deal_log_id = EXCLUDED.deal_log_id,
                    receipt_id = EXCLUDED.receipt_id,
                    sub_mch_id = EXCLUDED.sub_mch_id,
                    sub_mch_name = EXCLUDED.sub_mch_name,
                    bad_bill_state = EXCLUDED.bad_bill_state,
                    is_bad_bill = EXCLUDED.is_bad_bill,
                    has_split = EXCLUDED.has_split,
                    split_desc = EXCLUDED.split_desc,
                    visible_type = EXCLUDED.visible_type,
                    visible_desc_str = EXCLUDED.visible_desc_str,
                    can_revoke = EXCLUDED.can_revoke,
                    version = EXCLUDED.version,
                    meter_type = EXCLUDED.meter_type,
                    snapshot_size = EXCLUDED.snapshot_size,
                    now_size = EXCLUDED.now_size,
                    remark = EXCLUDED.remark,
                    bind_toll = EXCLUDED.bind_toll,
                    user_list = EXCLUDED.user_list,
                    last_op_time = EXCLUDED.last_op_time
                """,
                (
                    bill_id, community_id, charge_item_id, ci_snapshot_id, charge_item_name, charge_item_type, category_name,
                    asset_id, asset_name, asset_type, asset_type_str,
                    house_id, full_house_name, bind_house_id, bind_house_name,
                    park_id, park_name,
                    bill_month, in_month, start_time, end_time,
                    amount, bill_amount, discount_amount, late_money_amount, deposit_amount, second_pay_amount,
                    pay_status, pay_status_str, pay_type, pay_type_str, pay_time, receive_date, second_pay_channel,
                    bill_type, bill_type_str, deal_log_id, receipt_id, sub_mch_id, sub_mch_name,
                    bad_bill_state, is_bad_bill, has_split, split_desc,
                    visible_type, visible_desc_str, can_revoke, version, meter_type, snapshot_size, now_size,
                    remark, bind_toll, user_list, create_time, last_op_time
                )
            )
            
            # 同步写入 bill_users 从表
            cursor.execute("DELETE FROM bill_users WHERE bill_id = %s AND community_id = %s", (bill_id, community_id))
            user_list_raw = item.get("userList", [])
            if isinstance(user_list_raw, list):
                for u in user_list_raw:
                    cursor.execute(
                        """INSERT INTO bill_users (bill_id, community_id, user_id, user_name, is_system)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (bill_id, community_id, u.get("id"), u.get("name", ""), u.get("isSystem", 0))
                    )
            
            inserted_count += 1
        
        conn.commit()
        print(f"Sync: Inserted {inserted_count}, Skipped {skipped_count}")
        return {"inserted": inserted_count, "skipped": skipped_count}
    except Exception as e:
        conn.rollback()
        print(f"Database error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        


def sync_bills_for_community(community_id: int, task_id: str = None):
    """Fetch bills for a single community using pagination.
    
    Args:
        community_id: The community ID to fetch bills for
        task_id: Optional ID to track sync progress
    """
    page = 1
    total_inserted = 0
    total_skipped = 0
    
    current_year = datetime.now().year
    from database import SessionLocal
    from models import ExternalService, ExternalApi
    from utils.variable_parser import resolve_dict_variables
    
    db = SessionLocal()
    try:
        service = db.query(ExternalService).filter_by(service_name="marki").first()
        api = db.query(ExternalApi).filter_by(service_id=service.id, name="getBillList").first()
        
        preloaded_vars = {
            "communityID": str(community_id),
            "community_id": str(community_id),
            "year": str(current_year),
            "CURRENT_YEAR": str(current_year),
            "endMonth": f"{current_year}-12"
        }
        
        try:
            url = get_api_url("getBillList", preloaded_vars=preloaded_vars)
        except Exception as e:
            print(f"Failed to get bill list URL: {e}")
            return 0

        if not api or not api.request_body:
            request_data = {
                "badBillCheck": 0,
                "chargeItemVersion": 2,
                "communityID": community_id,
                "dealLogId": 0,
                "endMonth": f"{current_year}-12",
                "index": "",
                "pageSize": "1000",
                "payStatus": 3
            }
        else:
            import json
            raw_body = json.loads(api.request_body)
            request_data = resolve_dict_variables(raw_body, db, preloaded_vars=preloaded_vars)
            # Ensure types for some commonly used fields if they became strings
            # If resolve_variables returned a string for community_id, we might need it as int 
            # depending on what Marki expects, but requests json serializer usually works fine if API accepts string.
            if "communityID" in request_data and isinstance(request_data["communityID"], str) and request_data["communityID"].isdigit():
                request_data["communityID"] = int(request_data["communityID"])
    finally:
        db.close()
    
    print(f"Starting sync for community {community_id}...")
    
    while True:
        request_data["page"] = page
        try:
            result = marki_client.request("POST", url, json_data=request_data)
        except Exception as e:
            msg = f"Request failed for community {community_id} on page {page}: {e}"
            print(msg)
            if task_id:
                tracker.add_log(task_id, msg, "error")
            break

        # Extract bill list
        bills_data = []
        if "data" in result and isinstance(result["data"], dict) and "list" in result["data"]:
            bills_data = result["data"]["list"]
        elif isinstance(result, dict) and "list" in result:
            bills_data = result["list"]
        elif isinstance(result, list):
            bills_data = result
        else:
            print(f"No valid bill data found for community {community_id}")
            break

        if not bills_data:
            print(f"No more bills for community {community_id} on page {page}.")
            break

        # 传递当前正在同步的园区ID，确保数据库记录正确
        counts = insert_bills_data(bills_data, community_id_context=community_id)
        total_inserted += int(counts.get("inserted", 0) or 0)
        total_skipped += int(counts.get("skipped", 0) or 0)
        msg = (
            f"Community {community_id} - Page {page}: "
            f"fetched {len(bills_data)} inserted {counts.get('inserted', 0)} skipped {counts.get('skipped', 0)}."
        )
        print(msg)
        if task_id:
            tracker.add_log(task_id, msg, "info")
        page += 1
        time.sleep(1)  # Rate limiting
    
    print(f"Completed sync for community {community_id}: inserted {total_inserted}, skipped {total_skipped}.")
    return total_inserted


def sync_bills(community_ids: list = None, task_id: str = None):
    """Fetch bills from external API and insert into database.
    
    Args:
        community_ids: List of community IDs to sync. If None or empty, 
                      uses the default community ID (12382).
        task_id: Optional ID to track sync progress
    """
    if not community_ids:
        # If no specific IDs provided, fetch all from projects list in DB
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT proj_id FROM projects_lists")
            # Convert to internal ids
            community_ids = [row[0] for row in cur.fetchall()]
        except Exception as e:
            print(f"Failed to retrieve community_ids from DB: {e}")
            community_ids = []
        finally:
            if 'cur' in locals():
                cur.close()
            conn.close()

        if not community_ids:
            # Fallback to env var or fail
            fallback_var = os.getenv('MARKI_SYSTEM_ID', '')
            if fallback_var:
                community_ids = [int(fallback_var)]
            else:
                print("No community IDs provided and none found in DB.")
                return 0
    
    if task_id:
        tracker.update_status(task_id, "running")
        tracker.add_log(task_id, f"开始同步 {len(community_ids)} 个园区的数据", "info")

    total_records = 0
    for i, community_id in enumerate(community_ids):
        try:
            if task_id:
                # We update progress to (i) because i communities are fully finished? 
                # Or current is (i+1)? Let's use current index.
                tracker.update_progress(task_id, i, f"园区 ID: {community_id}")
            
            records = sync_bills_for_community(community_id, task_id)
            total_records += records
        except Exception as e:
            msg = f"Error syncing community {community_id}: {e}"
            print(msg)
            if task_id:
                tracker.add_log(task_id, msg, "error")
            continue
    
    if task_id:
        tracker.update_progress(task_id, len(community_ids), "同步完成")
        tracker.update_status(task_id, "completed")
        tracker.add_log(task_id, f"同步完成。共处理 {total_records} 条记录。", "info")

    print(f"Bill sync completed. Total records processed: {total_records}")
    return total_records


if __name__ == "__main__":
    # When run directly, use default community
    sync_bills()
