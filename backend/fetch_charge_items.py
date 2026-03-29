import json
import logging
import os

from dotenv import load_dotenv

from database import SessionLocal
from models import ChargeItem, ExternalApi, ProjectList
from utils.db_compat import fetch_all_project_ids, upsert_model_row
from utils.marki_client import get_api_url, marki_client
from utils.variable_parser import build_variable_map, resolve_dict_variables

load_dotenv()

logger = logging.getLogger("charge_item_sync")


def insert_charge_items(data_list):
    db = SessionLocal()
    inserted_count = 0
    skipped_count = 0

    try:
        for item in data_list:
            try:
                raw_item_id = item.get("id")
                if raw_item_id is None:
                    skipped_count += 1
                    continue
                item_id = int(raw_item_id)
            except (TypeError, ValueError):
                logger.warning("Skip invalid charge item id: %s", item.get("id"))
                skipped_count += 1
                continue

            community_id = str(item.get("communityID") or item.get("communityId") or "").strip()
            item_name = item.get("name")
            if not community_id or not item_name:
                skipped_count += 1
                continue

            values = {
                "communityid": community_id,
                "item_name": item_name,
                "charge_type": item.get("chargeType"),
                "charge_type_str": item.get("chargeTypeStr"),
                "category_id": item.get("categoryId"),
                "category_name": item.get("categoryName"),
                "period_type_str": item.get("periodTypeStr"),
                "remark": item.get("remark"),
            }

            upsert_model_row(db, ChargeItem, {"item_id": item_id}, values)
            inserted_count += 1

        db.commit()
        logger.info("Charge item sync completed: processed=%s, skipped=%s", inserted_count, skipped_count)
        return inserted_count
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def sync_charge_items_for_community(community_id: int):
    base_url = get_api_url("getChargeItemList")

    db_session = SessionLocal()
    try:
        api_config = db_session.query(ExternalApi).filter(ExternalApi.name == "getChargeItemList").first()

        preloaded_vars = build_variable_map(db_session)
        preloaded_vars.update({
            "communityID": str(community_id),
            "pageSize": "500",
        })

        params = {}
        json_body = {}
        method = "GET"

        if api_config:
            method = (api_config.method or "GET").upper()
            base_body = {}
            if api_config.request_body:
                try:
                    base_body = json.loads(api_config.request_body)
                except Exception:
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
                "version": 1,
            }

        logger.info("Start syncing charge items for community %s", community_id)
        result = marki_client.request(method, base_url, params=params, json_data=json_body)
    finally:
        db_session.close()

    data_list = []
    if "data" in result:
        if isinstance(result["data"], list):
            data_list = result["data"]
        elif isinstance(result["data"], dict) and isinstance(result["data"].get("list"), list):
            data_list = result["data"]["list"]
    elif isinstance(result, list):
        data_list = result

    if data_list:
        return insert_charge_items(data_list)

    logger.warning("No charge item data for community %s", community_id)
    return 0


def sync_charge_items(community_ids: list = None):
    if not community_ids:
        db = SessionLocal()
        try:
            community_ids = list(fetch_all_project_ids(db, ProjectList))
        except Exception as exc:
            logger.error("Failed to retrieve community IDs from DB: %s", exc)
            community_ids = []
        finally:
            db.close()

        if not community_ids:
            fallback_var = os.getenv("MARKI_SYSTEM_ID", "")
            if fallback_var and fallback_var.isdigit():
                community_ids = [int(fallback_var)]
            else:
                logger.warning("No community IDs provided and none found in DB")
                return 0

    total_all = 0
    for cid in community_ids:
        try:
            total_all += sync_charge_items_for_community(int(cid))
        except Exception as exc:
            logger.error("Charge item sync failed for community %s: %s", cid, exc)
    return total_all


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sync_charge_items()
