# -*- coding: utf-8 -*-
import json
import logging
import os

from dotenv import load_dotenv

from database import SessionLocal
from models import ChargeItem, ExternalApi, ProjectList
from utils.api_config import require_api_id
from utils.db_compat import fetch_all_project_ids, upsert_model_row
from utils.marki_client import get_api_url_by_id, marki_client
from utils.variable_parser import build_variable_map, resolve_dict_variables

load_dotenv()

logger = logging.getLogger("charge_item_sync")
MARKI_CHARGE_ITEM_API_ID = require_api_id("MARKI_CHARGE_ITEM_API_ID")


def _extract_charge_item_list(result):
    data_list = []
    has_more = False
    next_index = None

    if isinstance(result, dict):
        data_obj = result.get("data")
        if isinstance(data_obj, dict):
            if isinstance(data_obj.get("list"), list):
                data_list = data_obj.get("list") or []
            elif isinstance(data_obj.get("dataList"), list):
                data_list = data_obj.get("dataList") or []
            has_more = bool(data_obj.get("hasMore") or data_obj.get("has_more") or False)
            next_index = data_obj.get("index")
            if next_index is None:
                next_index = data_obj.get("nextId")
        elif isinstance(data_obj, list):
            data_list = data_obj

        if not data_list and isinstance(result.get("list"), list):
            data_list = result.get("list") or []
        if not has_more and "hasMore" in result:
            has_more = bool(result.get("hasMore"))
        if next_index is None:
            if "index" in result:
                next_index = result.get("index")
            elif "nextId" in result:
                next_index = result.get("nextId")
    elif isinstance(result, list):
        data_list = result

    return data_list, has_more, next_index


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
    base_url = get_api_url_by_id(MARKI_CHARGE_ITEM_API_ID)

    db_session = SessionLocal()
    try:
        api_config = db_session.query(ExternalApi).filter(ExternalApi.id == MARKI_CHARGE_ITEM_API_ID).first()

        preloaded_vars = build_variable_map(db_session)
        preloaded_vars.update({
            "communityID": str(community_id),
            "pageSize": "500",
            "page": "1",
            "pageNo": "1",
            "index": "",
            "nextId": "0",
        })

        method = "GET"
        body_template = {}

        if api_config:
            method = (api_config.method or "GET").upper()
            if api_config.request_body:
                try:
                    body_template = json.loads(api_config.request_body)
                except Exception:
                    logger.error("Failed to parse request_body JSON from database")
        page = 1
        next_index = None
        total_inserted = 0

        logger.info("Start syncing charge items for community %s", community_id)
        while True:
            current_vars = dict(preloaded_vars)
            current_vars.update({"page": str(page), "pageNo": str(page)})
            if next_index is not None:
                current_vars["index"] = str(next_index)
                current_vars["nextId"] = str(next_index)

            if api_config:
                request_data = resolve_dict_variables(body_template, db_session, preloaded_vars=current_vars)
                if not isinstance(request_data, dict):
                    request_data = {}
                if "communityID" not in request_data and "communityId" not in request_data:
                    request_data["communityID"] = int(community_id)
                request_data.setdefault("page", page)
                request_data.setdefault("pageSize", 500)
                if next_index is not None:
                    request_data.setdefault("index", next_index)
                    request_data.setdefault("nextId", next_index)
            else:
                request_data = {
                    "communityID": community_id,
                    "categoryIds": "",
                    "page": page,
                    "pageSize": 500,
                    "version": 1,
                }
                if next_index is not None:
                    request_data["index"] = next_index
                    request_data["nextId"] = next_index

            params = request_data if method == "GET" else None
            json_body = None if method == "GET" else request_data
            result = marki_client.request(method, base_url, params=params, json_data=json_body)

            data_list, has_more, resp_index = _extract_charge_item_list(result)
            if not data_list:
                break

            inserted = insert_charge_items(data_list)
            total_inserted += inserted
            logger.info(
                "Community %s charge items page %s: fetched=%s upserted=%s has_more=%s",
                community_id,
                page,
                len(data_list),
                inserted,
                has_more,
            )

            if not has_more:
                break

            if resp_index is not None and str(resp_index).strip() != "":
                next_index = resp_index
            else:
                next_index = None
            page += 1
    finally:
        db_session.close()

    if total_inserted == 0:
        logger.warning("No charge item data for community %s", community_id)
    return total_inserted


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
