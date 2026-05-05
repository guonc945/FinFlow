# -*- coding: utf-8 -*-
import json
import logging
import os
import time

from dotenv import load_dotenv
from sqlalchemy import and_

from database import SessionLocal
from models import ExternalApi, House, HouseUser, Park, ProjectList
from sync_tracker import tracker
from utils.api_config import require_api_id
from utils.db_compat import fetch_all_project_ids, upsert_model_rows
from utils.marki_client import get_api_url_by_id, marki_client
from utils.variable_parser import build_variable_map, resolve_dict_variables

load_dotenv()

logger = logging.getLogger("house_sync")
DEFAULT_HOUSE_PAGE_SIZE = max(1, int(os.getenv("MARKI_HOUSE_PAGE_SIZE", "500")))
MARKI_HOUSE_API_ID = require_api_id("MARKI_HOUSE_API_ID")


def _to_json_str(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def _iter_chunks(items, size):
    if size <= 0:
        raise ValueError("size must be positive")
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _extract_house_list(result):
    data_list = []
    has_more = None

    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, list):
            data_list = data
        elif isinstance(data, dict):
            if isinstance(data.get("list"), list):
                data_list = data["list"]
            elif isinstance(data.get("rows"), list):
                data_list = data["rows"]
            if "hasMore" in data:
                has_more = bool(data.get("hasMore"))
        elif isinstance(result.get("list"), list):
            data_list = result["list"]
    elif isinstance(result, list):
        data_list = result

    return data_list, has_more


def insert_houses(data_list, community_name=None):
    db = SessionLocal()
    inserted_count = 0
    skipped_count = 0

    try:
        staged_by_house_id = {}
        for item in data_list:
            house_id = str(item.get("id") or "").strip()
            item_community_id = item.get("communityID")
            item_community_name = item.get("communityName")

            community_id = str(item_community_id).strip() if item_community_id is not None else str(item.get("communityID") or "").strip()
            current_community_name = item_community_name if item_community_name is not None else community_name

            house_name = str(item.get("name") or item.get("houseName") or "").strip()
            if not house_id or not community_id or not house_name:
                skipped_count += 1
                continue

            building_size = item.get("buildingSize")
            usable_size = item.get("usableSize")
            layer = item.get("layer")
            area = item.get("buildArea") or item.get("area") or building_size or usable_size or 0

            values = {
                "house_id": house_id,
                "community_id": community_id,
                "community_name": current_community_name,
                "house_name": house_name,
                "building_id": item.get("buildingID"),
                "building_name": item.get("buildingName"),
                "unit_id": item.get("unitID"),
                "unit_name": item.get("unitName"),
                "layer": layer,
                "building_size": building_size,
                "usable_size": usable_size,
                "floor_name": str(layer) if layer is not None else str(item.get("floorName") or ""),
                "area": area,
                "user_num": item.get("userNum"),
                "charge_num": item.get("chargeNum"),
                "park_num": item.get("parkNum"),
                "car_num": item.get("carNum"),
                "combina_name": item.get("combinaName"),
                "create_uid": item.get("createUid"),
                "disable": bool(item.get("disable")) if item.get("disable") is not None else None,
                "expand": _to_json_str(item.get("expand")),
                "expand_info": _to_json_str(item.get("ExpandInfo")),
                "tag_list": _to_json_str(item.get("tagList")),
                "attachment_list": _to_json_str(item.get("attachmentList")),
                "house_type_name": item.get("houseTypeName"),
                "house_status_name": item.get("houseStatusName"),
            }

            staged_by_house_id[house_id] = {
                "house_row": values,
                "users": item.get("userList") or [],
            }

        if not staged_by_house_id:
            db.commit()
            logger.info("House sync completed: processed=%s, skipped=%s", inserted_count, skipped_count)
            return {"inserted": inserted_count, "skipped": skipped_count}

        house_rows = [entry["house_row"] for entry in staged_by_house_id.values()]
        upsert_model_rows(
            db,
            House,
            house_rows,
            key_fields=("house_id",),
        )

        house_id_to_pk = {}
        unique_house_ids = sorted(staged_by_house_id.keys())
        for house_id_chunk in _iter_chunks(unique_house_ids, 900):
            rows = (
                db.query(House.id, House.house_id)
                .filter(House.house_id.in_(house_id_chunk))
                .all()
            )
            for row in rows:
                house_id_to_pk[str(row.house_id)] = int(row.id)

        touched_house_fks = sorted(set(house_id_to_pk.values()))
        for house_fk_chunk in _iter_chunks(touched_house_fks, 900):
            db.query(HouseUser).filter(
                HouseUser.house_fk.in_(house_fk_chunk)
            ).delete(synchronize_session=False)

        house_user_rows = []
        for house_id, entry in staged_by_house_id.items():
            house_fk = house_id_to_pk.get(house_id)
            if not house_fk:
                continue
            for user in entry["users"]:
                if not isinstance(user, dict):
                    continue
                item_id = user.get("id")
                if item_id is None:
                    continue
                house_user_rows.append(
                    {
                        "house_fk": house_fk,
                        "origin_id": user.get("originId"),
                        "item_id": item_id,
                        "name": user.get("name"),
                        "item_type": user.get("itemType"),
                        "licence": user.get("licence"),
                        "park_name": user.get("parkName"),
                        "owner_name": user.get("ownerName"),
                        "owner_phone": user.get("ownerPhone"),
                        "charge_item_info": _to_json_str(user.get("chargeItemInfo")),
                        "start_time": user.get("startTime"),
                        "end_time": user.get("endTime"),
                        "community_name": user.get("communityName"),
                        "natural_period": user.get("naturalPeriod"),
                        "period_type": user.get("periodType"),
                        "period_num": user.get("periodNum"),
                    }
                )

        if house_user_rows:
            db.bulk_insert_mappings(HouseUser, house_user_rows)

        park_update_rows = []
        for house_id_chunk in _iter_chunks(unique_house_ids, 900):
            park_rows = (
                db.query(Park.id, Park.house_id, Park.house_fk)
                .filter(
                    and_(
                        Park.house_id.isnot(None),
                        Park.house_id.in_(house_id_chunk),
                    )
                )
                .all()
            )
            for row in park_rows:
                matched_house_fk = house_id_to_pk.get(str(row.house_id))
                if matched_house_fk and row.house_fk != matched_house_fk:
                    park_update_rows.append(
                        {
                            "id": int(row.id),
                            "house_fk": matched_house_fk,
                        }
                    )

        if park_update_rows:
            db.bulk_update_mappings(Park, park_update_rows)

        inserted_count = len(staged_by_house_id)

        db.commit()
        logger.info("House sync completed: processed=%s, skipped=%s", inserted_count, skipped_count)
        return {"inserted": inserted_count, "skipped": skipped_count}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def sync_houses_for_community(community_id: str, task_id: str = None):
    page = 1
    total_inserted = 0
    page_size = DEFAULT_HOUSE_PAGE_SIZE
    community_start = time.perf_counter()

    base_url = get_api_url_by_id(MARKI_HOUSE_API_ID)
    msg = f"Syncing houses for community {community_id}"
    logger.info(msg)
    if task_id:
        tracker.add_log(task_id, msg, "info")

    db_session = SessionLocal()
    try:
        proj = db_session.query(ProjectList).filter(ProjectList.proj_id == int(community_id)).first()
        community_name = proj.proj_name if proj else None

        api_config = db_session.query(ExternalApi).filter(ExternalApi.id == MARKI_HOUSE_API_ID).first()

        preloaded_vars = build_variable_map(db_session)
        preloaded_vars.update({
            "communityID": str(community_id),
            "pageSize": str(page_size),
        })

        while True:
            page_start = time.perf_counter()
            current_vars = dict(preloaded_vars)
            current_vars.update({"page": str(page)})

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

                resolved_body = resolve_dict_variables(base_body, db_session, preloaded_vars=current_vars)
                if method == "GET":
                    params = resolved_body
                else:
                    json_body = resolved_body
            else:
                params = {
                    "communityID": community_id,
                    "page": page,
                    "pageSize": page_size,
                }

            try:
                request_start = time.perf_counter()
                result = marki_client.request(method, base_url, params=params, json_data=json_body)
                request_elapsed = time.perf_counter() - request_start
            except Exception as exc:
                err_msg = f"Community {community_id} page {page} request failed: {exc}"
                logger.error(err_msg)
                if task_id:
                    tracker.add_log(task_id, err_msg, "error")
                break

            data_list, has_more = _extract_house_list(result)

            if not data_list:
                break

            write_start = time.perf_counter()
            counts = insert_houses(data_list, community_name)
            write_elapsed = time.perf_counter() - write_start
            total_inserted += counts["inserted"]
            page_elapsed = time.perf_counter() - page_start

            info_msg = (
                f"Community {community_id} page {page}: processed {len(data_list)} rows "
                f"in {page_elapsed:.2f}s (request {request_elapsed:.2f}s, write {write_elapsed:.2f}s)"
            )
            logger.info(info_msg)
            if task_id:
                tracker.add_log(task_id, info_msg, "info")

            if has_more is False:
                break
            if has_more is None and len(data_list) < page_size:
                break
            page += 1
    finally:
        db_session.close()

    community_elapsed = time.perf_counter() - community_start
    summary_msg = (
        f"Community {community_id} house sync finished: processed {total_inserted} rows "
        f"in {community_elapsed:.2f}s"
    )
    logger.info(summary_msg)
    if task_id:
        tracker.add_log(task_id, summary_msg, "info")

    return total_inserted


def sync_houses(community_ids: list = None, task_id: str = None):
    sync_start = time.perf_counter()
    if not community_ids:
        db = SessionLocal()
        try:
            community_ids = [str(cid) for cid in fetch_all_project_ids(db, ProjectList)]
        except Exception as exc:
            logger.error("Failed to retrieve community IDs from DB: %s", exc)
            community_ids = []
        finally:
            db.close()

        if not community_ids:
            fallback_var = os.getenv("MARKI_SYSTEM_ID", "")
            if fallback_var:
                community_ids = [fallback_var]
            else:
                logger.warning("No community IDs provided and none found in DB")
                return 0

    if task_id:
        tracker.update_status(task_id, "running")
        tracker.add_log(task_id, f"Start syncing houses for {len(community_ids)} communities", "info")

    total_all = 0
    for index, cid in enumerate(community_ids):
        try:
            if task_id:
                tracker.update_progress(task_id, index, f"Community ID: {cid}")
            total_all += sync_houses_for_community(str(cid), task_id)
        except Exception as exc:
            msg = f"Community {cid} house sync failed: {exc}"
            logger.error(msg)
            if task_id:
                tracker.add_log(task_id, msg, "error")

    if task_id:
        tracker.update_progress(task_id, len(community_ids), "sync completed")
        tracker.update_status(task_id, "completed")
        total_elapsed = time.perf_counter() - sync_start
        tracker.add_log(task_id, f"House sync completed. Processed {total_all} rows in {total_elapsed:.2f}s", "info")

    return total_all


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sync_houses(["10956"])
