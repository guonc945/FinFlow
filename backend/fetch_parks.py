# -*- coding: utf-8 -*-
import json
import logging
import os
import time

from dotenv import load_dotenv

from database import SessionLocal
from models import ExternalApi, House, Park, ProjectList
from sync_tracker import tracker
from utils.api_config import require_api_id
from utils.db_compat import fetch_all_project_ids, upsert_model_rows
from utils.marki_client import get_api_url_by_id, marki_client
from utils.variable_parser import build_variable_map, resolve_dict_variables

load_dotenv()

logger = logging.getLogger("park_sync")
DEFAULT_PARK_PAGE_SIZE = max(1, int(os.getenv("MARKI_PARK_PAGE_SIZE", "500")))
MARKI_PARK_API_ID = require_api_id("MARKI_PARK_API_ID")


def _iter_chunks(items, size):
    if size <= 0:
        raise ValueError("size must be positive")
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _extract_park_list(result):
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


def insert_parks(data_list, community_name=None):
    db = SessionLocal()
    inserted_count = 0
    skipped_count = 0

    try:
        staged_rows = []
        house_ids = set()
        for item in data_list:
            park_id = str(item.get("id") or "").strip()
            item_community_id = item.get("communityId")
            item_community_name = item.get("communityName")

            community_id = str(item_community_id).strip() if item_community_id is not None else str(item.get("communityId") or "").strip()
            current_community_name = item_community_name if item_community_name is not None else community_name

            name = str(item.get("name") or "").strip()
            if not park_id or not community_id or not name:
                skipped_count += 1
                continue

            user_item = item.get("userItem") if isinstance(item.get("userItem"), dict) else {}
            house_item = item.get("houseItem") if isinstance(item.get("houseItem"), dict) else {}

            house_id = house_item.get("id")
            house_id_str = str(house_id).strip() if house_id is not None else None
            if house_id_str:
                house_ids.add(house_id_str)

            staged_rows.append(
                {
                    "park_id": park_id,
                    "community_id": community_id,
                    "community_name": current_community_name,
                    "name": name,
                    "park_type_name": item.get("parkTypeName"),
                    "state": item.get("state"),
                    "user_name": user_item.get("name"),
                    "house_name": house_item.get("name"),
                    "house_id": house_id_str,
                }
            )

        house_id_to_fk = {}
        for house_id_chunk in _iter_chunks(sorted(house_ids), 900):
            rows = (
                db.query(House.house_id, House.id)
                .filter(House.house_id.in_(house_id_chunk))
                .all()
            )
            for row in rows:
                house_id_to_fk[str(row.house_id)] = int(row.id)

        park_rows = []
        for staged_row in staged_rows:
            park_rows.append(
                {
                    **staged_row,
                    "house_fk": house_id_to_fk.get(staged_row["house_id"]),
                }
            )

        if park_rows:
            upsert_model_rows(
                db,
                Park,
                park_rows,
                key_fields=("park_id", "community_id"),
            )
            inserted_count = len(park_rows)

        db.commit()
        return {"inserted": inserted_count, "skipped": skipped_count}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def sync_parks_for_community(community_id: str, task_id: str = None):
    page = 1
    total_inserted = 0
    page_size = DEFAULT_PARK_PAGE_SIZE
    community_start = time.perf_counter()

    base_url = get_api_url_by_id(MARKI_PARK_API_ID)

    msg = f"Syncing parks for community {community_id}"
    logger.info(msg)
    if task_id:
        tracker.add_log(task_id, msg, "info")

    db_session = SessionLocal()
    try:
        proj = db_session.query(ProjectList).filter(ProjectList.proj_id == int(community_id)).first()
        community_name = proj.proj_name if proj else None

        api_config = db_session.query(ExternalApi).filter(ExternalApi.id == MARKI_PARK_API_ID).first()

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

            data_list, has_more = _extract_park_list(result)

            if not data_list:
                break

            write_start = time.perf_counter()
            counts = insert_parks(data_list, community_name)
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
        f"Community {community_id} park sync finished: processed {total_inserted} rows "
        f"in {community_elapsed:.2f}s"
    )
    logger.info(summary_msg)
    if task_id:
        tracker.add_log(task_id, summary_msg, "info")

    return total_inserted


def sync_parks(community_ids: list = None, task_id: str = None):
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
        tracker.add_log(task_id, f"Start syncing parks for {len(community_ids)} communities", "info")

    total_all = 0
    for index, cid in enumerate(community_ids):
        try:
            if task_id:
                tracker.update_progress(task_id, index, f"Community ID: {cid}")
            total_all += sync_parks_for_community(str(cid), task_id)
        except Exception as exc:
            msg = f"Community {cid} park sync failed: {exc}"
            logger.error(msg)
            if task_id:
                tracker.add_log(task_id, msg, "error")

    if task_id:
        tracker.update_progress(task_id, len(community_ids), "sync completed")
        tracker.update_status(task_id, "completed")
        total_elapsed = time.perf_counter() - sync_start
        tracker.add_log(task_id, f"Park sync completed. Processed {total_all} rows in {total_elapsed:.2f}s", "info")

    return total_all


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sync_parks(["10956"])
