# -*- coding: utf-8 -*-
import json
import logging
import os

from dotenv import load_dotenv

from database import SessionLocal
from models import ExternalApi, ProjectList, Resident
from sync_tracker import tracker
from utils.db_compat import fetch_all_project_ids, upsert_model_row
from utils.marki_client import get_api_url, marki_client
from utils.variable_parser import build_variable_map, resolve_dict_variables

load_dotenv()

logger = logging.getLogger("resident_sync")


def insert_residents(data_list, community_name=None):
    db = SessionLocal()
    inserted_count = 0
    skipped_count = 0

    try:
        for item in data_list:
            resident_id = str(item.get("id") or "").strip()

            item_community_id = item.get("communityID")
            item_community_name = item.get("communityName")

            community_id = str(item_community_id).strip() if item_community_id is not None else str(item.get("communityID") or "").strip()
            current_community_name = item_community_name if item_community_name is not None else community_name

            name = str(item.get("name") or "").strip()
            phone = str(item.get("phone") or "").strip()

            houses_data = item.get("houseList", [])
            labels_data = item.get("labelList", [])
            houses_str = json.dumps(houses_data, ensure_ascii=False) if houses_data else None
            labels_str = json.dumps(labels_data, ensure_ascii=False) if labels_data else None

            if not resident_id or not community_id or not name:
                skipped_count += 1
                continue

            upsert_model_row(
                db,
                Resident,
                {"resident_id": resident_id, "community_id": community_id},
                {
                    "community_name": current_community_name,
                    "name": name,
                    "phone": phone,
                    "houses": houses_str,
                    "labels": labels_str,
                },
            )
            inserted_count += 1

        db.commit()
        return {"inserted": inserted_count, "skipped": skipped_count}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def sync_residents_for_community(community_id: str, task_id: str = None):
    page = 1
    total_inserted = 0
    page_size = 100

    base_url = get_api_url("getUserList")

    msg = f"Syncing residents for community {community_id}"
    logger.info(msg)
    if task_id:
        tracker.add_log(task_id, msg, "info")

    db_session = SessionLocal()
    try:
        proj = db_session.query(ProjectList).filter(ProjectList.proj_id == int(community_id)).first()
        community_name = proj.proj_name if proj else None

        api_config = db_session.query(ExternalApi).filter(ExternalApi.name == "getUserList").first()

        preloaded_vars = build_variable_map(db_session)
        preloaded_vars.update({
            "communityID": str(community_id),
            "pageSize": str(page_size),
        })

        index_val = ""
        while True:
            current_vars = dict(preloaded_vars)
            current_vars.update({
                "page": str(page),
                "index": index_val,
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
                if index_val:
                    params["index"] = index_val

            try:
                result = marki_client.request(method, base_url, params=params, json_data=json_body)
            except Exception as exc:
                err_msg = f"Community {community_id} page {page} request failed: {exc}"
                logger.error(err_msg)
                if task_id:
                    tracker.add_log(task_id, err_msg, "error")
                break

            data_list = []
            has_more = False
            next_index = ""

            if isinstance(result, dict):
                if "hasMore" in result:
                    has_more = bool(result.get("hasMore"))
                    next_index = str(result.get("index") or "")

                if isinstance(result.get("data"), list):
                    data_list = result["data"]
                elif isinstance(result.get("data"), dict):
                    if isinstance(result["data"].get("list"), list):
                        data_list = result["data"]["list"]
                    if "hasMore" in result["data"]:
                        has_more = bool(result["data"].get("hasMore"))
                        next_index = str(result["data"].get("index") or "")
                elif isinstance(result.get("list"), list):
                    data_list = result["list"]
            elif isinstance(result, list):
                data_list = result

            if not data_list:
                break

            counts = insert_residents(data_list, community_name)
            total_inserted += counts["inserted"]

            info_msg = f"Community {community_id} page {page}: processed {len(data_list)} rows"
            logger.info(info_msg)
            if task_id:
                tracker.add_log(task_id, info_msg, "info")

            if has_more:
                if next_index:
                    index_val = next_index
                page += 1
            else:
                if len(data_list) < page_size:
                    break
                page += 1
    finally:
        db_session.close()

    return total_inserted


def sync_residents(community_ids: list = None, task_id: str = None):
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
        tracker.add_log(task_id, f"Start syncing residents for {len(community_ids)} communities", "info")

    total_all = 0
    for index, cid in enumerate(community_ids):
        try:
            if task_id:
                tracker.update_progress(task_id, index, f"Community ID: {cid}")
            total_all += sync_residents_for_community(str(cid), task_id)
        except Exception as exc:
            msg = f"Community {cid} resident sync failed: {exc}"
            logger.error(msg)
            if task_id:
                tracker.add_log(task_id, msg, "error")

    if task_id:
        tracker.update_progress(task_id, len(community_ids), "sync completed")
        tracker.update_status(task_id, "completed")
        tracker.add_log(task_id, f"Resident sync completed. Processed {total_all} rows", "info")

    return total_all


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sync_residents(["10956"])
