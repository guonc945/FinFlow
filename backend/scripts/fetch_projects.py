# -*- coding: utf-8 -*-
import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add backend root to sys.path for local module imports.
sys.path.append(str(Path(__file__).resolve().parent.parent))

from database import SessionLocal
from models import ExternalApi, ExternalService, ProjectList
from utils.api_config import require_api_id
from utils.marki_client import get_api_url_by_id, marki_client
from utils.sqlserver_partitions import ensure_default_financial_partitions
from utils.variable_parser import resolve_dict_variables


load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

logger = logging.getLogger("project_sync")
MARKI_PROJECT_API_ID = require_api_id("MARKI_PROJECT_API_ID")


def fetch_projects():
    db = SessionLocal()
    try:
        logger.info("Fetching project list from external API...")
        service = db.query(ExternalService).filter_by(service_name="marki").first()
        if not service:
            logger.error("External service config 'marki' not found")
            return []

        api = db.query(ExternalApi).filter(ExternalApi.id == MARKI_PROJECT_API_ID).first()
        if not api:
            logger.error("External API config not found: external_apis.id=%s", MARKI_PROJECT_API_ID)
            return []

        url = get_api_url_by_id(MARKI_PROJECT_API_ID)
        method = api.method if api else "GET"

        system_id = os.getenv("MARKI_SYSTEM_ID", "")
        preloaded_vars = {"chargeSystemID": system_id}
        request_data = preloaded_vars if system_id else {}
        if api and api.request_body:
            request_data = resolve_dict_variables(json.loads(api.request_body), db, preloaded_vars=preloaded_vars)

        if method == "GET":
            result = marki_client.request("GET", url, params=request_data)
        else:
            result = marki_client.request(method, url, json_data=request_data)

        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            data = result.get("data")
            if isinstance(data, dict) and isinstance(data.get("list"), list):
                return data["list"]
            if isinstance(data, list):
                return data
            if isinstance(result.get("list"), list):
                return result["list"]
        return []
    except Exception as exc:
        logger.error("Failed to fetch projects: %s", exc)
        return []
    finally:
        db.close()


def upsert_projects(projects):
    if not projects:
        logger.warning("No project data to sync")
        return

    db = SessionLocal()
    success_count = 0
    synced_ids = []
    try:
        for proj in projects:
            proj_id = proj.get("id") or proj.get("communityID") or proj.get("proj_id")
            proj_name = proj.get("name") or proj.get("communityName") or proj.get("proj_name")
            if proj_id is None or proj_name is None:
                continue

            try:
                safe_proj_id = int(proj_id)
            except (ValueError, TypeError):
                logger.error("Skipping invalid project id: %s", proj_id)
                continue

            existing = db.query(ProjectList).filter(ProjectList.proj_id == safe_proj_id).first()
            if existing:
                existing.proj_name = str(proj_name)
            else:
                db.add(ProjectList(proj_id=safe_proj_id, proj_name=str(proj_name)))
            success_count += 1
            synced_ids.append(safe_proj_id)

        db.commit()
        logger.info("Project sync completed, processed %s rows", success_count)
        return synced_ids
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def main():
    projects = fetch_projects()
    synced_ids = upsert_projects(projects)
    try:
        ensure_default_financial_partitions(synced_ids)
    except Exception as exc:
        logger.error("Auto partition expansion failed after project sync: %s", exc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
