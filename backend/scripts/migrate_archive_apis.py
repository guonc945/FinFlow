
import json
import os
import sys
from sqlalchemy.orm import Session
from database import SessionLocal
import models

def migrate_archive_configs():
    db = SessionLocal()
    try:
        # 1. Find the accounting subject config
        config_var = db.query(models.GlobalVariable).filter(
            models.GlobalVariable.key == "ACCOUNTING_SUBJECT_CONFIG"
        ).first()
        
        if not config_var:
            print("No ACCOUNTING_SUBJECT_CONFIG found in global_variables.")
            return

        config = json.loads(config_var.value)
        service_id = config.get("service_id")
        
        if not service_id:
            print("Config found but contains no service_id.")
            return

        # 2. Check if already migrated (by name and service)
        existing_api = db.query(models.ExternalApi).filter(
            models.ExternalApi.service_id == service_id,
            models.ExternalApi.name == "会计科目同步接口"
        ).first()

        if existing_api:
            print("API '会计科目同步接口' already exists. Updating...")
            api = existing_api
        else:
            print("Creating new ExternalApi entry for '会计科目同步接口'...")
            api = models.ExternalApi(
                service_id=service_id,
                name="会计科目同步接口",
                is_active=True
            )
            db.add(api)

        # 3. Update fields
        api.method = config.get("method", "POST")
        api.url_path = config.get("url", config.get("url_path", ""))
        api.description = "系统内置：用于获取及同步会计科目基础数据"
        
        # Headers & Body
        headers = config.get("request_headers", {})
        if isinstance(headers, dict):
            api.request_headers = json.dumps(headers)
        else:
            api.request_headers = headers
            
        api.request_body = config.get("request_body", "")
        
        db.commit()
        print("Successfully migrated ACCOUNTING_SUBJECT_CONFIG to external_apis.")

    except Exception as e:
        print(f"Migration failed: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    migrate_archive_configs()
