# -*- coding: utf-8 -*-
import os
import sys
from sqlalchemy.orm import Session
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import SessionLocal
from models import ExternalService, ExternalApi

def migrate_marki_apis():
    db = SessionLocal()
    try:
        # Check if service exists
        service = db.query(ExternalService).filter_by(service_name="marki").first()
        if not service:
            # Create Marki external service
            service = ExternalService(
                service_name="marki",
                display_name="马克联物业系统",
                app_id=os.getenv("MARKI_USER", ""),
                app_secret=os.getenv("MARKI_PASSWORD", ""),
                auth_url="https://sttc-os-lgn.markiapp.com/lgn/login/authorize.do",
                base_url="https://charge-api.markiapp.com",
                auth_type="basic",
                is_active=True,
                extra_info=os.getenv("MARKI_COOKIE", "")
            )
            db.add(service)
            db.commit()
            db.refresh(service)
            print(f"Created ExternalService for Marki (ID: {service.id})")
        else:
            print(f"ExternalService 'marki' already exists (ID: {service.id})")
            # Update credentials if empty
            if not service.app_id and os.getenv("MARKI_USER"):
                service.app_id = os.getenv("MARKI_USER")
            if not service.app_secret and os.getenv("MARKI_PASSWORD"):
                service.app_secret = os.getenv("MARKI_PASSWORD")
            if not service.extra_info and os.getenv("MARKI_COOKIE"):
                 service.extra_info = os.getenv("MARKI_COOKIE")
            db.commit()

        # Define APIs
        apis_to_create = [
            {
                "name": "getCommunityList",
                "method": "GET",
                "url_path": "/mkg/api/v2/Charge/getCommunityList",
                "request_body": '{"chargeSystemID": "{chargeSystemID}"}',
                "description": "获取项目（园区）列表"
            },
            {
                "name": "getUserList",
                "method": "GET",
                "url_path": "/mkg/api/v2/Charge/getUserList",
                "request_body": '{"communityID": "{communityID}", "page": "{page}", "pageSize": "{pageSize}", "index": "{index}"}',
                "description": "获取住户列表"
            },
            {
                "name": "getHouseList",
                "method": "GET",
                "url_path": "/mkg/api/v2/Charge/getHouseList",
                "request_body": '{"communityID": "{communityID}", "page": "{page}", "pageSize": "{pageSize}"}',
                "description": "获取房屋列表"
            },
            {
                "name": "getParkList",
                "method": "GET",
                "url_path": "/mkg/api/v2/Charge/getParkList",
                "request_body": '{"communityID": "{communityID}", "page": "{page}", "pageSize": "{pageSize}"}',
                "description": "获取车位列表"
            },
            {
                "name": "getChargeItemList",
                "method": "GET",
                "url_path": "/mkg/api/v2/Charge/getChargeItemList",
                "request_body": '{"communityID": "{communityID}", "pageSize": "{pageSize}", "version": "1"}',
                "description": "获取收费项目列表"
            },
            {
                "name": "getBillList",
                "method": "POST",
                "url_path": "/mkg/api/v2/Charge/getBillList",
                "request_body": '{"badBillCheck": 0, "chargeItemVersion": 2, "communityID": "{communityID}", "dealLogId": 0, "endMonth": "{endMonth}", "index": "", "pageSize": "1000", "payStatus": 3, "page": "{page}"}',
                "description": "获取账单列表"
            }
        ]

        # Insert or update APIs
        for api_data in apis_to_create:
            api = db.query(ExternalApi).filter_by(service_id=service.id, name=api_data["name"]).first()
            if not api:
                api = ExternalApi(
                    service_id=service.id,
                    **api_data
                )
                db.add(api)
                print(f"Added API: {api_data['name']}")
            else:
                for k, v in api_data.items():
                    setattr(api, k, v)
                print(f"Updated API: {api_data['name']}")
                
        db.commit()
        print("Marki APIs migration completed successfully.")
        
    except Exception as e:
        db.rollback()
        print(f"Error migrating Marki APIs: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
    migrate_marki_apis()
