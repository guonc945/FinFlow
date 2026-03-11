
import sys
import os
import json
import logging

# Add current directory to path
sys.path.append(os.getcwd())

from database import SessionLocal
from models import ExternalApi, ExternalService
from utils.marki_client import marki_client, get_api_url
from utils.variable_parser import resolve_dict_variables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("debug_sync")

def debug_api(api_name, community_id):
    db = SessionLocal()
    try:
        service = db.query(ExternalService).filter_by(service_name="marki").first()
        api = db.query(ExternalApi).filter_by(service_id=service.id, name=api_name).first()
        
        if not api:
            print(f"API {api_name} not found")
            return

        preloaded_vars = {
            "communityID": str(community_id),
            "communityId": str(community_id),
            "pageSize": "100",
            "page": "1",
            "index": "",
            "chargeSystemID": os.getenv("MARKI_SYSTEM_ID", ""),
            "endMonth": "2026-12"
        }
        
        url = get_api_url(api_name, preloaded_vars=preloaded_vars)
        method = api.method or "GET"
        
        request_data = {}
        if api.request_body:
            request_data = resolve_dict_variables(json.loads(api.request_body), db, preloaded_vars=preloaded_vars)
        
        print(f"\n--- Debugging {api_name} ---")
        print(f"URL: {url}")
        print(f"Method: {method}")
        print(f"Data/Params: {request_data}")
        
        # Use our robust marki_client.request
        try:
            result = marki_client.request(method, url, params=request_data if method=="GET" else {}, json_data=request_data if method!="GET" else {})
            print(f"Success! Response keys: {list(result.keys()) if isinstance(result, dict) else 'List result'}")
            if isinstance(result, dict) and "data" in result:
                data = result["data"]
                if isinstance(data, dict) and "list" in data:
                    print(f"Item count: {len(data['list'])}")
        except Exception as e:
            print(f"Request failed: {e}")
            
    finally:
        db.close()

if __name__ == "__main__":
    cid = "10956"
    debug_api("getUserList", cid)
    debug_api("getParkList", cid)
